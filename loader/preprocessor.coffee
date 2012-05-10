fs = require('fs')
redis = require('redis')

SCORING = {
    scale : 10
    completeWordBonus : 1.4
    dupPrefixPenalty : 0.2
}
PIPELINE = 10000
COMMIT = true

ESTIMATE_LINES = 20000000
MIN_COMPLETE = 2
MAX_COMPLETE = 30
STOP_WORDS = fs.readFileSync('stop-words.txt', 'ascii').split('\n')
SCRIPT_HASH = ""
STORE_PREFIX_SCRIPT = fs.readFileSync('insertPrefix.lua', 'ascii')

args = process.argv.splice(2)
path = args[0]
totalWorkers = args[1]
workerIndex = args[2]
MAX_SET_SIZE = args[3] # need to match redis config
PORT = args[4] # need to match redis config
console.log(args)
uprefixes = {}
problemLines = []

i = 0

clients = []

client = redis.createClient(PORT, "127.0.0.1")
client.select(0)
clients.push(client)

termClient = redis.createClient(PORT, "127.0.0.1")
termClient.select(1)
clients.push(termClient)

###
client.send_command('script', ['load', STORE_PREFIX_SCRIPT, (err, res) ->
    if err then return console.log(err)
    SCRIPT_HASH = res
    readLines(stream, commitLine)
)
###

index = 1
stats = {lines:0, words:0, prefixes:0}

startTime = new Date()

sent = 0
completed = 0

finished_processing = false
stream = fs.createReadStream(path, {encoding:'ascii'})
stream.on('end', () ->
    clients.forEach((client) -> client.quit())
)

commitLine = (line, i) ->
    try
        line = JSON.parse(line)
    catch e
        return console.log(e)

    storePrefixes(line.id, processTitle(line.title))

    sent++
    hashSet(termClient, line.id, JSON.stringify(line), (err)->
        completed++
        fill_pipeline()
    )

    showStats(i) if i % 100000 is 0

storePrefixes = (id, prefixScores) ->
    sent++
    client.eval(
        STORE_PREFIX_SCRIPT,
        2,
        JSON.stringify(prefixScores),
        id,
        MAX_SET_SIZE,
        (err, ms) ->
            completed++
            fill_pipeline()
            if (err) then return console.log(err)
    )

processTitle = (phrase) ->
    tokens = phrase
        .toLowerCase()
        .replace(/[^a-z0-9 ]/ig, ' ')
        .trim()
        .split(' ')
        .filter((word)-> !~STOP_WORDS.indexOf(word))
        .map((word) ->
            unless COMMIT then stats.words++
            if word.length > MAX_COMPLETE
                upperlimit = MAX_COMPLETE - 1
            else
                upperlimit = word.length - 1

            lowerlimit = MIN_COMPLETE - 1
            if (lowerlimit < upperlimit)
                return [lowerlimit..upperlimit].map(
                    (length) ->
                        {
                            "phrase": word[0..length],
                            "score": prefixScore(length, upperlimit)
                        }
                )
            else
                return []
        )

    if (tokens.length > 0)
        return tokens
            .reduce((acc, prefixes) ->
                prefixes.forEach((prefix) ->
                    if acc[prefix.phrase]
                        acc[prefix.phrase] += Math.round(prefix.score * SCORING.dupPrefixPenalty) # dups are scored less
                    else
                        acc[prefix.phrase] = prefix.score + scale * 10 # add base to ensure that intersection always win
                )
                return acc
            , {})
    else
        return {}

prefixScore = (length, upperlimit) ->
    length -= MIN_COMPLETE - 2
    upperlimit -= MIN_COMPLETE - 2
    if (length is upperlimit)
        return SCORING.scale * SCORING.completeWordBonus
    else
        return Math.round((length/upperlimit) * SCORING.scale)

showStats = (i) ->
    stats["elapsed"] = ((new Date()).getTime() - startTime)/(60 * 1000)
    client.info((err,info) ->
        info = info.split('\r\n')
        memHuman = info[23].split(':')[1]
        memBytes = info[22].split(':')[1]
        console.log(
            {
                lines: i,
                elapsed: stats["elapsed"],
                memory: memHuman,
                sent: sent,
                completed: completed,
                estimateMem: ((ESTIMATE_LINES/i) * memBytes)/1073741824
                remainingTime: Math.round((ESTIMATE_LINES/i - 1) * stats["elapsed"])
            }
        )
    )

fill_pipeline = () ->
    stream.resume() if (sent - completed < PIPELINE)

readLines = (input, cb) ->
    id = 1
    buffer = ''

    input.on('data', (data) ->
        if (sent - completed > PIPELINE)
            #console.log("paused")
            stream.pause()
        buffer += data
        index = buffer.indexOf('\n')
        while (index > -1)
            line = buffer.substring(0, index)
            buffer = buffer.substring(index + 1)
            if ((id % totalWorkers) == parseInt(workerIndex, 10)) || totalWorkers is 1
                cb(line, id++)
            else
                id++
            index = buffer.indexOf('\n')
    )
    input.on('end', () ->
        if buffer.length > 0 then cb(buffer, id++)
    )


Array::unique = ->
    output = {}
    output[@[key]] = @[key] for key in [0...@length]
    value for key, value of output

hashKeyFields = (key) ->
    bucketSize = 1024
    {key: Math.round(key / bucketSize), field: key % bucketSize}

hashSet = (client, key, value, cb) ->
    keyfields = hashKeyFields(key)
    client.hset(keyfields.key, keyfields.field, value, cb)


readLines(stream, commitLine)
