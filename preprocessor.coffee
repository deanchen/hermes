fs = require('fs')
lazy = require('lazy')
redis = require('redis')

SCORING = {
    scale : 10
    completeWordBonus : 1.4
    dupPrefixPenalty : 0.5
}
PIPELINE = 10000
COMMIT = true

MIN_COMPLETE = 2
MAX_COMPLETE = 30
STOP_WORDS = fs.readFileSync('stop-words.txt', 'ascii').split('\n')
SCRIPT_HASH = ""
STORE_PREFIX_SCRIPT = fs.readFileSync('insertPrefix.lua', 'ascii')

args = process.argv.splice(2)
path = args[0]
totalWorkers = args[1]
workerIndex = args[2]
uprefixes = {}
problemLines = [];

i = 0

clients = []

client = redis.createClient(6400, "127.0.0.1")
client.select(0)
clients.push(client)
#clients[0].flushall()

termClient = redis.createClient(6400, "127.0.0.1")
termClient.select(1)
clients.push(termClient)

prefixClient = redis.createClient(6400, "127.0.0.1")
prefixClient.select(2)
clients.push(prefixClient)

lockClient = redis.createClient(6400, "127.0.0.1")
lockClient.select(3)
clients.push(lockClient)

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
        (err, ms) ->
            completed++
            fill_pipeline()
            if (err) then return console.log(err) 
    )
    ###
    Object.keys(prefixScores).forEach((prefix) ->
        return if prefix is ""
        score = prefixScores[prefix]
        sent++
        prefixClient.hincrby("prefixes", prefix, 1, (err, res) -> 
            unless res > 1023
                sent++
                client.zadd(prefix, score, id, (err) ->
                    completed++
                    fill_pipeline()
                )
            else
                uprefixes[prefix] = uprefixes[prefix] || 0
                if score > uprefixes[prefix]
                    lockSet(prefix, id, (err) ->
                        client.zrange(prefix, 0, 0, 'withscores', (err, min) ->
                            setMinId = parseInt(min[0], 10)
                            setMinScore = parseInt(min[1], 10)
                            uprefixes[prefix] = setMinScore
                            if score > setMinScore
                                console.log([setMinId, setMinScore], [id, score])
                                client.zrem(prefix, setMinId)
                                client.zadd(prefix, score, id)
                        )
                    )
        )
    )
    ###

lockWaitQueueSize = 0
###
lockSet = (set, id, cb, repeat) ->
    lockClient.setnx(set, id, (err, res) -> 
        unless repeat then lockWaitQueueSize++
        console.log(set + ":" + id + ":" + res)
        if res is 1
            lockWaitQueueSize--
            fill_pipeline()
            cb(null)
        else
            process.nextTick(() -> lockSet(set, id, cb, true))
    )
###

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
                        acc[prefix.phrase] += Math.round(prefix.score * SCORING.dupPrefixPenalty)
                    else
                        acc[prefix.phrase] = prefix.score
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
        console.log(
            {
                lines: i,
                elapsed: stats["elapsed"],
                memory: info[20],
                keys: info[43],
                sent: sent,
                completed: completed,
                estimateMem: ((20000000/i) * (info[19].split(":")[1]))/1073741824
                remainingTime: Math.round((20000000/i - 1) * stats["elapsed"])
            }
        )
    )

fill_pipeline = () ->
    stream.resume() if (sent - completed < PIPELINE) and (lockWaitQueueSize < 1)

readLines = (input, cb) ->
    id = 1
    buffer = ''

    input.on('data', (data) ->
        if (sent - completed > PIPELINE) or (lockWaitQueueSize > 1)
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
