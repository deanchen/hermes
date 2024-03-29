fs = require('fs')
redis = require('redis')
_ = require('underscore')
async = require('async')
exec = require('child_process').exec

SCORING = {
    scale : 10
    completeWordBonus : 1.4
    dupPrefixPenalty : 0.2
}
CONCURRENCY = 4

STREAM = null
ESTIMATE_LINES = 0
MIN_COMPLETE = 2
MAX_COMPLETE = 30
STOP_WORDS = fs.readFileSync('stop-words.txt', 'ascii').split('\n')
SCRIPT_HASH = ""
STORE_PREFIX_SCRIPT = fs.readFileSync('insertPrefix.lua', 'ascii')

args = process.argv.splice(2)
path = args.shift()
totalWorkers = parseInt(args.shift(), 10)
workerIndex = parseInt(args.shift(), 10)
###
totalWorkers = 1
workerIndex = 0
###
MAX_SET_SIZE = parseInt(args.shift(), 10)
PORT = parseInt(args.shift(), 10)

MIN_SCORES = {}

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

stats = {lines:0, words:0, prefixes:0}

startTime = new Date()

commitLine = (task, cb) ->
    line = task.line
    try
        line = JSON.parse(line)
    catch e
        return console.log(e)

    storePrefixes(line.id, removeLowScores(processTitle(line.title)), cb)

    hashSet(termClient, line.id, JSON.stringify(line))

storePrefixes = (id, prefixScores, cb) ->
    unless Object.keys(prefixScores).length > 0 then return cb(null)
    client.eval(
        STORE_PREFIX_SCRIPT,
        2,
        JSON.stringify(prefixScores),
        id,
        MAX_SET_SIZE,
        (err, minScores) ->
            try
                minScores = JSON.parse(minScores)
                _.extend(MIN_SCORES, minScores)
            catch e

            cb(null)
            fillQueue()
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
                        acc[prefix.phrase] = prefix.score + SCORING.scale * 10 # add base to ensure that intersection always win
                )
                return acc
            , {})
    else
        return {}

removeLowScores = (prefixes) ->
    Object.keys(prefixes).forEach((prefix) ->
        score = prefixes[prefix]
        if MIN_SCORES[prefix]
            unless score > MIN_SCORES[prefix]
                delete prefixes[prefix]
    )
    return prefixes

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
                estimateMem: ((ESTIMATE_LINES/i) * memBytes)/1073741824
                remainingTime: Math.round((ESTIMATE_LINES/i - 1) * stats["elapsed"])
            }
        )
    )



QUEUE = async.queue(commitLine, CONCURRENCY)
fillQueue = () ->
    STREAM.resume() if QUEUE.length() < CONCURRENCY * 3

COUNT = 1
readLines = () ->
    buffer = ''
    STREAM = fs.createReadStream(path, {encoding:'ascii'})
        .on('data', (data) ->
            if QUEUE.length() > CONCURRENCY * 4
                STREAM.pause()
            buffer += data
            buffer = processBuffer(buffer)
        )
    STREAM.on('end', () ->
        buffer = processBuffer(buffer)
    )


processBuffer = (buffer) ->
    index = buffer.indexOf('\n')
    while (index > -1)
        line = buffer.substring(0, index)
        buffer = buffer.substring(index + 1)
        if ((COUNT % totalWorkers) is workerIndex) or totalWorkers is 1
            QUEUE.push({line: line})
            showStats(COUNT) if COUNT % 10000 is 0
        COUNT++

        index = buffer.indexOf('\n')
    return buffer


hashKeyFields = (key) ->
    bucketSize = 1024
    {key: Math.round(key / bucketSize), field: key % bucketSize}

hashSet = (client, key, value, cb) ->
    keyfields = hashKeyFields(key)
    client.hset(keyfields.key, keyfields.field, value, cb)

exec('wc -l ' + path, (err, res) ->
    ESTIMATE_LINES = res.split(' ')[0]
    readLines()
)
