fs = require('fs')
lazy = require('lazy')
redis = require('redis')

PIPELINE = 10000
COMMIT = false

MIN_COMPLETE = 3
STOP_WORDS = fs.readFileSync('stop-words.txt', 'ascii').split('\n')
CLIENTS = 1

args = process.argv.splice(2)
type = args[0]
path = args[1]
totalWorkers = args[2]
workerIndex = args[3]

clients = []
if COMMIT
    i = 0
    while (i < CLIENTS)
        console.log(i)
        clients.push(redis.createClient(6379, "127.0.0.1", {return_buffers: false}))
        i++
    clients[0].flushall()

index = 1
stats = {lines:0, words:0, prefixes:0}

startTime = new Date()

sent = 0
completed = 0

finished_processing = false
stream = fs.createReadStream(path, {encoding:'ascii'})
commitLine = (line, i) ->
    if COMMIT
        client = clients[Math.round((i/totalWorkers))%CLIENTS]
        unless line then clients.forEach((client)-> client.quit())
        prefix(line).forEach((p) -> 
            sent++
            client.sadd(type + ":" + p, i, (err)->
                completed++
                fill_pipeline()
            )
        )
        sent++
        client.hset("soulmate-data:" + type, i, JSON.stringify({id: i, term: line}), (err)->
            completed++
            fill_pipeline()
        )
        if i % 1000 is 0
            stats["elapsed"] = ((new Date()).getTime() - startTime)/60000
            client.info((err,info) ->
                info = info.split('\r\n')
                console.log({lines: i, elapsed: stats["elapsed"], memory: info[20], keys: info[43], sent: sent, completed: completed})
                console.log()
            )
    else
        if line
            stats.lines++
            prefixes = prefix(line)
            prefixes.forEach((p)-> console.log(p))
            stats.prefixes += prefixes.length
            if i % 100000 is 0
                stats["words/line"] = stats.words/stats.lines
                stats["prefix/word"] = stats.prefixes/stats.words
                stats["prefix/line"] = stats.prefixes/stats.lines
                stats["elapsed"] = ((new Date()).getTime() - startTime)/60000
                console.log(stats)

fill_pipeline = () ->
    if sent - completed < PIPELINE
        stream.resume()

prefix = (phrase) ->
    phrase
        .toLowerCase()
        .replace('-', ' ')
        .replace(/[^a-z0-9 ]/ig, '')
        .trim()
        .split(' ')
        .filter((word)-> !~STOP_WORDS.indexOf(word))
        .map((word) -> 
            unless COMMIT then stats.words++
            [(MIN_COMPLETE - 1)..(word.length - 1)].map((length) -> word[0..length]))
        .reduce((acc, prefixes) -> 
            acc.concat(prefixes))
        .unique()

readLines = (input, cb) ->
    id = 1
    buffer = ''

    input.on('data', (data) ->
        if (sent - completed >  PIPELINE)
            console.log("paused")
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

readLines(stream, commitLine)
