fs = require('fs')
lazy = require('lazy')
redis = require('redis')

PIPELINE = 10000
COMMIT = true

MIN_COMPLETE = 3
MAX_COMPLETE = 30
STOP_WORDS = fs.readFileSync('stop-words.txt', 'ascii').split('\n')
CLIENTS = 1

args = process.argv.splice(2)
type = args[0]
path = args[1]
totalWorkers = args[2]
workerIndex = args[3]
uprefixes = {}
clients = []
i = 0
while (i < CLIENTS)
    console.log(i)
    clients.push(redis.createClient(6379, "127.0.0.1", {return_buffers: false}))
    i++
clients[0].flushall()

termClient = redis.createClient(6379, "127.0.0.1")
termClient.select(1)

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
            return if p is ""
            if (!(uprefixes[p] > 1023))
                uprefixes[p] = uprefixes[p] || 0
                uprefixes[p]++
                sent++
                clients[0].sadd(p, i, (err) ->
                    completed++
                    fill_pipeline()
                )
                
        )
        sent++
        hashSet(termClient, i, line, (err)->
            completed++
            fill_pipeline()
        )
        if i % 10000 is 0
            stats["elapsed"] = ((new Date()).getTime() - startTime)/60000
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
                        remainingTime: ((((20000000/i) * stats["elapsed"]) - stats["elapsed"]) / 60)
                    }
                )
                console.log()
            )
    else
        if line
            stats.lines++
            prefixes = prefix(line)
            prefixes.forEach((p)->
                return if p is ""
                if (!(uprefixes[p] > 1023))
                    uprefixes[p] = uprefixes[p] || 0
                    uprefixes[p]++
                    clients[0].sadd(p, i)
                else
                    #console.log(p, " exceded limit")
                
            )
            stats.prefixes += prefixes.length
            clients[0].incr("c:")
            if i % 10000 is 0
                stats["reallines"] = i
                stats["words/line"] = stats.words/stats.lines
                stats["prefix/word"] = stats.prefixes/stats.words
                stats["prefix/line"] = stats.prefixes/stats.lines
                stats["elapsed"] = ((new Date()).getTime() - startTime)/60000
                clients[0].info((err,info) ->
                    info = info.split('\r\n')
                    stats["memory"] = info[19].split(":")[1]
                    stats["keys"] = info[43].split(",")[0].split("=")[1]
                    stats["uprefix/line"] = stats.keys/i
                    stats["estimate"] = ((20000000/i) * stats.memory)/1073741824
                    console.log(stats)
                )

fill_pipeline = () ->
    if sent - completed < PIPELINE
        stream.resume()

prefix = (phrase) ->
    tokens = phrase
        .toLowerCase()
        .replace('-', ' ')
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
            [(MIN_COMPLETE - 1)..(upperlimit)].map((length) -> word[0..length]))
    if (tokens.length > 0)
        return tokens
            .reduce((acc, prefixes) ->
                acc.concat(prefixes))
            .unique()
    else
        return []

readLines = (input, cb) ->
    id = 1
    buffer = ''

    input.on('data', (data) ->
        if (sent - completed >  PIPELINE)
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

readLines(stream, commitLine)

hashKeyFields = (key) ->
    bucketSize = 1024
    {key: Math.round(key / bucketSize), field: key % bucketSize}

hashSet = (client, key, value, cb) ->
    keyfields = hashKeyFields(key)
    client.hset(keyfields.key, keyfields.field, value, cb)
