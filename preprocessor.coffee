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

args = process.argv.splice(2)
path = args[0]
totalWorkers = args[1]
workerIndex = args[2]
uprefixes = {}
problemLines = [];

i = 0
client = redis.createClient(6400, "127.0.0.1")
#clients[0].flushall()

termClient = redis.createClient(6400, "127.0.0.1")
termClient.select(1)

prefixClient = redis.createClient(6400, "127.0.0.1")
prefixClient.select(2)

index = 1
stats = {lines:0, words:0, prefixes:0}

startTime = new Date()

sent = 0
completed = 0

finished_processing = false
stream = fs.createReadStream(path, {encoding:'ascii'})
commitLine = (line, i) ->
    if COMMIT
        try 
            line = JSON.parse(line)
        catch e
            console.log('##############')
            console.log(line)
            console.log(e)
            console.log('##############')
            problemLines.push(line)
            return
        
        unless line
            console.log('###########')
            console.log(i + ": " + problemLines)
            console.log('###########')

            client.quit()

        prefixScores = prefix(line.title)
        Object.keys(prefixScores).forEach((prefix) ->
            return if (prefix is "") or (uprefixes[prefix])

            prefixClient.hincrby("prefixes", prefix, 1, (err, res)-> 
                unless res > 1023
                    sent++
                    client.zadd(p.phrase, p.score, line.id, (err) ->
                        completed++
                        fill_pipeline()
                    )
                else
                    uprefixes[p] = true
                    
            )
                
        )
        process.exit(1);
        sent++
        hashSet(termClient, i, JSON.stringify(line), (err)->
            completed++
            fill_pipeline()
        )
        if i % 100000 is 0
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

hashKeyFields = (key) ->
    bucketSize = 1024
    {key: Math.round(key / bucketSize), field: key % bucketSize}

hashSet = (client, key, value, cb) ->
    keyfields = hashKeyFields(key)
    client.hset(keyfields.key, keyfields.field, value, cb)


readLines(stream, commitLine)
