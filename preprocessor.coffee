fs = require('fs')
lazy = require('lazy')
redis = require('redis')

MIN_COMPLETE = 2
STOP_WORDS = fs.readFileSync('stop-words.txt', 'ascii').split('\n')
THROTTLE = 200

args = process.argv.splice(2)
type = args[0]
path = args[1]

client = redis.createClient()
client.flushall()

index = 1
buffer = []

stream = fs.createReadStream(path, {encoding:'ascii'})
commitLine = (line, i) ->
    if line is "0" then return client.quit()
    prefix(line).forEach((p) -> client.sadd(type + ":" + p, i))
    client.hset("soulmate-data:" + type, i, JSON.stringify({id: i, term: line}))
    if i % 10000 is 0 then console.log(i)

prefix = (phrase) ->
    phrase
        .toLowerCase()
        .replace('/[^a-z0-9/ig', '')
        .trim()
        .split(' ')
        .filter((word)-> !~STOP_WORDS.indexOf(word))
        .map((word) -> [(MIN_COMPLETE - 1)..(word.length - 1)].map((length) -> word[0..length]))
        .reduce((acc, prefixes) -> acc.concat(prefixes))
        .unique()

readLines = (input, cb) ->
    id = 1
    buffer = ''

    input.on('data', (data) ->
        buffer += data
        index = buffer.indexOf('\n')
        while (index > -1)
            line = buffer.substring(0, index)
            buffer = buffer.substring(index + 1)
            cb(line, id++)
            index = buffer.indexOf('\n')
    )
    input.on('end', () ->
        if buffer.length > 0 then cb(buffer, id++)
    )

setInterval(() ->
    stream.pause()
    setTimeout(()->
        stream.resume()
    , THROTTLE)
1000)

Array::unique = ->
    output = {}
    output[@[key]] = @[key] for key in [0...@length]
    value for key, value of output

readLines(stream, commitLine)
