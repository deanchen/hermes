fs = require('fs')
lazy = require('lazy')
redis = require('redis')

MIN_COMPLETE = 2
STOP_WORDS = fs.readFileSync('stop-words.txt', 'ascii').split('\n')

args = process.argv.splice(2)
type = args[0]
path = args[1]

client = redis.createClient()
client.flushall()

index = 0

stream = fs.createReadStream(path, {encoding:'ascii'})
lazy(stream)
    .lines
    .map(String)
    .forEach((line) ->
        if line is "0"
            return client.quit()
        console.log(JSON.stringify({id: index++, term:line}))
    )

prefix = (phrase) ->
    phrase
        .toLowerCase()
        .replace('/[^a-z2-9/ig', '')
        .trim()
        .split(' ')
        .filter((word)-> !~STOP_WORDS.indexOf(word))
        .map((word) -> [(MIN_COMPLETE - 1)..(word.length - 1)].map((length) -> word[0..length]))
        .reduce((acc, prefixes) -> acc.concat(prefixes))
        .unique()


Array::unique = ->
    output = {}
    output[@[key]] = @[key] for key in [0...@length]
    value for key, value of output


