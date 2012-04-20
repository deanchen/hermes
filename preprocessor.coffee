fs = require('fs')
lazy = require('lazy')

path = process.argv.splice(2)[0]

i = 0
new lazy(fs.createReadStream(path, {encoding:'ascii'}))
    .lines
    .map(String)
    .forEach((line) ->
        console.log(JSON.stringify({id: i++, term:line}))
    )

