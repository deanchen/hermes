PORT = 4000

http = require('http')
url = require('url')
redis = require('redis')
async = require('async')
_ = require('underscore')

client = redis.createClient(6400, '127.0.0.1')
client.select(0)
termClient = redis.createClient(6400, '127.0.0.1')
termClient.select(1)

cache = {}

http.createServer((req, httpRes) ->
    req = url.parse(req.url, true)
    if req.pathname is '/'
        terms = req.query.q?.split(' ')

        termsKey = "c:" + terms.join('|')
        args = [termsKey, terms.length]
        args = args.concat(terms)

        if (cache[termsKey])
            respond(httpRes, cache[termsKey])
            return
        
        async.waterfall([
            (cb) ->
                client.zunionstore(args, (err, res) -> cb(err))
            ,
            (cb) ->
                async.parallel([
                    (cb2) ->
                        client.zrevrange(termsKey, 0, 4, 'withscores', (err, topIds) ->
                            cb2(err, pair(topIds))
                        )
                    ,
                    (cb2) ->
                        client.del(termsKey, cb2)
                ],
                (err, res) ->
                    cb(err, res[0])
                )
            ,
            (array, cb) ->
                async.map(array, (row, cb2) ->
                    id = parseInt(row[0], 10)
                    termClient.hget(Math.round(id/1024), id%1024, (err, res) ->
                        row[0] = res
                        cb2(err, row)
                    )
                , cb
                )
            ,
            (rows, cb) ->
                content = JSON.stringify(formatJson(rows))
                respond(httpRes, content)
                cache[termsKey] = content
                
                args = _.flatten(reverse(rows))
                args.unshift(termsKey)
                client.zadd(args, (err, res) ->
                    cb(err)
                )
        ],
        (err, res) ->
            if err then return console.log("error: " + err)
        )

    else
        httpRes.end()
).listen(PORT, '0.0.0.0')
console.log('Server running on port ' + PORT)

formatJson = (array) ->
    array.map((el) -> JSON.parse(el[0]))

respond = (res, content) ->
    res.writeHead(200, {'Content-Type': 'application/json'})
    res.end(content)

pair = (array) ->
    [array[x], array[x+1]] for x in [0..array.length-1] by 2

reverse = (array) ->
    array.map((el) ->
        return [el[1], el[0]]
    )
