http = require('http')
url = require('url')

PORT = 80

http.createServer((req, res) ->
    req = url.parse(req.url, true)
    if req.pathname is '/'
        terms = req.query.q.split(' ')
        console.log(terms)
        res.writeHead(200, {'Content-Type': 'application/json'})
        res.end('["Hello Worl"]')
    else
        res.end()
).listen(PORT, '0.0.0.0')
console.log('Server running on port ' + PORT)
