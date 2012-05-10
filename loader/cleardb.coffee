PORT = process.argv[2]
redis = require('redis')

client = redis.createClient(PORT, '127.0.0.1');
console.log(client)
console.log("Clearing database...")
client.flushall(() -> 
    console.log("Cleared database")
    client.end()
)
