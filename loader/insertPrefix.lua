local prefixScores = KEYS[1]
local id = tonumber(KEYS[2])
local maxSetSize = tonumber(ARGV[1])
local prefixScores = cjson.decode(prefixScores)

for prefix, score in pairs(prefixScores) do
    score = tonumber(score)
    local summary = id .. ":" .. score

    local setSize = redis.call('zcard', prefix)
    if setSize < maxSetSize then
        redis.call('zadd', prefix, score, id)
    else
        local min = redis.call('zrange', prefix, 0, 0, 'WITHSCORES')
        if tonumber(min[2]) < score then
            redis.call('zrem', prefix, min[1]) 
            redis.call('zadd', prefix, score, id)
        end
    end
end

return 1
