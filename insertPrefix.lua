local prefix = KEYS[1]
local score = tonumber(KEYS[2])
local id = tonumber(KEYS[3])

local summary = id .. ":" .. score

local setSize = redis.call('zcard', prefix)
if setSize <= 1024 then
    redis.call('zadd', prefix, score, id)
    return "added " .. prefix .. " " .. summary
else
    local min = redis.call('zrange', prefix, 0, 0, 'WITHSCORES')
    if tonumber(min[2]) < score then
        redis.call('zrem', prefix, min[1]) 
        redis.call('zadd', prefix, score, id)
        return "replaced " .. prefix .. " " .. min[1] .. ":" .. min[2] .. " with " .. summary
    else
        print(min[2] .. ":" .. score)
        return "not added. lowest score " .. min[2] .. " >= " .. summary
    end
end
