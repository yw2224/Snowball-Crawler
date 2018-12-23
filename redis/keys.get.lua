local decode = cjson.decode
local encode = cjson.encode
local args = ARGV[1]
local res = {}

for index, item in pairs(decode(args)) do
    local value = redis.call('get', item)
    res[item] = decode(value)
end

return encode(res)
