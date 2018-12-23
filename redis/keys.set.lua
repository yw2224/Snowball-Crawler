local decode = cjson.decode
local encode = cjson.encode
local args = ARGV[1]
local insert = 0

for index, item in pairs(decode(args)) do
    local key = tostring(item.k)
    local value = encode(item.v)
    redis.call('set', key, value)
    insert = insert + 1
end

return insert
