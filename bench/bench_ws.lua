--local cjson = require('cjson')

local f = io.open("mblog.dat")

box.cfg{}

s = box.schema.space.create("bench", {engine="ws"})
s:create_index('a1')
local line_nu = 0;

data = {"a"}
--box.space.bench:insert({1}, {233, 2342})

offset = 0
lines = {}

for line in f:lines() do
    line_nu = line_nu + 1
--    json = cjson.decode(line)
--    lines[line_nu] = json
    lines[line_nu] = line 
    offset = offset + #line
    --box.space.bench:insert{line_nu, json}
    if line_nu % 10000 == 0 then
        print("read line: "..line_nu)
    end
    if line_nu == 100000 then
        break
    end
end
print("total line: "..line_nu)

count = 0
local start_time = os.time()
for k, v in pairs(lines) do
    count = count + 1
    if count % 10000 == 0 then
        print("insert line: "..count)
    end
    box.space.bench:insert{k, v}
end
print("insert line: "..line_nu)

local end_time = os.time()
print("bench over")

print("used time: "..start_time-end_time.."s")

os.exit(0)
