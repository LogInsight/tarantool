-- test for https://github.com/tarantool/tarantool/issues/769

s = box.schema.create_space('test')
i = s:create_index('primary', { type = 'TREE', parts = {1, 'num', 2, 'num'} })
s:insert{0, 0} s:insert{2, 0}
for i=1,100000 do s:insert{1, i} end
test_itrs = {'EQ', 'REQ', 'GT', 'LT', 'GE', 'LE'}
test_res = {}
too_longs = {}

--# setopt delimiter ';'
function test_run_itr(itr, key)
    for i=1,10000 do
        local gen, param, state = s.index.primary:pairs({key}, {iterator = itr})
        state, v =  gen(param, state)
        test_res[itr .. ' ' .. key] = v
    end
end;

function test_itr(itr, key)
    local t = os.time()
    test_run_itr(itr, key)
    if os.time() - t > 1 then
        table.insert(too_longs, 'Too long ' .. itr .. ' ' .. key)
    end
end;

for _,itr in pairs(test_itrs) do
    for key = 0,2 do
        test_itr(itr, key)
    end
end;
--# setopt delimiter ''

test_res
too_longs
s:drop()
test_itr = nil test_run_itr = nil test_itrs = nil s = nil
'done'