
console = require('console'); console.delimiter('!')
function example()
    local ta = {}, i, line
    for k, v in box.space._space:pairs() do
        i = 1
        line = ''
        while i <= #v do
            if type(v[i]) ~= 'table' then
                line = line .. v[i] .. ' '
            end
            i = i + 1
        end
        table.insert(ta, line)
    end
    return ta
end!
console.delimiter('')!
