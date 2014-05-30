-- tuple.lua (internal file)

local ffi = require('ffi')
local yaml = require('box.yaml')
local msgpackffi = require('box.msgpackffi')
local fun = require('fun')

ffi.cdef([[
struct tuple
{
    uint32_t _version;
    uint16_t _refs;
    uint16_t _format_id;
    uint32_t _bsize;
    char data[0];
} __attribute__((packed));

void
tuple_ref(struct tuple *tuple, int count);
uint32_t
tuple_field_count(const struct tuple *tuple);
const char *
tuple_field(const struct tuple *tuple, uint32_t i);

struct tuple_iterator {
    const struct tuple *tuple;
    const char *pos;
    int fieldno;
};

void
tuple_rewind(struct tuple_iterator *it, const struct tuple *tuple);

const char *
tuple_seek(struct tuple_iterator *it, uint32_t field_no);

const char *
tuple_next(struct tuple_iterator *it);

void
tuple_to_buf(struct tuple *tuple, char *buf);
]])

local builtin = ffi.C

local tuple_iterator_t = ffi.typeof('struct tuple_iterator')

local function tuple_iterator_next(it, tuple, pos)
    if pos == nil then
        pos = 0
    elseif type(pos) ~= "number" then
         error("error: invalid key to 'next'")
    end
    local field
    if it.tuple == tuple and it.fieldno == pos then
        -- Sequential iteration
        field = builtin.tuple_next(it)
    else
        -- Seek
        builtin.tuple_rewind(it, tuple)
        field = builtin.tuple_seek(it, pos);
    end
    if field == nil then
        if #tuple == pos then
            -- No more fields, stop iteration
            return nil
        else
            -- Invalid pos
            error("error: invalid key to 'next'")
        end
    end
    -- () used to shrink the return stack to one value
    return it.fieldno, (msgpackffi.decode_unchecked(field))
end;

-- precreated iterator for tuple_next
local next_it = ffi.new(tuple_iterator_t)

-- See http://www.lua.org/manual/5.2/manual.html#pdf-next
local function tuple_next(tuple, pos)
    return tuple_iterator_next(next_it, tuple, pos);
end

-- See http://www.lua.org/manual/5.2/manual.html#pdf-ipairs
local function tuple_ipairs(tuple, pos)
    local it = ffi.new(tuple_iterator_t)
    return fun.wrap(it, tuple, pos)
end

-- a precreated metatable for totable()
local tuple_totable_mt = {
    _serializer_compact = true;
}

local function tuple_totable(tuple)
    -- use a precreated iterator for tuple_next
    builtin.tuple_rewind(next_it, tuple)
    local ret = {}
    while true do
        local field = builtin.tuple_next(next_it)
        if field == nil then
            break
        end
        local val = msgpackffi.decode_unchecked(field)
        table.insert(ret, val)
    end
    return setmetatable(ret, tuple_totable_mt)
end

local function tuple_unpack(tuple)
    return unpack(tuple_totable(tuple))
end

local function tuple_find(tuple, offset, val)
    if val == nil then
        val = offset
        offset = 0
    end
    local r = tuple:pairs(offset):index(val)
    return r ~= nil and offset + r - 1 or nil -- tuple is zero-indexed
end

local function tuple_findall(tuple, offset, val)
    if val == nil then
        val = offset
        offset = 0
    end
    return tuple:pairs(offset):indexes(val)
        :map(function(i) return offset + i - 1 end) -- tuple is zero-indexed
        :totable()
end

-- Set encode hooks for msgpackffi
local function tuple_to_msgpack(buf, tuple)
    buf:reserve(tuple._bsize)
    builtin.tuple_to_buf(tuple, buf.p)
    buf.p = buf.p + tuple._bsize
end


msgpackffi.on_encode(ffi.typeof('const struct tuple &'), tuple_to_msgpack)


-- cfuncs table is set by C part

local methods = {
    ["next"]        = tuple_next;
    ["ipairs"]      = tuple_ipairs;
    ["pairs"]       = tuple_ipairs; -- just alias for ipairs()
    ["slice"]       = cfuncs.slice;
    ["transform"]   = cfuncs.transform;
    ["find"]        = tuple_find;
    ["findall"]     = tuple_findall;
    ["unpack"]      = tuple_unpack;
    ["totable"]     = tuple_totable;
    ["bsize"]       = function(tuple)
        return tonumber(tuple._bsize)
    end
}

local const_struct_tuple_ref_t = ffi.typeof('const struct tuple&')

local tuple_gc = function(tuple)
    builtin.tuple_ref(tuple, -1)
end

local tuple_bless = function(tuple)
    -- update in-place, do not spent time calling tuple_ref
    local tuple2 = ffi.gc(ffi.cast(const_struct_tuple_ref_t, tuple), tuple_gc)
    tuple._refs = tuple._refs + 1
    return tuple2
end

local tuple_field = function(tuple, field_n)
    local field = builtin.tuple_field(tuple, field_n)
    if field == nil then
        return nil
    end
    -- Use () to shrink stack to the first return value
    return (msgpackffi.decode_unchecked(field))
end


ffi.metatype('struct tuple', {
    __len = function(tuple)
        return builtin.tuple_field_count(tuple)
    end;
    __tostring = function(tuple)
        -- Unpack tuple, call yaml.encode, remove yaml header and footer
        -- 5 = '---\n\n' (header), -6 = '\n...\n' (footer)
        return yaml.encode(methods.totable(tuple)):sub(5, -6)
    end;
    __index = function(tuple, key)
        if type(key) == "number" then
            return tuple_field(tuple, key)
        end
        return methods[key]
    end;
    __eq = function(tuple_a, tuple_b)
        -- Two tuple are considered equal if they have same memory address
        return ffi.cast('void *', tuple_a) == ffi.cast('void *', tuple_b);
    end;
    __pairs = tuple_ipairs;  -- Lua 5.2 compatibility
    __ipairs = tuple_ipairs; -- Lua 5.2 compatibility
})

ffi.metatype(tuple_iterator_t, {
    __call = tuple_iterator_next;
    __tostring = function(it) return "<tuple iterator>" end;
})

-- Remove the global variable
cfuncs = nil

-- internal api for box.select and iterators
box.tuple.bless = tuple_bless