local log = require('log')
local uri = require('uri')

local typetable = {
    name = {
        'string',
        function () return 'memcached' end,
        function (x) return true end,
        [[Name of memcached instance]]
    },
    uri = {
        'string',
        function() return '0.0.0.0:11211' end,
        function(x) local a = uri.parse(x); return (a and a['service']) end,
        [[The read/write data URI]]
    },
    readahead = {
        'number',
        function() return box.cfg.readahead end,
        function(x) return x > 0 and x < math.pow(2, 10) end,
        [[size of readahead buffer]]
    },
    expire_enabled = {
        'boolean',
        function() return true end,
        function(x) return true end,
        [[configure availability of expiration daemon]]
    },
    expire_items_per_iter = {
        'number',
        function() return 200 end,
        function(x) return x > 0 end,
        [[configure scan count for expiration]]
    },
    expire_full_scan_time = {
        'number',
        function() return 3600 end,
        function(x) return x > 0 end,
        [[time required for full index scan (in seconds)]]
    },
--    flush_enabled = {
--        'boolean',
--        function() return true end,
--        [[ flush command availability ]]
--    },
}

local err_no_such_option  = "No such option '%s'"
local err_bad_option_type = "Bad '%s' option type, expected '%s', got '%s'"
local err_bad_value       = "Bad value for argument '%s'"

local function config_check(cfg)
    for k, v in pairs(cfg) do
        if typetable[k] == nil then
            return false, string.format(err_no_such_option, k)
        end
        if type(v) ~= typetable[k][1] then
            return false, string.format(err_bad_option_type, k,
                                        typetable[k][1], type(v))
        end
        if not typetable[k][3](v) then
            return false, string.format(err_bad_value, k)
        end
    end
    return true
end

local function config_initial(cfg)
    local newcfg = {}
    for k, v in pairs(typetable) do newcfg[k] = v[2]() end
    for k, v in pairs(cfg) do newcfg[k] = v end
    local stat, err = config_check(cfg)
    if stat then return newcfg end
    error(err)
end

local function config_help()
    for k, v in pairs(typetable) do
        log.info('%s: %s', k, v[4])
        log.info("%s type is '%s'",    string.rep(' ', #k + 1), v[1])
        log.info("%s default is '%s'", string.rep(' ', #k + 1), tostring(v[2]()))
    end
end

return {
    initial   = config_initial,
    help      = config_help,
    check     = config_check,
    cfg_table = typetable
}
