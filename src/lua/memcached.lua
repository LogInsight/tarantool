-- memcached.lua

local ffi  = require('ffi')
local mcfg = require('memcached_config')
package.loaded['memcached_config'] = nil

ffi.cdef[[
typedef double time_t;

struct memcached_stat {
    /* connection informations */
    unsigned int  curr_items;
    unsigned int  total_items;
    unsigned int  curr_conns;
    unsigned int  total_conns;
    uint64_t      bytes_read;
    uint64_t      bytes_written;
    /* time when process was started */
    time_t        started;
    /* get statistics */
    uint64_t      cmd_get;
    uint64_t      get_hits;
    uint64_t      get_misses;
    /* delete stats */
    uint64_t      cmd_delete;
    uint64_t      delete_hits;
    uint64_t      delete_misses;
    /* set statistics */
    uint64_t      cmd_set;
    uint64_t      cas_hits;
    uint64_t      cas_badval;
    uint64_t      cas_misses;
    /* incr/decr stats */
    uint64_t      cmd_incr;
    uint64_t      incr_hits;
    uint64_t      incr_misses;
    uint64_t      cmd_decr;
    uint64_t      decr_hits;
    uint64_t      decr_misses;
    /* touch/flush stats */
    uint64_t      cmd_touch;
    uint64_t      touch_hits;
    uint64_t      touch_misses;
    uint64_t      cmd_flush;
    /* expiration stats */
    uint64_t      evictions;
    uint64_t      reclaimed;
    /* authentication stats */
    uint64_t      auth_cmds;
    uint64_t      auth_errors;
};

void
memcached_set_opt (struct memcached_service *srv, int opt, ...);

enum memcached_options {
    MEMCACHED_OPT_READAHEAD = 0,
    MEMCACHED_OPT_EXPIRE_ENABLED,
    MEMCACHED_OPT_EXPIRE_COUNT,
    MEMCACHED_OPT_EXPIRE_TIME,
    MEMCACHED_OPT_FLUSH_ENABLED
};

struct memcached_stat *memcached_get_stat (struct memcached_service *);

struct memcached_service *memcached_create(const char *, uint32_t);
void memcached_start (struct memcached_service *, const char *);
void memcached_stop  (struct memcached_service *);
void memcached_free  (struct memcached_service *);
]]

local memcached_services = {}

local RUNNING = 'r'
local STOPPED = 's'
local ERRORED = 'e'

local stat_table = {
    'total_items', 'curr_items', 'started',
    'curr_conns', 'total_conns',
    'bytes_read', 'bytes_written',
    'cmd_get', 'get_hits', 'get_misses',
    'cmd_delete', 'delete_hits', 'delete_misses',
    'cmd_set', 'cas_hits', 'cas_badval', 'cas_misses',
    'cmd_incr', 'incr_hits', 'incr_misses',
    'cmd_decr', 'decr_hits', 'decr_misses',
    'cmd_touch', 'touch_hits', 'touch_misses',
    'cmd_flush',
    'evictions', 'reclaimed',
    'auth_cmds', 'auth_errors'
}

local conf_table = {
    readahead            = ffi.C.MEMCACHED_OPT_READAHEAD,
    expire_enabled       = ffi.C.MEMCACHED_OPT_EXPIRE_ENABLED,
    expire_items_per_item= ffi.C.MEMCACHED_OPT_EXPIRE_COUNT,
    expire_full_scan_time= ffi.C.MEMCACHED_OPT_EXPIRE_TIME
}

local memcached_mt = {
    cfg = function (self, opts)
        if type(opts) ~= 'table' then
            error('arguments must be in dictionary')
        end
        local stat, err = mcfg.check(opts or {})
        if stat == false then error(err) end
        for k, v in pairs(opts) do
            if conf_table[k] ~= nil then
                ffi.C.memcached_set_opt(self.service, conf_table[k], v)
            end
        end
    end,
    start = function (self)
        if self.status == RUNNING then
            error("memcached '%s' is already started", self.name)
        end
        box.error.clear()
        ffi.C.memcached_start(self.service, self.uri)
        if box.error.last() ~= nil then
            error("error while binding on port")
        end
        self.status = RUNNING
    end,
    stop = function (self)
        if self.status == STOPPED then
            error("memcached '%s' is already stopped", self.name)
        end
        box.error.clear()
        local rc = ffi.C.memcached_stop(self.service)
        if box.error.last() ~= nil then
            error('error while stopping memcached')
        end
        self.status = STOPPED
    end,
    info = function (self)
        stats = ffi.C.memcached_get_stat(self.service)
        retval = {}
        for k, v in pairs(stat_table) do
            retval[v] = stats[0][v]
        end
        return retval
    end
}

local function memcached_init(opts)
    local conf = mcfg.initial(opts)
    local instance = {}
    instance.opts = conf
    instance.name = conf.name; instance.opts.name = nil
    instance.uri  = conf.uri;  instance.opts.uri  = nil
    if box.space['__mc_' .. instance.name] ~= nil then
        error(string.format("Space with name '%s' is already created", sname))
    end
    instance.space = box.schema.create_space(instance.name)
    instance.space:create_index('primary', {parts = {1, 'str'}, type = 'hash'})
    local service = ffi.C.memcached_create(instance.name, instance.space.id)
    if service == nil then error("can't allocate memory") end
    instance.service = ffi.gc(service, ffi.C.memcached_free)
    memcached_services[instance.name] = setmetatable(instance,
        { __index = memcached_mt }
    )
    instance:cfg(opts)
    instance:start()
    return instance
end

return {
    create = memcached_init;
    get    = function (name) return memcached_services[name] end;
    debug  = memcached_services;
}
