#ifndef   TARANTOOL_BOX_MEMCACHED_LAYER_H_INCLUDED
#define   TARANTOOL_BOX_MEMCACHED_LAYER_H_INCLUDED

enum memcached_set {
	MEMCACHED_SET_CAS = 0,
	MEMCACHED_SET_ADD,
	MEMCACHED_SET_SET,
	MEMCACHED_SET_REPLACE
};

typedef int (* mc_process_func_t)(struct memcached_connection *con);

int memcached_process_set(struct memcached_connection *con);
int memcached_process_get(struct memcached_connection *con);
int memcached_process_delete(struct memcached_connection *con);
int memcached_process_noop(struct memcached_connection *con);
int memcached_process_flush(struct memcached_connection *con);
int memcached_process_verbosity(struct memcached_connection *con);
int memcached_process_gat(struct memcached_connection *con);
int memcached_process_version(struct memcached_connection *con);
int memcached_process_delta(struct memcached_connection *con);
int memcached_process_pend(struct memcached_connection *con);
int memcached_process_quit(struct memcached_connection *con);
int memcached_process_stat(struct memcached_connection *con);
int memcached_process_unknown(struct memcached_connection *con);
int memcached_process_unsupported(struct memcached_connection *con);

int memcached_error(struct memcached_connection *con,
		    uint16_t err, const char *errstr);
int memcached_errori(struct memcached_connection *con);

int
is_expired_tuple(struct memcached_service *p, box_tuple_t *tuple);

extern const mc_process_func_t mc_handler[];

#endif /* TARANTOOL_BOX_MEMCACHED_LAYER_H_INCLUDED */
