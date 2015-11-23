/**
 * @file: wt_index.cc
 * @author: nzinfo
 * @date: 2015/11/20 04:31:04
 **/
#include "wt_index.h"
#include "schema.h"
#include "tuple.h"
#include "space.h"

WTIndex::WTIndex(struct key_def *key_def_arg)
        : Index(key_def_arg)
{
    struct space *space = space_cache_find(key_def->space_id);
	format = space->format;
	tuple_format_ref(format, 1);
	printf("struct index format id = %u\n",format->id);
#if 0
    SophiaEngine *engine =
            (SophiaEngine *)space->handler->engine;
    env = engine->env;
    db = sophia_configure(space, key_def);
    if (db == NULL)
        sophia_error(env);
    /* start two-phase recovery for a space:
     * a. created after snapshot recovery
     * b. created during log recovery
    */
    int rc = sp_open(db);
    if (rc == -1)
        sophia_error(env);
    format = space->format;
    tuple_format_ref(format, 1);
#endif
}

void*
wt_tuple_new(const uint64_t key, WT_ITEM *value, struct key_def *key_def,
             struct tuple_format *format,
             uint32_t *bsize)
{
	assert(key_def->part_count == 1);
	(void) bsize;
	int size = 0;
	/*calc the tuple size*/
	const char *value_ptr = (const char*)value->data;
	const char *value_ptr_end = value_ptr + value->size;
	size += mp_sizeof_uint(key);
	size += value->size;
	int count = key_def->part_count;
	while (value_ptr < value_ptr_end) {
		count++;
		mp_next(&value_ptr);
	}
	size += mp_sizeof_array(count);
	if (bsize) {
		*bsize = size;
	}
	/* build tuple */
	struct tuple *tuple = NULL;
	char *p = NULL;
	char *raw = NULL;

	if (format) {
		tuple = tuple_alloc(format, size);
		p = tuple->data;
	} else {
		raw = (char *)malloc(size);
		if (raw == NULL)
			tnt_raise(ClientError, ER_MEMORY_ISSUE, size, "tuple");
		p = raw;
	}
	p = mp_encode_array(p, 2);
	for (int i = 0; i < key_def->part_count; i++) {
		if (key_def->parts[i].type == STRING)
			//p = mp_encode_str(p, parts[i].part, parts[i].size);
			tnt_raise(ClientError, ER_KEY_PART_TYPE, i, "STRING");
		else
			p = mp_encode_uint(p, key);
	}
	memcpy(p, value->data, value->size);
	if (format) {
		try {
			tuple_init_field_map(format, tuple, (uint32_t *)tuple);
		} catch (...) {
			tuple_delete(tuple);
			throw;
		}
		return tuple;
	}
	return raw;
}

struct tuple *
WTIndex::findByKey(const char *key, uint32_t part_count) const
{
	assert(part_count == 1);
	uint64_t recv_key = mp_decode_uint(&key);
	WT_ITEM recv_data;
	wk_server->get_value(table_name, recv_key, &recv_data);
	printf("get value in find by key = %.*s\n", (int)recv_data.size, (char *)recv_data.data);
	struct tuple *tuple = (struct tuple *)wt_tuple_new(recv_key, &recv_data, key_def, format, NULL);
	return tuple;
}

struct tuple *
WTIndex::replace(struct tuple*, struct tuple*, enum dup_replace_mode)
{
    /* This method is unused by sophia index.
     *
     * see ::replace_or_insert() */
    assert(0);
    return NULL;
}

struct wt_iterator {
	struct iterator base;
	const char *key;
	const char *keyend;
	struct space *space;
	struct key_def *key_def;
	int open;
	void *env;
	void *db;
	void *cursor;
	void *current;
};

struct tuple *
wt_iterator_next(struct iterator *ptr)
{
	assert(ptr->next == wt_iterator_next);
	struct wt_iterator *it = (struct wt_iterator *) ptr;
	(void) it;
#if 0
	assert(it->cursor != NULL);
	if (it->open) {
		it->open = 0;
		if (it->current) {
			return (struct tuple *)
				wt_tuple_new(it->current, it->key_def,
				                 it->space->format,
				                 NULL);
		} else {
			return NULL;
		}
	}
	/* try to read next key from cache */
	sp_setint(it->current, "async", 0);
	sp_setint(it->current, "cache_only", 1);
	sp_setint(it->current, "immutable", 1);
	void *obj = sp_get(it->cursor, it->current);
	sp_setint(it->current, "async", 1);
	sp_setint(it->current, "cache_only", 0);
	sp_setint(it->current, "immutable", 0);
	/* key found in cache */
	if (obj) {
		sp_destroy(it->current);
		it->current = obj;
		return (struct tuple *)
			sophia_tuple_new(obj, it->key_def, it->space->format, NULL);
	}

	/* retry search, but use disk this time */
	obj = sp_get(it->cursor, it->current);
	it->current = NULL;
	if (obj == NULL)
		return NULL;
	sp_destroy(obj);
	fiber_yield();
	obj = fiber_get_key(fiber(), FIBER_KEY_MSG);
	if (obj == NULL)
		return NULL;
	int rc = sp_getint(obj, "status");
	if (rc <= 0) {
		sp_destroy(obj);
		return NULL;
	}
	it->current = obj;
	return (struct tuple *)
		sophia_tuple_new(obj, it->key_def, it->space->format, NULL);
#endif
	return NULL;
}


struct tuple *
wt_iterator_last(struct iterator */*ptr*/)
{
	return NULL;
}

struct tuple *
wt_iterator_eq(struct iterator *ptr)
{
	ptr->next = wt_iterator_last;
	struct wt_iterator *it = (struct wt_iterator *) ptr;
	//assert(it->cursor == NULL);
	WTIndex *index = (WTIndex *)index_find(it->space, it->key_def->iid);
	return index->findByKey(it->key, it->key_def->part_count);
}

struct iterator *
WTIndex::allocIterator() const
{
	struct wt_iterator *it =
		(struct wt_iterator *) calloc (1, sizeof(wt_iterator));
	if (it == NULL) {
		tnt_raise(ClientError, ER_MEMORY_ISSUE,
		          sizeof(struct wt_iterator), "wt Index",
		          "iterator");
	}
	it->base.next = wt_iterator_next;
	it->base.free = NULL;
	it->cursor = NULL;
    return (struct iterator *)it;
}

void
WTIndex::initIterator(struct iterator * ptr,
                          enum iterator_type type,
                          const char * key, uint32_t part_count) const
{
	printf("part count = %u\n", part_count);
	struct wt_iterator *it = (struct wt_iterator *) ptr;
	//assert(it->cursor == NULL);
	if (part_count > 0) {
		if (part_count != key_def->part_count) {
			tnt_raise(ClientError, ER_UNSUPPORTED,
			          "wt Index iterator", "uncomplete keys");
		}
	} else {
		key = NULL;
	}
	it->key = key;
	it->key_def = key_def;
	it->space = space_cache_find(key_def->space_id);
	it->current = NULL;
	it->open = 1;
	const char *compare;
	switch (type) {
		case ITER_EQ:
			it->base.next = wt_iterator_eq;
			return;
		case ITER_ALL:
		case ITER_GE: compare = ">=";
			break;
		case ITER_GT: compare = ">";
			break;
		case ITER_LE: compare = "<=";
			break;
		case ITER_LT: compare = "<";
			break;
		default:
			tnt_raise(ClientError, ER_UNSUPPORTED,
			          "wt Index", "requested iterator type");
	}
	(void) compare;
	it->base.next = wt_iterator_next;
}

/* non-interface function  */
void
WTIndex::replace_or_insert(const char *tuple,
                               const char *tuple_end,
                               enum dup_replace_mode mode)
{
    uint32_t size = tuple_end - tuple;
    const char *recv_data = tuple_field_raw(tuple, size, 1);
    /* insert: ensure key does not exists */
    /*
    if (mode == DUP_INSERT) {
        struct tuple *found = findByKey(key);
        if (found) {
            tuple_delete(found);
            struct space *sp = space_cache_find(key_def->space_id);
            tnt_raise(ClientError, ER_TUPLE_FOUND,
                      index_name(this), space_name(sp));
        }
    }
    */
    (void)mode;
    // dup the value
    printf("index num = %u\n", key_def->iid);
#if 0
    while (i < key_def->part_count) {
        const char *part;
        uint32_t partsize;
        uint64_t num_part;
        if (key_def->parts[i].type == STRING) {
            part = mp_decode_str(&key, &partsize);
            printf("no %d, s=%.*s\t", i, partsize, part);
        } else {
            num_part = mp_decode_uint(&key);
            //(void)num_part;
            printf("no %d, n=%lu\t", i, num_part);
            //part = (char *)&num_part;
            //partsize = sizeof(uint64_t);
        }
        i++;
    } // end while
#endif
	(void)recv_data;
    uint64_t key = mp_decode_uint(&recv_data);
    //uint32_t value_size = 0;
	WT_ITEM insert_value;
	insert_value.data = recv_data;
	insert_value.size = tuple_end-recv_data;
    wk_server->put_value(table_name, key, &insert_value);

	WT_ITEM tmp_value;
    wk_server->get_value(table_name, key, &tmp_value);
	printf("get the value = %.*s\n", (int)tmp_value.size, (char*)tmp_value.data);

	printf("recv size = %lu, get size = %lu\n", tuple_end-recv_data, tmp_value.size);

	uint32_t value_size = 0;
	const char *value = mp_decode_str(&recv_data, &value_size);
	printf("key = %lu, value = %.*s, size = %u\n", key, value_size, value, value_size);
    /* replace */
    /*
    void *transaction = in_txn()->engine_tx;
    const char *value;
    size_t valuesize;
    void *obj = createObject(key, false, &value);
    valuesize = size - (value - tuple);
    if (valuesize > 0)
        sp_setstring(obj, "value", value, valuesize);
    int rc;
    rc = sp_set(transaction, obj);
    if (rc == -1)
        sophia_error(env);
    */
}

/*- end of file -*/
