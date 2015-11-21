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
    //struct space *space = space_cache_find(key_def->space_id);
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

struct tuple *
WTIndex::findByKey(const char * /* key */ , uint32_t /*part_count*/ = 0) const
{
    panic("findByKey, not implemented");
#if 0
    (void)part_count;
    void *obj = ((SophiaIndex *)this)->createObject(key, true, NULL);
    void *transaction = db;
    /* engine_tx might be empty, even if we are in txn context.
     *
     * This can happen on a first-read statement. */
    if (in_txn())
        transaction = in_txn()->engine_tx;
    obj = sp_get(transaction, obj);
    if (obj == NULL)
        return NULL;
    int rc = sp_getint(obj, "status");
    if (rc == 0) {
        sp_destroy(obj);
        fiber_yield();
        obj = fiber_get_key(fiber(), FIBER_KEY_MSG);
        if (obj == NULL)
            return NULL;
        rc = sp_getint(obj, "status");
        if (rc <= 0 || rc == 2) {
            sp_destroy(obj);
            return NULL;
        }
    }
    struct tuple *tuple =
            (struct tuple *)sophia_tuple_new(obj, key_def, format, NULL);
    sp_destroy(obj);
    return tuple;
#endif
    return NULL;
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

struct iterator *
WTIndex::allocIterator() const
{
    panic("allocIterator, not implemented");
#if 0
    struct sophia_iterator *it =
            (struct sophia_iterator *) calloc(1, sizeof(*it));
    if (it == NULL) {
        tnt_raise(ClientError, ER_MEMORY_ISSUE,
                  sizeof(struct sophia_iterator), "Sophia Index",
                  "iterator");
    }
    it->base.next = sophia_iterator_next;
    it->base.free = sophia_iterator_free;
    it->cursor = NULL;
    return (struct iterator *) it;
#endif
    return NULL;
}

void
WTIndex::initIterator(struct iterator * /* ptr */,
                          enum iterator_type /* type */,
                          const char * /* key */, uint32_t /* part_count */) const
{
    panic("initIterator, not implemented");
#if 0
    struct sophia_iterator *it = (struct sophia_iterator *) ptr;
    assert(it->cursor == NULL);
    if (part_count > 0) {
        if (part_count != key_def->part_count) {
            tnt_raise(ClientError, ER_UNSUPPORTED,
                      "Sophia Index iterator", "uncomplete keys");
        }
    } else {
        key = NULL;
    }
    it->key = key;
    it->key_def = key_def;
    it->env = env;
    it->db = db;
    it->space = space_cache_find(key_def->space_id);
    it->current = NULL;
    it->open = 1;
    const char *compare;
    switch (type) {
        case ITER_EQ:
            it->base.next = sophia_iterator_eq;
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
                      "Sophia Index", "requested iterator type");
    }
    it->base.next = sophia_iterator_next;
    it->cursor = sp_cursor(env);
    if (it->cursor == NULL)
        sophia_error(env);
    void *obj = ((SophiaIndex *)this)->createObject(key, true, &it->keyend);
    sp_setstring(obj, "order", compare, 0);
    /* Position first key here, since key pointer might be
     * unavailable from lua.
     *
     * Read from disk and fill cursor cache.
     */
    obj = sp_get(it->cursor, obj);
    if (obj == NULL) {
        sp_destroy(it->cursor);
        it->cursor = NULL;
        return;
    }
    sp_destroy(obj);
    fiber_yield();
    obj = fiber_get_key(fiber(), FIBER_KEY_MSG);
    if (obj == NULL) {
        it->current = NULL;
        return;
    }
    int rc = sp_getint(obj, "status");
    if (rc <= 0) {
        it->current = NULL;
        sp_destroy(obj);
        return;
    }
    it->current = obj;
#endif

}

/* non-interface function  */
void
WTIndex::replace_or_insert(const char *tuple,
                               const char *tuple_end,
                               enum dup_replace_mode mode)
{
    uint32_t size = tuple_end - tuple;
    const char *key = tuple_field_raw(tuple, size, key_def->parts[0].fieldno);
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
    {
        int i = 0;
        while (i < key_def->part_count) {
            const char *part;
            uint32_t partsize;
            uint64_t num_part;
            if (key_def->parts[i].type == STRING) {
                part = mp_decode_str(&key, &partsize);
                printf("no %d, s=%.*s\t", i, partsize, part);
            } else {
                num_part = mp_decode_uint(&key);
                printf("no %d, n=%lu\t", i, num_part);
                //part = (char *)&num_part;
                //partsize = sizeof(uint64_t);
            }
            i++;
        } // end while
        fflush(stdout);  // force flush
    }
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
