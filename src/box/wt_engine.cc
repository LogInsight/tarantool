/**
 * @file: wt_engine.cc
 * @author: wangjicheng
 * @mail: 602860321@qq.com
 * @date: 2015/11/20
 **/

#include "wt_engine.h"
#include "wt_index.h"
#include "tuple.h"
#include "txn.h"
#include "index.h"
#include "space.h"
#include "schema.h"
#include "msgpuck/msgpuck.h"
#include "small/rlist.h"
#include "request.h"
#include "iproto_constants.h"

/**
* A version of space_replace for a space which has
* no indexes (is not yet fully built).
*/
static void
wt_replace_no_keys(struct txn * /* txn */, struct space * /* space */,
					  struct tuple * /* old_tuple */,
					  struct tuple * /* new_tuple */,
					  enum dup_replace_mode /* mode */)
{
#if 0
	Index *index = index_find(space, 0);
	assert(index == NULL); /* not reached. */
	(void) index;
#endif
}

struct WTSpace: public Handler {
	WTSpace(Engine *e)
			: Handler(e)
	{
		replace = wt_replace_no_keys;
	}
	virtual ~WTSpace()
	{
		/* do nothing */
		/* engine->close(this); */
	}
	virtual struct tuple *
			executeReplace(struct txn *txn, struct space *space,
						   struct request *request);
	virtual struct tuple *
			executeDelete(struct txn *txn, struct space *space,
						  struct request *request);
	virtual struct tuple *
			executeUpdate(struct txn *txn, struct space *space,
						  struct request *request);
	virtual void
			executeUpsert(struct txn *txn, struct space *space,
						  struct request *request);
	virtual void
			executeSelect(struct txn *, struct space *space,
						  uint32_t index_id, uint32_t iterator,
						  uint32_t offset, uint32_t limit,
						  const char *key, const char * /* key_end */,
						  struct port *port);
	virtual void onAlter(Handler *old);

public:
	/**
	 * @brief A single method to handle REPLACE, DELETE and UPDATE.
	 *
	 * @param sp space
	 * @param old_tuple the tuple that should be removed (can be NULL)
	 * @param new_tuple the tuple that should be inserted (can be NULL)
	 * @param mode      dup_replace_mode, used only if new_tuple is not
	 *                  NULL and old_tuple is NULL, and only for the
	 *                  primary key.
	 *
	 * For DELETE, new_tuple must be NULL. old_tuple must be
	 * previously found in the primary key.
	 *
	 * For REPLACE, old_tuple must be NULL. The additional
	 * argument dup_replace_mode further defines how REPLACE
	 * should proceed.
	 *
	 * For UPDATE, both old_tuple and new_tuple must be given,
	 * where old_tuple must be previously found in the primary key.
	 *
	 * Let's consider these three cases in detail:
	 *
	 * 1. DELETE, old_tuple is not NULL, new_tuple is NULL
	 *    The effect is that old_tuple is removed from all
	 *    indexes. dup_replace_mode is ignored.
	 *
	 * 2. REPLACE, old_tuple is NULL, new_tuple is not NULL,
	 *    has one simple sub-case and two with further
	 *    ramifications:
	 *
	 *	A. dup_replace_mode is DUP_INSERT. Attempts to insert the
	 *	new tuple into all indexes. If *any* of the unique indexes
	 *	has a duplicate key, deletion is aborted, all of its
	 *	effects are removed, and an error is thrown.
	 *
	 *	B. dup_replace_mode is DUP_REPLACE. It means an existing
	 *	tuple has to be replaced with the new one. To do it, tries
	 *	to find a tuple with a duplicate key in the primary index.
	 *	If the tuple is not found, throws an error. Otherwise,
	 *	replaces the old tuple with a new one in the primary key.
	 *	Continues on to secondary keys, but if there is any
	 *	secondary key, which has a duplicate tuple, but one which
	 *	is different from the duplicate found in the primary key,
	 *	aborts, puts everything back, throws an exception.
	 *
	 *	For example, if there is a space with 3 unique keys and
	 *	two tuples { 1, 2, 3 } and { 3, 1, 2 }:
	 *
	 *	This REPLACE/DUP_REPLACE is OK: { 1, 5, 5 }
	 *	This REPLACE/DUP_REPLACE is not OK: { 2, 2, 2 } (there
	 *	is no tuple with key '2' in the primary key)
	 *	This REPLACE/DUP_REPLACE is not OK: { 1, 1, 1 } (there
	 *	is a conflicting tuple in the secondary unique key).
	 *
	 *	C. dup_replace_mode is DUP_REPLACE_OR_INSERT. If
	 *	there is a duplicate tuple in the primary key, behaves the
	 *	same way as DUP_REPLACE, otherwise behaves the same way as
	 *	DUP_INSERT.
	 *
	 * 3. UPDATE has to delete the old tuple and insert a new one.
	 *    dup_replace_mode is ignored.
	 *    Note that old_tuple primary key doesn't have to match
	 *    new_tuple primary key, thus a duplicate can be found.
	 *    For this reason, and since there can be duplicates in
	 *    other indexes, UPDATE is the same as DELETE +
	 *    REPLACE/DUP_INSERT.
	 *
	 * @return old_tuple. DELETE, UPDATE and REPLACE/DUP_REPLACE
	 * always produce an old tuple. REPLACE/DUP_INSERT always returns
	 * NULL. REPLACE/DUP_REPLACE_OR_INSERT may or may not find
	 * a duplicate.
	 *
	 * The method is all-or-nothing in all cases. Changes are either
	 * applied to all indexes, or nothing applied at all.
	 *
	 * Note, that even in case of REPLACE, dup_replace_mode only
	 * affects the primary key, for secondary keys it's always
	 * DUP_INSERT.
	 *
	 * The call never removes more than one tuple: if
	 * old_tuple is given, dup_replace_mode is ignored.
	 * Otherwise, it's taken into account only for the
	 * primary key.
	 */
	engine_replace_f replace;
};



struct tuple *
WTSpace::executeReplace(struct txn * txn, struct space * space,
						   struct request * request)
{
#if 0
	struct tuple *new_tuple = tuple_new(space->format, request->tuple,
										request->tuple_end);
	/* GC the new tuple if there is an exception below. */
	TupleRef ref(new_tuple);
	space_validate_tuple(space, new_tuple);
	enum dup_replace_mode mode = dup_replace_mode(request->type);
	this->replace(txn, space, NULL, new_tuple, mode);
	/** The new tuple is referenced by the primary key. */
	return new_tuple;
#endif
	(void) txn;
    int size = request->tuple_end - request->tuple;
    const char *tmp = tuple_field_raw(request->tuple, size, 0);
    uint64_t index_id = mp_decode_uint(&tmp);
    //printf("insert index_id = %lu\n", index_id); // build fail on mac 
	WTIndex *index = (WTIndex*)index_find(space, index_id);
	// 如果 space 定义了 fields, 需要在此检查 request 的 tuple 是否有效
	space_validate_tuple_raw(space, request->tuple);
	tuple_field_count_validate(space->format, request->tuple);

	//int size = request->tuple_end - request->tuple;
	//const char *key =
	//		tuple_field_raw(request->tuple, size,
	//						index->key_def->parts[0].fieldno);
	//primary_key_validate(index->key_def, key, index->key_def->part_count);

	enum dup_replace_mode mode = DUP_REPLACE_OR_INSERT;
	if (request->type == IPROTO_INSERT) {
		//SophiaEngine *engine = (SophiaEngine *)space->handler->engine;
		//if (engine->recovery_complete)
			mode = DUP_INSERT;
	}
	index->replace_or_insert(request->tuple, request->tuple_end, mode);
	return NULL;
}

struct tuple *
WTSpace::executeDelete(struct txn * /* txn */, struct space * /* space */,
						  struct request * /* request */ )
{
#if 0
	/* Try to find the tuple by unique key. */
	Index *pk = index_find_unique(space, request->index_id);
	const char *key = request->key;
	uint32_t part_count = mp_decode_array(&key);
	primary_key_validate(pk->key_def, key, part_count);
	struct tuple *old_tuple = pk->findByKey(key, part_count);
	if (old_tuple == NULL)
		return NULL;

	this->replace(txn, space, old_tuple, NULL, DUP_REPLACE_OR_INSERT);
	return old_tuple;
#endif
	panic("executeDelete, not implemented");
    return NULL;
}

struct tuple *
WTSpace::executeUpdate(struct txn * /* txn */, struct space * /* space */,
						  struct request * /* request */)
{
#if 0
	/* Try to find the tuple by unique key. */
	Index *pk = index_find_unique(space, request->index_id);
	const char *key = request->key;
	uint32_t part_count = mp_decode_array(&key);
	primary_key_validate(pk->key_def, key, part_count);
	struct tuple *old_tuple = pk->findByKey(key, part_count);

	if (old_tuple == NULL)
		return NULL;

	/* Update the tuple; legacy, request ops are in request->tuple */
	struct tuple *new_tuple = tuple_update(space->format,
										   region_alloc_ex_cb,
										   &fiber()->gc,
										   old_tuple, request->tuple,
										   request->tuple_end,
										   request->index_base);
	TupleRef ref(new_tuple);
	space_validate_tuple(space, new_tuple);
	this->replace(txn, space, old_tuple, new_tuple, DUP_REPLACE);
	return new_tuple;
#endif
	panic("executeUpdate, not implemented");
    return NULL;
}

void
WTSpace::executeUpsert(struct txn * /* txn */ , struct space * /* space */,
						  struct request * /* request */ )
{

#if 0
	Index *pk = index_find_unique(space, request->index_id);

	/* Check field count in tuple */
	space_validate_tuple_raw(space, request->tuple);
	tuple_field_count_validate(space->format, request->tuple);
	uint32_t part_count = pk->key_def->part_count;
	/*
	 * Extract the primary key from tuple.
	 * Allocate enough memory to store the key.
	 */
	uint32_t key_len = request->tuple_end - request->tuple;
	char *key = (char *) region_alloc_xc(&fiber()->gc, key_len);
	key_len = key_parts_create_from_tuple(pk->key_def, request->tuple,
										  key, key_len);

	/* Try to find the tuple by primary key. */
	primary_key_validate(pk->key_def, key, part_count);
	struct tuple *old_tuple = pk->findByKey(key, part_count);

	if (old_tuple == NULL) {
		/**
		 * Old tuple was not found. In a "true"
		 * non-reading-write engine, this is known only
		 * after commit. Thus any error that can happen
		 * at this point is ignored. Emulate this by
		 * suppressing the error. It's logged and ignored.
		 *
		 * What sort of exception can happen here:
		 * - the format of the default tuple is incorrect
		 *   or not acceptable by this space.
		 * - we're out of memory for a new tuple.
		 * - unique key validation failure for the new tuple
		 */
		try {
			struct tuple *new_tuple = tuple_new(space->format,
												request->tuple,
												request->tuple_end);
			TupleRef ref(new_tuple);
			space_validate_tuple(space, new_tuple);
			this->replace(txn, space, NULL,
						  new_tuple, DUP_INSERT);
		} catch (ClientError *e) {
			say_error("UPSERT failed:");
			e->log();
		}
	} else {
		/* Update the tuple. */
		struct tuple *new_tuple =
				tuple_upsert(space->format, region_alloc_ex_cb,
							 &fiber()->gc, old_tuple,
							 request->ops, request->ops_end,
							 request->index_base);
		TupleRef ref(new_tuple);

		/** The rest must remain silent. */
		try {
			space_validate_tuple(space, new_tuple);
			this->replace(txn, space, old_tuple, new_tuple,
						  DUP_REPLACE);
		} catch (ClientError *e) {
			say_error("UPSERT failed:");
			e->log();
		}
	}
	/* Return nothing: UPSERT does not return data. */
#endif
	panic("executeUpsert, not implemented");
}

void
WTSpace::onAlter(Handler *old)
{
	WTSpace *handler = (WTSpace *) old;
	replace = handler->replace;
}

void
WTSpace::executeSelect(struct txn *, struct space * /* space */,
						  uint32_t /* index_id */, uint32_t /* iterator */,
						  uint32_t /* offset */, uint32_t /* limit */,
						  const char * /* key */, const char * /* key_end */,
						  struct port * /* port */)
{
#if 0
	MemtxIndex *index = (MemtxIndex *) index_find(space, index_id);

	ERROR_INJECT_EXCEPTION(ERRINJ_TESTING);

	uint32_t found = 0;
	if (iterator >= iterator_type_MAX)
		tnt_raise(IllegalParams, "Invalid iterator type");
	enum iterator_type type = (enum iterator_type) iterator;

	uint32_t part_count = key ? mp_decode_array(&key) : 0;
	key_validate(index->key_def, type, key, part_count);

	struct iterator *it = index->position();
	index->initIterator(it, type, key, part_count);

	struct tuple *tuple;
	while ((tuple = it->next(it)) != NULL) {
		if (offset > 0) {
			offset--;
			continue;
		}
		if (limit == found++)
			break;
		port_add_tuple(port, tuple);
	}
#endif
	panic("executeSelect, not implemented");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

WiredtigerEngine::WiredtigerEngine() : Engine("wiredtiger")
{
	//flags = ENGINE_CAN_BE_TEMPORARY;  // enable temporary
}

Handler * WiredtigerEngine::open() {
	return new WTSpace(this);
}

Index* WiredtigerEngine::createIndex(struct key_def *key_def) {
    printf("call the create index\n");
    char buf[16];
    uint32_t index_id = key_def->iid;
    uint32_t sid = key_def->space_id;
    char* buf_pos = buf;
    buf_pos = mp_encode_array(buf_pos, 2);
    buf_pos = mp_encode_uint(buf_pos, sid);
	buf_pos = mp_encode_uint(buf_pos, index_id);
    box_tuple_t *tp;
    box_index_get(BOX_INDEX_ID, 0, buf, buf_pos, &tp);
    if (tp){
         const char *value_format = tuple_field_cstr(tp, 6);
         printf("index value_format = %s\n", value_format);
    }
	return new WTIndex(key_def);
}

bool WiredtigerEngine::needToBuildSecondaryKey(struct space *space) {
	(void)space;
	//say_debug("space = %p\n", space);
	//panic("needToBuildSecondaryKey, not implemented");
	return false;
}
