/**
 * @file: wumpus_engine.cc
 * @author: wangjia
 * @mail: 89946525@qq.com
 * @date: 2015/11/19 22:27:24
 **/

#include <string>
#include "ws_engine.h"

#include "ws_index.h"
#include "tuple.h"
#include "iproto_constants.h"
#include "port.h"

WsEngine::WsEngine() : Engine("ws") { }

Handler *WsEngine::open() {
	say_error("WumpusEngin::open is called");
	return new WumpusSapce(this);
}

Index* WsEngine::createIndex(struct key_def *key_def) {
	WsIndex* index = new WsIndex(key_def);
	if (index->init() != 0) {
		say_error("craete WsIndex erroe");
		return NULL;
	}
	say_info("createIndex is called, name=[%s], index=[%p]",
	         key_def->name, index);
	return index;
}

bool WsEngine::needToBuildSecondaryKey(struct space *space) {
	say_info("space=[%p]", space);
	return false;
}

void WumpusSapce::executeUpsert(struct txn *txn, space *structspace,
	                            request *structrequest) {
	say_info("WumpusSapce::executeUpsert(txn=%p, structspace=%p, request=%p)",
	          txn, structspace, structrequest);
	panic("WumpusSapce::executeUpsert");
}


struct tuple *WumpusSapce::executeReplace(txn *txn, space *space,
                                          request *request) {

	(void) txn;
	(void) space;
	(void) request;

	int size = request->tuple_end - request->tuple;
	const char *key =
			tuple_field_raw(request->tuple, size, 0);
	WsIndex * index = (WsIndex *)index_find(space, 0);
	say_info("key=[%s], index=[%p], pkey=[%p], ptuple=[%p]",
	         key, index, key, request->tuple);

	enum dup_replace_mode mode = DUP_REPLACE_OR_INSERT;
	if (request->type == IPROTO_INSERT) {
		mode = DUP_INSERT;
	}

	index->insert(request->tuple, request->tuple_end, mode);

	return nullptr;

}

void WumpusSapce::executeSelect(struct txn *txn, space *space,
                                uint32_t index_id, uint32_t iterator,
                                uint32_t offset, uint32_t limit,
                                const char *key, const char *key_end,
                                struct port *port) {
	say_info("WumpusSapce::executeSelect(txn=%p, structspace=%p, index_id=%u,"
			          "iterator=%u, offset=%u, limit=%u, ",
	         txn, space, index_id, iterator, offset,
	         limit);

	(void) txn;
	(void) space;
	(void) index_id;
	(void) iterator;
	(void) offset;
	(void) limit;
	(void) key;
	(void) key_end;
	(void) port;

	WsIndex *index = (WsIndex *) index_find(space, index_id);
	if (index == NULL) {
		tnt_raise(IllegalParams, "can't find index, index_id=[%u]", index_id);
	}

	if (iterator >= iterator_type_MAX) {
		tnt_raise(IllegalParams, "Invalid iterator type");
	}

	uint32_t part_count = key ? mp_decode_array(&key) : 0;
#if 0
	struct iterator *it = index->position();
	enum iterator_type type = (enum iterator_type) iterator;
	index->initIterator(it, type, key, part_count);
	struct tuple *tuple = NULL;


	if ((tuple = it->next(it)) != NULL) {
		say_info("port_add_tuple");
		port_add_tuple(port, tuple);
	} else {
		say_info("port_add_tuple");
	}
#endif

	if (part_count == 0) {
		return;
	}

	uint32_t len;
	const char* query = mp_decode_str(&key, &len);

	std::string q(query, len);

	std::string result;

	index->get_result(q, result);
	say_info("q=[%s], result=[%s]", q.c_str(), result.c_str());

}


