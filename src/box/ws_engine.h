/**
 * @file: wumpus_engine.h
 * @author: wangjia
 * @mail: 89946525@qq.com
 * @date: 2015/11/19 22:27:12
 **/

#ifndef TARANTOOL_BOX_WUMPUS_ENGINE_H_INCLUDED
#define TARANTOOL_BOX_WUMPUS_ENGINE_H_INCLUDED

#include "engine.h"
#include "txn.h"
#include "request.h"

class WsEngine : public Engine {
public:
	WsEngine();
	virtual Handler *open();
	virtual Index *createIndex(struct key_def *key_def);
	virtual bool needToBuildSecondaryKey(struct space *space);
};

struct WumpusSapce : public Handler {
    WumpusSapce(Engine *e)
            : Handler(e) {

    }

	virtual struct tuple *
			executeReplace(struct txn *, struct space *,
			               struct request *);
	virtual void
	        executeUpsert(struct txn *, struct space *,
	                      struct request *);

	virtual void executeSelect(txn *structtxn, space *space,
	                           uint32_t index_id, uint32_t iterator,
	                           uint32_t offset, uint32_t limit, const char *key,
	                           const char *key_end, port *structport) override;


};

#endif

