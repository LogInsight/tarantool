/**
 * @file: wt_engine.h
 * @author: wangjicheng
 * @mail: 602860321@qq.com
 * @date: 2015/11/20
 **/

#ifndef TARANTOOL_BOX_WIREDTIGER_ENGINE_H
#define TARANTOOL_BOX_WIREDTIGER_ENGINE_H

#include "engine.h"

class WiredtigerEngine : public Engine {
public:
	WiredtigerEngine();
	virtual Handler *open();
	virtual Index *createIndex(struct key_def *key_def);
	virtual bool needToBuildSecondaryKey(struct space *space);
};

#endif
