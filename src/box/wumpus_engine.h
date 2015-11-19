/**
 * @file: wumpus_engine.h
 * @author: wangjia
 * @mail: 89946525@qq.com
 * @date: 2015/11/19 22:27:12
 **/

#ifndef TARANTOOL_BOX_WUMPUS_ENGINE_H_INCLUDED
#define TARANTOOL_BOX_WUMPUS_ENGINE_H_INCLUDED

#include "engine.h"

class WumpusEngine : public Engine {
public:
    WumpusEngine();
    virtual Handler *open();
    virtual Index *createIndex(struct key_def *key_def);
    virtual bool needToBuildSecondaryKey(struct space *space);
};


#endif

