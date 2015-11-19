/**
 * @file: wumpus_engine.cc
 * @author: wangjia
 * @mail: 89946525@qq.com
 * @date: 2015/11/19 22:27:24
 **/

#include "wumpus_engine.h"

WumpusEngine::WumpusEngine() : Engine("wumpus") { }

Handler *WumpusEngine::open() {
    say_debug("WumpusEngin::open is called");
    panic("not implemented");
    return NULL;
}

Index* WumpusEngine::createIndex(struct key_def *key_def) {
    say_debug("createIndex is called, name=%s", key_def->name);
    panic("not implemented");
    return NULL;
}

bool WumpusEngine::needToBuildSecondaryKey(struct space *space) {
    say_debug("space=[%p]", space);
    return false;
}
