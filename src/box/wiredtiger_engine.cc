/**
 * @file: wiredtiger_engine.cc
 * @author: wangjicheng
 * @mail: 602860321@qq.com
 * @date: 2015/11/20
 **/

#include "wiredtiger_engine.h"

WiredtigerEngine::WiredtigerEngine() : Engine("wiredtiger") { }

Handler *WiredtigerEngine::open() {
	say_debug("open the wiredtiger");
	panic("not implemented");
	return NULL;
}

Index* WiredtigerEngine::createIndex(struct key_def *key_def) {
	say_debug("key_def = %p\n", key_def);
	return NULL;
}

bool WiredtigerEngine::needToBuildSecondaryKey(struct space *space) {
	say_debug("space = %p\n", space);
    return false;
}