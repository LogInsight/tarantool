/**
 * @file: wumpus_index.cc
 * @author: wangjia
 * @mail: 89946525@qq.com
 * @date: 2015/11/20 04:31:04
 **/


#include "ws_index.h"
#include "tuple.h"

#include "stdio.h"
#include <string>
#include <sstream>

#include "misc/time_util.h"

struct tuple*WsIndex::findByKey(const char *key,
                                uint32_t part_count) const {
	say_info("%s, %u", key, part_count);
	return NULL;
}

struct tuple *WsIndex::findByTuple(struct tuple *tuple) const {
	return Index::findByTuple(tuple);
}

struct tuple *WsIndex::replace(struct tuple *old_tuple,
                               struct tuple *new_tuple,
                               enum dup_replace_mode mode) {
	say_info("old_tuple=[%p], new_tuple=[%p], mode=[%d]",
	         old_tuple, new_tuple, mode);
	return nullptr;
}

size_t WsIndex::bsize() const {
	return Index::bsize();
}

void WsIndex::initIterator(struct iterator *iterator,
                           enum iterator_type type, const char *key,
                           uint32_t part_count) const {
	say_info("iterator=[%p], type=[%d], key=[%s], part_count=[%d]",
	         iterator, type, key, part_count);

	if (part_count > 0) {
		if (part_count != key_def->part_count) {
			tnt_raise(ClientError, ER_UNSUPPORTED,
			          "ws Index iterator", "uncomplete keys");
		}
	} else {
		key = NULL;
	}



}

struct iterator *WsIndex::allocIterator() const {
	return nullptr;
}

void WsIndex::insert(const char *tuple, const char *tuple_end,
                     dup_replace_mode mode) {
	(void) tuple;
	(void) tuple_end;
	(void) mode;

	uint32_t size = tuple_end - tuple;

	const char * recv_data = tuple_field_raw(tuple, size, 0);
	uint64_t offset = mp_decode_uint(&recv_data);

	uint32_t len = 0;
	const char * str = mp_decode_str(&recv_data, &len);

	(void) str;
	(void) offset;

	ws::TimeUtil *timer = ws::TimeUtil::getInstance();
	timer->time_start("addDoc");
	m_ws_index->addDoc(offset, str, len);
	timer->time_end("addDoc");

}

int WsIndex::init() {
	int argc = 2;

	std::string confstr("--config=");
	confstr += "ws.cfg";
	printf("init_search_instance is called\n");
	char** conf = (char **) malloc(sizeof(char*) * 2);
	conf[0] = (char *) malloc(256);
	conf[1] = (char *) malloc(256);
	memcpy(conf[0], confstr.c_str(), confstr.size());
	memcpy(conf[1], confstr.c_str(), confstr.size());

	ws::initializeConfiguratorFromCommandLineParameters(argc, (const char **) conf);

	free(conf[0]);
	free(conf[1]);
	free(conf);

	char workDir[256];
	if (!ws::getConfigurationValue("DIRECTORY", workDir)) {
		printf("FATAL: No directory specified. Check give directory as command-line parameter.\n\n");
		return -2;
	}

	m_ws_index = new ws::Index(workDir, false);
	printf("workdir=%s\n", workDir);

	return false;
}

void WsIndex::get_result(const std::string &query, std::string &result) {

	ws::ExtentList *list = m_ws_index->getPostings(query.c_str(), getuid());
	ws::ExtentList *doc = m_ws_index->getPostings(ws::START_DOC, getuid());
	say_info("list=[%p], size=[%lu], term=[%s], doc_size=[%lu]",
	         list, list->getTotalSize(), query.c_str(), doc->getTotalSize());

	result += "abc";
}
