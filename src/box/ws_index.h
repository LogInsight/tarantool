/**
 * @file: wumpus_index.h
 * @author: wangjia
 * @mail: 89946525@qq.com
 * @date: 2015/11/20 04:31:44
 **/

#ifndef TARANTOOL_BOX_WUMPUS_INDEX_H_INCLUDED
#define TARANTOOL_BOX_WUMPUS_INDEX_H_INCLUDED

#include "index.h"

#include <string>

#include "index/index.h"
#include "query/query.h"
#include "tuple.h"


class WsIndex : public Index
{
public:
	WsIndex(struct key_def *key_def) : Index(key_def), m_ws_index(nullptr) {

	}

	int init();

	~WsIndex() {
		if (m_ws_index) {
			delete m_ws_index;
			m_ws_index = nullptr;
		}
	};

	virtual struct tuple *findByKey(const char *key, uint32_t part_count) const;

	virtual struct tuple *findByTuple(struct tuple *tuple) const;
	virtual struct tuple *replace(struct tuple *old_tuple,
	                              struct tuple *new_tuple,
	                              enum dup_replace_mode mode);
	virtual size_t bsize() const;

	/**
	 * Create a structure to represent an iterator. Must be
	 * initialized separately.
	 */
	virtual struct iterator *allocIterator() const;
	virtual void initIterator(struct iterator *iterator,
	                          enum iterator_type type,
	                          const char *key, uint32_t part_count) const;

	void insert(const char *tuple, const char *tuple_end,
		            dup_replace_mode mode);


	inline struct iterator *position() const {
		if (m_position == NULL)
			m_position = allocIterator();
		return m_position;
	}

	void get_result(const std::string &query, std::string &result);

protected:
	mutable struct iterator *m_position;

private:
	std::string m_cur_dir;
	ws::Index* m_ws_index;
};


class WsIndexIterator {
public:

	WsIndexIterator() :
			m_ws_index(nullptr),
			m_doc(nullptr),
			m_list(nullptr) {

	}

	~WsIndexIterator() {

	}

	tuple* next() {
		return nullptr;
	}

private:
	const ws::Index* m_ws_index;
	ws::ExtentList* m_doc;
	ws::ExtentList* m_list;

};

#endif

