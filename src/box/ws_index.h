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


class WsIndex : public Index
{
public:
	WsIndex(struct key_def *key_def) : Index(key_def) {
	}

	int init();

	~WsIndex() {};

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


	inline struct iterator *position() const
	{
		if (m_position == NULL)
			m_position = allocIterator();
		return m_position;
	}

	void get_result(const std::string query, std::string& result) {
		ws::Query *q = new ws::Query(m_ws_index, query.c_str(), getuid());
		q->parse();
		char responseLine[1024];
		int statusCode;
		while (q->getNextLine(responseLine))
			printf("%s\n", responseLine);
		q->getStatus(&statusCode, responseLine);
		printf("@%d-%s\n", statusCode, responseLine);
		delete q;
		result += "123, 321";
	}
protected:
	mutable struct iterator *m_position;

private:
	ws::Index* m_ws_index;
};

#endif

