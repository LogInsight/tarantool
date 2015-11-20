/**
 * @file: wt_index.h
 * @author: nzinfo
 * @date: 2015/11/20 04:31:44
 **/

#ifndef TARANTOOL_BOX_WT_INDEX_H_INCLUDED
#define TARANTOOL_BOX_WT_INDEX_H_INCLUDED

#include "index.h"

class WTIndex: public Index {

public:
    WTIndex(struct key_def *key_def);
    virtual ~WTIndex() {
        if (m_position != NULL)
            m_position->free(m_position);
    }
    // must override virtual function
    virtual struct tuple *findByKey(const char *key, uint32_t part_count) const;
    virtual struct tuple *replace(struct tuple *old_tuple,
                                  struct tuple *new_tuple,
                                  enum dup_replace_mode mode);
    /**
	 * Create a structure to represent an iterator. Must be
	 * initialized separately.
	 */
    virtual struct iterator *allocIterator() const;
    virtual void initIterator(struct iterator *iterator,
                              enum iterator_type type,
                              const char *key, uint32_t part_count) const;

    // virtual struct tuple *min(const char *key, uint32_t part_count) const;
    //virtual struct tuple *max(const char *key, uint32_t part_count) const;
    //virtual size_t count(enum iterator_type type, const char *key,
    //                     uint32_t part_count) const;

    inline struct iterator *position() const
    {
        if (m_position == NULL)
            m_position = allocIterator();
        return m_position;
    }

    /**
     * Two-phase index creation: begin building, add tuples, finish.
     */
    // virtual void beginBuild();
    /**
     * Optional hint, given to the index, about
     * the total size of the index. If given,
     * is given after beginBuild().
     */
    // virtual void reserve(uint32_t /* size_hint */);
    // virtual void buildNext(struct tuple *tuple);
    // virtual void endBuild();

    void replace_or_insert(const char *tuple,
                           const char *tuple_end,
                           enum dup_replace_mode mode);
protected:
    /*
     * Pre-allocated iterator to speed up the main case of
     * box_process(). Should not be used elsewhere.
     */
    mutable struct iterator *m_position;
};

/** Build this index based on the contents of another index. */
//void index_build(MemtxIndex *index, MemtxIndex *pk);

#endif

