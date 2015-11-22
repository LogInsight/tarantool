/**
 * @file: wk_server.h
 *
 * @author: wangjicheng
 * @mail: 602860321@qq.com
 * @date: 2015/11/4
**/

#ifndef TARANTOOL_BOX_WK_SERVER_H_
#define TARANTOOL_BOX_WK_SERVER_H_

#include <string>
#include <assert.h>
#include <vector>
#include <sstream>
#include <iostream>

#include "wiredtiger_ext.h"
#include "wiredtiger.h"

namespace wukong {

    struct Options  // the wiredtiger open Options.
    {
        std::string key_format;
        std::string value_format;
        std::vector <std::string> columns;
    };

    class OpContext {
    public:
        OpContext(WT_CONNECTION *conn) {
            int ret = conn->open_session(conn, NULL, NULL, &m_session);
            assert(ret == 0);
        }

        ~OpContext() {
            int ret = Close();
            assert(ret == 0);
        }

        int Close() {
            int ret = 0;
            if (m_session != NULL)
                ret = m_session->close(m_session, NULL);
            m_session = NULL;
            return (ret);
        }

        WT_CURSOR *GetCursor() { return m_cursor; }

        void SetCursor(WT_CURSOR *c) { m_cursor = c; }

        WT_SESSION *GetSession() { return m_session; }

    private:
        WT_SESSION *m_session;
        WT_CURSOR *m_cursor;
    };

    class WKServer {
    public:
        WKServer();

        ~WKServer();

        bool connect_db();

        void disconnect_db();

        bool create_table(const char *table_name, const char *table_config);

        bool drop_table(const std::string *table_name);

        bool put_value(const std::string *table_name, const WT_ITEM &key, const WT_ITEM &value);

        bool put_multi_value(const std::string *table_name, const std::vector <std::string> &mul_key,
                             const std::vector <std::string> &mul_value);

        bool get_value(const std::string *table_name, const WT_ITEM &key, std::string &value);

        bool get_multi_value(const std::string *table_name, const std::vector <std::string> &key,
                             std::vector <std::string> &value);

        bool update_value(const std::string *table_name, const WT_ITEM &key, const WT_ITEM &value);

        bool remove_value(const std::string *table_name, const WT_ITEM &key);

        WT_CONNECTION *get_connection() { return m_conn; };

        WT_EXTENSION_API *get_wt_api() { return m_wt_api; };
    private:
        WT_CONNECTION *m_conn;
        OpContext *m_context;
        int m_status;
        const char *m_home;
        WT_EXTENSION_API *m_wt_api;
    };
}
#endif
