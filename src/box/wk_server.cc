/**
 * @file: wk_server.cpp
 *
 * @author: wangjicheng
 * @mail: 602860321@qq.com
 * @date: 2015/11/4
**/

#include "wk_server.h"

namespace wukong {
    inline void error_log(const char *str, int ret_value) {
        fprintf(stderr, "%s : %s\n", str, wiredtiger_strerror(ret_value));
    }

    WKServer::WKServer() {
        m_conn = NULL;
    }

    WKServer::~WKServer() {
        disconnect_db();
    }

    bool WKServer::connect_db() {
        int ret;
        if (getenv("WIREDTIGER_HOME") == NULL) {
            printf("need export WIREDTIGER_HOME\n");
            m_home = "WT_HOME";
            ret = system("rm -rf WT_HOME && mkdir WT_HOME");
        } else {
            m_home = NULL;
        }
        if ((ret = wiredtiger_open(m_home, NULL, "create", &m_conn)) != 0) {
            fprintf(stderr, "Error open the connect :%s\n", wiredtiger_strerror(ret));
            return false;
        }
        m_wt_api = m_conn->get_extension_api(m_conn);
        printf("connect ok\n");
        return true;
    }
    
    void WKServer::disconnect_db() {
        if (m_conn != NULL) {
            m_conn->close(m_conn, NULL);
            m_conn = NULL;
        }
    }

    bool WKServer::create_table(const char *table_name, const char *table_config) {
        int ret = 0;
        WT_SESSION *session;
        if ((ret = m_conn->open_session(m_conn, NULL, NULL, &session)) != 0) {
            error_log("create_table", ret);
            return false;
        }
        if ((ret = session->create(session, table_name, table_config)) != 0) {
            session->close(session, NULL);
            error_log("create_table", ret);
            return false;
        }
        if ((ret = session->close(session, NULL)) != 0) {
            error_log("create_table", ret);
            return false;
        }
        return true;
    }

    bool WKServer::drop_table(const std::string *table_name) {
        int ret = 0;
        WT_SESSION *session;
        if ((ret = m_conn->open_session(m_conn, NULL, NULL, &session)) != 0) {
            error_log("drop_table", ret);
            return false;
        }
        if ((ret = session->drop(session, table_name->c_str(), "force")) != 0) {
            session->close(session, NULL);
            error_log("drop_table", ret);
            return false;
        }
        if ((ret = session->close(session, NULL)) != 0) {
            error_log("drop_table", ret);
            return false;
        }
        return true;
    }

    bool WKServer::put_value(const std::string *table_name, const WT_ITEM &key, const WT_ITEM &value) {
        int ret = 0;
        bool status = true;
        WT_SESSION *session;
        if ((ret = m_conn->open_session(m_conn, NULL, NULL, &session)) != 0) {
            error_log("drop_table", ret);
            return false;
        }
        WT_CURSOR *cursor;
        if ((ret = session->open_cursor(session, table_name->c_str(), NULL, "raw, overwrite = true", &cursor)) != 0) {
            session->close(session, NULL);
            error_log("drop_table", ret);
            return false;
        }
        cursor->set_key(cursor, &key);
        cursor->set_value(cursor, &value);
        ret = cursor->insert(cursor);
        if (ret != 0) {
            error_log("put_value", ret);
            status = false;
        }
        cursor->close(cursor);
        if ((ret = session->close(session, NULL)) != 0) {
            error_log("put_vlaue", ret);
            return false;
        }
        return status;
    }

    bool WKServer::put_multi_value(const std::string *table_name, const std::vector <std::string> &mul_key,
                                   const std::vector <std::string> &mul_value) {
        int ret = 0;
        bool status = true;
        WT_SESSION *session;
        if ((ret = m_conn->open_session(m_conn, NULL, NULL, &session)) != 0) {
            error_log("put_multi_value", ret);
            return false;
        }
        WT_CURSOR *cursor;
        if ((ret = session->open_cursor(session, table_name->c_str(), NULL, "raw, overwrite = true", &cursor)) != 0) {
            session->close(session, NULL);
            error_log("put_multi_value", ret);
            return false;
        }
        WT_ITEM item;
        for (size_t i = 0; i < mul_key.size(); i++) {
            item.size = mul_key[i].size();
            item.data = mul_key[i].c_str();
            cursor->set_key(cursor, &item);
            item.size = mul_value[i].size();
            item.data = mul_value[i].c_str();
            cursor->set_value(cursor, &item);
            ret = cursor->insert(cursor);
            if (ret != 0) {
                printf("insert data error\n");
            }
        }
        cursor->close(cursor);
        ret = session->open_cursor(session, table_name->c_str(), NULL, NULL, &cursor);
        cursor->reset(cursor);
        while(cursor->next(cursor) == 0){
            uint64_t key = 0;
            cursor->get_key(cursor, &key);
        }
        if ((ret = session->close(session, NULL)) != 0) {
            error_log("put_multi_value", ret);
            return false;
        }
        return status;
    }

    bool WKServer::get_value(const std::string *table_name, const WT_ITEM &key, std::string &value) {
        int ret = 0;
        bool status = true;
        WT_SESSION *session;
        if ((ret = m_conn->open_session(m_conn, NULL, NULL, &session)) != 0) {
            fprintf(stderr, "get value :%s\n", wiredtiger_strerror(ret));
            return false;
        }
        WT_CURSOR *cursor;
        if ((ret = session->open_cursor(session, table_name->c_str(), NULL, "raw", &cursor)) != 0) {
            fprintf(stderr, "get value :%s\n", wiredtiger_strerror(ret));
            session->close(session, NULL);
            return false;
        }

        WT_ITEM item;
        cursor->set_key(cursor, &key);
        ret = cursor->search(cursor);
        if (0 == ret) {
            ret = cursor->get_value(cursor, &item);
            if (0 == ret) {
                value = std::string((const char *) item.data, item.size);
                ret = cursor->reset(cursor);
            }
        }
        else if (ret == WT_NOTFOUND) {
            fprintf(stderr, "get value :%s\n", wiredtiger_strerror(ret));
            status = false;
        }
        cursor->close(cursor);
        if ((ret = session->close(session, NULL)) != 0) {
            fprintf(stderr, "get value :%s\n", wiredtiger_strerror(ret));
            return false;
        }
        return status;
    }

    bool WKServer::get_multi_value(const std::string *table_name, const std::vector <std::string> &key,
                                   std::vector <std::string> &value) {
        int ret = 0;
        bool status = true;
        WT_SESSION *session;
        if ((ret = m_conn->open_session(m_conn, NULL, NULL, &session)) != 0) {
            fprintf(stderr, "get multi value :%s\n", wiredtiger_strerror(ret));
            return false;
        }
        WT_CURSOR *cursor;
        if ((ret = session->open_cursor(session, table_name->c_str(), NULL, "raw", &cursor)) != 0) {
            fprintf(stderr, "get multi value :%s\n", wiredtiger_strerror(ret));
            session->close(session, NULL);
            return false;
        }

        WT_ITEM item;
        std::string tempStrValue;
        for (size_t i = 0; i < key.size(); i++) {
            item.data = key[i].c_str();
            item.size = key[i].size();
            cursor->set_key(cursor, &item);
            ret = cursor->search(cursor);
            if (0 == ret) {
                ret = cursor->get_value(cursor, &item);
                if (0 == ret) {
                    tempStrValue.assign((const char *) item.data, item.size);
                    value.push_back(tempStrValue);
                    ret = cursor->reset(cursor);
                }
            }
            else if (WT_NOTFOUND == ret) {
                fprintf(stderr, "get multi value :%s\n", wiredtiger_strerror(ret));
            }
        }

        cursor->close(cursor);
        if ((ret = session->close(session, NULL)) != 0) {
            fprintf(stderr, "get multi value :%s\n", wiredtiger_strerror(ret));
            return false;
        }
        return status;
    }

    bool WKServer::update_value(const std::string *table_name, const WT_ITEM &key, const WT_ITEM &value) {
        int ret = 0;
        bool status = true;
        WT_SESSION *session;
        if ((ret = m_conn->open_session(m_conn, NULL, NULL, &session)) != 0) {
            fprintf(stderr, "update value :%s\n", wiredtiger_strerror(ret));
            return false;
        }
        WT_CURSOR *cursor;
        if ((ret = session->open_cursor(session, table_name->c_str(), NULL, "raw", &cursor)) != 0) {
            fprintf(stderr, "update value :%s\n", wiredtiger_strerror(ret));
            session->close(session, NULL);
            return false;
        }
        cursor->set_key(cursor, &key);
        cursor->set_value(cursor, &value);
        ret = cursor->update(cursor);
        if (ret != 0) {
            fprintf(stderr, "update value :%s\n", wiredtiger_strerror(ret));
            status = false;
        }
        cursor->close(cursor);
        if ((ret = session->close(session, NULL)) != 0) {
            fprintf(stderr, "update value :%s\n", wiredtiger_strerror(ret));
            return false;
        }
        return status;
    }

    bool WKServer::remove_value(const std::string *table_name, const WT_ITEM &key) {
        int ret = 0;
        int status = true;
        WT_SESSION *session;
        if ((ret = m_conn->open_session(m_conn, NULL, NULL, &session)) != 0) {
            fprintf(stderr, "remove value :%s\n", wiredtiger_strerror(ret));
            return false;
        }
        WT_CURSOR *cursor;
        if ((ret = session->open_cursor(session, table_name->c_str(), NULL, "raw, overwrite = true", &cursor)) != 0) {
            fprintf(stderr, "remove value :%s\n", wiredtiger_strerror(ret));
            session->close(session, NULL);
            return false;
        }
        cursor->set_key(cursor, &key);
        ret = cursor->remove(cursor);
        if (ret != 0) {
            fprintf(stderr, "remove value :%s\n", wiredtiger_strerror(ret));
            return false;
        }
        cursor->close(cursor);
        if ((ret = session->close(session, NULL)) != 0) {
            fprintf(stderr, "remove value :%s\n", wiredtiger_strerror(ret));
            return false;
        }
        return status;
    }
}
