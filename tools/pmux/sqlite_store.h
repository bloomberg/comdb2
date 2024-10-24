#ifndef SQLITE_STORE_H
#define SQLITE_STORE_H

#include <sqlite3.h>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <sstream>
#include <stdexcept>
#include <string>
#include <iostream>
#include "pmux_store.h"
#include "raii.h"

#include "str_util.h" /* QUOTE */

class sqlite_store : public pmux_store
{
  private:
    raii<sqlite3 *> db;
    raii<sqlite3_stmt *> sav_stmt;
    raii<sqlite3_stmt *> del_stmt;
    void prepare_stmts()
    {
        const char *sql =
            "insert or replace into pmux(svc, port) values(@svc, @port);";
        if (sqlite3_prepare(db, sql, -1, &sav_stmt, NULL) != SQLITE_OK) {
            throw std::runtime_error(sqlite3_errmsg(db));
        }

        sql = "delete from pmux where svc=@svc";
        if (sqlite3_prepare(db, sql, -1, &del_stmt, NULL) != SQLITE_OK) {
            throw std::runtime_error(sqlite3_errmsg(db));
        }
    }
    void create_table()
    {
        char *err;
        const char *sql;
        sql = "create table if not exists pmux(svc text, port int)";
        if (sqlite3_exec(db, sql, NULL, NULL, &err) != SQLITE_OK) {
            throw std::runtime_error(err);
        }
        sql = "create unique index if not exists uniq on pmux(svc, port)";
        if (sqlite3_exec(db, sql, NULL, NULL, &err) != SQLITE_OK) {
            throw std::runtime_error(err);
        }
    }

  public:
    sqlite_store()
        : db{sqlite3_close}, sav_stmt{sqlite3_finalize},
          del_stmt{sqlite3_finalize}
    {
        std::stringstream ss;
        const char *storefile = getenv("COMDB2_PMUX_FILE");
        std::string path;

        if(storefile) 
            path = storefile;
        else {
            const char *root = getenv("COMDB2_ROOT");
            ss << (root ? root : QUOTE(COMDB2_ROOT)) 
               << "/" << "/var/lib/cdb2/pmux.sqlite";
            path = ss.str();
        }
#ifdef VERBOSE
        puts(path.c_str());
#endif
        if (sqlite3_open(path.c_str(), &db) != 0) {
            std::string err(sqlite3_errmsg(db));
            err += " " + path;
            throw std::runtime_error(err);
        }
        create_table();
        prepare_stmts();
    }
    int sav_port(const char *svc, uint16_t port)
    {
        sqlite3_reset(sav_stmt);
        int rc = sqlite3_bind_text(sav_stmt, 1, svc, -1, NULL);
        rc |= sqlite3_bind_int(sav_stmt, 2, port);
        if (rc) {
            std::cerr << __func__ << " failed " << sqlite3_errmsg(db)
                      << std::endl;
            return -1;
        }
        if (sqlite3_step(sav_stmt) != SQLITE_DONE) {
            std::cerr << __func__ << " failed " << sqlite3_errmsg(db)
                      << std::endl;
            return -1;
        }
        return 0;
    }
    int del_port(const char *svc)
    {
        sqlite3_reset(del_stmt);
        int rc = sqlite3_bind_text(del_stmt, 1, svc, -1, NULL);
        if (rc != SQLITE_OK) {
            std::cerr << __func__ << " failed " << sqlite3_errmsg(db)
                      << std::endl;
            return -1;
        }
        if (sqlite3_step(del_stmt) != SQLITE_DONE) {
            std::cerr << __func__ << " failed " << sqlite3_errmsg(db)
                      << std::endl;
            return -1;
        }
        return 0;
    }
    std::map<std::string, int> get_ports()
    {
        raii<sqlite3_stmt *> stmt(sqlite3_finalize);
        const char *sql = "select svc, port from pmux";
        if (sqlite3_prepare(db, sql, -1, &stmt, NULL) != SQLITE_OK) {
            throw std::runtime_error(sqlite3_errmsg(db));
        }
        int rc;
        std::map<std::string, int> ret;
        while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
            char *svc = (char *)sqlite3_column_text(stmt, 0);
            int port = sqlite3_column_int(stmt, 1);
            ret[svc] = port;
        }
        if (rc != SQLITE_DONE) {
            throw std::runtime_error(sqlite3_errmsg(db));
        }
        return ret;
    }
};

#endif
