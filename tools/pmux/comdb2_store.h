#ifndef COMDB2_STORE_H
#define COMDB2_STORE_H

#include <cdb2api.h>
#include <cstring>
#include "pmux_store.h"
#include "raii.h"

class comdb2_store : public pmux_store
{
  private:
    std::string host;
    // std::string dbname;
    // std::string cluster;
    raii<cdb2_hndl_tp *> db;
    int run_query(const char *sql)
    {
        int rc;
        if ((rc = cdb2_run_statement(db, sql)) != 0)
            return rc;
        while ((rc = cdb2_next_record(db)) == CDB2_OK)
            ;
        return rc == CDB2_OK_DONE ? 0 : -1;
    }

  public:
    comdb2_store(const char *host, const char *dbname, const char *cluster)
        : host{host},
          // dbname{dbname},
          // cluster{cluster},
          db{cdb2_close}
    {
        if (cdb2_open(&db, dbname, cluster, 0) != 0) {
            throw std::runtime_error(cdb2_errstr(db));
        }
    }
    int sav_port(const char *svc, uint16_t port)
    {
        int rc;
        if ((rc = del_port(svc)) != 0) {
            return rc;
        }
        cdb2_clearbindings(db);
        int64_t portval = port;
        rc = cdb2_bind_param(db, "host", CDB2_CSTRING, host.c_str(),
                             host.length());
        rc |= cdb2_bind_param(db, "svc", CDB2_CSTRING, svc, strlen(svc));
        rc |= cdb2_bind_param(db, "port", CDB2_INTEGER, &portval,
                              sizeof(portval));
        if (rc) {
            fprintf(stderr, "bind rc %d %s\n", rc, cdb2_errstr(db));
            return -1;
        }
        return run_query(
            "insert into pmux(host, svc, port) values(@host, @svc, @port)");
    }
    int del_port(const char *svc)
    {
        cdb2_clearbindings(db);
        int rc;
        rc = cdb2_bind_param(db, "host", CDB2_CSTRING, host.c_str(),
                             host.length());
        rc |= cdb2_bind_param(db, "svc", CDB2_CSTRING, svc, strlen(svc));
        if (rc) {
            fprintf(stderr, "bind rc %d %s\n", rc, cdb2_errstr(db));
            return -1;
        }
        return run_query("delete from pmux where host=@host and svc=@svc");
    }
    std::map<std::string, int> get_ports()
    {
        int rc;
        cdb2_clearbindings(db);
        const char *sql = "select svc, port from pmux where host=@host";
        if ((rc = cdb2_bind_param(db, "host", CDB2_CSTRING, host.c_str(),
                                  host.length())) != 0) {
            throw std::runtime_error(cdb2_errstr(db));
        }
        if ((rc = cdb2_run_statement(db, sql)) != 0) {
            throw std::runtime_error(cdb2_errstr(db));
        }
        std::map<std::string, int> ret;
        while ((rc = cdb2_next_record(db)) == CDB2_OK) {
            char *svc = (char *)cdb2_column_value(db, 0);
            int port = *(int64_t *)cdb2_column_value(db, 1);
            ret[svc] = port;
        }
        if (rc != CDB2_OK_DONE) {
            throw std::runtime_error(cdb2_errstr(db));
        }
        return ret;
    }
};

#endif
