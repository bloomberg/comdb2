/*
   Copyright 2017 Bloomberg Finance L.P.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */

/*
 * Comdb2 sql tool to replay sql logs
 *
 */
#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdarg>

#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include <vector>
#include <map>
#include <set>
#include <list>
#include <map>
#include <algorithm>
#include <unistd.h>

#include "cdb2api.h"
#include "cson_amalgamation_core.h"

static cdb2_hndl_tp *cdb2h = nullptr;
std::map<std::string, std::string> sqltrack;
std::map<std::string, std::list<cson_value*>> transactions;

void replay(cdb2_hndl_tp *db, cson_value *val);

static const char *usage_text = 
    "Usage: cdb2sqlreplay dbname [FILE]\n" \
    "\n" \
    "Basic options:\n" \
    "-f                 run the sql as fast as possible\n";

/* Start of functions */
void usage() {
    std::cout << usage_text;
    exit(EXIT_FAILURE);
}


void add_fingerprint(std::string fingerprint, std::string sql) {
    std::pair<std::string, std::string> v(fingerprint, sql);
    sqltrack.insert(v);
    std::cout << fingerprint << " -> " << sql << std::endl;
}

static const char *get_strprop(cson_value *objval, const char *key) {
    cson_object *obj;
    cson_value_fetch_object(objval, &obj);
    cson_value *propval = cson_object_get(obj, key);
    if (propval == nullptr || !cson_value_is_string(propval))
        return nullptr;
    cson_string *cstr;
    cson_value_fetch_string(propval, &cstr);
    return cson_string_cstr(cstr);
}

static cson_array *get_arrprop(cson_value *objval, const char *key) {
    cson_object *obj;
    cson_value_fetch_object(objval, &obj);
    cson_value *propval = cson_object_get(obj, key);
    if (propval == nullptr || !cson_value_is_array(propval))
        return nullptr;
    return cson_value_get_array(propval);
}

bool get_intprop(cson_value *objval, const char *key, int64_t *val) {
    cson_object *obj;
    cson_value_fetch_object(objval, &obj);
    cson_value *propval = cson_object_get(obj, key);
    if (propval == nullptr || !cson_value_is_integer(propval))
        return false;
    int rc = cson_value_fetch_integer(propval, (cson_int_t*) val);
    return true;
}

bool get_doubleprop(cson_value *objval, const char *key, double *val) {
    cson_object *obj;
    cson_value_fetch_object(objval, &obj);
    cson_value *propval = cson_object_get(obj, key);
    if (propval == nullptr || !cson_value_is_double(propval))
        return false;
    int rc = cson_value_fetch_double(propval, (cson_double_t*) val);
    return true;
}

bool have_json_key(cson_value *objval, const char *key) {
    cson_object *obj;
    cson_value_fetch_object(objval, &obj);
    cson_value *propval = cson_object_get(obj, key);
    return propval != nullptr;
}

bool is_transactional(cson_value *val) {
    return have_json_key(val, "id");
}

bool is_replayable(cson_value *val) {
    int64_t nbound;

    // Have an array of bound parameters?  Should be replayable.
    bool have_bindings = have_json_key(val, "bound_parameters");
    if (have_bindings)
        return true;
    
    // Don't have any bound parameters?  Should be replayable.
    have_bindings = get_intprop(val, "nbindings", &nbound);
    if (!have_bindings || nbound == 0)
        return true;

    return false;
}

bool event_is_txn(cson_value *val) {
    return get_strprop(val, "type") == std::string("txn");
}

bool event_is_sql(cson_value *val) {
    return get_strprop(val, "type") == std::string("sql");
}

void replay_transaction(cdb2_hndl_tp *db, std::list<cson_value*> &list) {
    cson_value *statement;

    std::cout << "replay" << std::endl;

    auto it = list.begin();
    while (it != list.end()) {
        std::cout << "replaying txn" << std::endl;
        if (event_is_sql(*it))
            replay(db, *it);
        
        cson_free_value(*it);
        it = list.erase(it);
    }
}

void add_to_transaction(cdb2_hndl_tp *db, cson_value *val) {
    const char *s = get_strprop(val, "id");
    const char *type = get_strprop(val, "type");

    auto i = transactions.find(s);
    if (i == transactions.end()) {
        std::cout << "new transaction " << s << std::endl;
        std::list<cson_value*> statements;
        statements.push_back(val);
        transactions.insert(std::pair<std::string, std::list<cson_value*>>(s, statements));
    }
    else {
        auto &list = (*i).second;
        std::cout << "add to existing transaction " << list.size() << " (" << event_is_txn(list.front()) <<  ") " << s << std::endl;
        if (list.size() == 1 && event_is_txn(list.front())) {
            /* This is a single statement, and we just saw it's transaction.  We can 
               now replay the whole list. */
            list.push_back(list.front());
            list.pop_front();
            replay_transaction(db, list);
        }
        else {
            list.push_back(val);
            if (event_is_txn(val))
                replay_transaction(db, list);
        }
    }
}

/* TODO: */
bool do_bindings(cdb2_hndl_tp *db, cson_value *event_val) {
    cson_array *bound_parameters = get_arrprop(event_val, "bound_parameters");
    if(bound_parameters == nullptr)
        return true;

    unsigned int len = cson_array_length_get(bound_parameters);
    for (int i = 0; i < len; i++) {
        cson_value *bp = cson_array_get(bound_parameters, i);
        const char *name = get_strprop(bp, "name");
        const char *type = get_strprop(bp, "type");
        int ret;

        if (strcmp(type, "largeint") == 0 || strcmp(type, "int") == 0 || strcmp(type, "smallint") == 0) {
            int64_t *iv = new int64_t;
            bool succ = get_intprop(bp, "value", iv);
            if (!succ) {
                std::cerr << "error getting " << type << " value of bound parameter " << name << std::endl;
                return false;
            }
            if ((ret = cdb2_bind_param(cdb2h, name, CDB2_INTEGER, iv, sizeof(*iv))) != 0) {
                std::cerr << "error binding column " << name << ", ret=" << ret << std::endl;
                return false;
            }
            std::cout << "binding "<< type << " column " << name << " to value " << *iv << std::endl;
        } 
        else if (strcmp(type, "float") == 0 || strcmp(type, "doublefloat") == 0) {
            double *dv = new double;
            bool succ = get_doubleprop(bp, "value", dv);
            if (!succ) {
                std::cerr << "error getting " << type << " value of bound parameter " << name << std::endl;
                return false;
            }
            if ((ret = cdb2_bind_param(cdb2h, name, CDB2_REAL, dv, sizeof(*dv))) != 0) {
                std::cerr << "error binding column " << name << ", ret=" << ret << std::endl;
                return false;
            }
            std::cout << "binding "<< type << " column " << name << " to value " << *dv << std::endl;
        }
        else if (strcmp(type, "char") == 0) {
            const char *strp = get_strprop(bp, "value");
            if (strp == nullptr) {
                std::cerr << "error getting " << type << " value of bound parameter " << name << std::endl;
                return false;
            }
            if ((ret = cdb2_bind_param(cdb2h, name, CDB2_CSTRING, strp, strlen(strp) )) != 0) {
                std::cerr << "error binding column " << name << ", ret=" << ret << std::endl;
                return false;
            }
            std::cout << "binding "<< type << " column " << name << " to value " << strp << std::endl;
        }
        else
            std::cout << "binding unknown "<< type << " column " << name << std::endl;
    }

    return true;
}

enum {
    DEFAULT = 0x0000, /* default output */
    TABS    = 0x0001, /* separate columns by tabs */
    BINARY  = 0x0002,  /* output binary */
    GENSQL  = 0x0004,  /* generate insert statements */
    /* flags */
    STDERR  = 0x1000
};

static void hexdump(FILE *f, void *datap, int len)
{
    u_char *data = (u_char *)datap;
    int i;
    for (i = 0; i < len; i++)
        fprintf(f, "%02x", (unsigned int)data[i]);
}

void dumpstring(FILE *f, char *s, int quotes, int quote_quotes)
{
    if (quotes)
        fprintf(f, "'");
    while (*s) {
        if (*s == '\'' && quote_quotes)
            fprintf(f, "''");
        else
            fprintf(f, "%c", *s);
        s++;
    }
    if (quotes)
        fprintf(f, "'");
}

void printCol(FILE *f, cdb2_hndl_tp *cdb2h, void *val, int col, int printmode)
{
    int string_blobs = 1;
    switch (cdb2_column_type(cdb2h, col)) {
    case CDB2_INTEGER:
        if (printmode == DEFAULT)
            fprintf(f, "%s=%lld", cdb2_column_name(cdb2h, col),
                    *(long long *)val);
        else
            fprintf(f, "%lld", *(long long *)val);
        break;
    case CDB2_REAL:
        if (printmode == DEFAULT)
            fprintf(f, "%s=", cdb2_column_name(cdb2h, col));
        fprintf(f, "%f", *(double *)val);
        break;
    case CDB2_CSTRING:
        if (printmode == DEFAULT) {
            fprintf(f, "%s=", cdb2_column_name(cdb2h, col));
            dumpstring(f, (char *)val, 1, 0);
        } else if (printmode & TABS)
            dumpstring(f, (char *)val, 0, 0);
        else
            dumpstring(f, (char *)val, 1, 1);
        break;
    case CDB2_BLOB:
        if (printmode == DEFAULT)
            fprintf(f, "%s=", cdb2_column_name(cdb2h, col));
        if (string_blobs) {
            char *c = (char*) val;
            int len = cdb2_column_size(cdb2h, col);
            fputc('\'', stdout);
            while (len > 0) {
                if (isprint(*c) || *c == '\n' || *c == '\t') {
                    fputc(*c, stdout);
                } else {
                    fprintf(f, "\\x%02x", (int)*c);
                }
                len--;
                c++;
            }
            fputc('\'', stdout);
        } else {
            if (printmode == BINARY) {
                int rc = write(1, val, cdb2_column_size(cdb2h, col));
                exit(0);
            } else {
                fprintf(f, "x'");
                hexdump(f, val, cdb2_column_size(cdb2h, col));
                fprintf(f, "'");
            }
        }
        break;
    case CDB2_DATETIME: {
        cdb2_client_datetime_t *cdt = (cdb2_client_datetime_t *)val;
        if (printmode == DEFAULT)
            fprintf(f, "%s=", cdb2_column_name(cdb2h, col));
        fprintf(f, "\"%4.4u-%2.2u-%2.2uT%2.2u%2.2u%2.2u.%3.3u %s\"",
                cdt->tm.tm_year + 1900, cdt->tm.tm_mon + 1, cdt->tm.tm_mday,
                cdt->tm.tm_hour, cdt->tm.tm_min, cdt->tm.tm_sec, cdt->msec,
                cdt->tzname);
        break;
    }
    case CDB2_DATETIMEUS: {
        cdb2_client_datetimeus_t *cdt = (cdb2_client_datetimeus_t *)val;
        if (printmode == DEFAULT)
            fprintf(f, "%s=", cdb2_column_name(cdb2h, col));
        fprintf(f, "\"%4.4u-%2.2u-%2.2uT%2.2u%2.2u%2.2u.%6.6u %s\"",
                cdt->tm.tm_year + 1900, cdt->tm.tm_mon + 1, cdt->tm.tm_mday,
                cdt->tm.tm_hour, cdt->tm.tm_min, cdt->tm.tm_sec, cdt->usec,
                cdt->tzname);
        break;
    }
    case CDB2_INTERVALYM: {
        cdb2_client_intv_ym_t *ym = (cdb2_client_intv_ym_t *)val;
        if (printmode == DEFAULT)
            fprintf(f, "%s=", cdb2_column_name(cdb2h, col));
        fprintf(f, "\"%s%u-%u\"", (ym->sign < 0) ? "- " : "", ym->years,
                ym->months);
        break;
    }
    case CDB2_INTERVALDS: {
        cdb2_client_intv_ds_t *ds = (cdb2_client_intv_ds_t *)val;
        if (printmode == DEFAULT)
            fprintf(f, "%s=", cdb2_column_name(cdb2h, col));
        fprintf(f, "\"%s%u %2.2u:%2.2u:%2.2u.%3.3u\"",
                (ds->sign < 0) ? "- " : "", ds->days, ds->hours, ds->mins,
                ds->sec, ds->msec);
        break;
    }
    case CDB2_INTERVALDSUS: {
        cdb2_client_intv_dsus_t *ds = (cdb2_client_intv_dsus_t *)val;
        if (printmode == DEFAULT)
            fprintf(f, "%s=", cdb2_column_name(cdb2h, col));
        fprintf(f, "\"%s%u %2.2u:%2.2u:%2.2u.%6.6u\"",
                (ds->sign < 0) ? "- " : "", ds->days, ds->hours, ds->mins,
                ds->sec, ds->usec);
        break;
    }
    }
}


void replay(cdb2_hndl_tp *db, cson_value *event_val) {
    const char *sql = get_strprop(event_val, "sql");
    if(sql == nullptr) {
	    const char *fp = get_strprop(event_val, "fingerprint");
	    if (fp == nullptr) {
		    std::cerr << "No fingerprint logged?" << std::endl;
		    return;
	    }
	    auto s = sqltrack.find(fp);
	    if (s == sqltrack.end()) {
		    std::cerr << "Unknown fingerprint? " << fp << std::endl;
		    return;
	    }
	    sql = (*s).second.c_str();
    }

    bool ok = do_bindings(db, event_val);
    if (!ok)
        return;

    std::cout << sql << std::endl;
    int rc = cdb2_run_statement(db, sql);
    cdb2_clearbindings(db);

    if (rc != CDB2_OK) {
        std::cerr << "run rc " << rc << ": " << cdb2_errstr(db) << std::endl;
        return;
    }

    /* TODO: have switch to print or not results */
    int ncols = cdb2_numcolumns(db);
    while ((rc = cdb2_next_record(db)) == CDB2_OK) {
        for (int col = 0; col < ncols; col++) {
            void *val = cdb2_column_value(db, col);
            if (val == NULL) {
                fprintf(stdout, "%s=NULL", cdb2_column_name(db, col));
            } else {
                printCol(stdout, db, val, col, DEFAULT);
            }
            if (col != ncols - 1) {
                fprintf(stdout, ", ");
            }
        }
        std::cout << std::endl;
    }
    if (rc != CDB2_OK_DONE) {
        std::cerr << "next rc " << rc << ": " << cdb2_errstr(db) << std::endl;
        return;
    }
}

/* Event handlers.  Notes:
   * Caller already validated that we have a valid object,
     so handlers can access its properties.
   * We silently ignore any types we don't know, so schemas
     can be extended easily.
   * Unexpected types handling events we know should raise
     an error.
   * There's a regretable amount of boilerplate.  Want to
     avoid infrastructure growth syndrome for now to
     eliminate it.
  */

void handle_sql(cdb2_hndl_tp *db, cson_value *event_val) {
    int rc;
    /* We can only replay if we have the full SQL, including parameters.
       That means
       1) event logged a bindings array
       2) event logged a nbindings integer, and it's 0

       If there's non-zero bindings and we don't have them, we can't replay.

       Another complication is transactions - we don't want to play one back
       until we have all the statements collected, and see a commit event.
       Anything with an id logged is part of a transaction - a commit
       event will have the same id.  SQL writes that are not part of
       a transaction get logged after transaction.  SQL in BEGIN/COMMIT
       blocks gets logged before the transaction.

       */ 

    if (!is_replayable(event_val)) {
        cson_free_value(event_val);
        return;
    }

#if 0
    if (is_transactional(event_val)) {
        add_to_transaction(db, event_val);
        return;
    }
#endif
    
    replay(db, event_val);
    cson_free_value(event_val);
}

/* TODO: error messages? */
void handle_newsql(cdb2_hndl_tp *db, cson_value *val) {
    const char *sql = get_strprop(val, "sql");
    const char *fingerprint = get_strprop(val, "fingerprint");

    if (sql == nullptr || fingerprint == nullptr)
        return;

    /* If we already know this fingerprint, ignore it. 
       Checking if SQL matches is useless since diffent sql
       statements can have identical fingerprints (eg: 'select 1'
       and 'select 2'.  Fingerprints are md5 hashes of the parse
       tree.  We can rely on them being unique for different
       statement types, unless the user is malicious and extremely 
       clever, in which case we punish them with bad logging.
     */
    if (sqltrack.find(fingerprint) != sqltrack.end())
        return;

    add_fingerprint(std::string(fingerprint), std::string(sql));
}


void handle_txn(cdb2_hndl_tp *db, cson_value *val) {
    add_to_transaction(db, val);
}

typedef void (*event_handler)(cdb2_hndl_tp *db, cson_value *val);
std::map<std::string, event_handler> handlers;
    
void init_handlers(void) {
    handlers.insert(std::pair<std::string, event_handler>("sql", handle_sql));
    handlers.insert(std::pair<std::string, event_handler>("newsql", handle_newsql));
    handlers.insert(std::pair<std::string, event_handler>("txn", handle_txn));
}

void handle(cdb2_hndl_tp *db, const char *event, cson_value *event_val) {
    auto h = handlers.find(event);
    if (h == handlers.end()) {
        cson_free_value(event_val);
        return;
    }
    else
        h->second(db, event_val);
}

void process_events(cdb2_hndl_tp *db, std::istream &in) {
    std::string line;
    int linenum = 0;

    while (std::getline(in, line)) {
        int rc;
        linenum++;

        cson_value *event_val;
        cson_parse_info pinfo = cson_parse_info_empty_m;

        rc = cson_parse_string(&event_val, line.c_str(), line.length(), &cson_parse_opt_empty, &pinfo);
        if (rc) {
            std::cerr << "Malformed input on line " << linenum << std::endl;
            continue;
        }
        if (!cson_value_is_object(event_val)) {
            std::cerr << "Not an object  on line " << linenum << std::endl;
            continue;
        }
        const char *type = get_strprop(event_val, "type");

        if (type != nullptr)
            handle(cdb2h, type, event_val);
        else
            cson_free_value(event_val);
    }
    std::cout << "got " << linenum  << " lines" << std::endl;
}

int main(int argc, char **argv) {
    char *dbname;
    char *filename = nullptr;

    init_handlers();

    if (argc < 2) {
        usage();
    }
    dbname = argv[1];

    if (argc >= 3)
        filename = argv[2];

    /* TODO: tier should be an option */
    int rc;
    char *conf = getenv("CDB2_CONFIG");
    if (conf) {
        cdb2_set_comdb2db_config(conf);
        rc = cdb2_open(&cdb2h, dbname, "default", 0);
    }
    else { 
        rc = cdb2_open(&cdb2h, dbname, "local", 0);
    }

    if (rc) {
        std::cerr << "cdb2_open() failed: " << cdb2_errstr(cdb2h) << std::endl;
        exit(EXIT_FAILURE);
    }

    if (filename == nullptr) {
        process_events(cdb2h, std::cin);
    }
    else {
        std::ifstream f;
        f.open(filename);
        if (!f.good()) {
            std::cerr << "Can't open " << filename << ": " << strerror(errno) << std::endl;
            return 1;
        }

        process_events(cdb2h, f);
    }

    // cdb2_close(cdb2h);
    return 0;
}
