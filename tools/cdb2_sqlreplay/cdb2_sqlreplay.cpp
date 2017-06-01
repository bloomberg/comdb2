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

#include "cdb2api.h"
#include "cson_amalgamation_core.h"

static cdb2_hndl_tp *cdb2h = nullptr;
std::map<std::string, std::string> sqltrack;
std::map<std::string, std::list<cson_value*>> transactions;

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

static int get_intprop(cson_value *objval, const char *key, int64_t *val) {
    cson_object *obj;
    cson_value_fetch_object(objval, &obj);
    cson_value *propval = cson_object_get(obj, key);
    if (propval == nullptr || !cson_value_is_integer(propval))
        return 1;
    cson_string *cstr;
    int rc = cson_value_fetch_integer(propval, (cson_int_t*) val);
    return rc;
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
 void handle_sql(cdb2_hndl_tp *db, cson_value *val) {
    int64_t nparms = -99;
    int rc;
    rc = get_intprop(val, "bindings", &nparms);
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
    if (sqltrack.find(sql) == sqltrack.end())
        return;

    add_fingerprint(std::string(fingerprint), std::string(sql));
}


typedef void (*event_handler)(cdb2_hndl_tp *db, cson_value *val);
std::map<std::string, event_handler> handlers;
    
void init_handlers(void) {
    handlers.insert(std::pair<std::string, event_handler>("sql", handle_sql));
    handlers.insert(std::pair<std::string, event_handler>("newsql", handle_newsql));
}

void handle(cdb2_hndl_tp *db, const char *event, cson_value *val) {
    auto h = handlers.find(event);
    if (h == handlers.end())
        return;
    else
        h->second(db, val);
}

void process_events(cdb2_hndl_tp *db, std::istream &in) {
    std::string line;
    int linenum = 0;

    while (std::getline(in, line)) {
        int rc;
        linenum++;

        cson_value *obj;
        cson_parse_info pinfo = cson_parse_info_empty_m;

        rc = cson_parse_string(&obj, line.c_str(), line.length(), &cson_parse_opt_empty, &pinfo);
        if (rc) {
            std::cerr << "Malformed input on line " << linenum << std::endl;
            continue;
        }
        if (!cson_value_is_object(obj)) {
            std::cerr << "Not an object  on line " << linenum << std::endl;
            continue;
        }
        const char *type = get_strprop(obj, "type");

        if (type != nullptr)
            handle(cdb2h, type, obj);

        // cdb2_run_statement(cdb2h, "");

        cson_free_value(obj);
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
    if (cdb2_open(&cdb2h, dbname, "local", 0)) {
        std::cerr << "cdb2_open() failed: " << cdb2_errstr(cdb2h) << std::endl;
        cdb2_close(cdb2h);
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

    cdb2_close(cdb2h);
    return 0;
}
