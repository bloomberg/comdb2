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
#include <cstdio>
#include <cstdlib>
#include <cstring>

#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include <vector>
#include <map>
#include <list>
#include <algorithm>
#include <unistd.h>
#include <ctime>
#include <sys/time.h>
#include <cstdint>
#include <cinttypes>
#include <cassert>
#include <limits.h>

#include "cdb2api.h"
#include "cson.h"

static cdb2_hndl_tp *cdb2h = nullptr;
char *dbname;
int had_errors = 0;
std::map<std::string, std::string> sqltrack;
std::map<std::string, std::list<cson_value*>> transactions;

bool diffs = false;
bool verbose = false;
int threshold_percent = 5;

int64_t maxevents = 0;

void replay(cdb2_hndl_tp *db, cson_value *val);

static const char *usage_text =
    "Usage: cdb2sqlreplay [options] dbname FILE [FILE]\n"
    "\n"
    "Basic options:\n"
    "  --diff                 Dump performance/cost diffs\n"
    "  --verbose              Lots of verbose output\n"
    "  --threshold N          Set diff threshold to N% (default 5)\n"
    "  --stopat N             Stop after N events processed\n"
    "\n"
    ;

/* Start of functions */
void usage() {
    std::cout << usage_text;
    exit(EXIT_FAILURE);
}


void add_fingerprint(const std::string &fingerprint, const std::string &sql) {
    std::pair<std::string, std::string> v(fingerprint, sql);
    sqltrack.insert(v);
    if (verbose)
        std::cout << fingerprint << " -> " << sql << std::endl;
}

static bool get_ispropnull(cson_value *objval, const char *key) 
{
    cson_object *obj;
    cson_value_fetch_object(objval, &obj);
    cson_value *propval = cson_object_get(obj, key);
    if (propval == nullptr || cson_value_is_null(propval))
        return true;
    return false;
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
    const cson_value *propval = cson_object_get(obj, key);
    if (propval == nullptr || !cson_value_is_integer(propval))
        return false;
    int rc = cson_value_fetch_integer(propval, (cson_int_t*) val);
    if (rc)
        return false;
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

bool is_replayable(cson_value *val) {
    int64_t nbound;

    if(have_json_key(val, "error"))
        return false;

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

bool event_is_sql(cson_value *val) {
    return get_strprop(val, "type") == std::string("sql");
}

void replay_transaction(cdb2_hndl_tp *db, cson_value *val)
{
    const char *cnonce = get_strprop(val, "cnonce");
    assert(cnonce != nullptr);
    auto jt = transactions.find(cnonce);
    assert(jt != transactions.end());
    auto &list = (*jt).second;
    if (verbose)
        std::cout << "replay transaction " << cnonce << std::endl;

    auto it = list.begin();
    while (it != list.end()) {
        assert(event_is_sql(*it));
        replay(db, *it);
        cson_free_value(*it);
        it = list.erase(it);
    }
    transactions.erase(jt);
    replay(db, val); // finally 'commit' or 'rollback'
}

bool is_part_of_transaction(const char *cnonce)
{
    auto it = transactions.find(cnonce);
    return (it != transactions.end());
}

void add_to_transaction(cson_value *val)
{
    const char *cnonce = get_strprop(val, "cnonce");
    const char *type = get_strprop(val, "type");

    auto it = transactions.find(cnonce);
    if (it == transactions.end()) {
        if (verbose)
            std::cout << "new transaction " << cnonce << std::endl;
        std::list<cson_value*> statements;
        statements.push_back(val);
        transactions.insert(std::pair<std::string, std::list<cson_value*>>(cnonce, statements));
    } else {
        auto &list = (*it).second;
        if (verbose)
            std::cout << "add to existing transaction " << cnonce << " size=" << list.size() << std::endl;
        list.push_back(val);
    }
}

/* out should be appropriately sized */
void fromhex(uint8_t *out, const uint8_t *in, size_t len)
{
    const uint8_t *end = in + len;
    while (in != end) {
        uint8_t i0 = tolower(*(in++));
        uint8_t i1 = tolower(*(in++));
        i0 -= isdigit(i0) ? '0' : ('a' - 0xa);
        i1 -= isdigit(i1) ? '0' : ('a' - 0xa);
        *(out++) = (i0 << 4) | i1;
    }
}


/* TODO: add all types supported */
bool do_bindings(cdb2_hndl_tp *db, cson_value *event_val, 
                 std::vector<uint8_t *> &blobs_vect)
{
    cson_array *bound_parameters = get_arrprop(event_val, "bound_parameters");
    if(bound_parameters == nullptr)
        return true;

    unsigned int arr_len = cson_array_length_get(bound_parameters);
    for (int i = 0; i < arr_len; i++) {
        cson_value *bp = cson_array_get(bound_parameters, i);
        const char *name = get_strprop(bp, "name");
        const char *type = get_strprop(bp, "type");
        int cdb2_type = 0;
        int length = 0;
        void *varaddr = NULL;
        int ret;
        if(get_ispropnull(bp, "value")) {   /* bind null value as type INT for simplicity */
            cdb2_type = CDB2_INTEGER;
            varaddr = NULL;
            length = 0;
            if (verbose)
                std::cout << "binding "<< type << " column " << name << " to NULL " << std::endl;
        }
        else if (strcmp(type, "largeint") == 0 || strcmp(type, "int") == 0 || strcmp(type, "smallint") == 0) {
            int64_t *iv = (int64_t*) malloc(sizeof(int64_t));
            bool succ = get_intprop(bp, "value", iv);
            if (!succ) {
                std::cerr << "Error getting " << type << " value of bound parameter " << name << std::endl;
                return false;
            }
            cdb2_type = CDB2_INTEGER;
            varaddr = iv;
            length = sizeof(*iv);
            if (verbose)
                std::cout << "binding "<< type << " column " << name << " to value " << *iv << std::endl;
        } 
        else if (strcmp(type, "float") == 0 || strcmp(type, "doublefloat") == 0) {
            double *dv = (double*) malloc(sizeof(double));
            bool succ = get_doubleprop(bp, "value", dv);
            if (!succ) {
                std::cerr << "Error getting " << type << " value of bound parameter " << name << std::endl;
                return false;
            }
            cdb2_type = CDB2_REAL;
            varaddr = dv;
            length = sizeof(*dv);
            if (verbose)
                std::cout << "binding "<< type << " column " << name << " to value " << *dv << std::endl;
        }
        else if (strcmp(type, "char") == 0 || strcmp(type, "datetime") == 0 ||
                 strcmp(type, "datetimeus") == 0 || strcmp(type, "interval month") == 0 ||
                 strcmp(type, "interval sec") == 0 || strcmp(type, "interval usec") == 0 
                 ) {
            const char *strp = get_strprop(bp, "value");
            if (strp == nullptr) {
                std::cerr << "Error getting " << type << " value of bound parameter " << name << std::endl;
                return false;
            }
            cdb2_type = CDB2_CSTRING;
            varaddr = strdup(strp);
            length = strlen(strp);
            if (verbose)
                std::cout << "binding "<< type << " column " << name << " to value " << strp << std::endl;
        }
        else if( strcmp(type, "byte") == 0 || strcmp(type, "blob") == 0) {
            const char *strp = get_strprop(bp, "value");
            if (strp == nullptr) {
                std::cerr << "Error getting " << type << " value of bound parameter " << name << std::endl;
                return false;
            }
            assert(strp[0] == 'x' && strp[1] == '\'' && "Blob field needs to be in x'123abc' format");

            int slen = strlen(strp) - 3; /* without the x'' */
            int unexlen = slen/2;
            uint8_t *unexpanded = (uint8_t *) malloc(unexlen + 1);
            assert(unexpanded != NULL);

            fromhex(unexpanded, (const uint8_t *) strp + 2, slen); /* no x' */
            unexpanded[unexlen] = '\0';

            cdb2_type = CDB2_BLOB;
            varaddr = unexpanded;
            length = unexlen;

            if (verbose)
                std::cout << "binding "<< type << " column " << name << " to value " << strp << std::endl;
        }
        else {
            std::cerr << "error binding unknown "<< type << " column " << name << std::endl;
            return false;
        }

        if (name[0] == '?') {
            int idx = atoi(name + 1);
            if ((ret = cdb2_bind_index(cdb2h, idx, cdb2_type, varaddr, length)) != 0) {
                std::cerr << "Error from cdb2_bind_index() column " << name << ", ret=" << ret << std::endl;
                return false;
            }
        }
        else {
            if ((ret = cdb2_bind_param(cdb2h, name, cdb2_type, varaddr, length)) != 0) {
                std::cerr << "Error from cdb2_bind_param column " << name << ", ret=" << ret << std::endl;
                return false;
            }
        }
        blobs_vect.push_back((uint8_t*) varaddr);
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

int64_t hrtime() {
    struct timeval t;
    int rc = gettimeofday(&t, nullptr);
    if (rc) {
        perror("gettimeofday");
        return 0;
    }
    return t.tv_sec * 1000000 + t.tv_usec;
}

bool within_threshold(int64_t val1, int64_t val2, int percent) {
    if (percent == 0)
        return false;
    int64_t larger = val1 > val2 ? val1 : val2;
    int64_t smaller = val1 < val2 ? val1 : val2;
    if (larger == 0 || smaller == 0)
        return true;
    double change = ((((double) larger - (double) smaller) / (double) smaller) * 100);
    bool t = change < (double) percent;
#if 0
    if (!t) {
        printf("%" PRId64 " %" PRId64 " %f >= %f\n", val1, val2, (double) change, (double) percent);
    }
#endif
    return t;
}

void printCol(FILE *f, cdb2_hndl_tp *hndl, void *val, int col, int printmode)
{
  int string_blobs = 1;
  switch (cdb2_column_type(hndl, col)) {
    case CDB2_INTEGER:
        if (printmode == DEFAULT)
            fprintf(f, "%s=%lld", cdb2_column_name(hndl, col),
                    *(long long *)val);
        else
            fprintf(f, "%lld", *(long long *)val);
        break;
    case CDB2_REAL:
        if (printmode == DEFAULT)
            fprintf(f, "%s=", cdb2_column_name(hndl, col));
        fprintf(f, "%f", *(double *)val);
        break;
    case CDB2_CSTRING:
        if (printmode == DEFAULT) {
            fprintf(f, "%s=", cdb2_column_name(hndl, col));
            dumpstring(f, (char *)val, 1, 0);
        } else if (printmode & TABS)
            dumpstring(f, (char *)val, 0, 0);
        else
            dumpstring(f, (char *)val, 1, 1);
        break;
    case CDB2_BLOB:
        if (printmode == DEFAULT)
            fprintf(f, "%s=", cdb2_column_name(hndl, col));
        if (string_blobs) {
            char *c = (char*) val;
            int len = cdb2_column_size(hndl, col);
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
                int rc = write(1, val, cdb2_column_size(hndl, col));
                exit(0);
            } else {
                fprintf(f, "x'");
                hexdump(f, val, cdb2_column_size(hndl, col));
                fprintf(f, "'");
            }
        }
        break;
    case CDB2_DATETIME: {
        cdb2_client_datetime_t *cdt = (cdb2_client_datetime_t *)val;
        if (printmode == DEFAULT)
            fprintf(f, "%s=", cdb2_column_name(hndl, col));
        fprintf(f, "\"%4.4u-%2.2u-%2.2uT%2.2u%2.2u%2.2u.%3.3u %s\"",
                cdt->tm.tm_year + 1900, cdt->tm.tm_mon + 1, cdt->tm.tm_mday,
                cdt->tm.tm_hour, cdt->tm.tm_min, cdt->tm.tm_sec, cdt->msec,
                cdt->tzname);
        break;
    }
    case CDB2_DATETIMEUS: {
        cdb2_client_datetimeus_t *cdt = (cdb2_client_datetimeus_t *)val;
        if (printmode == DEFAULT)
            fprintf(f, "%s=", cdb2_column_name(hndl, col));
        fprintf(f, "\"%4.4u-%2.2u-%2.2uT%2.2u%2.2u%2.2u.%6.6u %s\"",
                cdt->tm.tm_year + 1900, cdt->tm.tm_mon + 1, cdt->tm.tm_mday,
                cdt->tm.tm_hour, cdt->tm.tm_min, cdt->tm.tm_sec, cdt->usec,
                cdt->tzname);
        break;
    }
    case CDB2_INTERVALYM: {
        cdb2_client_intv_ym_t *ym = (cdb2_client_intv_ym_t *)val;
        if (printmode == DEFAULT)
            fprintf(f, "%s=", cdb2_column_name(hndl, col));
        fprintf(f, "\"%s%u-%u\"", (ym->sign < 0) ? "- " : "", ym->years,
                ym->months);
        break;
    }
    case CDB2_INTERVALDS: {
        cdb2_client_intv_ds_t *ds = (cdb2_client_intv_ds_t *)val;
        if (printmode == DEFAULT)
            fprintf(f, "%s=", cdb2_column_name(hndl, col));
        fprintf(f, "\"%s%u %2.2u:%2.2u:%2.2u.%3.3u\"",
                (ds->sign < 0) ? "- " : "", ds->days, ds->hours, ds->mins,
                ds->sec, ds->msec);
        break;
    }
    case CDB2_INTERVALDSUS: {
        cdb2_client_intv_dsus_t *ds = (cdb2_client_intv_dsus_t *)val;
        if (printmode == DEFAULT)
            fprintf(f, "%s=", cdb2_column_name(hndl, col));
        fprintf(f, "\"%s%u %2.2u:%2.2u:%2.2u.%6.6u\"",
                (ds->sign < 0) ? "- " : "", ds->days, ds->hours, ds->mins,
                ds->sec, ds->usec);
        break;
    }
  }
}

inline void free_blobs(std::vector<uint8_t *> &blobs_vect) {
    for(std::vector<uint8_t *>::iterator it = blobs_vect.begin(); it != blobs_vect.end(); ++it)
        free(*it);
}

int64_t last_cost(cdb2_hndl_tp *db) {
    int64_t cost = 0;
    cdb2_clearbindings(db);

    int rc = cdb2_run_statement(db, "select comdb2_prevquerycost()");
    if (rc) {
        fprintf(stderr, "can't get cost? run rc %d %s\n", rc, cdb2_errstr(db));
        had_errors = 1;
        return 0;
    }

    rc = cdb2_next_record(db);
    while (rc == CDB2_OK) {
        char *s = (char*) cdb2_column_value(db, 0);
        if (s) {
            // This is not pretty but we don't expose a nicer way to do this.
            char *coststr = strstr(s, "Cost: ");
            if (coststr) {
                coststr += 6;
                cost = std::strtol(coststr, nullptr, 10);
            }
        }
        rc = cdb2_next_record(db);
    }
    if (rc != CDB2_OK_DONE) {
        fprintf(stderr, "last cost next rc %d %s\n", rc, cdb2_errstr(db));
        had_errors = 1;
    }

    return cost;
}

void dump_param(cson_value *param) {
    char *name;
    char *type;

    cson_object *obj;
    cson_value_fetch_object(param, &obj);

    cson_value *nameval = cson_object_get(obj, "name");
    if (nameval == nullptr)
        return;
    name = (char*) cson_value_get_string(nameval);

    cson_value *typeval = cson_object_get(obj, "type");
    if (typeval == nullptr)
        return;
    type = cson_value_get_string(typeval);

    cson_value *val = cson_object_get(obj, "value");
    if (val == nullptr)
        return;

#define typecheck(type) \
    do {                \
        if (!cson_value_is_##type(val)) { \
            fprintf(stderr, ">Unexpected type for param %s\n", name); \
            return;                \
        }                   \
    } while(0)

    if (strcmp(type, "largeint") == 0) {
        typecheck(integer);
        printf("@bind CDB2_INTEGER %s %" PRId64"\n", name, (int64_t) cson_value_get_integer(val));
    }
    else if (strcmp(type, "doublefloat") == 0) {
        typecheck(double);
        printf("@bind CDB2_REAL %s %f\n", name, cson_value_get_double(val));
    }
    else if (strcmp(type, "char") == 0) {
        typecheck(string);
        printf("@bind CDB2_CSTRING %s %s\n", name, (char*) cson_value_get_string(val));
    }
    else if (strcmp(type, "blob") == 0) {
        typecheck(string);
        printf("@bind CDB2_BLOB %s %s\n", name, (char*) cson_value_get_string(val));
    }
    else if (strcmp(type, "datetime") == 0) {
        typecheck(string);
        printf("@bind CDB2_DATETIME %s %s\n", name, (char*) cson_value_get_string(val));
    }
    else if (strcmp(type, "datetimeus") == 0) {
        typecheck(string);
        printf("@bind CDB2_DATETIMEUS %s %s\n", name, (char*) cson_value_get_string(val));
    }
    else {
        fprintf(stderr, ">Unknown bound_parameters type %s\n", type);
    }
}

void dump_sql_event(cson_value *event) {
    cson_object *obj;

    cson_value_fetch_object(event, &obj);
    cson_value *params_obj = cson_object_get(obj, "bound_parameters");
    if (params_obj != nullptr) {
        cson_array *params = cson_value_get_array(params_obj);
        for (int i = 0; i < cson_array_length_get(params); i++) {
            cson_value *v = cson_array_get(params, i);
            if (!cson_value_is_object(v)) {
                fprintf(stderr, "Unexpected bound parameter type\n");
            }
            dump_param(v);
        }
    }
    cson_value *sqlval = cson_object_get(obj, "sql");
    if (sqlval == nullptr) {
        fprintf(stderr, ">No sql statement in sql event?\n");
        return;
    }
    if (!cson_value_is_string(sqlval)) {
        fprintf(stderr, ">Unexpected type of sql member in sql event?\n");
        return;
    }
    char *sqlptr = cson_value_get_string(sqlval);
    std::string sql(sqlptr);
    std::replace(sql.begin(), sql.end(), '\n', ' ');
    std::replace(sql.begin(), sql.end(), '\r', ' ');
    printf("tranid 0 %s", sql.c_str());
}

void replay(cdb2_hndl_tp *db, cson_value *event_val) {
    const char *sql = get_strprop(event_val, "sql");
    if(sql == nullptr) {
	    const char *fp = get_strprop(event_val, "fingerprint");
	    if (fp == nullptr) {
		    std::cerr << "Error: No fingerprint logged?" << std::endl;
		    return;
	    }
	    auto s = sqltrack.find(fp);
	    if (s == sqltrack.end()) {
		    std::cerr << "Error: Unknown fingerprint? " << fp << std::endl;
		    return;
	    }
	    sql = (*s).second.c_str();
    }

    std::vector<uint8_t *> blobs_vect;
    bool ok = do_bindings(db, event_val, blobs_vect);
    if (!ok) {
        free_blobs(blobs_vect);
        return;
    }

    if (verbose)
        std::cout << sql << std::endl;
    int64_t start_time = hrtime();
    int rc = cdb2_run_statement(db, sql);
    cdb2_clearbindings(db);
    free_blobs(blobs_vect);

    if (rc != CDB2_OK) {
        std::cerr << "Error: run rc " << rc << ": " << cdb2_errstr(db) << std::endl;
        return;
    }

    /* TODO: have switch to print or not results */
    int ncols = cdb2_numcolumns(db);
    int64_t rows = 0;
    rc = cdb2_next_record(db);
    while (rc == CDB2_OK) {
        rows++;
        for (int col = 0; col < ncols; col++) {
            void *val = cdb2_column_value(db, col);
            if (val == NULL) {
                if (verbose)
                    fprintf(stdout, "%s=NULL", cdb2_column_name(db, col));
            } else {
                if (verbose)
                    printCol(stdout, db, val, col, DEFAULT);
            }
            if (col != ncols - 1) {
                if (verbose)
                    fprintf(stdout, ", ");
            }
        }
        if (verbose)
            std::cout << std::endl;
        rc = cdb2_next_record(db);
    }
    if (rc != CDB2_OK_DONE) {
        fprintf(stderr, "%s next rc %d %s\n", sql, rc, cdb2_errstr(db));
        return;
    }
    int64_t end_time = hrtime();
    int64_t new_cost = last_cost(db);

    cson_object *obj;
    cson_value_fetch_object(event_val, &obj);
    cson_value *jcost = cson_object_get(obj, "cost");
    int64_t old_cost;
    if (jcost == nullptr)
        old_cost = 0;
    else if (cson_value_is_integer(jcost))
        old_cost = cson_value_get_integer(jcost);
    else if (cson_value_is_double(jcost))
        old_cost = (int64_t) cson_value_get_double(jcost);
    cson_value *jrows = cson_object_get(obj, "rows");
    int64_t old_rows = 0;
    if (jrows != nullptr && cson_value_is_integer(jrows))
        old_rows = cson_value_get_integer(jrows);

    cson_value *perf = cson_object_get(obj, "perf");
    if (perf != nullptr && !cson_value_is_null(perf) && cson_value_is_object(perf)) {
        cson_value *jtime;

        cson_value_fetch_object(perf, &obj);
        jtime = cson_object_get(obj, "tottime");
        int64_t old_time = 0;
        if (jtime != nullptr)
            old_time = cson_value_get_integer(jtime) / 1000;

        bool have_diffs = false;

        int64_t new_time = (end_time - start_time) / 1000;
        if (old_time && new_time && !within_threshold(new_time, old_time, threshold_percent) && new_time > old_time) {
            have_diffs = true;
        }
        else if (old_cost && new_cost && !within_threshold(new_cost, old_cost, threshold_percent) && new_cost > old_cost) {
            have_diffs = true;
        }

        if (diffs && have_diffs) {
            dump_sql_event(event_val);
            printf(" -- time %" PRId64 " (was %" PRId64 ") cost %" PRId64 " (was %" PRId64 ") rows %" PRId64 " (was %" PRId64 ")\n",
                   new_time, old_time,
                   new_cost, old_cost,
                   rows, old_rows);
        }
    }

    if (rc != CDB2_OK_DONE) {
        std::cerr << "Error: next rc " << rc << ": " << cdb2_errstr(db) << std::endl;
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


/* We can only replay if we have the full SQL, including parameters.
   That means
   1) event logged a bindings array
   2) event logged a nbindings integer, and it's 0

   If there's non-zero bindings and we don't have them, we can't replay.

   Another complication is transactions - we don't want to play one back
   until we have all the statements collected, and see a commit event.
   Anything with a cnonce logged is part of a transaction - a commit
   event will have the same cnonce.  SQL writes that are not part of
   a transaction get logged before txn log entry.  SQL in BEGIN/COMMIT
   blocks gets logged before the txn log entry as well.
   */ 
void handle_sql(cdb2_hndl_tp *db, cson_value *event_val) {
    int rc;
    if (!is_replayable(event_val)) {
        cson_free_value(event_val);
        return;
    }

    const char *cnonce = get_strprop(event_val, "cnonce");
    if (cnonce == nullptr) { // no cnonce if comdb2api -- just replay it
        replay(db, event_val);
        cson_free_value(event_val);
        return;
    }

    const char *sql = get_strprop(event_val, "sql");
    if (sql == NULL)
        return;
    if (strcmp(sql, "commit") == 0 || strcmp(sql, "rollback") == 0) {
        if (!is_part_of_transaction(cnonce)) {
            std::cerr << "Error: Commit/rollback record without cnonce in txn list (possibly already commited)"
                      << std::endl;
        } else {
            replay_transaction(db, event_val);
        }
        cson_free_value(event_val);
    } else if (strcmp(sql, "begin") == 0 || is_part_of_transaction(cnonce)) {
        add_to_transaction(event_val); // new entry in list, or append
    } else { // normal 'sql' statement entry which are not part of a transaction
        replay(db, event_val); // replay immediately
        cson_free_value(event_val);
    }
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
    /* TODO: To achieve closer output to the original db,
     * we should commit when we encounter 'txn' record, not upon 'commit'
     * However, to have the sql within a txn replayed correctly we
     * should run it immediately (instead of at commit time).
     *
     * Otherwise the results are different from the original db:
     * create table t1 (i int);
     * create table t2 (i int);
     * txn1 begin
     * txn2 begin
     * txn1 insert into t1 select * from t2
     * txn2 insert into t2 select * from t3
     * txn1 commit
     * txn2 commit
     * 
     * if we run the entire transaction txn1 and commit, then run txn2 
     * then commit, the result will be different from the original
     * run which read (when t2 was empty). 
     *
     * We could run different transactions in separate threads, 
     * and issue sql at the correct time for the reads to be consistent
     * with original db. tools/serial.c does something like this and
     * could be an improvement in future.
     *
    const char *cnonce = get_strprop(val, "cnonce");
    if (cnonce != nullptr && is_part_of_transaction(cnonce))
        replay_transaction(db, val);
    */
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

struct event_source {
    std::string fname;
    std::ifstream file;
    bool done;
    int64_t timestamp;
    cson_value *value;
    int linenum;

    event_source(const char *name) : fname(name), file(), done(false), value(nullptr), timestamp(0), linenum(0) {
        file.open(name);
        if (!file.is_open()) {
            std::cerr << "can't open " << fname << std::endl;
            done = true;
        }
        get();
    }

    event_source(const event_source &from) {
        fname = from.fname;
        done = false;
        file.open(fname);
        if (!file.is_open()) {
            std::cerr << "can't open " << fname << std::endl;
            done = true;
        }
        value = nullptr;
        timestamp = 0;
        linenum = 0;
        get();
    }

    cson_value *consume() {
        cson_value *v = value;
        value = nullptr;
        return v;
    }

    void get() {
        std::string line;
        if (done)
            return;
        for (;;) {
            if (value != nullptr) {
                cson_free_value(value);
                value = nullptr;
            }
            if (!std::getline(file, line)) {
                done = true;
                return;
            }
            linenum++;
            if (line.empty())
                continue;
            int rc = cson_parse_string(&value, line.c_str(), line.length());
            if (rc) {
                std::cerr << "Error: Malformed input on line " << linenum << std::endl;
                continue;
            }
            if (!cson_value_is_object(value)) {
                std::cerr << "Error: Not an object  on line " << linenum << std::endl;
                continue;
            }
            const char *type = get_strprop(value, "type");
            // skip anything with no type field (we don't know what it is) or timestamp (we don't know the order of replay)
            if (type == nullptr || strcmp(type, "sql") != 0)
                continue;
            if (!get_intprop(value, "time", &timestamp))
                continue;
            break;
        }
    }

    bool is_done() const {
        return done;
    }

    bool operator<(const event_source &s2) const {
        return value < s2.value;
    }
};

class event_queue {
public:
    event_queue() = default;

    void add_source(const char *name) {
        sources.emplace_back(event_source(name));
    }

    bool empty() {
        for (auto &i : sources) {
            if (!i.is_done())
                return false;
        }
        return true;
    }
    cson_value *get() {
        int64_t min_timestamp = LLONG_MAX;
        int minix = -1;
        for (int i = 0; i < sources.size(); i++) {
            if (!sources[i].is_done()) {
                if (sources[i].timestamp < min_timestamp) {
                    minix = i;
                    min_timestamp = sources[i].timestamp;
                }
            }
        }
        cson_value *ret = sources[minix].consume();
        if (minix != -1)
            sources[minix].get();
        return ret;
    }

protected:
    std::vector<event_source> sources;
};

void process_events(cdb2_hndl_tp *db, event_queue &queue) {
    std::string line;
    int linenum = 0;
    int64_t numevents = 0;

    while (!queue.empty()) {
        int rc;
        linenum++;
        cson_value *event_val = queue.get();
        const char *type = get_strprop(event_val, "type");
        if (type != nullptr) {
            handle(cdb2h, type, event_val);
            if (had_errors) {
                had_errors = 0;
                cdb2_close(cdb2h);
                char *conf = getenv("CDB2_CONFIG");
                if (conf) {
                    cdb2_set_comdb2db_config(conf);
                    rc = cdb2_open(&cdb2h, dbname, "default", 0);
                } else { 
                    rc = cdb2_open(&cdb2h, dbname, "local", 0);
                }
                db = cdb2h;
            }
            numevents++;
            if (maxevents && numevents >= maxevents)
                break;
        }
        else
            cson_free_value(event_val);
    }
    if (verbose)
        std::cout << "got " << linenum  << " lines" << std::endl;
}

int main(int argc, char **argv) {
    char *filename = nullptr;

    init_handlers();

    if (argc < 2) {
        usage();
    }
    argc--;
    argv++;
    while (argc && argv[0][0] == '-') {
        if (strcmp(argv[0], "--diff") == 0)
            diffs = true;
        else if (strcmp(argv[0], "--verbose") == 0 || strcmp(argv[0], "-v") == 0)
            verbose = true;
        else if (strcmp(argv[0], "--threshold") == 0) {
            argc--;
            argv++;
            if (argc == 0) {
                fprintf(stderr, "--threshold expected an argument");
                return 1;
            }
            threshold_percent = (int) strtol(argv[0], nullptr, 10);
        }
        else if (strcmp(argv[0], "--stopat") == 0) {
            argc--;
            argv++;
            if (argc == 0) {
                fprintf(stderr, "--stopat expected an argument");
                return 1;
            }
            maxevents = (int) strtol(argv[0], nullptr, 10);
        }
        else {
            fprintf(stderr, "Unknown option %s\n", argv[0]);
        }
        argc--;
        argv++;
    }
    dbname = argv[0];
    argc--;
    argv++;

    /* TODO: tier should be an option */
    int rc;
    char *conf = getenv("CDB2_CONFIG");
    if (conf) {
        cdb2_set_comdb2db_config(conf);
        rc = cdb2_open(&cdb2h, dbname, "default", 0);
    } else { 
        rc = cdb2_open(&cdb2h, dbname, "local", 0);
    }

    if (rc) {
        std::cerr << "Error: cdb2_open() failed: " << cdb2_errstr(cdb2h) << std::endl;
        exit(EXIT_FAILURE);
    }
    cdb2_run_statement(cdb2h, "set getcost on");

    event_queue events;
    while (argc) {
        events.add_source(argv[0]);
        argc--;
        argv++;
    }
    process_events(cdb2h, events);

    cdb2_close(cdb2h);
    return 0;
}
