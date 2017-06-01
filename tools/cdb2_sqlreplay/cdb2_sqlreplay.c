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
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <unistd.h>
#include <zlib.h>
#include <inttypes.h>

#include "cdb2api.h"
#include "cson_amalgamation_core.h"

static cdb2_hndl_tp *cdb2h = NULL;

static const char *usage_text = 
    "Usage: cdb2sqlreplay dbname [FILE]\n" \
    "\n" \
    "Basic options:\n" \
    "-f                 run the sql as fast as possible\n";

/* Start of functions */
void usage() {
    printf(usage_text);
    exit(EXIT_FAILURE);
}

struct sql_track {
    char fingerprint[32];
    int crc32;
    char *sql;
};

struct sql_track *statements = NULL;
static int nalloc_statements = 0;
static int count_statements = 0;

void add_fingerprint(const char *fingerprint, const char *sql) {
    if (count_statements == nalloc_statements) {
        nalloc_statements = nalloc_statements * 2 + 100;
        statements = realloc(statements, sizeof(struct sql_track) * nalloc_statements);
    }
    memcpy(statements[count_statements].fingerprint, fingerprint, 32);
    statements[count_statements].sql = strdup(sql);
    count_statements++;
}

const char *fingerprint_to_sql(const char *fingerprint) {
    for (int i = 0; i < count_statements; i++) {
        if (memcmp(fingerprint, statements[i].fingerprint, 32) == 0)
            return (const char*) statements[i].sql;
    }
    return NULL;
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
 

static const char *get_strprop(cson_value *objval, char *key) {
    cson_object *obj;
    cson_value_fetch_object(objval, &obj);
    cson_value *propval = cson_object_get(obj, key);
    if (propval == NULL || !cson_value_is_string(propval))
        return NULL;
    cson_string *cstr;
    cson_value_fetch_string(propval, &cstr);
    return cson_string_cstr(cstr);
}

static int get_intprop(cson_value *objval, char *key, int64_t *val) {
    cson_object *obj;
    cson_value_fetch_object(objval, &obj);
    cson_value *propval = cson_object_get(obj, key);
    if (propval == NULL || !cson_value_is_integer(propval))
        return 1;
    cson_string *cstr;
    int rc = cson_value_fetch_integer(propval, (cson_int_t*) val);
    return rc;
}


void handle_sql(cdb2_hndl_tp *db, cson_value *val) {
    int64_t nparms = -99;
    int rc;
    rc = get_intprop(val, "bindings", &nparms);
    printf("getintprop rc %d %"PRId64"\n", rc, nparms);
}

/* TODO: error messages? */
void handle_newsql(cdb2_hndl_tp *db, cson_value *val) {
    const char *sql = get_strprop(val, "sql");
    const char *fingerprint = get_strprop(val, "fingerprint");

    if (sql == NULL || fingerprint == NULL)
        return;

    /* If we already know this fingerprint, ignore it. 
       Checking if SQL matches is useless since diffent sql
       statements can have identical fingerprints (eg: 'select 1'
       and 'select 2'.  Fingerprints are md5 hashes of the parse
       tree.  We can rely on them being unique for different
       statement types, unless the user is malicious and extremely 
       clever, in which case we punish them with bad logging.
     */
    if (fingerprint_to_sql(fingerprint))
        return;
    add_fingerprint(fingerprint, sql);
}


struct event_handler {
    char *event;
    void (*handler)(cdb2_hndl_tp *db, cson_value *val);
};

struct event_handler handlers[] = {
    { .event = "newsql", .handler = handle_newsql },
    { .event = "sql",    .handler = handle_sql    }
};

void handle(cdb2_hndl_tp *db, const char *event, cson_value *val) {
    for (int i = 0; i < sizeof(handlers)/sizeof(handlers[0]); i++) {
        if (strcmp(handlers[i].event, event) == 0) {
            handlers[i].handler(db, val);
            return;
        }
    }
}

int main(int argc, char **argv) {
    FILE *infile;
    size_t line_alloc;
    char *line;
    char *post_prefix;
    int stmt_no;
    int wait_sec;
    int linenum;
    int linelen;
    char *dbname;

    if (argc < 2) {
        usage();
    }
    dbname = argv[1];
    if (argc >= 3) {
        if ((infile = fopen(argv[2], "r")) == NULL) {
            perror("Can't open input file");
            exit(EXIT_FAILURE);
        }
    } else {
        infile = stdin;
    }
    /* TODO: tier should be an option */
#if 0
    if (cdb2_open(&cdb2h, dbname, "local", 0)) {
        fprintf(stderr,
                "cdb2_open() failed %s\n", cdb2_errstr(cdb2h));
        cdb2_close(cdb2h);
        exit(EXIT_FAILURE);
    }
#endif

    line = NULL;
    line_alloc = 0;
    linenum = 0;
    while ((linelen = getline(&line, &line_alloc, infile)) != -1) {
        int rc;
        linenum++;
        cson_value *obj;
        cson_object *o;
        cson_parse_info pinfo = cson_parse_info_empty_m;

        rc = cson_parse_string(&obj, line, linelen, &cson_parse_opt_empty, &pinfo);
        if (rc) {
            fprintf(stderr, "Malformed input on line %d\n", linenum);
            continue;
        }
        if (cson_value_fetch_object(obj, &o)) {
            fprintf(stderr, "Not an object on line %d\n", linenum);
            continue;
        }
        cson_value *v;
        v = cson_object_get(o, "type");
        if (v == NULL || !cson_value_is_string(v))
            /* no type field?  we dont know how to parse this */
            continue;
        const char *type;
        cson_string *s;
        cson_value_fetch_string(v, &s);
        type = cson_string_cstr(s);

        handle(cdb2h, type, obj);

        /* We have our statement, wait until we need to use it */
#if 0
        gettimeofday(&now, NULL);
        timeval_subtract(&diff, &now, &prgm_start);
        wait_sec = difftime(timelocal(&stmt_tm), timelocal(&init_tm));
        wait_sec -= diff.tv_sec;
        if (wait_sec > 0)
            sleep(wait_sec);
#endif

        // cdb2_run_statement(cdb2h, "");

        cson_free_value(obj);
    }
    // cdb2_close(cdb2h);
    return 0;
}
