#include <pthread.h>
#include "sql.h"
#include <sqlinterfaces.h>
#include <segstr.h>
#include "sqlquery.pb-c.h"

#include <unistd.h>

static pthread_once_t once = PTHREAD_ONCE_INIT;

static int latch_response_value(struct sqlclntstate *clnt, int type, void *p, int huh)
{
    struct response_data *data = (struct response_data *)p;
    if (clnt->appdata == NULL)
        return 0;
    if (type == RESPONSE_ROW) {
        *((int *)clnt->appdata) = sqlite3_column_int(data->stmt, 0);
    }
    return 0;
}

static void simpleAuthInit(void)
{
    struct sqlclntstate clnt;
    start_internal_sql_clnt(&clnt);
    clnt.admin = 1;
    clnt.dbtran.mode = TRANLEVEL_RECOM;
    int rc = run_internal_sql_clnt(
        &clnt, "create table if not exists comdb2_simple_auth(cluster cstring(20) default '*', user cstring(20) "
               "default '*', bpkg cstring(50) default '*', verb cstring(20) default '*', resourcetype cstring(20) "
               "default '*', resourcename cstring(50) default '*')");
    if (rc)
        exit(1);
    rc = run_internal_sql_clnt(&clnt, "create unique index if not exists comdb2_simple_auth_ix on "
                                      "comdb2_simple_auth(cluster, user, bpkg, verb, resourcetype, resourcename)");
    if (rc)
        exit(1);

    int authcount;
    char *sql = "select count(*) from comdb2_simple_auth";
    start_internal_sql_clnt(&clnt);
    clnt.admin = 1;
    clnt.dbtran.mode = TRANLEVEL_RECOM;
    clnt.plugin.write_response = latch_response_value;
    clnt.sql = sql;
    clnt.appdata = &authcount;
    rc = run_internal_sql_clnt(&clnt, sql);
    if (rc) {
        fprintf(stderr, "Can't figure out if comdb2_simple_auth has records\n");
        exit(1);
    }

    if (authcount == 0) {
        rc = run_internal_sql_clnt(&clnt, "insert into comdb2_simple_auth(cluster, user, bpkg, verb, resourcetype, "
                                          "resourcename) values('*', '*', '*', '*', '*', '*') on conflict do nothing");
        if (rc)
            exit(1);
        rc = run_internal_sql_clnt(&clnt,
                                   "insert into comdb2_simple_auth(cluster, user, bpkg, verb, resourcetype, "
                                   "resourcename) values('*', '*', '*', 'Connect', '*', '*') on conflict do nothing");
        if (rc)
            exit(1);
        rc = run_internal_sql_clnt(
            &clnt, "insert into comdb2_simple_auth(cluster, user, bpkg, verb, resourcetype, resourcename) values('*', "
                   "'*', '*', 'Read', 'table', 'comdb2_simple_auth') on conflict do nothing");
        if (rc)
            exit(1);
    }

    end_internal_sql_clnt(&clnt);
}

// bpi:procauth:cluster:c2btda:user:mponomar:bpkg:cdb2sql
static void parsePrincipal(const char *principal, char **cluster, char **user, char **bpkg)
{
    int off = 0;
    int ltok = 0;
    char plen = strlen(principal);
    *cluster = NULL;
    *user = NULL;
    *bpkg = NULL;

    for (;;) {
        int flen = 0;
        char *field = segtokx((char *)principal, plen, &off, &flen, ":");
        if (field == NULL || flen == 0)
            break;
        char *value = segtokx((char *)principal, plen, &off, &ltok, ":");
        if (value == NULL || ltok == 0)
            break;

        if (tokcmp(field, flen, "cluster") == 0) {
            *cluster = tokdup(value, ltok);
        } else if (tokcmp(field, flen, "user") == 0) {
            *user = tokdup(value, ltok);
        } else if (tokcmp(field, flen, "bpkg") == 0) {
            *bpkg = tokdup(value, ltok);
        }
    }
}

static void parseVerb(const char *verb, char **verbout)
{
    int off = 0;
    int ltok = 0;
    int flen = 0;
    char vlen = strlen(verb);

    *verbout = NULL;

    char *field = segtokx((char *)verb, vlen, &off, &flen, ":");
    if (field == NULL || flen == 0) {
        return;
    }
    char *value = segtokx((char *)verb, vlen, &off, &ltok, ":");
    if (value == NULL || ltok == 0) {
        return;
    }
    if (tokcmp(field, flen, "comdb2") == 0) {
        *verbout = tokdup(value, ltok);
    }
    return;
}

static void parseResource(const char *resource, char **database, char **table)
{
    int off = 0;
    int ltok = 0;
    char rlen = strlen(resource);
    *database = NULL;
    *table = NULL;

    for (;;) {
        int flen = 0;
        char *field = segtokx((char *)resource, rlen, &off, &flen, ":");
        if (field == NULL || flen == 0)
            break;
        char *value = segtokx((char *)resource, rlen, &off, &ltok, ":");
        if (value == NULL || ltok == 0)
            break;

        if (tokcmp(field, flen, "database") == 0) {
            *database = tokdup(value, ltok);
        } else if (tokcmp(field, flen, "table") == 0) {
            *table = tokdup(value, ltok);
        }
    }
}

int simpleAuthCheck(const char *principal, const char *verb_in, const char *resource)
{
    pthread_once(&once, simpleAuthInit);
    struct sqlclntstate clnt;
    int allowed = 0;

    char sql[1000];
    strcpy(sql, "select exists (select * from comdb2_simple_auth where ");

    char *cluster, *user, *bpkg;
    parsePrincipal(principal, &cluster, &user, &bpkg);
    char *verb;
    parseVerb(verb_in, &verb);
    char *database, *table;
    parseResource(resource, &database, &table);

    if (verb_in && (strcmp(verb_in, "DDL") == 0 || strcmp(verb_in, "OP") == 0) && table &&
        strcmp(table, "comdb2_simple_auth") == 0)
        return -1;

    strcat(sql, "(cluster='*' ");
    if (cluster) {
        strcat(sql, "or cluster = '");
        strcat(sql, cluster);
        strcat(sql, "' ");
    }
    strcat(sql, ") and (user='*' ");
    if (user) {
        strcat(sql, "or user = '");
        strcat(sql, user);
        strcat(sql, "' ");
    }
    strcat(sql, ") and (bpkg='*' ");
    if (user) {
        strcat(sql, "or bpkg = '");
        strcat(sql, bpkg);
        strcat(sql, "' ");
    }
    strcat(sql, ") and (verb='*' ");
    if (verb) {
        strcat(sql, "or verb='");
        strcat(sql, verb);
        strcat(sql, "' ");
    }
    strcat(sql, ") and ((resourcetype='table' or resourcetype='*') and resourcename='*' ");
    if (table) {
        strcat(sql, "or resourcename='");
        strcat(sql, table);
        strcat(sql, "' ");
    }
    strcat(sql, "))");

    start_internal_sql_clnt(&clnt);
    clnt.admin = 1;
    clnt.dbtran.mode = TRANLEVEL_RECOM;
    clnt.plugin.write_response = latch_response_value;
    clnt.sql = sql;
    clnt.appdata = &allowed;
    int rc = run_internal_sql_clnt(&clnt, sql);
    if (rc)
        allowed = 0;
    end_internal_sql_clnt(&clnt);

    printf("%s %s %s -> %d\n", principal, verb, resource, allowed);

    free(cluster);
    free(user);
    free(bpkg);
    free(verb);
    free(database);
    free(table);

    return allowed ? 0 : -1;
}
