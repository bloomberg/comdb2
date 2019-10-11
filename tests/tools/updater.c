#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <cdb2api.h>
#include <malloc.h>
#include <errno.h>
#include <time.h>
#include <inttypes.h>
#include "testutil.h"

static char *argv0=NULL;

#define APPIDSZ 30
#define KEYSZ 128
#define OCCSZ 17

struct index_rec {
    char applicationid[APPIDSZ];
    char key[KEYSZ];
    char *sessionids;
    int sessionids_sz;
    char *occtoken;
    int occtoken_sz;
};

struct index_rec *rec_array = NULL;
int rec_alloc = 0;
int rec_index = 0;
int prthresholdms = 100;
int print_interval = 1000;
char *stage = "default";

char *dbname = NULL;
#define RECINIT 1000

void rec_add(char *applicationid, char *key, char *sessionids, int sessionids_sz, char *occtoken, int occtoken_sz)
{
    if (rec_index >= rec_alloc) {
        rec_alloc = (!rec_alloc) ? RECINIT : (rec_alloc * 2);
        rec_array = realloc(rec_array, sizeof(struct index_rec) * rec_alloc);
    }
    strncpy(rec_array[rec_index].applicationid, applicationid, APPIDSZ);
    strncpy(rec_array[rec_index].key, key, KEYSZ);
    rec_array[rec_index].sessionids_sz = sessionids_sz;
    rec_array[rec_index].sessionids = malloc(sessionids_sz);
    memcpy(rec_array[rec_index].sessionids, sessionids, sessionids_sz);
    rec_array[rec_index].occtoken_sz = occtoken_sz;
    rec_array[rec_index].occtoken = malloc(occtoken_sz);
    memcpy(rec_array[rec_index].occtoken, occtoken, occtoken_sz);
    rec_index++;
}

void usage(FILE *f)
{
    fprintf(stderr, "Usage: %s <cmd-line>\n", argv0);
    fprintf(stderr, " -d <dbname>           - dbname\n");
    fprintf(stderr, " -c <config>           - config\n");
    fprintf(stderr, " -i <iterations>       - iterations\n");
    fprintf(stderr, " -t <threashold-ms>    - fail-threshold\n");
    fprintf(stderr, " -P <threashold-ms>    - print-threshold\n");
    fprintf(stderr, " -s <stage>            - stage\n");
    fprintf(stderr, " -p <print-interval>   - print interval\n");
    fprintf(stderr, " -h                    - help\n");
}

void print_node(void)
{
    int rc;
    cdb2_hndl_tp *hndl = NULL;
    rc = cdb2_open(&hndl, dbname, "default", 0);
    if (hndl == NULL) {
        fprintf(stderr, "Failed to open handle for %s, rc = %d\n", dbname, rc);
        exit(1);
    }

    rc = cdb2_run_statement(hndl, "select comdb2_host()");
    if (rc != 0) {
        fprintf(stderr, "Error running machine select, %s\n", cdb2_errstr(hndl));
        exit(1);
    }
    rc = cdb2_next_record(hndl);
    if (rc != CDB2_OK) {
        fprintf(stderr, "Error nexting machine select, %s\n", cdb2_errstr(hndl));
        exit(1);
    }
    char *mach = cdb2_column_value(hndl, 0);
    fprintf(stderr, "Handle pointed to %s\n", mach);
}

void populate(void)
{
    int rc;
    cdb2_hndl_tp *hndl = NULL;
    rc = cdb2_open(&hndl, dbname, "default", 0);
    if (hndl == NULL) {
        fprintf(stderr, "Failed to open handle for %s, rc = %d\n", dbname, rc);
        exit(1);
    }
    rc = cdb2_run_statement(hndl, "select * from sessions_index");
    if (rc != 0) {
        fprintf(stderr, "Error running initial select, %s\n", cdb2_errstr(hndl));
        exit(1);
    }

    do {
        rc = cdb2_next_record(hndl);
        if (rc == CDB2_OK) {
            char *app, *key, *sids, *occ;
            int sids_sz, occ_sz;
            app = cdb2_column_value(hndl, 0);
            key = cdb2_column_value(hndl, 1);
            sids = cdb2_column_value(hndl, 2);
            sids_sz = cdb2_column_size(hndl, 2);
            occ = cdb2_column_value(hndl, 3);
            occ_sz = cdb2_column_size(hndl, 3);
            rec_add(app, key, sids, sids_sz, occ, occ_sz);
        }
    } while (rc == CDB2_OK);
    cdb2_close(hndl);
}

void update(int iterations, int thresholdms)
{
    cdb2_hndl_tp *hndl = NULL;
    int64_t start, mid, end;

    int rc = cdb2_open(&hndl, dbname, stage, 0);
    if (hndl == NULL) {
        fprintf(stderr, "Failed to open handle for %s, rc = %d\n", dbname, rc);
        exit(1);
    }

    for (int64_t i = 0; (iterations <= 0) || (i < iterations); i++) {
        int ix = (rand() % rec_index);
        struct index_rec *rec = &rec_array[ix];
        cdb2_effects_tp effects;
        cdb2_clearbindings(hndl);

        cdb2_bind_param(hndl, "appid", CDB2_CSTRING, rec->applicationid, APPIDSZ);
        cdb2_bind_param(hndl, "key", CDB2_CSTRING, rec->key, KEYSZ);
        cdb2_bind_param(hndl, "sessionids", CDB2_BLOB, rec->sessionids, rec->sessionids_sz);
        cdb2_bind_param(hndl, "occtoken", CDB2_BLOB, rec->occtoken, rec->occtoken_sz);

        start = timems();
        rc = cdb2_run_statement(hndl, "UPDATE sessions_index SET sessionIds = @sessionids, occToken = @occtoken WHERE applicationId = @appid AND key = @key AND occToken = @occtoken");
        if (rc != 0) {
            fprintf(stderr, "Error updating, %s\n", cdb2_errstr(hndl));
            exit(1);
        }
        mid = timems();
        rc = cdb2_get_effects(hndl, &effects);
        if (rc) {
            fprintf(stderr, "Error getting effects, %s\n", cdb2_errstr(hndl));
            exit(1);
        }
        end = timems();
	if ((end - start) > thresholdms) {
            fprintf(stderr, "%u request iteration %"PRId64" took %"PRId64"ms start-interval was %"PRId64
                    "ms, end-interval was %"PRId64"ms\n",
                    (uint32_t)getpid(), i, (end - start), (mid - start), (end - mid));
            exit(1);
        } else if ((end - start) > prthresholdms) {
            fprintf(stderr, "%u request iteration %"PRId64" took %"PRId64"ms start-interval was %"PRId64
                    "ms, end-interval was %"PRId64"ms\n",
                    (uint32_t)getpid(), i, (end - start), (mid - start), (end - mid));
	}
        if (print_interval > 0 && !(i % print_interval)) {
            fprintf(stderr, "Completed %"PRId64" updates\n", i);
        }
    }

    cdb2_close(hndl);
}

int main(int argc,char *argv[])
{
    int c, thresholdms=1200, iterations=-1;
    argv0=argv[0];
    setvbuf(stdout, NULL, _IOLBF, 0);
    setvbuf(stderr, NULL, _IOLBF, 0);
    srand(time(NULL) ^ getpid());

    while ((c = getopt(argc, argv, "hd:c:i:p:s:t:P:"))!=EOF) {
        switch(c) {
            case 'd':
                dbname = optarg;
                break;
            case 'i':
                iterations = atoi(optarg);
                break;
            case 'P':
                prthresholdms = atoi(optarg);
                break;
            case 't':
                thresholdms = atoi(optarg);
                break;
            case 'c':
                cdb2_set_comdb2db_config(optarg);
                break;
            case 's':
                stage = optarg;
                break;
            case 'p':
                print_interval = atoi(optarg);
                break;
            case 'h':
                usage(stdout);
                exit(0);
                break;
        }
    }
    if (dbname == NULL) {
        fprintf(stderr, "Unset dbname\n");
        exit(1);
    }
    print_node();
    populate();
    update(iterations, thresholdms);
    return 0;
}


