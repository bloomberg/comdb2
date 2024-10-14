#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <inttypes.h>
#include <list.h>
#include <cdb2api.h>
#include <sys/socket.h>
#include <cdb2api.c>
#include <sbuf2.c>
#include "stepper_client.h"

/* storing clients (i.e. comm pipes) */
struct client
{
   int   id;
   cdb2_hndl_tp *db;

   LINKC_T(struct client) lnk;
};

LISTC_T(client_t) clients;

/**
 * Opens a client 
 * Returns a client to be used with the next calls
 * Returns NULL if error
 *
 */
client_t* clnt_open(const char *db, const char *stage, int id)
{
    struct client *c;
    int rc;

    char *conf = getenv("CDB2_CONFIG");
    if (conf)
        cdb2_set_comdb2db_config(conf);

    c = malloc(sizeof(struct client));
    c->id = id;
    rc = cdb2_open(&c->db, db, stage, 0);
    if (rc) {
        fprintf(stderr, "open %s (stage %s) rc %d\n", db, stage, rc);
        return NULL;
    }
    listc_abl(&clients, c);
    return c;
}



static void hexdump(void *datap, int len, FILE *out) 
{
   uint8_t *data = (uint8_t*) datap;
   int i;
   for (i = 0; i < len; i++)
      fprintf(out, "%02x", (unsigned int)data[i]);
}

static void printcol(cdb2_hndl_tp * db, void *val, int col, FILE *out) {
    if (val == NULL) {
        fprintf(out, "NULL");
        return;
    }
    switch (cdb2_column_type(db, col)) 
    {
        case CDB2_INTEGER:
            fprintf(out, "%lld", *(long long*)val);
            break;
        case CDB2_REAL:
            fprintf(out, "%f", *(double*)val);
            break;
        case CDB2_CSTRING:
            fprintf(out, "'%s'", (char*) val);
            break;
        case CDB2_BLOB: {
            fprintf(out, "x'");
            hexdump(val, cdb2_column_size(db, col), out);
            fprintf(out, "'");
            break;
        }
        case CDB2_DATETIME: {
            cdb2_client_datetime_t   *cdt = (cdb2_client_datetime_t*)val;
            fprintf(out, "\"%4.4u-%2.2u-%2.2uT%2.2u%2.2u%2.2u.%3.3u %s\"",
                    cdt->tm.tm_year+1900, cdt->tm.tm_mon+1, cdt->tm.tm_mday,
                    cdt->tm.tm_hour, cdt->tm.tm_min, cdt->tm.tm_sec, 
                    cdt->msec, cdt->tzname);
        }
        case CDB2_INTERVALYM: {
            cdb2_client_intv_ym_t    *ym = (cdb2_client_intv_ym_t*)val;
            fprintf(out, "\"%s%u-%u\"", (ym->sign<0)?"- ":"", ym->years, ym->months);
            break;
        }
        case CDB2_INTERVALDS: {
                cdb2_client_intv_ds_t    *ds = (cdb2_client_intv_ds_t*)val;
                fprintf(out, "\"%s%u %u:%u:%u.%3.3u\"", (ds->sign<0)?"- ":"",
                        ds->days, ds->hours, ds->mins, ds->sec, ds->msec);
                break;
        }
    }
}



/**
 * Send query using the "clnt" client
 * Returns 0 on success and CDB2ERR_IO_ERROR on I/O error
 *
 */
int clnt_run_query( client_t *clnt, char *query, FILE *out)
{
    int rc = cdb2_run_statement(clnt->db, query);
    if (rc) {
        const char *err = cdb2_errstr(clnt->db);
        fprintf(out, "[%s] failed with rc %d %s\n", query, rc, err ? err : "");
    }
    if (rc == CDB2ERR_IO_ERROR) {
        return rc;
    }
    int ncols;
    rc = cdb2_next_record(clnt->db);
    ncols = cdb2_numcolumns(clnt->db);
    while (rc == CDB2_OK) {
        fprintf(out, "(");
        for (int col = 0; col < ncols; col++) {
            void *val;
            val = cdb2_column_value(clnt->db, col);
            fprintf(out, "%s=", cdb2_column_name(clnt->db,col));
            printcol(clnt->db, val, col, out);
            if (col != ncols - 1)
                fprintf(out, ", ");
        }
        fprintf(out, ")\n");
        rc = cdb2_next_record(clnt->db);
    }
    fprintf(out, "done\n");

    cdb2_effects_tp ef;
    rc = cdb2_get_effects(clnt->db, &ef);
    /*
    if(rc == 0) {
        if(ef.num_updated > 0)
            fprintf(out, "(rows updated=%d)\n", ef.num_updated);
        else if(ef.num_deleted > 0)
            fprintf(out, "(rows deleted=%d)\n", ef.num_deleted);
        else if(ef.num_inserted > 0)
            fprintf(out, "(rows inserted=%d)\n", ef.num_inserted);
    }
    */

    return 0;
}

/**
 * Close "clnt" client
 * Returns 0 on success
 *
 */
int clnt_close( client_t *clnt)
{
    cdb2_close(clnt->db);
    listc_rfl(&clients, clnt);
    free(clnt);
    return 0;
}

/**
 * Initialize the client repository 
 *
 */
int clnt_repo_init( int isdebug)
{
    listc_init(&clients, offsetof(struct client, lnk));
    return 0;
}


/**
 * Destroy the client repository
 * All pending clients are terminated
 *
 */
int clnt_repo_destroy(void)
{
    struct client *clnt;
    clnt = listc_rtl(&clients);
    while (clnt) {
        cdb2_close(clnt->db);
        free(clnt);
        clnt = listc_rtl(&clients);
    }
    return 0;
}


/**
 * Get a client based on a id
 * NULL if cannot find it
 *
 */
client_t* clnt_get( int id)
{
    struct client *clnt;
    LISTC_FOR_EACH( &clients, clnt, lnk)
    {
        if (clnt->id == id)
            return clnt;
    }

    return NULL;
}

static int disconnect_cdb2h(cdb2_hndl_tp * cdb2h) {
    const int rc = cdb2h->sb ? shutdown(cdb2h->sb->fd, 2) : 0;
    if (rc) {
        fprintf( stderr, "%s: Failed with errno(%s)", __func__, strerror(errno));
    }

    return rc;
}

int clnt_disconnect_one(client_t * const client)
{
    return disconnect_cdb2h(client->db);
}

int clnt_disconnect_all(void)
{
    client_t *client;

    LISTC_FOR_EACH(&clients, client, lnk) {
        const int rc = clnt_disconnect_one(client);
        if (rc) { return rc; }
    }

    return 0;
}
