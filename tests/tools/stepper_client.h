#ifndef _COMDB2QL_CLNT_H_
#define _COMDB2QL_CLNT_H_

#include <stdio.h>
#include <cdb2api.h>
#include <list.h>

/**
 * Implements a multiple client testing configuration
 * which allows submitting string queries to a server
 * in a specific order 
 * Currently, this implementation assumes that only
 * one request is executed at a time (no parallelism
 *
 */


/* storing clients (i.e. comm pipes) */
struct client
{
   int   id;
   cdb2_hndl_tp *db;

   LINKC_T(struct client) lnk;
};

typedef struct client client_t;

LISTC_T(client_t) clients;

/**
 * Opens a client using "prgname" comdb2sql
 * Returns a client to be used with the next calls
 * Returns NULL if error
 *
 */
client_t* clnt_open(const char *db, const char *stage, int id);


/**
 * Run query using the "clnt" client
 * Returns 0 on success
 *
 */
int clnt_run_query( client_t *clnt, char *query, FILE *out);

/**
 * Close "clnt" client
 * Returns 0 on success
 *
 */
int clnt_close( client_t *clnt);

/**
 * Initialize the client repository 
 *
 */
int clnt_repo_init(int debug);

/**
 * Destroy the client repository
 * All pending clients are terminated
 *
 */
int clnt_repo_destroy(void);

/**
 * Get a client based on a id
 * NULL if cannot find it
 *
 */
client_t* clnt_get( int id);

#endif
