#ifndef _COMDB2QL_CLNT_H_
#define _COMDB2QL_CLNT_H_

#include <stdio.h>

/**
 * Implements a multiple client testing configuration
 * which allows submitting string queries to a server
 * in a specific order 
 * Currently, this implementation assumes that only
 * one request is executed at a time (no parallelism
 *
 */

struct client;
typedef struct client client_t;

/**
 * Opens a client using "prgname" comdb2sql
 * Returns a client to be used with the next calls
 * Returns NULL if error
 *
 */
client_t* clnt_open( char *prgname, char **argv, int id);


/**
 * Send query using the "clnt" client
 * Returns 0 on success
 *
 */
int clnt_send( client_t *clnt, char *query);


/** 
 * Stores "clnt" answer
 * Returns 0 on success
 *
 */
int clnt_recv( client_t *clnt, FILE *out);

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
