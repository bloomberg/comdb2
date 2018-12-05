/*
   Copyright 2015 Bloomberg Finance L.P.

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

#ifndef INCLUDED_SHARD_RANGE_H
#define INCLUDED_SHARD_RANGE_H

enum {
    SHARD_NOERR_DONE = 1,
    SHARD_NOERR = 0,
    SHARD_ERR_GENERIC = -1,
    SHARD_ERR_EXIST = -2,
    SHARD_ERR_MALLOC = -3,
};

struct Expr;
struct Token;
struct ExprList;
struct Parse;

/**
 * A range sharding uses row prefixes that match the index key structure.
 *
 * Each indexes that are considered for parallel execution have this
 * structure saved, and parallelizable sql select statements will run
 * use this to overlap work
 *
 */
struct shard_limits {
    int nlimits;
    struct Token *col;
    struct Expr **low;
    struct Expr **high;
};
typedef struct shard_limits shard_limits_t;

/* Create a range structure */
void shard_range_create(struct Parse *pParser, const char *tblname,
                        struct Token *col, struct ExprList *limits);

/* Destroy a range structure */
void shard_range_destroy(shard_limits_t *shards);

/* Merging concurrent rows */
void shard_flush_conns(struct sqlclntstate *clnt, int waitfordone);

#endif
