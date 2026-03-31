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

#ifndef INCLUDED_COMDB2_DBINFO
#define INCLUDED_COMDB2_DBINFO

#ifndef __COMDB2_DBINFO_H__
#define __COMDB2_DBINFO_H__

#include <compile_time_assert.h>
#include <stddef.h>

enum db_info2types {
    DBINFO2_FIRSTRQ = 1,
    DBINFO2_KEYINFO = 1,
    DBINFO2_TABLEINFO = 2,
    DBINFO2_CLUSTERINFO = 3,
    DBINFO2_GENDBINFO = 4,
    DBINFO2_CLUSTERCONN = 5,
    DBINFO2_DIR = 6,     /* data directory, given back as a string */
    DBINFO2_UNKNOWN = 7, /* used in reply for an unknown opcode */
    DBINFO2_STATS = 8,
    DBINFO2_PROX2_TABLEINFO =
        9, /* only list tables that have database numbers */
    DBINFO2_PROX2_CONFIG = 10,
    DBINFO2_PROX2_NODEINFO = 11,
    DBINFO2_IOSTATS = 12,
    DBINFO2_LASTRQ = 12
};

enum db_info2cnst { DBINFO2_MAXRQS = 16 };

struct dbinfopc {
    int type;
    int len; /* length of data in bytes- does not include this header */
    char data[1];
};

struct db_info2_unknown {
    int opcode; /* the code we didn't recognise */
};

struct db_info2_tbl_info {
    int ntables;
    int fluff[8];
    struct tblinfo {
        char table_name[32];
        int table_lux;
        int table_dbnum;
        int table_lrl;
        int table_nix;
        int fluff[8];
        struct keyinfo {
            int keylen;
            int dupes;
            int recnums;
            int primary;
            char keytag[64];
        } table_keys[1];
    } tables[1];
};

struct db_info2_cluster_info {
    int master;
    int syncmode;
    int formkey_enabled;
    int retry;
    int port;
    int nsiblings;
    int incoherent; /* 1 if incoherent, otherwise 0 */
    int fluff[7];
    struct node_info {
        char hostname[64];
        int node;
    } siblings[1];
};

struct db_info2_node {
    int node;
    int port;
};

enum { DB_INFO2_NODE_LEN = 4 + 4 };

BB_COMPILE_TIME_ASSERT(db_info2_node_len,
                       sizeof(struct db_info2_node) == DB_INFO2_NODE_LEN);

struct db_info2_node_info {
    int numnodes;
    struct db_info2_node nodes[1];
};

enum { DB_INFO2_NODE_INFO_LEN = 4 };

BB_COMPILE_TIME_ASSERT(db_info2_node_info_len,
                       offsetof(struct db_info2_node_info, nodes) ==
                           DB_INFO2_NODE_INFO_LEN);

struct db_info2_req {
    short prccom[3];
    short opcode; /* dbinfo2 request (opcode 102) */
    int ninforq;
    int inforqs[DBINFO2_MAXRQS]; /* type of information request */
    int fluff[64];               /* fluff for expansion */
};

struct db_info2_resp {
    int prccom[2];
    int comdb2resp;
    int lrl;
    int nix;
    int shmflags;
    int ninfo;
    int moreflags;
    int physrep_src_dbnum;
    int fluff[6];
    struct dbinfopc dbdata[1];
};

/* reply for DBINFO2_STATS - performance related stats.  These stats are all
 * from when the db was started up.  The layout of this struct can't change,
 * but we may well add more stuff on the end. */
struct db_info2_stats {
    int32_t starttime;       /* when db was started, as unix epoch time */
    uint32_t n_qtraps;       /* no. of qtraps */
    uint32_t n_appsock;      /* no. of socket connections */
    uint32_t n_sql;          /* number of sql queries */
    uint64_t cache_hits;     /* berkeley db cache hits */
    uint64_t cache_misses;   /* berkeley db cache misses */
    uint32_t n_retries;      /* number of request retries forced by deadlock */
    uint16_t q_max_conf;     /* maximum configured request queue depth */
    uint16_t q_max_reached;  /* maximum queue depth reached */
    uint16_t q_mean_reached; /* mean queue depth */
    uint16_t thr_max;        /* max allowed threads */
    uint16_t thr_maxwr;      /* max allowed threads (that can write,
                                0 for no limit) */
    uint16_t thr_cur;        /* current num threads */
    uint32_t padding0;       /* only pads to here on linux */
    uint32_t padding1;       /* pads to here on sun & ibm */
};

/* Cache/IO stats. */
struct db_info2_iostats {
    int sz;
    int unused;
    int64_t page_reads;
    int64_t page_writes;
    int64_t cachesz;
    int64_t cachereqs;
    int64_t cachehits;
    int64_t reqs;           /* non-dbinfo requests */
    int64_t sql_statements; /* sql statements */
    int64_t sql_steps;      /* sql steps */
    /* add new stats here */
};
enum { DB_INFO2_IOSTATS_LEN = 4 + 4 + 8 + 8 + 8 + 8 + 8 + 8 + 8 + 8 };
BB_COMPILE_TIME_ASSERT(db_info2_iostats_len,
                       sizeof(struct db_info2_iostats) == DB_INFO2_IOSTATS_LEN);

#endif

#endif
