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

#ifndef INCLUDED_COMDB2_SHM_H
#define INCLUDED_COMDB2_SHM_H

#include <sys/types.h>

/* These are the public APIs.  Getters can be used by all client applications,
 * setters are only to be used by comdb2/prox2 like applications */

int comdb2_shm_is_comdb2(int db);

int comdb2_shm_master_set(int db, char *master);
int comdb2_shm_master_get(int db);

int comdb2_shm_retrylen_set(int db, int retry_len);
int comdb2_shm_retrylen_get(int db);

int comdb2_shm_attr_get(int db);
int comdb2_shm_attr_set(int db, int attr);

int comdb2_shm_set_flag(int db, int flag);
int comdb2_shm_clr_flag(int db, int flag);
int comdb2_shm_chk_flag(int db, int flag);
int comdb2_shm_get_flags(int db, int *flags);
int comdb2_shm_clear_and_set_flags(int db, int shmflags);

int comdb2_shm_localdb_get(int db);
int comdb2_shm_localdb_set(int db, int localdb);

int comdb2_shm_clear(int db);

/* altname is expected to contain 9 bytes of storage */
int comdb2_shm_altname_get(int db, char *altname);

/* altname is a c string.  at most 8 chars will be preserved */
int comdb2_shm_altname_set(int db, const char *altname);

enum comdb2_shm_flags {
    CMDB2_SHMFLG_KEYLESS_API = 0x00000001,
    CMDB2_SHMFLG_PROX2 = 0x00000002, /* to distinguish prox2 from real comdb2 */
    CMDB2_SHMFLG_TZ = 0x00000004,    /* supports datetime field */
    CMDB2_SHMFLG_TZDMP = 0x00000008, /* supports dumping datetime field */
    CMDB2_SHMFLG_ERRSTAT = 0x00000010, /* supports extra erro codes */
    CMDB2_SHMFLG_OSQL = 0x00000020, /* supports sql offloading, i.e blocksql */
    CMDB2_SHMFLG_FDAVAIL =
        0x00000040, /* indicates that sqlproxy has an available
                       file descriptor for regular sql */
    CMDB2_SHMFLG_RECOM =
        0x00000080, /* comdb2 server supports read-commited sql mode */
    CMDB2_SHMFLG_OSQL_SOCK =
        0x00000100, /* comdb2 server supports block sql over socket */
    CMDB2_SHMFLG_SERIAL =
        0x00000200, /* comdb2 server supports serial sql mode */

    CMDB2_SHMFLG_DBQUEUE = 0x00000400, /* this is a dbqueue instance fronting
                                          a comdb2 or proxy.  The other flags
                                          will be set the same as the fronted
                                          database. */

    /* if nove of the defaults mode is specified, the default is COMDB2
       PREFER_SOSQL allows switching only blocksql consumer clients to socksql
       */
    CMDB2_SHMFLG_OSQL_DEF = 0x00001000, /* block sql is the default sql mode */
    CMDB2_SHMFLG_SOSQL_DEF =
        0x00002000, /* block sql over the socket is the default mode */
    CMDB2_SHMFLG_RECOM_DEF =
        0x00004000, /* recom (read committed) is the default sql mode */
    CMDB2_SHMFLG_SERIAL_DEF = 0x00008000, /* serial is the default sql mode */
    CMDB2_SHMFLG_PREFER_SOSQL =
        0x00010000, /* only if client sets blocksql, it gets sosql */
    CMDB2_SHMFLG_HEARTBEAT = 0x00020000, /* server supports heartbeats */
    CMDB2_SHMFLG_STATS_OK =
        0x00040000, /* db can understand dbglog stat requests */
    CMDB2_SHMFLG_GOODSQLCODES =
        0x00080000, /* server will send correct sql error codes */
    CMDB2_SHMFLG_FKRCODE = 0x00100000, /* fk violation rcode. */
    CMDB2_SHMFLG_SOCK_FSTSND = 0x00200000, /* use sockets instead of fstsnd. */
    CMDB2_SHMFLG_READONLY = 0x00400000,    /* the proxy is read only. */
    CMDB2_SHMFLG_ALLOCV2_ENABLED =
        0x00800000, /* the db has alloc V2 enabled. */
    CMDB2_SHMFLG_FAILEDDISP =
        0x01000000, /* db sends back failed dispatch messages */
    CMDB2_SHMFLG_POSITION_API =
        0x02000000, /* Position APIs enabled for the server. */
    CMDB2_SHMFLG_LINUX_CLIENT =
        0x04000000, /* Linux client support enabled on this server. */
    CMDB2_SHMFLG_SOSQL_DFLT = 0x08000000, /* */
    CMDB2_SHMFLG_SQL_RANDNODE =
        0x10000000, /* default sql read node allocation to random */
    CMDB2_SHMFLG_BLOCK_OFFLOAD =
        0x20000000 /* The database supports offloading block requests. */
};

/* These are in a second flags word */
enum comdb2_shm_flags2 { CMDB2_SHMFLG_SUPPORTS_SOCK_LUXREF = 0x00000001 };

/* To be calledonly by the init program; these are not public APIs */
int comdb2_shm_init(void);
int comdb2_shm_init_n(size_t num_dbs);

/* These are the getters and setters for the new comdb2 shared memory
 * area. Tasks should not call thse directly - instead, call the public
 * functions listed above. */
int comdb2_new_shm_is_comdb2(int db);

int comdb2_new_shm_master_set(int db, int master);
int comdb2_new_shm_master_get(int db);

int comdb2_new_shm_retrylen_set(int db, int retry_len);
int comdb2_new_shm_retrylen_get(int db);

int comdb2_new_shm_attach_readonly(void);
int comdb2_new_shm_attach_readwrite(void);
int comdb2_new_shm_attach_readwrite_protected(void);
int comdb2_new_shm_init_n(size_t num_dbs);

int comdb2_new_shm_attr_get(int db);
int comdb2_new_shm_attr_set(int db, int attr);

int comdb2_new_shm_set_flag(int db, int flag);
int comdb2_new_shm_clr_flag(int db, int flag);
int comdb2_new_shm_chk_flag(int db, int flag);
int comdb2_new_shm_get_flags(int db, int *flags);
int comdb2_new_shm_clear_and_set_flags(int db, int shmflags);

int comdb2_new_shm_localdb_get(int db);
int comdb2_new_shm_localdb_set(int db, int localdb);

int comdb2_new_shm_clear(int db);

/* altname is expected to contain 9 bytes of storage */
int comdb2_new_shm_altname_get(int db, char *altname);

/* altname is a c string.  at most 8 chars will be preserved */
int comdb2_new_shm_altname_set(int db, const char *altname);

short comdb2_shm_read_timeout_get(int db);
int comdb2_shm_read_timeout_set(int db, short read_timeout);

short comdb2_shm_default_timeout_get(int db);
int comdb2_shm_default_timeout_set(int db, short default_timeout);

unsigned int comdb2_shm_pq_shmkey_get(int db);
int comdb2_shm_pq_shmkey_set(int db, unsigned int pq_shmkey);

unsigned int comdb2_new_shm_pq_shmkey_get(int db);
int comdb2_new_shm_read_timeout_get(int db);
int comdb2_new_shm_default_timeout_get(int db);
int comdb2_new_shm_pq_shmkey_set(int db, unsigned int pq_shmkey);
int comdb2_new_shm_read_timeout_set(int db, short read_timeout);
int comdb2_new_shm_default_timeout_set(int db, short default_timeout);
int comdb2_shm_attach_readonly_(void);
int *comdb2_shm_coherent_nodes_get(int db);

#endif
