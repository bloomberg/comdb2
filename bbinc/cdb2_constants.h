/*
   Copyright 2015, 2018 Bloomberg Finance L.P.

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

#ifndef INCLUDED_UTIL_CDB2_CONSTANTS_H
#define INCLUDED_UTIL_CDB2_CONSTANTS_H

#define COMDB2_MAX_RECORD_SIZE 16384
#define LONG_REQMS 2000
#define MAXBLOBLENGTH ((1 << 28) - 1)   /* (1 << ODH_LENGTH_BITS) - 1 */
#define MAXBLOBS 15                     /* Should be bdb's MAXDTAFILES - 1 */
#define MAXCOLNAME 99                   /* not incl. \0 */
#define MAXCOLUMNS 1024
#define INITREVCONSTRAINTS 4
#define MAXCONSUMERS 32 /* to match bdblib limit */
#define MAXCUSTOPNAME 32
#define MAX_DBNAME_LENGTH 64
#define MAXDYNTAGCOLUMNS 2048
#define MAXKEYLEN 512
#define MAXLRL 65536
#define MAXBBNODENUM 32768 /* Legacy: maximum number assigned to a machine in BBENV */
#define MAXNODES MAXBBNODENUM
#define MAXPLUGINS 100
#define MAXPSTRLEN 256
#define MAX_QUEUE_HITS_PER_TRANS 8
#define MAX_SPNAME MAXTABLELEN
#define MAX_SPVERSION_LEN 80
#define MAXTABLELEN 32
#define MAXTAGLEN 64
#define REPMAX 32
/* Maximum buffer length for generated key name. */
#define MAXGENKEYLEN 25
/* Maximum buffer length for generated constraint name. */
#define MAXGENCONSLEN 25
/* Maximum allowed constraint name length */
#define MAXCONSLEN 64
#define MAXQUERYLEN 262144
#define MAXCUR 100
#define MAXRECSZ (17 * 1024)
#define MAXKEYSZ 1024
#define MAX_NUM_TABLES 1024
#define MAX_NUM_QUEUES 1024
#define MAX_NUM_VIEWS 1024
#define NUM_ADMIN_TABLES 3
#define MAX_CHILDREN (2 * (MAX_NUM_TABLES + NUM_ADMIN_TABLES) + MAX_NUM_QUEUES)
#define MAXINDEX 50
/* Primary data file + 15 blobs files */
#define MAXDTAFILES 16
#define MAXDTASTRIPE 16
#define MAX_USERNAME_LEN 16
#define MAX_PASSWORD_LEN 19
#define GENIDLEN sizeof(unsigned long long)
/* moved here from csc2, better place */
/*max length of index name, its char[64] in stat1 - 10 for $_12345678*/
#define MAXIDXNAMELEN 54

/*
  Print at the given offset, detect overflow and update offset
  accordingly.
*/
#define SNPRINTF(str, size, off, fmt, ...)                                     \
    {                                                                          \
        int ret;                                                               \
        ret = snprintf(str + off, size - off, fmt, __VA_ARGS__);               \
        if (ret >= size - off) {                                               \
            off += size - off;                                                 \
            goto done;                                                         \
        }                                                                      \
        off += ret;                                                            \
    }

/* Access to base tables/indices */
#define CDB2_WRITE_COST 100.0
#define CDB2_FIND_COST 10.0
#define CDB2_BLOB_FETCH_COST 10.0
#define CDB2_MOVE_COST 1.0

/* Access to temporary tables (& VDBE sorter) */
#define CDB2_TEMP_WRITE_COST 0.2
#define CDB2_TEMP_FIND_COST 0.1
#define CDB2_TEMP_MOVE_COST 0.1

/* Access to sqlite_statN & sqlite_master tables is considered free */
#define CDB2_SQLITE_STAT_COST 0.0

enum NET_NAMES { NET_REPLICATION, NET_SQL, NET_MAX };

#endif /* INCLUDED_UTIL_CDB2_CONSTANTS_H */
