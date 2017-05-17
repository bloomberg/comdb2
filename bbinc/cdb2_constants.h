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

#ifndef INCLUDED_UTIL_CDB2_CONSTANTS_H
#define INCLUDED_UTIL_CDB2_CONSTANTS_H

#define MAXNODES 32768
#define REPMAX 32 // Maximum number of replicants
#define COMDB2_MAX_RECORD_SIZE 16384
#define MAX_DBNAME_LENGTH 64

enum COMDB_LIMITS {
    LONG_REQMS = 2000,
    MAXKEYLEN = 512 /*max key len. to clients it is 256, but tagged mode
                      adds an extra byte to each column so internally we
                      allow twice that.*/
    ,
    MAXLRL = 65536 /*max dta lrl*/
    ,
    MAXINDEX = 50 /*max # of indices*/
    ,
    MAXBLOBS = 15 /*max # of blobs - should be bdb's MAXDTAFILES - 1 */
    ,
    MAXSIBLINGS = 64,
    MAXCOLUMNS = 1024 /*max columns in table, hard limit*/
    ,
    MAXDYNTAGCOLUMNS = 2048,
    MAXTAGLEN = 64,
    MAXCOLNAME = 99 /*maximum column name length (not incl. \0)*/
    ,
    MAXPSTRLEN = 256,
    MAXTABLELEN = 32,
    MAXCOMDBGNODES = 32,
    MAXBLOBLENGTH = 256 * 1024 * 1024 /* TODO set a good maximum here */
    ,
    MAXCONSTRAINTS = 32,
    MAXCONSUMERS = 32 /* max queue consumers, to match bdblib limit */
    ,
    MAXCUSTOPNAME = 32 /* max length of a custom operation name */
    ,
    MAXDTASTRIPE = 16,
    MAX_QUEUE_HITS_PER_TRANS = 8 /* how many queue hits we efficiently
                                    remember per trans */
    ,
    MAXALIASNAME = 32,
    MAX_SPNAME = MAXTABLELEN,
    MAX_SPVERSION_LEN = 80,
    MAXNETS = 3
};

#endif
