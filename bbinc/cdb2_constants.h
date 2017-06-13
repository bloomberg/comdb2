/*
   Copyright 2015, 2017 Bloomberg Finance L.P.

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
#define MAXBLOBLENGTH 255 * 1024 * 1024 /* TODO: set a good maximum here */
#define MAXBLOBS 15                     /* Should be bdb's MAXDTAFILES - 1 */
#define MAXCOLNAME 99                   /* not incl. \0 */
#define MAXCOLUMNS 1024
#define MAXCONSTRAINTS 32
#define MAXCONSUMERS 32 /* to match bdblib limit */
#define MAXCUSTOPNAME 32
#define MAX_DBNAME_LENGTH 64
#define MAXDTASTRIPE 16
#define MAXDYNTAGCOLUMNS 2048
#define MAXINDEX 50
#define MAXKEYLEN                                                              \
    512 /* to clients it is 256, but tagged mode adds an extra byte to each    \
           column so internally we allow twice that.*/
#define MAXLRL 65536
#define MAXNETS 3
#define MAXNODES 32768
#define MAX_QUEUE_HITS_PER_TRANS 8
#define MAXSIBLINGS 64
#define MAX_SPNAME MAXTABLELEN
#define MAX_SPVERSION_LEN 80
#define MAXTABLELEN 32
#define MAXTAGLEN 64
#define REPMAX 32
#define SEQUENCE_EXHAUSTED 0x01 /* Flag for indicating all values dispensed for a sequence */

#endif
