/*
   Copyright 2020 Bloomberg Finance L.P.

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

#ifndef _OSQL_RPL_TYPES_H_
#define _OSQL_RPL_TYPES_H_

const char *osql_reqtype_str(int type); // used for printing string of type

// clang-format off

#define OSQL_RPL_TYPES \
XMACRO_OSQL_RPL_TYPES( OSQL_RPLINV,             0, "OSQL_RPLINV" ) /* no longer in use */                                    \
XMACRO_OSQL_RPL_TYPES( OSQL_DONE,               1, "OSQL_DONE" )                                                             \
XMACRO_OSQL_RPL_TYPES( OSQL_USEDB,              2, "OSQL_USEDB" )                                                            \
XMACRO_OSQL_RPL_TYPES( OSQL_DELREC,             3, "OSQL_DELREC" )                                                           \
XMACRO_OSQL_RPL_TYPES( OSQL_INSREC,             4, "OSQL_INSREC" ) /* R7 uses OSQL_INSERT */                                 \
XMACRO_OSQL_RPL_TYPES( OSQL_CLRTBL,             5, "OSQL_CLRTBL" )                                                           \
XMACRO_OSQL_RPL_TYPES( OSQL_QBLOB,              6, "OSQL_QBLOB" )                                                            \
XMACRO_OSQL_RPL_TYPES( OSQL_UPDREC,             7, "OSQL_UPDREC" )                                                           \
XMACRO_OSQL_RPL_TYPES( OSQL_XERR,               8, "OSQL_XERR" )                                                             \
XMACRO_OSQL_RPL_TYPES( OSQL_UPDCOLS,            9, "OSQL_UPDCOLS" )                                                          \
XMACRO_OSQL_RPL_TYPES( OSQL_DONE_STATS,        10, "OSQL_DONE_STATS" ) /* just like OSQL_DONE, but have additional stats */  \
XMACRO_OSQL_RPL_TYPES( OSQL_DBGLOG,            11, "OSQL_DBGLOG" )                                                           \
XMACRO_OSQL_RPL_TYPES( OSQL_RECGENID,          12, "OSQL_RECGENID" )                                                         \
XMACRO_OSQL_RPL_TYPES( OSQL_UPDSTAT,           13, "OSQL_UPDSTAT" )                                                          \
XMACRO_OSQL_RPL_TYPES( OSQL_EXISTS,            14, "OSQL_EXISTS" )                                                           \
XMACRO_OSQL_RPL_TYPES( OSQL_SERIAL,            15, "OSQL_SERIAL" )                                                           \
XMACRO_OSQL_RPL_TYPES( OSQL_SELECTV,           16, "OSQL_SELECTV" )                                                          \
XMACRO_OSQL_RPL_TYPES( OSQL_DONE_SNAP,         17, "OSQL_DONE_SNAP" )                                                        \
XMACRO_OSQL_RPL_TYPES( OSQL_SCHEMACHANGE,      18, "OSQL_SCHEMACHANGE" )                                                     \
XMACRO_OSQL_RPL_TYPES( OSQL_BPFUNC,            19, "OSQL_BPFUNC" )                                                           \
XMACRO_OSQL_RPL_TYPES( OSQL_DBQ_CONSUME,       20, "OSQL_DBQ_CONSUME" )                                                      \
XMACRO_OSQL_RPL_TYPES( OSQL_DELETE,            21, "OSQL_DELETE" ) /* for partial indexes */                                 \
XMACRO_OSQL_RPL_TYPES( OSQL_INSERT,            22, "OSQL_INSERT" ) /* for partial indexes */                                 \
XMACRO_OSQL_RPL_TYPES( OSQL_UPDATE,            23, "OSQL_UPDATE" ) /* for partial indexes */                                 \
XMACRO_OSQL_RPL_TYPES( OSQL_DELIDX,            24, "OSQL_DELIDX" ) /* for indexes on expressions */                          \
XMACRO_OSQL_RPL_TYPES( OSQL_INSIDX,            25, "OSQL_INSIDX" ) /* for indexes on expressions */                          \
XMACRO_OSQL_RPL_TYPES( OSQL_DBQ_CONSUME_UUID,  26, "OSQL_DBQ_CONSUME_UUID" ) /* not in use */                                \
XMACRO_OSQL_RPL_TYPES( OSQL_STARTGEN,          27, "OSQL_STARTGEN" )                                                         \
XMACRO_OSQL_RPL_TYPES( OSQL_DONE_WITH_EFFECTS, 28, "OSQL_DONE_WITH_EFFECTS" )                                                \
XMACRO_OSQL_RPL_TYPES( OSQL_PREPARE,           29, "OSQL_PREPARE" ) /* participant should prepare */                         \
XMACRO_OSQL_RPL_TYPES( OSQL_DIST_TXNID,        30, "OSQL_DIST_TXNID" ) /* send dist-txnid to coordinator */                  \
XMACRO_OSQL_RPL_TYPES( OSQL_PARTICIPANT,       31, "OSQL_PARTICIPANT" ) /* a participant (to coordinator) */                 \
XMACRO_OSQL_RPL_TYPES( OSQL_TIMESPEC,          32, "OSQL_TIMESPEC" )                                                         \
XMACRO_OSQL_RPL_TYPES( MAX_OSQL_TYPES,         33, "OSQL_MAX")

// clang-format on

#ifdef XMACRO_OSQL_RPL_TYPES
#   undef XMACRO_OSQL_RPL_TYPES
#endif
// the following will expand to enum OSQL_RPL_TYPE { OSQL_RPLINV = 0, OSQL_DONE = 1, ..., MAX_OSQL_TYPES = 29, };
#define XMACRO_OSQL_RPL_TYPES(a, b, c) a = b,
enum OSQL_RPL_TYPE { OSQL_RPL_TYPES };
#undef XMACRO_OSQL_RPL_TYPES


#endif
