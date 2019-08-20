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

#ifndef INCLUDED_THRMAN_H
#define INCLUDED_THRMAN_H

struct thr_handle;

enum thrtype {
    THRTYPE_UNKNOWN = -1,
    THRTYPE_APPSOCK = 0,
    THRTYPE_SQLPOOL = 1,
    THRTYPE_SQL = 2,
    THRTYPE_REQ = 3,
    THRTYPE_CONSUMER = 4,
    THRTYPE_PURGEBLKSEQ = 5,
    THRTYPE_PREFAULT = 6,
    THRTYPE_VERIFY = 7,
    THRTYPE_ANALYZE = 8,
    THRTYPE_PUSHLOG = 9,
    THRTYPE_SCHEMACHANGE = 10,
    THRTYPE_BBIPC_WAITFT = 11,
    THRTYPE_LOGDELHOLD = 12,
    THRTYPE_OSQL = 13,
    THRTYPE_COORDINATOR = 14,
    THRTYPE_APPSOCK_POOL = 15,
    THRTYPE_SQLENGINEPOOL = 16,
    THRTYPE_APPSOCK_SQL = 17,
    THRTYPE_MTRAP = 18,
    THRTYPE_QSTAT = 19,
    THRTYPE_PURGEFILES = 20,
    THRTYPE_BULK_IMPORT = 21,
    THRTYPE_TRIGGER = 22,
    THRTYPE_PGLOGS_ASOF = 23,
    THRTYPE_MAX
};

enum thrsubtype {
    THRSUBTYPE_UNKNOWN = -1,
    THRSUBTYPE_TOPLEVEL_SQL = 0,
    THRSUBTYPE_LUA_SQL = 1
};

void thrman_init(void);
struct thr_handle *thrman_register(enum thrtype type);
void thrman_change_type(struct thr_handle *thr, enum thrtype newtype);
void thrman_unregister(void);
struct thr_handle *thrman_self(void);
void thrman_where(struct thr_handle *thr, const char *where);
void thrman_wheref(struct thr_handle *thr, const char *fmt, ...);
void thrman_origin(struct thr_handle *thr, const char *origin);
struct sqlpool;
void thrman_set_sqlpool(struct thr_handle *thr, struct sqlpool *);
void thrman_setid(struct thr_handle *thr, const char *idstr);
void thrman_setfd(struct thr_handle *thr, int fd);
void thrman_setsqlthd(struct thr_handle *thr);
enum thrtype thrman_get_type(struct thr_handle *thr);
const char *thrman_type2a(enum thrtype type);
char *thrman_describe(struct thr_handle *thr, char *buf, size_t szbuf);
void thrman_dump(void);
int thrman_count_type(enum thrtype type);
struct reqlogger *thrman_get_reqlogger(struct thr_handle *thr);
void thrman_stop_sql_connections(void);
int thrman_wait_type_exit(enum thrtype type);

enum thrsubtype thrman_get_subtype(struct thr_handle *thr);
void thrman_set_subtype(struct thr_handle *thr, enum thrsubtype subtype);

#endif
