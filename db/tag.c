/*
   Copyright 2015, 2021, Bloomberg Finance L.P.

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

#include <alloca.h>
#include <ctype.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <stddef.h>
#include <string.h>
#include <strings.h>
#include <pthread.h>
#include <limits.h>
#include <float.h>
#include <poll.h>
#include <flibc.h>
#include <inttypes.h>
#include <unistd.h>
#include "osqluprec.h"

#include <str0.h>
#include <epochlib.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <plbitlib.h>
#include <segstr.h>
#include <fsnapf.h>

#include <netinet/in.h>
#include "util.h"
#include "tohex.h"
#include <plhash.h>
#include "tag.h"
#include "types.h"
#include "comdb2.h"
#include "block_internal.h"
#include "prefault.h"

#include "sql.h"
#include <bdb_api.h>
#include <strbuf.h>

#include "utilmisc.h"
#include "views.h"
#include "debug_switches.h"
#include "logmsg.h"
#include "schemachange.h" /* sc_errf() */
#include "dynschematypes.h"

extern struct dbenv *thedb;
extern pthread_mutex_t csc2_subsystem_mtx;

char gbl_ondisk_ver[] = ".ONDISK.VER.";
char gbl_ondisk_ver_fmt[] = ".ONDISK.VER.%d";
const int gbl_ondisk_ver_len = sizeof ".ONDISK.VER.255xx";

int _dbg_tags = 0;
int gbl_debug_alter_sequences_sleep = 0;

#define TAGLOCK_RW_LOCK
#ifdef TAGLOCK_RW_LOCK
static pthread_rwlock_t taglock;
#else
static pthread_mutex_t taglock;
#endif

hash_t *gbl_tag_hash;

int compare_tag_int(struct schema *old, struct schema *new, FILE *out,
                    int strict);
int compare_indexes(const char *table, FILE *out);
static void freeschema_internals(struct schema *schema);

static inline void lock_taglock_read(void)
{
#ifdef TAGLOCK_RW_LOCK
    Pthread_rwlock_rdlock(&taglock);
#else
    Pthread_mutex_lock(&taglock);
#endif
}

void lock_taglock(void)
{
#ifdef TAGLOCK_RW_LOCK
    Pthread_rwlock_wrlock(&taglock);
#else
    Pthread_mutex_lock(&taglock);
#endif
}

void unlock_taglock(void)
{
#ifdef TAGLOCK_RW_LOCK
    Pthread_rwlock_unlock(&taglock);
#else
    Pthread_mutex_unlock(&taglock);
#endif
}

static inline void init_taglock(void)
{
#ifdef TAGLOCK_RW_LOCK
    Pthread_rwlock_init(&taglock, NULL);
#else
    Pthread_mutex_init(&taglock, NULL);
#endif
}

/* set dbstore (or null) value for a column */
static inline void set_dbstore(const dbtable *db, int col, void *field,
                               int flen)
{
    const struct dbstore *dbstore = db->dbstore;
    if (dbstore[col].len) {
        memcpy(field, dbstore[col].data, dbstore[col].len);
    } else {
        set_null(field, flen);
    }
}

static int ctag_to_stag_int(struct dbtable *table, const char *ctag,
                            const char *inbuf, int len,
                            const unsigned char *innulls, const char *stag,
                            void *outbufp, int flags, int ondisk_lim,
                            struct convert_failure *fail_reason,
                            blob_buffer_t *inblobs, blob_buffer_t *outblobs,
                            int maxblobs, const char *tzname);

int schema_init(void)
{
    init_taglock();
    gbl_tag_hash = hash_init_strcaseptr(offsetof(struct dbtag, tblname));

    logmsg(LOGMSG_INFO, "Schema module init ok\n");
    return 0;
}

#if defined DEBUG_STACK_TAG_SCHEMA
void comdb2_cheapstack_sym(FILE *f, char *fmt, ...);
#ifdef __GLIBC__
extern int backtrace(void **, int);
#else
#define backtrace(A, B) 1
#endif
#endif

static void add_tag_schema_lk(const char *table, struct schema *schema)
{
    struct dbtag *tag;
    struct schema *fnd;

    tag = hash_find_readonly(gbl_tag_hash, &table);
    if (tag == NULL) {
        tag = malloc(sizeof(struct dbtag));
        if (tag == NULL) {
            logmsg(LOGMSG_FATAL, "malloc failed\n");
            exit(1);
        }

        tag->tblname = strdup(table);
        tag->tags = hash_init_strcaseptr(offsetof(struct schema, tag));
        hash_add(gbl_tag_hash, tag);
        listc_init(&tag->taglist, offsetof(struct schema, lnk));
    }
    if ((fnd = hash_find_readonly(tag->tags, &schema->tag)) != NULL) {
        listc_rfl(&tag->taglist, fnd);
        if (_dbg_tags)
            logmsg(LOGMSG_DEBUG, "Removing %s:%s\n", table, fnd->tag);
        hash_del(tag->tags, fnd);
        free(fnd);
    }
    if (_dbg_tags)
        logmsg(LOGMSG_DEBUG, "Adding %s:%s\n", table, schema->tag);
    hash_add(tag->tags, schema);
    listc_abl(&tag->taglist, schema);
#if defined DEBUG_STACK_TAG_SCHEMA
    comdb2_cheapstack_sym(stderr, "%s:%d -> %s:%s ", __func__, __LINE__, table,
                          schema->tag);
    schema->frames = backtrace(schema->buf, MAX_TAG_STACK_FRAMES);
    schema->tid = pthread_self();
#endif
}

void add_tag_schema(const char *table, struct schema *schema)
{
    lock_taglock();
    add_tag_schema_lk(table, schema);
    unlock_taglock();
}

static void _tags_free_schema(struct dbtag *tag, struct schema *s,
                              const char *tablename)
{
    if (_dbg_tags)
        logmsg(LOGMSG_DEBUG, "2 Removing %s:%s\n", tablename, s->tag);
    hash_del(tag->tags, s);
    listc_rfl(&tag->taglist, s);
    freeschema(s, 0);
}


static void del_tag_schema_lk(const char *table, const char *tagname)
{
    struct dbtag *tag = hash_find_readonly(gbl_tag_hash, &table);
    if (tag == NULL)
        return;

    struct schema *s = hash_find(tag->tags, &tagname);

    if (s) {
#if defined DEBUG_STACK_TAG_SCHEMA
        comdb2_cheapstack_sym(stderr, "%s:%d -> %s:%s ", __func__, __LINE__,
                              table, tagname);
#endif
        _tags_free_schema(tag, s, table);
    }
}

void del_tag_schema(const char *table, const char *tagname)
{
    lock_taglock();
    del_tag_schema_lk(table, tagname);
    unlock_taglock();
}

static struct schema *find_tag_schema_lk(const char *table, const char *tagname)
{
    struct dbtag *tag = hash_find_readonly(gbl_tag_hash, &table);
    if (unlikely(tag == NULL))
        return NULL;
    struct schema *s = hash_find_readonly(tag->tags, &tagname);
    return s;
}

struct schema *find_tag_schema(const struct dbtable *table, const char *tagname)
{
    struct schema *s;
    if (!table) return NULL;
    lock_taglock_read();
    s = find_tag_schema_lk(table->tablename, tagname);
    unlock_taglock();
    return s;
}

/* this will go away once the tags become part of a dbtable */
struct schema *find_tag_schema_by_name(const char *tblname, const char *tagname)
{
    struct schema *s;
    lock_taglock_read();
    s = find_tag_schema_lk(tblname, tagname);
    unlock_taglock();
    return s;
}


int find_field_idx_in_tag(const struct schema *tag, const char *field)
{
    int i;
    for (i = 0; i < tag->nmembers; i++)
        if (strcasecmp(tag->member[i].name, field) == 0)
            return i;
    return -1;
}

int find_field_idx(struct dbtable *table, const char *tagname, const char *field)
{
    struct schema *tag = find_tag_schema(table, tagname);

    if (unlikely(tag == NULL))
        return -1;

    return find_field_idx_in_tag(tag, field);
}

int client_type_to_server_type(int type)
{
    switch (type) {
    case CLIENT_UINT:
        return SERVER_UINT;
    case CLIENT_INT:
        return SERVER_BINT;
    case CLIENT_REAL:
        return SERVER_BREAL;
    case CLIENT_CSTR:
    case CLIENT_PSTR2:
    case CLIENT_PSTR:
        return SERVER_BCSTR;
    case CLIENT_BYTEARRAY:
        return SERVER_BYTEARRAY;
    case CLIENT_BLOB:
        return SERVER_BLOB;
    case CLIENT_BLOB2:
        return SERVER_BLOB2;
    case CLIENT_DATETIME:
        return SERVER_DATETIME;
    case CLIENT_DATETIMEUS:
        return SERVER_DATETIMEUS;
    case CLIENT_INTVYM:
        return SERVER_INTVYM;
    case CLIENT_INTVDS:
        return SERVER_INTVDS;
    case CLIENT_INTVDSUS:
        return SERVER_INTVDSUS;
    case CLIENT_VUTF8:
        return SERVER_VUTF8;
    case CLIENT_SEQUENCE:
        return SERVER_SEQUENCE;
    case CLIENT_FUNCTION:
        return SERVER_FUNCTION;
    default:
        abort();
    }
}

int client_type_to_csc2_type(int type, int inlen, int *csc2type)
{
    int gothit = 0;

    switch (type) {
    case CLIENT_UINT:
        switch (inlen) {
        case 2:
            *csc2type = COMDB2_USHORT;
            gothit = 1;
            break;
        case 4:
            *csc2type = COMDB2_UINT;
            gothit = 1;
            break;
        case 8:
            *csc2type = COMDB2_ULONGLONG;
            gothit = 1;
            break;
        }
        break;

    case CLIENT_INT:
        switch (inlen) {
        case 2:
            *csc2type = COMDB2_SHORT;
            gothit = 1;
            break;
        case 4:
            *csc2type = COMDB2_INT;
            gothit = 1;
            break;
        case 8:
            *csc2type = COMDB2_LONGLONG;
            gothit = 1;
            break;
        }
        break;

    case CLIENT_REAL:
        switch (inlen) {
        case 4:
            *csc2type = COMDB2_FLOAT;
            gothit = 1;
            break;
        case 8:
            *csc2type = COMDB2_DOUBLE;
            gothit = 1;
            break;
        }
        break;

    case CLIENT_PSTR:
        *csc2type = COMDB2_PSTR;
        gothit = 1;
        break;

    case CLIENT_CSTR:
        *csc2type = COMDB2_CSTR;
        gothit = 1;
        break;

    case CLIENT_BYTEARRAY:
        *csc2type = COMDB2_BYTE;
        gothit = 1;
        break;

    case CLIENT_BLOB:
    case CLIENT_BLOB2:
        *csc2type = COMDB2_BLOB;
        gothit = 1;
        break;

    case CLIENT_VUTF8:
        *csc2type = COMDB2_VUTF8;
        gothit = 1;
        break;

    case CLIENT_DATETIME:
        *csc2type = COMDB2_DATETIME;
        gothit = 1;
        break;

    case CLIENT_DATETIMEUS:
        *csc2type = COMDB2_DATETIMEUS;
        gothit = 1;
        break;

    case CLIENT_INTVYM:
        *csc2type = COMDB2_INTERVALYM;
        gothit = 1;
        break;

    case CLIENT_INTVDS:
        *csc2type = COMDB2_INTERVALDS;
        gothit = 1;
        break;

    case CLIENT_INTVDSUS:
        *csc2type = COMDB2_INTERVALDSUS;
        gothit = 1;
        break;
    }

    if (gothit) {
        return 0;
    } else {
        logmsg(LOGMSG_ERROR, "client_type_to_csc2_type_len type=%d len=%d error\n",
                type, inlen);
        return -1;
    }
}

char *csc2type(struct field *f)
{
    int dlen = f->len;

    switch (f->type) {
    case SERVER_BINT:
        dlen--;
    case CLIENT_INT:
        switch (dlen) {
        case 2:
            return "short";
        case 4:
            return "int";
        case 8:
            return "longlong";
        default:
            return NULL;
        }

    case SERVER_UINT:
        dlen--;
    case CLIENT_UINT:
        switch (dlen) {
        case 2:
            return "u_short";
        case 4:
            return "u_int";
        case 8:
            return "u_longlong";
        default:
            return NULL;
        }

    case SERVER_BREAL:
        dlen--;
    case CLIENT_REAL:
        switch (dlen) {
        case 4:
            return "float";
        case 8:
            return "double";
        default:
            return NULL;
        }

    case SERVER_BCSTR:
        dlen--;
    case CLIENT_CSTR:
    case CLIENT_PSTR2:
    case CLIENT_PSTR:
        return "cstring";

    case SERVER_BYTEARRAY:
        dlen--;
    case CLIENT_BYTEARRAY:
        return "byte";

    case CLIENT_BLOB:
    case SERVER_BLOB:
    case CLIENT_BLOB2:
    case SERVER_BLOB2:
        return "blob";

    case CLIENT_VUTF8:
    case SERVER_VUTF8:
        return "vutf8";

    case CLIENT_DATETIME:
        logmsg(LOGMSG_FATAL, "THIS SHOULD NOT RUN! !");
        exit(1);

    case CLIENT_DATETIMEUS:
        logmsg(LOGMSG_FATAL, "THIS SHOULD NOT RUN! !");
        exit(1);

    case SERVER_DATETIME:
        return "datetime";

    case SERVER_DATETIMEUS:
        return "datetimeus";

    case CLIENT_INTVYM:
    case SERVER_INTVYM:
        return "intervalym";

    case CLIENT_INTVDS:
    case SERVER_INTVDS:
        return "intervalds";

    case CLIENT_INTVDSUS:
    case SERVER_INTVDSUS:
        return "intervaldsus";

    case SERVER_DECIMAL:
        switch (dlen) {
        case (sizeof(server_decimal32_t)):
            return "decimal32";
        case (sizeof(server_decimal64_t)):
            return "decimal64";
        case (sizeof(server_decimal128_t)):
            return "decimal128";
        default:
            return NULL;
        }
    }

    return NULL;
}

int server_type_to_client_type_len(int type, int inlen, int *client_type,
                                   int *client_len)
{
    int gothit = 0;

    switch (type) {
    case SERVER_UINT:
        switch (inlen) {
        case 3:
            *client_len = sizeof(unsigned short);
            *client_type = CLIENT_UINT;
            gothit = 1;
            break;
        case 5:
            *client_len = sizeof(unsigned int);
            *client_type = CLIENT_UINT;
            gothit = 1;
            break;
        case 9:
            *client_len = sizeof(unsigned long long);
            *client_type = CLIENT_UINT;
            gothit = 1;
            break;
        }
        break;

    case SERVER_BINT:
        switch (inlen) {
        case 3:
            *client_type = CLIENT_INT;
            *client_len = sizeof(short);
            gothit = 1;
            break;
        case 5:
            *client_type = CLIENT_INT;
            *client_len = sizeof(int);
            gothit = 1;
            break;
        case 9:
            *client_type = CLIENT_INT;
            *client_len = sizeof(long long);
            gothit = 1;
            break;
        }
        break;

    case SERVER_BREAL:
        switch (inlen) {
        case 3:
            *client_type = CLIENT_REAL;
            *client_len = sizeof(float);
            gothit = 1;
            break;
        case 5:
            *client_type = CLIENT_REAL;
            *client_len = sizeof(float);
            gothit = 1;
            break;
        case 9:
            *client_type = CLIENT_REAL;
            *client_len = sizeof(double);
            gothit = 1;
            break;
        }
        break;

    case SERVER_BCSTR:
        *client_type = CLIENT_CSTR;
        *client_len = inlen; /* -1 for flags byte, +1 for null term */
        gothit = 1;
        break;

    case SERVER_BYTEARRAY:
        *client_type = CLIENT_BYTEARRAY;
        *client_len = inlen - 1;
        gothit = 1;
        break;

    case SERVER_BLOB2:
    case SERVER_BLOB:
        *client_type = CLIENT_BLOB;
        *client_len = CLIENT_BLOB_TYPE_LEN;
        gothit = 1;
        break;

    case SERVER_VUTF8:
        *client_type = CLIENT_VUTF8;
        *client_len = CLIENT_BLOB_TYPE_LEN;
        gothit = 1;
        break;

    case SERVER_DATETIME:
        *client_type = CLIENT_DATETIME;
        *client_len = CLIENT_DATETIME_LEN;
        gothit = 1;
        break;

    case SERVER_DATETIMEUS:
        *client_type = CLIENT_DATETIMEUS;
        *client_len = CLIENT_DATETIME_LEN;
        gothit = 1;
        break;

    case SERVER_INTVYM:
        *client_type = CLIENT_INTVYM;
        *client_len = CLIENT_INTV_YM_LEN;
        gothit = 1;
        break;

    case SERVER_INTVDS:
        *client_type = CLIENT_INTVDS;
        *client_len = CLIENT_INTV_DS_LEN;
        gothit = 1;
        break;

    case SERVER_INTVDSUS:
        *client_type = CLIENT_INTVDSUS;
        *client_len = CLIENT_INTV_DS_LEN;
        gothit = 1;
        break;

    case SERVER_DECIMAL:
        *client_type = CLIENT_CSTR;
        switch (inlen) {
        case sizeof(server_decimal32_t):
            gothit = 1;
            *client_len = DFP_32_MAX_STR;
            break;
        case sizeof(server_decimal64_t):
            gothit = 1;
            *client_len = DFP_64_MAX_STR;
            break;
        case sizeof(server_decimal128_t):
            gothit = 1;
            *client_len = DFP_128_MAX_STR;
            break;
        }
        break;
    }

    if (gothit) {
        return 0;
    } else {
        logmsg(LOGMSG_ERROR, 
                "server_type_to_client_type_len: type=%d len=%d error\n", type,
                inlen);
        return -1;
    }
}

int server_type_to_csc2_type_len(int type, int inlen, int *csc2type,
                                 int *csc2len)
{
    int gothit = 0;

    switch (type) {
    case SERVER_UINT:
        switch (inlen) {
        case 3:
            *csc2type = COMDB2_USHORT;
            *csc2len = sizeof(unsigned short);
            gothit = 1;
            break;
        case 5:
            *csc2type = COMDB2_UINT;
            *csc2len = sizeof(unsigned int);
            gothit = 1;
            break;
        case 9:
            *csc2type = COMDB2_ULONGLONG;
            *csc2len = sizeof(unsigned long long);
            gothit = 1;
            break;
        }
        break;

    case SERVER_BINT:
        switch (inlen) {
        case 3:
            *csc2type = COMDB2_SHORT;
            *csc2len = sizeof(short);
            gothit = 1;
            break;
        case 5:
            *csc2type = COMDB2_INT;
            *csc2len = sizeof(int);
            gothit = 1;
            break;
        case 9:
            *csc2type = COMDB2_LONGLONG;
            *csc2len = sizeof(long long);
            gothit = 1;
            break;
        }
        break;

    case SERVER_BREAL:
        switch (inlen) {
        case 5:
            *csc2type = COMDB2_FLOAT;
            *csc2len = sizeof(float);
            gothit = 1;
            break;
        case 9:
            *csc2type = COMDB2_DOUBLE;
            *csc2len = sizeof(double);
            gothit = 1;
            break;
        }
        break;

    case SERVER_BCSTR:
        *csc2type = COMDB2_CSTR;
        *csc2len = inlen; /* + 1 for flags byte, -1 for null term */
        gothit = 1;
        break;

    case SERVER_BYTEARRAY:
        *csc2type = COMDB2_BYTE;
        *csc2len = inlen - 1;
        gothit = 1;
        break;

    case SERVER_BLOB:
        *csc2type = COMDB2_BLOB;
        *csc2len = CLIENT_BLOB_TYPE_LEN;
        gothit = 1;
        break;

    case SERVER_VUTF8:
        *csc2type = COMDB2_VUTF8;
        *csc2len = CLIENT_BLOB_TYPE_LEN;
        gothit = 1;
        break;

    case SERVER_DATETIME:
        *csc2type = COMDB2_DATETIME;
        *csc2len = CLIENT_DATETIME_LEN;
        gothit = 1;
        break;

    case SERVER_DATETIMEUS:
        *csc2type = COMDB2_DATETIMEUS;
        *csc2len = CLIENT_DATETIME_LEN;
        gothit = 1;
        break;

    case SERVER_INTVYM:
        *csc2type = COMDB2_INTERVALYM;
        *csc2len = CLIENT_INTV_YM_LEN;
        gothit = 1;
        break;

    case SERVER_INTVDS:
        *csc2type = COMDB2_INTERVALDS;
        *csc2len = CLIENT_INTV_DS_LEN;
        gothit = 1;
        break;

    case SERVER_INTVDSUS:
        *csc2type = COMDB2_INTERVALDSUS;
        *csc2len = CLIENT_INTV_DS_LEN;
        gothit = 1;
        break;

    case SERVER_DECIMAL:
        *csc2type = COMDB2_CSTR;
        switch (inlen) {
        case sizeof(server_decimal32_t):
            gothit = 1;
            *csc2len = DFP_32_MAX_STR;
            break;
        case sizeof(server_decimal64_t):
            gothit = 1;
            *csc2len = DFP_64_MAX_STR;
            break;
        case sizeof(server_decimal128_t):
            gothit = 1;
            *csc2len = DFP_128_MAX_STR;
            break;
        }
        break;
    }

    if (gothit) {
        return 0;
    } else {
        logmsg(LOGMSG_ERROR, 
                "server_type_to_csc2_type_len type=%d len=%d error\n",
                type, inlen);
        return -1;
    }
}

#define MAX_SERVER_FUNCTION_LEN 256

int max_type_size(int type, int len)
{
    switch (type) {
    case CLIENT_UINT:
    case CLIENT_INT:
        return sizeof(long long);
    case SERVER_UINT:
    case SERVER_BINT:
        return sizeof(long long) + 1;
    case CLIENT_REAL:
        return sizeof(double);
    case SERVER_BREAL:
        return sizeof(double) + 1;
    case SERVER_BLOB:
    case SERVER_VUTF8:
        return sizeof(unsigned int) + 1;
    case SERVER_DATETIME:
        return sizeof(server_datetime_t);
    case SERVER_DATETIMEUS:
        return sizeof(server_datetimeus_t);
    case SERVER_INTVYM:
        return sizeof(server_intv_ym_t);
    case SERVER_INTVDS:
        return sizeof(server_intv_ds_t);
    case SERVER_INTVDSUS:
        return sizeof(server_intv_dsus_t);
    case SERVER_FUNCTION:
        return MAX_SERVER_FUNCTION_LEN;
    default:
        return len;
    }
}

const char *strtype(int type)
{
    switch (type) {
    case CLIENT_UINT:
        return "CLIENT_UINT";
    case CLIENT_INT:
        return "CLIENT_INT";
    case CLIENT_REAL:
        return "CLIENT_REAL";
    case CLIENT_CSTR:
        return "CLIENT_CSTR";
    case CLIENT_PSTR:
        return "CLIENT_PSTR";
    case CLIENT_PSTR2:
        return "CLIENT_PSTR2";
    case CLIENT_BYTEARRAY:
        return "CLIENT_BYTEARRAY";
    case CLIENT_BLOB:
        return "CLIENT_BLOB";
    case CLIENT_DATETIME:
        return "CLIENT_DATETIME";
    case CLIENT_DATETIMEUS:
        return "CLIENT_DATETIMEUS";
    case CLIENT_INTVYM:
        return "CLIENT_INTVYM";
    case CLIENT_INTVDS:
        return "CLIENT_INTVDS";
    case CLIENT_INTVDSUS:
        return "CLIENT_INTVDSUS";
    case CLIENT_VUTF8:
        return "CLIENT_VUTF8";
    case SERVER_UINT:
        return "SERVER_UINT";
    case SERVER_BINT:
        return "SERVER_BINT";
    case SERVER_BREAL:
        return "SERVER_BREAL";
    case SERVER_BCSTR:
        return "SERVER_BCSTR";
    case SERVER_BYTEARRAY:
        return "SERVER_BYTEARRAY";
    case SERVER_BLOB:
        return "SERVER_BLOB";
    case SERVER_DATETIME:
        return "SERVER_DATETIME";
    case SERVER_DATETIMEUS:
        return "SERVER_DATETIMEUS";
    case SERVER_INTVYM:
        return "SERVER_INTVYM";
    case SERVER_INTVDS:
        return "SERVER_INTVDS";
    case SERVER_INTVDSUS:
        return "SERVER_INTVDSUS";
    case SERVER_VUTF8:
        return "SERVER_VUTF8";
    case SERVER_DECIMAL:
        return "SERVER_DECIMAL";
    default:
        return "???";
    }
}

int get_size_of_schema(const struct schema *sc)
{
    int sz;
    struct field *last_field = &sc->member[sc->nmembers - 1];
    sz = last_field->offset + last_field->len;
    if (!(sc->flags & SCHEMA_TABLE))
        return sz;

    /* if it's a table, fiddle with size so alignment rules match cmacc */
    /* actually, we might have this information from csc2lib, in which case
     * just believe him. -- Sam J */
    if (sc->recsize > 0)
        return sc->recsize;

    /* we wind up in here for dynamic schemas */
    int maxalign = 0;
    for (int idx = 0; idx < sc->nmembers; idx++) {
        if ((sc->member[idx].type < SERVER_MINTYPE ||
             sc->member[idx].type > SERVER_MAXTYPE) &&
            sc->member[idx].type != CLIENT_CSTR &&
            sc->member[idx].type != CLIENT_PSTR &&
            sc->member[idx].type != CLIENT_PSTR2 &&
            sc->member[idx].type != CLIENT_BYTEARRAY &&
            sc->member[idx].len > maxalign) {
            maxalign = sc->member[idx].len;
        }
    }

    if (maxalign && (sz % maxalign) )
        sz += (maxalign - sz % maxalign);

    return sz;
}

int get_size_of_schema_by_name(struct dbtable *table, const char *schema)
{
    struct schema *sc = find_tag_schema(table, schema);
    if (sc == NULL)
        return 0;
    return get_size_of_schema(sc);
}

static void dumpval(char *buf, int type, int len);

int dump_tag(const struct schema *s, void *dum)
{
    int field, ix, nix, nfields;

    if (s->flags & SCHEMA_INDEX)
        return 0;

    logmsg(LOGMSG_USER, "%s:\n", s->tag);
    nfields = s->nmembers;
    for (field = 0; field < nfields; field++) {
        struct field *f = &s->member[field];
        logmsg(LOGMSG_USER, "   dta   %3d %s type=%s offset=%u len=%d idx=%d nulls_ok=%s",
               field + 1, f->name, strtype(f->type), f->offset, f->len, f->idx,
               YESNO(!(f->flags & NO_NULL)));
        if (f->in_default) {
            logmsg(LOGMSG_USER, " in_default=");
            dumpval(f->in_default, f->in_default_type, f->in_default_len);
        } else {
            logmsg(LOGMSG_USER, " in_default=NULL");
        }

        if (f->out_default) {
            logmsg(LOGMSG_USER, " out_default=");
            dumpval(f->out_default, f->out_default_type, f->out_default_len);
        } else {
            logmsg(LOGMSG_USER, " out_default=NULL");
        }
        logmsg(LOGMSG_USER, "\n");
    }

    nix = s->nix;
    for (ix = 0; ix < nix; ix++) {
        struct schema *is = s->ix[ix];
        nfields = is->nmembers;
        for (field = 0; field < nfields; field++) {
            struct field *f = &is->member[field];
            logmsg(LOGMSG_USER, "   ix%-3d %3d %s type=%s offset=%u len=%d idx=%d%s\n", ix,
                   field, f->name, strtype(f->type), f->offset, f->len, f->idx,
                   f->flags & INDEX_DESCEND ? " descending" : " ascending");
        }
    }

    return 0;
}

void debug_dump_tags(const char *tblname)
{
    struct dbtag *tag;

    lock_taglock_read();
    logmsg(LOGMSG_USER, "TABLE %s\n", tblname);
    tag = hash_find_readonly(gbl_tag_hash, &tblname);
    if (tag == NULL) {
        unlock_taglock();
        return;
    }
    hash_for(tag->tags, (hashforfunc_t *)dump_tag, NULL);
    unlock_taglock();
}

static int _count_tag(const struct schema *s, void *count)
{
    *(int*)count += 1;
    return 0;
}

static int _count_tag_cols(const struct schema *s, void *count)
{
    *(int*)count += s->nmembers;
    return 0;
}

int get_table_tags_count(const char *tblname, int columns)
{
    struct dbtag *tag;
    int count = 0;

    lock_taglock_read();
    tag = hash_find_readonly(gbl_tag_hash, &tblname);
    if (tag == NULL) {
        unlock_taglock();
        return 0;
    }
    if (!columns)
        hash_for(tag->tags, (hashforfunc_t *)_count_tag, &count);
    else
        hash_for(tag->tags, (hashforfunc_t *)_count_tag_cols, &count);

    unlock_taglock();

    return count;
}

struct tagc {
    int num;
    int maxnum;
    void *p;
    const char *tblname;
    int columns;
};

struct systable_tag;
struct systable_tag_col;
extern int systable_set_tag(struct systable_tag *, int idx, const char *,
                             const struct  schema *);
extern int systable_set_tag_cols(struct systable_tag_col *, int, int,
                                 const char *, const struct  schema *);

static int _save_tags(const struct schema *s, void *pt)
{
    struct tagc *t = (struct tagc*)pt;


    if (!t->columns) {
        if (t->num >= t->maxnum)
            return -1;
        t->num += systable_set_tag(t->p, t->num, t->tblname, s);
    } else {
        if ((t->num + s->nmembers - 1)  >= t->maxnum)
            return -1;
        t->num += systable_set_tag_cols(t->p, t->num, t->maxnum, t->tblname, s);
    }
    
    return 0;
}

int get_table_tags(const char *tblname, void *p, int columns, int maxn)
{
    struct tagc t = {0};
    struct dbtag *tag;

    t.maxnum = maxn;
    t.p = p;
    t.tblname = tblname;
    t.columns = columns;

    lock_taglock_read();

    tag = hash_find_readonly(gbl_tag_hash, &tblname);
    if (tag == NULL) {
        unlock_taglock();
        return 0;
    }

    hash_for(tag->tags, (hashforfunc_t *)_save_tags, &t);

    unlock_taglock();

    return t.num;
}

static void dumpval(char *buf, int type, int len)
{
    int slen;
    /* variables for all possible types */
    long long lval;
    int ival;
    short sval;
    unsigned long long ulval;
    unsigned int uival;
    unsigned short usval;
    float fval;
    double dval;
    int8b blval;
    int4b bival;
    int2b bsval;
    ieee4b bfval;
    ieee8b bdval;
    int i;

    logmsg(LOGMSG_USER, "    ");
    if (stype_is_null(buf))
        logmsg(LOGMSG_USER, "NULL");
    else {
        switch (type) {
        case CLIENT_UINT:
            switch (len) {
            case 2:
                memcpy(&usval, buf, len);
                usval = ntohs(usval);
                logmsg(LOGMSG_USER, "%hu", usval);
                break;
            case 4:
                memcpy(&uival, buf, len);
                uival = ntohl(uival);
                logmsg(LOGMSG_USER, "%u", uival);
                break;
            case 8:
                memcpy(&ulval, buf, len);
                ulval = flibc_ntohll(ulval);
                logmsg(LOGMSG_USER, "%llu", ulval);
                break;
            default:
                logmsg(LOGMSG_USER, "ERROR: invalid CLIENT_UINT length\n");
            }
            break;
        case CLIENT_INT:
            switch (len) {
            case 2:
                memcpy(&sval, buf, len);
                sval = ntohs(sval);
                logmsg(LOGMSG_USER, "%hd", sval);
                break;
            case 4:
                memcpy(&ival, buf, len);
                ival = ntohl(ival);
                logmsg(LOGMSG_USER, "%d", ival);
                break;
            case 8:
                memcpy(&lval, buf, len);
                lval = flibc_ntohll(lval);
                logmsg(LOGMSG_USER, "%lld", lval);
                break;
            default:
                logmsg(LOGMSG_ERROR, "ERROR: invalid CLIENT_INT length\n");
            }
            break;
        case CLIENT_REAL:
            switch (len) {
            case 4:
                memcpy(&fval, buf, len);
                fval = flibc_ntohf(fval);
                logmsg(LOGMSG_USER, "%f", (double)fval);
                break;
            case 8:
                memcpy(&dval, buf, len);
                dval = flibc_ntohd(dval);
                logmsg(LOGMSG_USER, "%f", dval);
                break;
            default:
                logmsg(LOGMSG_ERROR, "ERROR: invalid CLIENT_REAL length\n");
            }
            break;
        case CLIENT_CSTR:
            logmsg(LOGMSG_USER, "\"%s\"", buf);
            break;
        case CLIENT_PSTR:
            slen = pstrlenlim(buf, len);
            logmsg(LOGMSG_USER, "\"%.*s\"", slen, buf);
            break;
        case CLIENT_PSTR2:
            slen = len;
            logmsg(LOGMSG_USER, "\"%.*s\"", slen, buf);
            break;
        case CLIENT_BYTEARRAY:
            logmsg(LOGMSG_USER, "0x");
            for (i = 0; i < len; i++)
                logmsg(LOGMSG_USER, "%2.2x", buf[i] & 0xff);
            break;
        case CLIENT_BLOB: {
            const struct client_blob_type *cblob =
                (const struct client_blob_type *)buf;
            if (cblob->notnull)
                logmsg(LOGMSG_USER, "<blob of %u bytes>", ntohl(cblob->length));
            else
                logmsg(LOGMSG_USER, "NULL");
            break;
        }
        case CLIENT_DATETIME: {
            cdb2_client_datetime_t dt;
            client_datetime_get(&dt, (uint8_t *)buf,
                                (uint8_t *)buf +
                                    sizeof(cdb2_client_datetime_t));
            logmsg(LOGMSG_USER, "%4.4u-%2.2u-%2.2uT%2.2u%2.2u%2.2u.%3.3u %s", dt.tm.tm_year,
                   dt.tm.tm_mon, dt.tm.tm_mday, dt.tm.tm_hour, dt.tm.tm_min,
                   dt.tm.tm_sec, dt.msec, dt.tzname);
            break;
        }

        case CLIENT_DATETIMEUS: {
            cdb2_client_datetimeus_t dt;
            client_datetimeus_get(&dt, (uint8_t *)buf,
                                  (uint8_t *)buf +
                                      sizeof(cdb2_client_datetimeus_t));
            logmsg(LOGMSG_USER, "%4.4u-%2.2u-%2.2uT%2.2u%2.2u%2.2u.%6.6u %s", dt.tm.tm_year,
                   dt.tm.tm_mon, dt.tm.tm_mday, dt.tm.tm_hour, dt.tm.tm_min,
                   dt.tm.tm_sec, dt.usec, dt.tzname);
            break;
        }

        case CLIENT_INTVYM: {
            cdb2_client_intv_ym_t ym;
            client_intv_ym_get(&ym, (uint8_t *)buf,
                               (uint8_t *)buf + sizeof(cdb2_client_intv_ym_t));
            logmsg(LOGMSG_USER, "%s%u-%u", (ym.sign < 0) ? "- " : "", ym.years, ym.months);
            break;
        }

        case CLIENT_INTVDS: {
            cdb2_client_intv_ds_t ds;
            client_intv_ds_get(&ds, (uint8_t *)buf,
                               (uint8_t *)buf + sizeof(cdb2_client_intv_ds_t));
            logmsg(LOGMSG_USER, "%s%u %u:%u:%u.%u", (ds.sign < 0) ? "- " : "", ds.days,
                   ds.hours, ds.mins, ds.sec, ds.msec);
            break;
        }

        case CLIENT_INTVDSUS: {
            cdb2_client_intv_dsus_t ds;
            client_intv_dsus_get(&ds, (uint8_t *)buf,
                                 (uint8_t *)buf +
                                     sizeof(cdb2_client_intv_dsus_t));
            logmsg(LOGMSG_USER, "%s%u %u:%u:%u.%6.6u", (ds.sign < 0) ? "- " : "", ds.days,
                   ds.hours, ds.mins, ds.sec, ds.usec);
            break;
        }

        case SERVER_UINT:
            switch (len) {
            case 3:
                memcpy(&usval, ((char *)buf) + 1, len - 1);
                usval = ntohs(usval);
                logmsg(LOGMSG_USER, "%hu", usval);
                break;
            case 5:
                memcpy(&uival, ((char *)buf) + 1, len - 1);
                uival = ntohl(uival);
                logmsg(LOGMSG_USER, "%u", uival);
                break;
            case 9:
                memcpy(&ulval, ((char *)buf) + 1, len - 1);
                ulval = flibc_ntohll(ulval);
                logmsg(LOGMSG_USER, "%llu", ulval);
                break;
            default:
                logmsg(LOGMSG_ERROR, "Invalid biased unsigned int length\n");
                break;
            }
            break;
        case SERVER_BINT:
            switch (len) {
            case 3:
                memcpy(&bsval, ((char *)buf) + 1, len - 1);
                bsval = ntohs(bsval);
                int2b_to_int2(bsval, &sval);
                logmsg(LOGMSG_USER, "%hd", sval);
                break;
            case 5:
                memcpy(&bival, ((char *)buf) + 1, len - 1);
                bival = ntohl(bival);
                int4b_to_int4(bival, &ival);
                logmsg(LOGMSG_USER, "%d", ival);
                break;
            case 9:
                memcpy(&blval, ((char *)buf) + 1, len - 1);
                blval = flibc_ntohll(blval);
                int8b_to_int8(blval, &lval);
                logmsg(LOGMSG_USER, "%lld", lval);
                break;
            default:
                logmsg(LOGMSG_USER, "Invalid biased int length\n");
                break;
            }
            break;
        case SERVER_BREAL:
            switch (len) {
            case 5:
                memcpy(&bfval, ((char *)buf) + 1, len - 1);
                bfval = ntohl(bfval);
                ieee4b_to_ieee4(bfval, &fval);
                logmsg(LOGMSG_USER, "%f", (double)fval);
                break;
            case 9:
                memcpy(&bdval, ((char *)buf) + 1, len - 1);
                bdval = flibc_ntohll(bdval);
                ieee8b_to_ieee8(bdval, &dval);
                logmsg(LOGMSG_USER, "%f", dval);
                break;
            default:
                logmsg(LOGMSG_ERROR, "Invalid biased real length\n");
                break;
            }
            break;
        case SERVER_BCSTR:
            logmsg(LOGMSG_USER, "\"%.*s\"", len, ((char *)buf) + 1);
            break;
        case SERVER_BYTEARRAY:
            logmsg(LOGMSG_USER, "0x");
            for (i = 1; i < len; i++)
                logmsg(LOGMSG_USER, "%2.2x", buf[i] & 0xff);
            break;
        case SERVER_BLOB:
            logmsg(LOGMSG_USER, "<blob of %u bytes>", *((int *)(buf + 1)));
            break;
        case SERVER_DATETIME: {
            server_datetime_t dt;
            server_datetime_get(&dt, (uint8_t *)buf,
                                (uint8_t *)buf + sizeof(server_datetime_t));
            logmsg(LOGMSG_USER, "%llu.%hu", dt.sec, dt.msec);
            break;
        }
        case SERVER_DATETIMEUS: {
            server_datetimeus_t dt;
            server_datetimeus_get(&dt, (uint8_t *)buf,
                                  (uint8_t *)buf + sizeof(server_datetimeus_t));
            logmsg(LOGMSG_USER, "%llu.%u", dt.sec, dt.usec);
            break;
        }
        case SERVER_INTVYM: {
            server_intv_ym_t si;
            server_intv_ym_get(&si, (uint8_t *)buf,
                               (uint8_t *)buf + sizeof(server_intv_ym_t));
            logmsg(LOGMSG_USER, "%d", si.months);
            break;
        }
        case SERVER_INTVDS: {
            server_intv_ds_t ds;
            server_intv_ds_get(&ds, (uint8_t *)buf,
                               (uint8_t *)buf + sizeof(server_intv_ds_t));
            logmsg(LOGMSG_USER, "%lld.%hu", ds.sec, ds.msec);
            break;
        }
        case SERVER_INTVDSUS: {
            server_intv_dsus_t ds;
            server_intv_dsus_get(&ds, (uint8_t *)buf,
                                 (uint8_t *)buf + sizeof(server_intv_dsus_t));
            logmsg(LOGMSG_USER, "%lld.%u", ds.sec, ds.usec);
            break;
        }
        case SERVER_VUTF8:
            logmsg(LOGMSG_USER, "<vutf8 of %u bytes>", *((int *)(buf + 1)));
            break;
        default:
            logmsg(LOGMSG_ERROR, "Invalid type %d\n", type);
            break;
        }
    }
}

char *typestr(int type, int len)
{
    switch (type) {
    case CLIENT_UINT:
        if (len == 2)
            return "uint2";
        else if (len == 4)
            return "uint4";
        else if (len == 8)
            return "uint8";
        else
            return "uint?";
    case CLIENT_INT:
        if (len == 2)
            return "int2";
        else if (len == 4)
            return "int4";
        else if (len == 8)
            return "int8";
        else
            return "int?";
    case CLIENT_REAL:
        if (len == 4)
            return "float";
        else if (len == 8)
            return "double";
        else
            return "real?";
    case CLIENT_CSTR:
        return "cstring";
    case CLIENT_PSTR:
        return "pstring";
    case CLIENT_PSTR2:
        return "pstring2";
    case CLIENT_BYTEARRAY:
        return "bytearray";
    case CLIENT_BLOB:
        return "blob";
    case CLIENT_DATETIME:
        return "datetime";
    case CLIENT_DATETIMEUS:
        return "datetimeus";
    case CLIENT_INTVYM:
        return "intervalym";
    case CLIENT_INTVDS:
        return "intervalds";
    case CLIENT_INTVDSUS:
        return "intervaldsus";
    case SERVER_UINT:
        if (len == 3)
            return "buint2";
        else if (len == 5)
            return "buint4";
        else if (len == 9)
            return "buint8";
        else
            return "buint?";
    case SERVER_BINT:
        if (len == 3)
            return "bint2";
        else if (len == 5)
            return "bint4";
        else if (len == 9)
            return "bint8";
        else
            return "bint?";
    case SERVER_BREAL:
        if (len == 5)
            return "bfloat";
        else if (len == 9)
            return "bdouble";
        else
            return "breal?";
    case SERVER_BCSTR:
        return "bcstring";
    case SERVER_BYTEARRAY:
        return "bytearray";
    case SERVER_BLOB:
    case SERVER_BLOB2:
        return "blob";
    case SERVER_DATETIME:
        return "datetime";
    case SERVER_DATETIMEUS:
        return "datetimeus";
    case SERVER_INTVYM:
        return "intervalym";
    case SERVER_INTVDS:
        return "intervalds";
    case SERVER_INTVDSUS:
        return "intervaldsus";
    case SERVER_VUTF8:
        return "vutf8";
    case SERVER_DECIMAL:
        return "decimal";
    default:
        return "???";
        break;
    }
}

void dump_tagged_buf_with_schema(struct schema *sc, const unsigned char *buf)
{
    int i;
    struct field *f;

    for (i = 0; i < sc->nmembers; i++) {
        f = &sc->member[i];
        logmsg(LOGMSG_USER, "   [%3d - %10s] %s: ", i, typestr(f->type, f->len), f->name);
        dumpval((char *)buf + f->offset, f->type, f->len);
        logmsg(LOGMSG_USER, "\n");
    }
}

void dump_tagged_buf(struct dbtable *table, const char *tag,
                     const unsigned char *buf)
{
    struct schema *sc = find_tag_schema(table, tag);
    if (sc == NULL)
        return;
    dump_tagged_buf_with_schema(sc, buf);
}

/* NOTE: tag is already strdup-ed
 * NOTE2: tag is NULL for bound parameters
 */
struct schema * alloc_schema(char *tag, int nmembers, int flags)
{
    struct schema *to;

    to = calloc(1, sizeof(struct schema));
    if (!to) {
        free(tag);
        return NULL;
    }
    to->tag = tag;
    to->nmembers = nmembers;
    to->member = calloc(to->nmembers, sizeof(struct field));
    if (!to->member) {
        free(to->tag);
        free(to);
        return NULL;
    }
    to->flags = flags;
    return to;
}

/* used to clone ONDISK to ONDISK_CLIENT */
struct schema *clone_server_to_client_tag(struct schema *from, const char *newtag)
{
    struct schema *to = NULL;
    int field, offset;
    struct field *from_field, *to_field;
    int rc;

    char *tmp = strdup(newtag);
    if (tmp)
        to = alloc_schema(tmp, from->nmembers, from->flags);
    if (!to)
        return NULL;
    to->numblobs = from->numblobs;
    offset = 0;
    for (field = 0; field < from->nmembers; field++) {
        from_field = &from->member[field];
        to_field = &to->member[field];

        to->member[field].idx = -1;
        to->member[field].name = strdup(from->member[field].name);
        to->member[field].flags = from->member[field].flags;

        rc = server_type_to_client_type_len(
            from->member[field].type, from->member[field].len,
            &to->member[field].type, (int *)&to->member[field].len);

        if (rc != 0) {
            logmsg(LOGMSG_ERROR, "%s: severe error - returning\n", __func__);
            goto err;
        }
        to->member[field].offset = offset;
        offset += to->member[field].len;

        to_field->blob_index = from_field->blob_index;

        /* do not clone out_default/in_default - those are only used for
         * .ONDISK tag itself */
    }

    return to;
err:
    freeschema(to, 0);
    return NULL;
}

/* Given a partial string, find the length of a key.
   Partial string MUST be a field prefix. */
int partial_key_length(struct dbtable *table, const char *keyname,
                       const char *pstring, int len)
{
    int tlen, toff = 0;
    char *tok;
    int fldnum = 0;
    int klen = 0;
    char *s;
    int plen;
    int is_last = 0;

    struct schema *sc = find_tag_schema(table, keyname);
    if (sc == NULL)
        return -1;

    tok = segtokx((char *)pstring, len, &toff, &tlen, "+");
    while (tlen > 0) {
        if (is_last) /* previous field was partial and this one follows */
            return -1;
        if (fldnum >= sc->nmembers)
            return -1;
        s = strnchr(tok, tlen, ':');
        if (s) {
            /* only string and byte arrays can be partial */
            if (sc->member[fldnum].type != SERVER_BCSTR &&
                sc->member[fldnum].type != SERVER_BYTEARRAY)
                return -1;
            s++;
            plen = toknum(s, tlen - (s - tok));
            tlen = s - tok - 1;
            if (plen <= 0 || plen >= sc->member[fldnum].len)
                return -1;
            klen += (plen + 1); /* +1 for ondisk extra byte */
            is_last = 1;
        } else
            klen += sc->member[fldnum].len;
        /* is it the next field in the index? */
        if (strncasecmp(sc->member[fldnum].name, tok, tlen))
            return -1;
        fldnum++;
        tok = segtokx((char *)pstring, len, &toff, &tlen, "+");
    }
    return klen;
}

int client_keylen_to_server_keylen(struct dbtable *table, const char *tag,
                                   int ixnum, int keylen)
{
    char skeytag[MAXTAGLEN];
    struct schema *from, *to;
    int fnum;
    int slen = 0;

    snprintf(skeytag, MAXTAGLEN, ".ONDISK_ix_%d", ixnum);

    from = find_tag_schema(table, tag);
    if (from == NULL)
        return -1;

    to = find_tag_schema(table, skeytag);
    if (to == NULL)
        return -1;

    if (keylen > from->member[from->nmembers - 1].offset +
                     from->member[from->nmembers - 1].len)
        return -1;

    /* shouldn't happen */
    if (from->nmembers != to->nmembers)
        return -1;

    /* find last field */
    for (fnum = 0; fnum < from->nmembers; fnum++) {
        if (from->member[fnum].offset + from->member[fnum].len > keylen) {
            if (from->member[fnum].offset < keylen) {
                if (to->member[fnum].type != SERVER_BCSTR &&
                    to->member[fnum].type != SERVER_BYTEARRAY)
                    return -1;
                slen += keylen - from->member[fnum].offset;
            }
            break;
        } else
            slen += to->member[fnum].len;
    }

    return slen;
}

static int copy_partial_client_buf(const char *inbuf, int len, int isnull,
                                   char *outbuf)
{
    if (isnull)
        set_null(outbuf, len + 1);
    else
        set_data(outbuf, inbuf, len + 1);
    return 0;
}

/* forward */
static int _stag_to_stag_buf_flags_blobs(const struct dbtable *tbl, struct schema *fromsch,
                                         struct schema *tosch, const char *inbuf, char *outbuf,
                                         int flags, struct convert_failure *fail_reason, blob_buffer_t *inblobs,
                                         blob_buffer_t *outblobs, int maxblobs, const char *tzname);

int ctag_to_stag_buf(struct dbtable *table, const char *ctag, const char *inbuf,
                     int len, const unsigned char *innulls, const char *stag,
                     void *outbufp, int flags,
                     struct convert_failure *fail_reason)
{
    return ctag_to_stag_int(table, ctag, inbuf, len, innulls, stag, outbufp,
                            flags, WHOLE_BUFFER, fail_reason, NULL, NULL, 0,
                            NULL);
}

int ctag_to_stag_buf_tz(struct dbtable *table, const char *ctag, const char *inbuf,
                        int len, const unsigned char *innulls, const char *stag,
                        void *outbufp, int flags,
                        struct convert_failure *fail_reason, const char *tzname)
{
    return ctag_to_stag_int(table, ctag, inbuf, len, innulls, stag, outbufp,
                            flags, WHOLE_BUFFER, fail_reason, NULL, NULL, 0,
                            tzname);
}

static int _ctag_to_stag_blobs(struct dbtable *table, const char *ctag,
                               const char *inbuf, int len,
                               const unsigned char *innulls, const char *stag,
                               void *outbufp, int flags,
                               struct convert_failure *fail_reason,
                               blob_buffer_t *blobs, int maxblobs,
                               const char *tzname)
{
    int rc;
    blob_buffer_t newblobs[MAXBLOBS];
    if (!blobs || maxblobs != MAXBLOBS) {
        logmsg(LOGMSG_ERROR, "ctag_to_stag_blobs with no blobs maxblobs=%d!\n",
                maxblobs);
        return -1;
    }
    bzero(newblobs, sizeof(newblobs));
    rc = ctag_to_stag_int(table, ctag, inbuf, len, innulls, stag, outbufp,
                          flags, WHOLE_BUFFER, fail_reason, blobs, newblobs,
                          maxblobs, tzname);
    free_blob_buffers(blobs, maxblobs);
    if (rc < 0)
        free_blob_buffers(newblobs, MAXBLOBS);
    else
        memcpy(blobs, newblobs, sizeof(newblobs));
    return rc;
}

int ctag_to_stag_blobs(struct dbtable *table, const char *ctag, const char *inbuf,
                       int len, const unsigned char *innulls, const char *stag,
                       void *outbufp, int flags,
                       struct convert_failure *fail_reason,
                       blob_buffer_t *blobs, int maxblobs)
{

    return _ctag_to_stag_blobs(table, ctag, inbuf, len, innulls, stag, outbufp,
                               flags, fail_reason, blobs, maxblobs, NULL);
}

int ctag_to_stag_blobs_tz(struct dbtable *table, const char *ctag,
                          const char *inbuf, int len,
                          const unsigned char *innulls, const char *stag,
                          void *outbufp, int flags,
                          struct convert_failure *fail_reason,
                          blob_buffer_t *blobs, int maxblobs,
                          const char *tzname)
{

    return _ctag_to_stag_blobs(table, ctag, inbuf, len, innulls, stag, outbufp,
                               flags, fail_reason, blobs, maxblobs, tzname);
}

int ctag_to_stag_buf_p(struct dbtable *table, const char *ctag, const char *inbuf,
                       int len, const unsigned char *innulls, const char *stag,
                       void *outbufp, int flags, int ondisk_lim,
                       struct convert_failure *fail_reason)
{
    return ctag_to_stag_int(table, ctag, inbuf, len, innulls, stag, outbufp,
                            flags, ondisk_lim, fail_reason, NULL, NULL, 0,
                            NULL);
}

int ctag_to_stag_buf_p_tz(struct dbtable *table, const char *ctag,
                          const char *inbuf, int len,
                          const unsigned char *innulls, const char *stag,
                          void *outbufp, int flags, int ondisk_lim,
                          struct convert_failure *fail_reason,
                          const char *tzname)
{
    return ctag_to_stag_int(table, ctag, inbuf, len, innulls, stag, outbufp,
                            flags, ondisk_lim, fail_reason, NULL, NULL, 0,
                            tzname);
}

void init_convert_failure_reason(struct convert_failure *fail_reason)
{
    fail_reason->reason = CONVERT_OK;
    fail_reason->source_field_idx = -1;
    fail_reason->target_field_idx = -1;
    fail_reason->source_sql_field_flags = 0;
    fail_reason->source_schema = NULL;
    fail_reason->target_schema = NULL;
}

void convert_failure_reason_str(const struct convert_failure *reason,
                                const char *table, const char *fromtag,
                                const char *totag, char *out, size_t outlen)
{
    const char *str = "?";
    int len;

    switch (reason->reason) {
    case CONVERT_FAILED_INVALID_INPUT_TAG:
        snprintf(out, outlen, "invalid input tag");
        return;
    case CONVERT_FAILED_INPUT_TAG_HAS_INVALID_FIELD:
        str = "invalid field in input tag";
        break;
    case CONVERT_FAILED_INVALID_OUTPUT_TAG:
        snprintf(out, outlen, "invalid output tag");
        return;
    case CONVERT_FAILED_INVALID_LENGTH:
        str = "invalid length";
        break;
    case CONVERT_FAILED_NULL_CONSTRAINT_VIOLATION:
        str = "null constraint violation";
        break;
    case CONVERT_FAILED_SHOULDNT_HAPPEN_1:
        str = "shouldn't happen 1";
        break;
    case CONVERT_FAILED_INCOMPATIBLE_VALUES:
        str = "incompatible values";
        break;
    case CONVERT_FAILED_SHOULDNT_HAPPEN_2:
        str = "shouldn't happen 2";
        break;
    case CONVERT_FAILED_INVALID_PARTIAL_TYPE:
        str = "invalid partial type";
        break;
    case CONVERT_FAILED_BAD_BLOB_PROGRAMMER:
        str = "bad blob programming";
        break;
    case CONVERT_FAILED_BLOB_SIZE:
        str = "blob size exceeds max";
        break;
    case CONVERT_OK:
        str = "no error";
        break;
    default:
        str = "unknown failure code";
        logmsg(LOGMSG_ERROR, "convert_failure_reason_str: reason->reason=%d\n",
                reason->reason);
        break;
    }

    len = snprintf(out, outlen, "%s", str);
    out += len;
    if (outlen <= len)
        return;
    outlen -= len;

    if (reason->source_schema && reason->source_field_idx >= 0 &&
        reason->source_field_idx <= reason->source_schema->nmembers) {
        const struct field *field =
            &reason->source_schema->member[reason->source_field_idx];
        len = snprintf(out, outlen, " from %s field '%s'",
                       typestr(field->type, field->len), field->name);
        out += len;
        outlen -= len;
    }

    len = convert_sql_failure_reason_str(reason, out, outlen);
    out += len;
    if (outlen <= len)
        return;
    outlen -= len;

    if (reason->target_schema && reason->target_field_idx >= 0 &&
        reason->target_field_idx <= reason->target_schema->nmembers) {
        const struct field *field =
            &reason->target_schema->member[reason->target_field_idx];
        len = snprintf(out, outlen, " to %s field '%s'",
                       typestr(field->type, field->len), field->name);
        out += len;
        if (outlen <= len)
            return;
        outlen -= len;
    }

    if (table) {
        len = snprintf(out, outlen, " for table '%s'", table);
        out += len;
        if (outlen <= len)
            return;
        outlen -= len;
    }
}

void print_verbose_convert_failure(struct ireq *iq,
                                   const struct convert_failure *fail_reason,
                                   char *fromtag, char *totag)
{
    char str[256];

    if (!iq->debug || fail_reason == NULL || fail_reason->reason == CONVERT_OK)
        return;

    convert_failure_reason_str(fail_reason, iq->usedb->tablename, fromtag,
                               totag, str, sizeof(str));
    reqprintf(iq, "convert: %s\n", str);
    return;
}


/*
 * Detect which blobs appear in this static tag, and set them to 'exists', and
 * the length to 0.  Previously static tag api differentiated between NULL and
 * 0-length blobs via the 'ptr' element in the client-side blob (a NULL ptr
 * signified a NULL blob).  This is a special case which we want to eliminate.
 */
int static_tag_blob_conversion(const struct schema *scm, void *record, blob_buffer_t *blobs, size_t maxblobs)
{
    struct field *fld;
    client_blob_tp *clb;
    int ii;

    /* Return immediately if we don't have both lrl blob-fix options. */
    if (!gbl_disallow_null_blobs || !gbl_force_notnull_static_tag_blobs) {
        return 0;
    }

    /* If this is dynamic the blob descriptor is correct already. */
    if (scm->flags & SCHEMA_DYNAMIC) {
        return 0;
    }

    /* If this is an ondisk then we are in an sql statement.  */
    if (0 == strcmp(scm->tag, ".ONDISK")) {
        return 0;
    }

    /* Examine each field. */
    for (ii = 0; ii < scm->nmembers; ii++) {
        fld = &scm->member[ii];

        /* Continue if this isn't a blob-field. */
        if (fld->blob_index < 0) {
            continue;
        }

        /* Make sure this is in range. */
        if (fld->blob_index >= maxblobs) {
            continue;
        }

        /* If this already exists don't modify. */
        if (blobs[fld->blob_index].exists) {
            continue;
        }

        /* This blob must exist. */
        blobs[fld->blob_index].exists = 1;

        /* The length is 0. */
        blobs[fld->blob_index].length = 0;

        /* Set the data to NULL. */
        blobs[fld->blob_index].data = NULL;

        /* Find offset into record. */
        clb = (client_blob_tp *)(((uint8_t *)record) + fld->offset);

        /* Must not be null. */
        clb->notnull = 1;
    }
    return 0;
}

/* Form server side record from client record.
*
* Inputs:
*  table
*  ctag            - Client tag name.
*  inbuf           - Client record.
*  len             - Length of the client record to convert.  Pass in
*                    WHOLE_BUFFER if the client record is full length,
*                    otherwise pass in the number of bytes to convert.
*  innulls         - Client record nulls.
*  stag            - Server tag name.
*  outbufp         - Pointer to buffer to form server record in.  It must be
*                    large enough for a full length server record.
*  flags           - Specifies data attributes (e.g., little-endian).
*  ondisk_lim      - The length of the ondisk server record will be limited
*                    to this, unless it is WHOLE_BUFFER.
*  fail_reason     - If not null this gets filled in with reason for failure
*  inblobs         - Array of blobs for the input tag.
*  outblobs        - Array of blobs for the output tag.
*  maxblobs        - Size of blob array - ought to be MAXBLOBS.
*  tzname          - timezone information, if provided; NULL or empty string
*otherwise
*
* Returns:
*  -1          - Conversion failure - client record could not be converted
 *                to server record.
 *  ondisk_len  - Size of the resultant server side record (this may be smaller
 *                than a full record for a partial key).
 *
 * On success only outblobs will be valid, there is no need to free up inblobs.
 * On failure the caller should free inblobs and outblobs.
 */
static int ctag_to_stag_int(struct dbtable *table, const char *ctag,
                            const char *inbuf, int len,
                            const unsigned char *innulls, const char *stag,
                            void *outbufp, int flags, int ondisk_lim,
                            struct convert_failure *fail_reason,
                            blob_buffer_t *inblobs, blob_buffer_t *outblobs,
                            int maxblobs, const char *tzname)
{
    int field;
    struct schema *from;
    struct schema *to;
    struct field *to_field, *from_field;
    int field_idx = 0;
    int rc;
    unsigned char *outbuf = (unsigned char *)outbufp;
    int schema_full_buffer_size, ondisk_schema_full_buffer_size;
    int ondisk_len = 0;
    int fflags = 0;
    int got_a_partial_string = 0;
    int rec_srt_off = 1;

    if (gbl_sort_nulls_correctly)
        rec_srt_off = 0;

    if (fail_reason)
        init_convert_failure_reason(fail_reason);

    from = find_tag_schema(table, ctag);
    if (from == NULL) {
        if (fail_reason)
            fail_reason->reason = CONVERT_FAILED_INVALID_INPUT_TAG;
        return -1;
    }
    if (fail_reason)
        fail_reason->source_schema = from;
    to = find_tag_schema(table, stag);
    if (to == NULL) {
        if (fail_reason)
            fail_reason->reason = CONVERT_FAILED_INVALID_OUTPUT_TAG;
        return -1;
    }
    if (fail_reason)
        fail_reason->target_schema = to;

    if (len == 0)
        return 0;

    schema_full_buffer_size = get_size_of_schema(from);
    ondisk_schema_full_buffer_size = get_size_of_schema(to);
    if (ondisk_lim < 0)
        ondisk_lim = ondisk_schema_full_buffer_size;

    /* allow partial buffers for partial key searches */
    /* NOTE: len != WHOLE_BUFFER only makes sense for keys */
    if (len == WHOLE_BUFFER)
        len = schema_full_buffer_size;
    if (len < 0) {
        if (fail_reason)
            fail_reason->reason = CONVERT_FAILED_INVALID_LENGTH;
        return -1;
    }

    if (gbl_check_client_tags && !strcmp(to->tag, ".ONDISK")) {
        /*** MAKE SURE THAT CLIENT TAG HAS ALL MEMBERS IN ONDISK. IF IT DOES
         * NOT, SOME THING IS WRONG. ERROR OUT, OR USE SOFT WARNING, DEPENDING
         * ON SETTINGS ***/

        for (field = 0; field < from->nmembers; field++) {
            from_field = &from->member[field];
            field_idx = find_field_idx_in_tag(to, from_field->name);
            if (field_idx == -1) {
                if (gbl_check_client_tags == 1) {
                    if (fail_reason) {
                        fail_reason->reason =
                            CONVERT_FAILED_INPUT_TAG_HAS_INVALID_FIELD;
                        fail_reason->source_field_idx = field;
                        fail_reason->source_schema = from;
                        fail_reason->target_field_idx = -1;
                    }
                    return -1;
                } else if (gbl_check_client_tags == 2) {
                    logmsg(LOGMSG_WARN, 
                            "WARNING: THIS DATABASE RECEIVED CLIENT "
                            "TAG '%s' WITH\n"
                            "FIELD '%s' WHICH IS NOT IN SERVER TAG '%s'\n",
                            ctag, from_field->name, stag);
                }
            }
        }
    }

    for (field = 0; field < to->nmembers; field++) {
        int outdtsz = 0;
        blob_buffer_t *outblob = NULL;
        to_field = &to->member[field];
        field_idx = find_field_idx_in_tag(from, to_field->name);
        /* field in index set to be descending if converting from
           a client index and that field is marked descending
           */

        if (gbl_replicate_local &&
            (strcasecmp(to_field->name, "comdb2_seqno") == 0) &&
            !(flags & CONVERT_UPDATE)) {
            unsigned long long val;
            int outsz;
            const struct field_conv_opts outopts = {0};
            struct field_conv_opts inopts = {0};
            /* just ignore what they gave us (if anything)
               and put a genid in there

               int CLIENT_to_SERVER(const void *in, int inlen, int intype, int
               isnull, const struct field_conv_opts *inopts,
               blob_buffer_t *inblob,
               void *out, int outlen, int outtype, int flags, int *outdtsz,
               const struct field_conv_opts *outopts, blob_buffer_t *outblob);

*/
            if (flags & CONVERT_LITTLE_ENDIAN_CLIENT)
                inopts.flags |= FLD_CONV_LENDIAN;

            val = get_unique_longlong(thedb);

            rc =
                CLIENT_to_SERVER(&val, sizeof(unsigned long long), CLIENT_INT,
                                 0, (const struct field_conv_opts *)&inopts,
                                 NULL, outbuf + to_field->offset, to_field->len,
                                 to_field->type, flags, &outsz, &outopts, NULL);
            if (rc) {
                if (fail_reason)
                    fail_reason->reason = CONVERT_FAILED_SHOULDNT_HAPPEN_1;
                return -1;
            }
            continue;
        }

        if (fail_reason) {
            fail_reason->target_field_idx = field;
            fail_reason->source_field_idx = -1;
        }

        if (outblobs && to_field->blob_index >= 0) {
            if (to_field->blob_index >= maxblobs) {
                if (fail_reason)
                    fail_reason->reason = CONVERT_FAILED_BAD_BLOB_PROGRAMMER;
                return -1;
            }
            outblob = &outblobs[to_field->blob_index];
        }

        if (field_idx == -1) {
            if (flags & CONVERT_UPDATE) /* this is an update, so don't touch the
                                         * output buffer */
                continue;
            if (to_field->in_default_type != SERVER_SEQUENCE &&
                (to_field->in_default == NULL || stype_is_null(to_field->in_default))) {
                if (to_field->flags & NO_NULL) {
                    if (fail_reason)
                        fail_reason->reason =
                            CONVERT_FAILED_NULL_CONSTRAINT_VIOLATION;
                    return -1;
                }
                rc = NULL_to_SERVER(outbuf + to_field->offset, to_field->len,
                                    to_field->type);
                if (rc) {
                    if (fail_reason)
                        fail_reason->reason = CONVERT_FAILED_SHOULDNT_HAPPEN_1;
                    return -1;
                }
            } else {
                if (to_field->in_default_type == SERVER_FUNCTION) {
                    rc = run_internal_sql_function(outbuf, to_field, to_field->in_default, 
                                                   to, outblob, tzname, fail_reason);
                } else {
                    rc = SERVER_to_SERVER(
                        to_field->in_default, to_field->in_default_len,
                        to_field->in_default_type, NULL, /*convopts*/
                        NULL,                            /*blob*/
                        0, outbuf + to_field->offset, to_field->len, to_field->type,
                        fflags, &outdtsz, NULL, /*convopts*/
                        outblob                 /*blob*/
                        );
                }
                if (rc) {
                    if (fail_reason)
                        fail_reason->reason =
                            CONVERT_FAILED_INCOMPATIBLE_VALUES;
                    return -1;
                }
                /* if converting FROM a flipped index */
                if (to_field->flags & INDEX_DESCEND)
                    xorbuf(((char *)outbuf) + to_field->offset + rec_srt_off,
                           to_field->len - rec_srt_off);
            }
        } else {
            int fromdatalen = 0;
            blob_buffer_t *inblob = NULL;

            if (fail_reason)
                fail_reason->source_field_idx = field_idx;
            from_field = &from->member[field_idx];
            fromdatalen = from_field->len;

            if (inblobs && from_field->blob_index >= 0) {
                if (from_field->blob_index >= maxblobs) {
                    if (fail_reason)
                        fail_reason->reason =
                            CONVERT_FAILED_BAD_BLOB_PROGRAMMER;
                    return -1;
                }
                inblob = &inblobs[from_field->blob_index];
                if (!inblob->exists && (to_field->flags & NO_NULL) &&
                    gbl_disallow_null_blobs) {
                    if (fail_reason)
                        fail_reason->reason =
                            CONVERT_FAILED_NULL_CONSTRAINT_VIOLATION;

                    return -1;
                }
            }

            if (from->flags & SCHEMA_DYNAMIC &&
                from_field->type == CLIENT_PSTR) {
                fromdatalen = from_field->datalen;
            }

            if (len != schema_full_buffer_size && from_field->offset > len) {
                /* shouldn't happen */
                if (fail_reason)
                    fail_reason->reason = CONVERT_FAILED_SHOULDNT_HAPPEN_2;
                return -1;
            }
            /* if the sent field is marked NULL but we don't allow them, fail */
            if ((btst(innulls, field_idx)) && (to_field->flags & NO_NULL)) {
                if (fail_reason)
                    fail_reason->reason =
                        CONVERT_FAILED_NULL_CONSTRAINT_VIOLATION;
                return -1;
            }
            /* is field partial in client buffer? */
            if (len != schema_full_buffer_size && from_field->offset < len &&
                from_field->offset + fromdatalen > len) {
                int partial_length = len - from_field->offset;

                /* can only have partial strings or byte arrays */
                if (!CLIENT_TYPE_CAN_BE_PARTIAL(from_field->type)) {
                    if (fail_reason)
                        fail_reason->reason =
                            CONVERT_FAILED_INVALID_PARTIAL_TYPE;
                    return -1;
                }

                if (from_field->flags & INDEX_DESCEND)
                    xorbuf(inbuf + from_field->offset, partial_length);
                rc = copy_partial_client_buf(
                    inbuf + from_field->offset, partial_length,
                    btst(innulls, field_idx),
                    (char *)(outbuf + to_field->offset));
                if (from_field->flags & INDEX_DESCEND)
                    xorbuf(inbuf + from_field->offset, partial_length);
                if (to_field->flags & INDEX_DESCEND)
                    xorbuf(((char *)outbuf + to_field->offset) + rec_srt_off,
                           partial_length + 1 - rec_srt_off);
                ondisk_len += partial_length + 1;
                got_a_partial_string = 1;
            }
            /* same thing, but affected through the ondisk_lim parameter
             * (saves the caller having to calculate two partial key lengths -
             * one for the ondisk tag, and one for the client tag). */
            else if (ondisk_lim != ondisk_schema_full_buffer_size &&
                     to_field->offset < ondisk_lim &&
                     to_field->offset + to_field->len > ondisk_lim) {
                int partial_length = ondisk_lim - to_field->offset;

                /* can only have partial strings or byte arrays */
                if (!CLIENT_TYPE_CAN_BE_PARTIAL(from_field->type)) {
                    if (fail_reason)
                        fail_reason->reason = CONVERT_FAILED_INVALID_INPUT_TAG;
                    return -1;
                }
                if (from_field->flags & INDEX_DESCEND)
                    xorbuf(inbuf + from_field->offset, partial_length);
                rc = copy_partial_client_buf(
                    inbuf + from_field->offset, partial_length,
                    btst(innulls, field_idx),
                    (char *)(outbuf + to_field->offset));
                if (from_field->flags & INDEX_DESCEND)
                    xorbuf(inbuf + from_field->offset, partial_length);
                if (to_field->flags & INDEX_DESCEND)
                    xorbuf(((char *)outbuf + to_field->offset) + rec_srt_off,
                           partial_length + 1 - rec_srt_off);
                ondisk_len += partial_length + 1;
            } else {
                if (from_field->flags & INDEX_DESCEND)
                    xorbuf(inbuf + from_field->offset, fromdatalen);

                if (tzname && tzname[0]) {
                    struct field_conv_opts_tz tzopts;

                    /* Provide the timezone to the conversion routines */
                    bzero(&tzopts, sizeof(tzopts));
                    memcpy(&tzopts, &from_field->convopts,
                           sizeof(struct field_conv_opts));
                    tzopts.flags |= FLD_CONV_TZONE;
                    strncpy0(tzopts.tzname, tzname, sizeof(tzopts.tzname));

                    /* The client data is little endian. */
                    if (flags & CONVERT_LITTLE_ENDIAN_CLIENT)
                        tzopts.flags |= FLD_CONV_LENDIAN;

                    rc = CLIENT_to_SERVER(
                        inbuf + from_field->offset, fromdatalen,
                        from_field->type, btst(innulls, field_idx),
                        (const struct field_conv_opts *)&tzopts, inblob,
                        outbuf + to_field->offset, to_field->len,
                        to_field->type, fflags, &outdtsz, &to_field->convopts,
                        outblob);

#if 0
                    printf("in:\n");
                    fsnapf(stdout, inbuf + from_field->offset, fromdatalen);
                    printf("out:\n");
                    fsnapf(stdout, outbuf + to_field->offset, to_field->len);
#endif

                } else {
                    struct field_conv_opts convopts;
                    memcpy(&convopts, &from_field->convopts,
                           sizeof(struct field_conv_opts));

                    /* The client data is little endian. */
                    if (flags & CONVERT_LITTLE_ENDIAN_CLIENT)
                        convopts.flags |= FLD_CONV_LENDIAN;

                    rc = CLIENT_to_SERVER(
                        inbuf + from_field->offset, fromdatalen,
                        from_field->type, btst(innulls, field_idx), &convopts,
                        inblob, outbuf + to_field->offset, to_field->len,
                        to_field->type, fflags, &outdtsz, &to_field->convopts,
                        outblob);
                }
                if (from_field->flags & INDEX_DESCEND)
                    xorbuf(inbuf + from_field->offset, fromdatalen);
                if (to_field->flags & INDEX_DESCEND)
                    xorbuf(((char *)outbuf + to_field->offset) + rec_srt_off,
                           to_field->len - rec_srt_off);
                ondisk_len += to_field->len;
            }
            /* rc test needs to come first to ensure that conversion failures
             * are caught. */
            if (rc < 0) {
                if (fail_reason)
                    fail_reason->reason = CONVERT_FAILED_INCOMPATIBLE_VALUES;
                /* TODO: we could pass rc here I guess */
                return -1;
            }
            if (len != schema_full_buffer_size &&
                from_field->offset + from_field->len == len)
                return ondisk_len;
            if (ondisk_len >= ondisk_lim)
                return ondisk_lim;

            if (got_a_partial_string)
                return ondisk_len;
        }
    }
    return ondisk_len;
}

uint8_t *flddtasizes_put(const struct flddtasizes *p_flddtasizes,
                         uint8_t *p_buf, const uint8_t *p_buf_end)
{
    unsigned i;

    if (p_buf_end < p_buf ||
        (FLDDTASIZES_NO_FLDSZS_LEN +
         (p_flddtasizes->nflds * FLDDTASIZES_FLDSZ_LEN)) >
            (p_buf_end - p_buf) ||
        p_flddtasizes->nflds > MAXCOLUMNS)
        return NULL;

    p_buf = buf_put(&(p_flddtasizes->type), sizeof(p_flddtasizes->type), p_buf,
                    p_buf_end);
    p_buf = buf_put(&(p_flddtasizes->nflds), sizeof(p_flddtasizes->nflds),
                    p_buf, p_buf_end);

    for (i = 0; i < p_flddtasizes->nflds; ++i)
        p_buf = buf_put(&(p_flddtasizes->fldszs[i]),
                        sizeof(p_flddtasizes->fldszs[i]), p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *flddtasizes_get(struct flddtasizes *p_flddtasizes,
                               const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    unsigned i;

    if (p_buf_end < p_buf || FLDDTASIZES_NO_FLDSZS_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_flddtasizes->type), sizeof(p_flddtasizes->type), p_buf,
                    p_buf_end);
    p_buf = buf_get(&(p_flddtasizes->nflds), sizeof(p_flddtasizes->nflds),
                    p_buf, p_buf_end);

    if ((p_flddtasizes->nflds * FLDDTASIZES_FLDSZ_LEN) > (p_buf_end - p_buf) ||
        p_flddtasizes->nflds > MAXCOLUMNS)
        return NULL;

    for (i = 0; i < p_flddtasizes->nflds; ++i)
        p_buf = buf_get(&(p_flddtasizes->fldszs[i]),
                        sizeof(p_flddtasizes->fldszs[i]), p_buf, p_buf_end);

    return p_buf;
}

/* Convert record from old version to ondisk.
 * This is a faster version of vtag_to_ondisk()
 * it uses a map of every version to current ondisk so we avoid lookup
 * of field names over and over unecessarily.
 * If the schemas are compatible it uses the buffer from the original vers
 * and only sets the dbstore elements it needs onto the buffer.
 * If schemas are not compatible, we convert from old version to ondisk.
 * Return ondisk rec len if record was changed;
 * Also return the len of new record if len != NULL.
 * 0 otherwise */
int vtag_to_ondisk_vermap(const dbtable *db, uint8_t *rec, int *len,
                          uint8_t ver)
{
    struct schema *from_schema;
    struct schema *to_schema;
    char ver_tag[MAXTAGLEN];
    void *inbuf;
    int rc;
    struct convert_failure reason;

    if (!db || !db->instant_schema_change || !rec)
        return 0;

    /* version sanity check */
    if (unlikely(ver == 0)) {
        logmsg(LOGMSG_FATAL, "%s:%d %s() %s %d -> %d\n", __FILE__, __LINE__,
               __func__, db->tablename, ver, db->schema_version);
        cheap_stack_trace();
        exit(1);
    }

    if (ver == db->schema_version) {
        goto done;
    }

    /* fix dbstore values */
    to_schema = db->schema;
    if (to_schema == NULL) {
        logmsg(LOGMSG_FATAL, "could not get to_schema for .ONDISK in %s\n",
               db->tablename);
        exit(1);
    }

    if (db->versmap[ver] == NULL) { // not possible
        logmsg(LOGMSG_FATAL, "vtag_to_ondisk_vermap: db->versmap[%d] should NOT be null\n",
               ver);
        bdb_flush(thedb->bdb_env, &rc);
        cheap_stack_trace();
        abort();
    }

    if (!db->vers_compat_ondisk[ver]) { /* need to convert buffer */
        /* old version will necessarily be smaller in size.
         * it would make more sense to make a copy of the
         * smaller of the two buffers */
        /* find schema for older version */
        snprintf(ver_tag, sizeof ver_tag, "%s%d", gbl_ondisk_ver, ver);
        from_schema = find_tag_schema(db, ver_tag);
        if (unlikely(from_schema == NULL)) {
            logmsg(LOGMSG_FATAL, "%s:%d %s() %s %d -> %d\n", __FILE__, __LINE__,
                   __func__, db->tablename, ver, db->schema_version);
            cheap_stack_trace();
            exit(1);
        }

        /* convert to ondisk */
        inbuf = alloca(from_schema->recsize);
        if (inbuf == NULL) {
            logmsg(LOGMSG_FATAL, "%s: alloca failed\n", __func__);
            exit(1);
        }
        memcpy(inbuf, rec, from_schema->recsize);

        /* call new cached version instead of stag_to_stag_buf_flags() */
        rc = stag_to_stag_buf_cachedmap(db, (int *)db->versmap[ver],
                                        from_schema, to_schema, inbuf, (char *)rec,
                                        CONVERT_NULL_NO_ERROR, &reason, NULL, 0);

        if (rc) {
            char err[1024];
            convert_failure_reason_str(&reason, db->tablename, ver_tag,
                                       ".ONDISK", err, sizeof(err));
            logmsg(LOGMSG_ERROR, "%s: %s -> %s failed: %s\n", __func__, ver_tag,
                    ".ONDISK", err);
            return 0;
        }

        /* This fill the dbstore for all versions up to curr */
        for (int i = 0; i < to_schema->nmembers; ++i) {
            if (db->dbstore[i].ver > ver) {
                unsigned int offset = to_schema->member[i].offset;
                set_dbstore(db, i, &rec[offset], 0 /* only set null bit */);
            }
        }
    } else {
        // same ordering of fields between ver and ondisk
        // so we can loop from the end until when we hit .ver < ver
        int i = to_schema->nmembers - 1;
        while (i > 0 && db->dbstore[i].ver > ver) {
            unsigned int offset = to_schema->member[i].offset;
            unsigned int flen = to_schema->member[i].len;
            set_dbstore(db, i, &rec[offset],
                        flen /* set null bit and memset field 0 */);
            i--;
        }
    }

done:
    if (len)
        *len = db->lrl;
    return db->lrl;
}

/* Convert record from old version to ondisk.
 * Conversion is done in a tmp buffer, then
 * memcpy-ed onto the provided record.
 * Return ondisk rec len if record was changed;
 * Also return the len of new record if len != NULL.
 * 0 otherwise */
int vtag_to_ondisk(const dbtable *db, uint8_t *rec, int *len, uint8_t ver,
                   unsigned long long genid)
{
    struct field *field;
    unsigned int offset;
    struct schema *from_schema;
    struct schema *to_schema;
    char ver_tag[MAXTAGLEN];
    void *from;
    int rc;
    struct convert_failure reason;

    if (!db || !db->instant_schema_change || !rec)
        return 0;

    /* version sanity check */
    if (unlikely(ver == 0)) {
        logmsg(LOGMSG_FATAL, "%s:%d %s() %s %d -> %d\n", __FILE__, __LINE__,
               __func__, db->tablename, ver, db->schema_version);
        cheap_stack_trace();
        exit(1);
    }

    if (ver == db->schema_version) {
        goto done;
    }

    if (gbl_num_record_upgrades > 0 && genid != 0)
        offload_comm_send_upgrade_records(db, genid);

    if (BDB_ATTR_GET(thedb->bdb_attr, USE_VTAG_ONDISK_VERMAP))
        return vtag_to_ondisk_vermap(db, rec, len, ver);

    /* find schema for older version */
    snprintf(ver_tag, sizeof ver_tag, "%s%d", gbl_ondisk_ver, ver);
    from_schema = find_tag_schema(db, ver_tag);
    if (unlikely(from_schema == NULL)) {
        logmsg(LOGMSG_FATAL, "%s:%d %s() %s %d -> %d\n", __FILE__, __LINE__,
               __func__, db->tablename, ver, db->schema_version);
        cheap_stack_trace();
        exit(1);
    }

    /* old version will necessarily be smaller in size.
     * it would make more sense to make a copy of the
     * smaller of the two buffers */

    /* convert to ondisk */
    from = alloca(from_schema->recsize);
    if (from == NULL) {
        logmsg(LOGMSG_FATAL, "%s: alloca failed\n", __func__);
        exit(1);
    }
    memcpy(from, rec, from_schema->recsize);

    rc = _stag_to_stag_buf_flags_blobs(db, from_schema, find_tag_schema(db, ".ONDISK"), from, (char *)rec,
                                       CONVERT_NULL_NO_ERROR, &reason, NULL, NULL, 0, NULL);

    if (rc) {
        char err[1024];
        convert_failure_reason_str(&reason, db->tablename, ver_tag, ".ONDISK",
                                   err, sizeof(err));
        logmsg(LOGMSG_ERROR, "%s: %s -> %s failed: %s\n", __func__, ver_tag,
                ".ONDISK", err);
        return 0;
    }

    /* fix dbstore values */
    to_schema = db->schema;
    if (to_schema == NULL) {
        logmsg(LOGMSG_FATAL, "could not get to_schema for .ONDISK in %s\n",
               db->tablename);
        exit(1);
    }
    for (int i = 0; i < to_schema->nmembers; ++i) {
        field = &to_schema->member[i];
        offset = field->offset;
        if (db->dbstore[i].ver > ver) {
            set_dbstore(db, i, &rec[offset], 0 /* only set null bit */);
        }
    }

done:
    if (len)
        *len = db->lrl;
    return db->lrl;
}

static int field_is_null(struct schema *s, struct field *field, const void *buf)
{
    uint8_t hdr = *((uint8_t *)buf + field->offset);
    if ((s->flags & SCHEMA_INDEX) && (field->flags & INDEX_DESCEND) &&
        gbl_sort_nulls_correctly)
        hdr = ~hdr;
    return stype_is_null(&hdr);
}

static int _stag_to_ctag_buf(struct dbtable *table, const char *stag,
                             const char *inbuf, int len, const char *ctag,
                             void *outbufp, unsigned char *outnulls, int flags,
                             uint8_t **pp_flddtsz, const uint8_t *p_flddtsz_end,
                             blob_buffer_t *inblobs, blob_buffer_t *outblobs,
                             int maxblobs, const char *tzname)
{
    int field;
    int todatalen;
    struct schema *from;
    struct schema *to;
    struct field *to_field, *from_field;
    int field_idx;
    int rc;
    unsigned char *outbuf = (unsigned char *)outbufp;
    int null;
    int fflags;
    int field_len;
    int tlen = 0;
    int rec_srt_off = 1;
    struct field_conv_opts_tz outopts = {0};

    struct flddtasizes flddtasz;

    if (gbl_sort_nulls_correctly)
        rec_srt_off = 0;

    flddtasz.type = 1; /*COMDB2_RSP_DATA_FIELD_SIZES;*/
    flddtasz.nflds = 0;

    from = find_tag_schema(table, stag);
    if (from == NULL)
        return -1;

    to = find_tag_schema(table, ctag);
    if (to == NULL)
        return -1;

    /* The client data is little endian. */
    if (flags & CONVERT_LITTLE_ENDIAN_CLIENT)
        outopts.flags |= FLD_CONV_LENDIAN;

    for (field = 0; field < to->nmembers; field++) {
        int outdtsz = 0;
        blob_buffer_t *outblob = NULL;
        to_field = &to->member[field];
        field_idx = find_field_idx_in_tag(from, to_field->name);
        null = 0;

        if (outblobs && to_field->blob_index >= 0) {
            if (to_field->blob_index >= maxblobs)
                return -1;
            outblob = &outblobs[to_field->blob_index];
        }

        /* Get the destination data length. */
        todatalen = to_field->len;

        if (field_idx == -1) {
            if (to_field->out_default == NULL) {
                if (to_field->flags & NO_NULL)
                    return -1;
                bset(outnulls, field);
                flddtasz.fldszs[flddtasz.nflds++] = 0;
                tlen += todatalen;
            } else {
                rc = SERVER_to_CLIENT(
                    to_field->out_default, to_field->out_default_len,
                    to_field->out_default_type, NULL /*inopts*/, NULL /*blob*/,
                    0 /*flags*/, outbuf + to_field->offset, todatalen,
                    to_field->type, &null, &outdtsz,
                    (const struct field_conv_opts *)&outopts /*convopts*/,
                    outblob);
                if (rc)
                    return -1;
                tlen += todatalen;
                if (to_field->flags & INDEX_DESCEND)
                    xorbuf(outbuf + to_field->offset, outdtsz);
                flddtasz.fldszs[flddtasz.nflds++] = outdtsz;
            }
        } else {
            blob_buffer_t *inblob = NULL;

            from_field = &from->member[field_idx];
            if ((from->flags & SCHEMA_INDEX) &&
                (from_field->flags & INDEX_DESCEND))
                fflags = INDEX_DESCEND;
            else
                fflags = 0;
            if (len != -1) {
                /* past the end of what's requested */
                if (from_field->offset >= len)
                    break;
                else if ((from_field->offset < len) &&
                         (from_field->offset + from_field->len > len)) {
                    /* partial only for strings / byte arrays */
                    if ((from_field->type != SERVER_BCSTR) &&
                        (from_field->type != SERVER_BYTEARRAY))
                        return -1;
                    field_len = len - from_field->offset;
                } else
                    field_len = from_field->len;
            } else
                field_len = from_field->len;

            if (inblobs && from_field->blob_index >= 0) {
                if (from_field->blob_index >= maxblobs)
                    return -1;
                inblob = &inblobs[from_field->blob_index];
            }

            if (field_is_null(from, from_field, (void *)inbuf)) {
                if (from_field->out_default) {
                    struct field_conv_opts_tz tzopts;

                    /* provide tzname info to lower levels */
                    bzero(&tzopts, sizeof(tzopts));
                    memcpy(&tzopts, &to_field->convopts,
                           sizeof(struct field_conv_opts));

                    /* The client data is little endian. */
                    if (flags & CONVERT_LITTLE_ENDIAN_CLIENT)
                        tzopts.flags |= FLD_CONV_LENDIAN;

                    if (tzname && tzname[0]) {
                        tzopts.flags |= FLD_CONV_TZONE;
                        strncpy0(tzopts.tzname, tzname, DB_MAX_TZNAMEDB);

                        rc = SERVER_to_CLIENT(
                            from_field->out_default,
                            from_field->out_default_len,
                            from_field->out_default_type, &from_field->convopts,
                            inblob, fflags, outbuf + to_field->offset,
                            todatalen, to_field->type, &null, &outdtsz,
                            (const struct field_conv_opts *)&tzopts, outblob);
                    } else {

                        rc = SERVER_to_CLIENT(
                            from_field->out_default,
                            from_field->out_default_len,
                            from_field->out_default_type, &from_field->convopts,
                            inblob, fflags, outbuf + to_field->offset,
                            todatalen, to_field->type, &null, &outdtsz,
                            (const struct field_conv_opts *)&tzopts, outblob);
                    }
                    flddtasz.fldszs[flddtasz.nflds++] = outdtsz;
                    if (to_field->flags & INDEX_DESCEND)
                        xorbuf(outbuf + to_field->offset, outdtsz);
                    tlen += field_len;
                } else {
                    memset(outbuf + to_field->offset, 0, todatalen);
                    rc = 0;
                    flddtasz.fldszs[flddtasz.nflds++] = 0;
                }
                bset(outnulls, field);
            } else {
                struct field_conv_opts_tz tzopts;

                /* provide tzname info to lower levels */
                bzero(&tzopts, sizeof(tzopts));
                memcpy(&tzopts, &to_field->convopts,
                       sizeof(struct field_conv_opts));

                /* The client data is little endian. */
                if (flags & CONVERT_LITTLE_ENDIAN_CLIENT)
                    tzopts.flags |= FLD_CONV_LENDIAN;

                if (from_field->flags & INDEX_DESCEND)
                    xorbuf(((char *)inbuf + from_field->offset) + rec_srt_off,
                           from_field->len - rec_srt_off);
                if (tzname && tzname[0]) {
                    tzopts.flags |= FLD_CONV_TZONE;
                    strncpy0(tzopts.tzname, tzname, DB_MAX_TZNAMEDB);

                    rc = SERVER_to_CLIENT(
                        inbuf + from_field->offset, field_len, from_field->type,
                        &from_field->convopts, inblob, fflags,
                        outbuf + to_field->offset, todatalen, to_field->type,
                        &null, &outdtsz,
                        (const struct field_conv_opts *)&tzopts, outblob);
                } else {
                    rc = SERVER_to_CLIENT(
                        inbuf + from_field->offset, field_len, from_field->type,
                        &from_field->convopts, inblob, fflags,
                        outbuf + to_field->offset, todatalen, to_field->type,
                        &null, &outdtsz,
                        (const struct field_conv_opts *)&tzopts, outblob);
                }
                if (from_field->flags & INDEX_DESCEND)
                    xorbuf(((char *)inbuf + from_field->offset) + rec_srt_off,
                           from_field->len - rec_srt_off);
                if (to_field->flags & INDEX_DESCEND)
                    xorbuf(outbuf + to_field->offset, outdtsz);
                flddtasz.fldszs[flddtasz.nflds++] = outdtsz;
                if ((from_field->offset < len) &&
                    (from_field->offset + from_field->len > len))
                    break;
                tlen += todatalen;
            }
            if (rc) {
                if ((to_field->type == CLIENT_BLOB || to_field->type == CLIENT_BLOB2 || to_field->type == CLIENT_VUTF8) && (flags & CONVERT_IGNORE_BLOBS))
                    rc = 0;
                else
                    return -1;
            }
        }
    }

    /* if we were given a place to put them, pack the flddtaszs */
    if (pp_flddtsz &&
        !(*pp_flddtsz = flddtasizes_put(&flddtasz, *pp_flddtsz, p_flddtsz_end)))
        return -1;

    return tlen;
}

int reset_sequence(const char *table, const char *column, int64_t val)
{
    struct dbtable *db = get_dbtable_by_name(table);
    if (!db) {
        logmsg(LOGMSG_ERROR, "%s couldn't find table %s\n", __func__, table);
        return -1;
    }

    for (int i = 0; i < db->schema->nmembers; i++) {
        struct field *f = &db->schema->member[i];
        if (!strcmp(f->name, column)) {
            if (f->in_default_type != SERVER_SEQUENCE) {
                logmsg(LOGMSG_ERROR, "%s table %s column %s is not a sequence\n", __func__, table, column);
                return -1;
            }
            int bdberr = 0, rc;
            void *t = bdb_tran_begin(thedb->bdb_env, NULL, &bdberr);
            if (!t) {
                logmsg(LOGMSG_ERROR, "%s couldn't begin tran\n", __func__);
                return -1;
            }
            rc = bdb_set_sequence(t, table, column, val, &bdberr);
            if (rc) {
                logmsg(LOGMSG_ERROR, "Error setting sequence %d bdberr %d\n", rc, bdberr);
                bdb_tran_abort(db->handle, t, &bdberr);
                return -1;
            }
            bdb_tran_commit(db->handle, t, &bdberr);
            return 0;
        }
    }
    logmsg(LOGMSG_ERROR, "%s couldn't find column %s in table %s\n", __func__, column, table);
    return -1;
}

int upd_master_columns(struct ireq *iq, void *intrans, void *record, size_t reclen)
{
    tran_type *tran = (tran_type *)intrans;
    char *crec = record;
    int rc = 0, bdberr = 0;
    int64_t val;
    struct schema *schema = get_schema(iq->usedb, -1);
    for (int nfield = 0; nfield < schema->nmembers; nfield++) {
        const struct field *field = &schema->member[nfield];

        switch (field->in_default_type) {
        case SERVER_SEQUENCE: {
            struct field_conv_opts inopts = {0};
            struct field_conv_opts outopts = {0};
            int outsz, isnull;
#ifdef _LINUX_SOURCE
            outopts.flags |= FLD_CONV_LENDIAN;
#endif
            rc = SERVER_to_CLIENT(crec + field->offset, field->len, field->type,
                                  (const struct field_conv_opts *)&inopts, NULL, 0, &val, sizeof(val), CLIENT_INT,
                                  &isnull, &outsz, (const struct field_conv_opts *)&outopts, NULL);
            if (rc) {
                logmsg(LOGMSG_ERROR, "Failed to convert field to client?\n");
                abort();
            }
            if (!isnull) {
                rc = bdb_check_and_set_sequence(tran, iq->usedb->tablename, field->name, val, &bdberr);
                if (rc) {
                    if (bdberr == BDBERR_DEADLOCK) {
                        rc = bdberr;
                    } else {
                        logmsg(LOGMSG_ERROR, "%s error writing sequence %d bdberr %d\n", __func__, rc, bdberr);
                    }
                }
            }
            break;
        }
        }
    }
    return 0;
}

int set_master_columns(struct ireq *iq, void *intrans, void *record, size_t reclen)
{
    tran_type *tran = (tran_type *)intrans;
    char *crec = record;
    int rc = 0, bdberr = 0;
    int64_t seq;
    struct schema *schema = get_schema(iq->usedb, -1);
    for (int nfield = 0; nfield < schema->nmembers; nfield++) {
        const struct field *field = &schema->member[nfield];
        int outsz;
        // switch on the dbstore value, invoke handler to fill
        switch (field->in_default_type) {
        case SERVER_SEQUENCE:
            if (stype_is_resolve_master(crec + field->offset)) {
                struct field_conv_opts inopts = {0};
                struct field_conv_opts outopts = {0};
#ifdef _LINUX_SOURCE
                inopts.flags |= FLD_CONV_LENDIAN;
#endif
                rc = bdb_increment_and_set_sequence(tran, iq->usedb->tablename, field->name, &seq, &bdberr);
                if (rc) {
                    if (bdberr == BDBERR_DEADLOCK || bdberr == BDBERR_MAX_SEQUENCE) {
                        rc = bdberr;
                    } else {
                        logmsg(LOGMSG_ERROR, "%s error incrementing sequence %d bdberr %d\n", __func__, rc, bdberr);
                    }
                    return rc;
                }
                rc = CLIENT_to_SERVER(&seq, sizeof(seq), CLIENT_INT, 0, (const struct field_conv_opts *)&inopts, NULL,
                                      crec + field->offset, field->len, field->type, 0, &outsz, &outopts, NULL);
                if (rc) {
                    switch (field->len) {
                    case 3:
                        if (seq > INT16_MAX)
                            rc = BDBERR_MAX_SEQUENCE;
                        break;
                    case 5:
                        if (seq > INT32_MAX)
                            rc = BDBERR_MAX_SEQUENCE;
                        break;
                    case 9:
                        if (seq > INT64_MAX)
                            rc = BDBERR_MAX_SEQUENCE;
                        break;
                    }
                    logmsg(LOGMSG_ERROR, "Failed to convert seq %" PRId64 " to %s %s\n", seq, iq->usedb->tablename,
                           field->name);
                    return rc;
                }
            } else if (!stype_is_null(crec + field->offset)) {
                struct field_conv_opts inopts = {0};
                struct field_conv_opts outopts = {0};
                int isnull = 0;
                int64_t val;
#ifdef _LINUX_SOURCE
                outopts.flags |= FLD_CONV_LENDIAN;
#endif
                rc = SERVER_to_CLIENT(crec + field->offset, field->len, field->type,
                                      (const struct field_conv_opts *)&inopts, NULL, 0, &val, sizeof(val), CLIENT_INT,
                                      &isnull, &outsz, (const struct field_conv_opts *)&outopts, NULL);
                if (rc) {
                    logmsg(LOGMSG_ERROR, "Failed to convert field to client?\n");
                    abort();
                }
                rc = bdb_check_and_set_sequence(tran, iq->usedb->tablename, field->name, val, &bdberr);
                if (rc) {
                    if (bdberr == BDBERR_DEADLOCK) {
                        rc = bdberr;
                    } else {
                        logmsg(LOGMSG_ERROR, "%s error writing sequence %d bdberr %d\n", __func__, rc, bdberr);
                    }
                }
            }

            break;

        /* other master resolved types here */
        default:
            break;
        }
    }
    return 0;
}

/*
 * Scan a server format record and make sure that all fields validate for
 * null constraints.
 */
int validate_server_record(struct ireq *iq, const void *record, size_t reclen, const char *tag, struct schema *schema)
{
    const char *crec = record;
    for (int nfield = 0; nfield < schema->nmembers; nfield++) {
        const struct field *field = &schema->member[nfield];
        if ((field->flags & NO_NULL) && (stype_is_null(crec + field->offset))) {
            /* field can't be NULL */
            struct convert_failure reason = {
                .reason = CONVERT_FAILED_NULL_CONSTRAINT_VIOLATION,
                .source_schema = schema,
                .source_field_idx = nfield,
                .target_schema = schema,
                .target_field_idx = nfield,
                .source_sql_field_flags = 0,
                .source_sql_field_info = {0}};

            char str[128];
            convert_failure_reason_str(&reason, iq->usedb->tablename, tag, schema->tag, str, sizeof(str));
            if (iq->debug) {
                reqprintf(iq, "ERR VERIFY DTA %s->.ONDISK '%s'", tag, str);
            }
            reqerrstrhdr(
                iq, "Null constraint violation for column '%s' on table '%s'. ",
                reason.target_schema->member[reason.target_field_idx].name,
                iq->usedb->tablename);
            reqerrstr(iq, COMDB2_ADD_RC_CNVT_DTA,
                      "null constraint error data %s->.ONDISK '%s'", tag, str);

            return -1;
        }
    }
    return 0;
}

int stag_to_ctag_buf(struct dbtable *table, const char *stag, const char *inbuf,
                     int len, const char *ctag, void *outbufp,
                     unsigned char *outnulls, int flags, uint8_t **pp_flddtsz,
                     const uint8_t *p_flddtsz_end)
{
    return _stag_to_ctag_buf(table, stag, inbuf, len, ctag, outbufp, outnulls,
                             flags, pp_flddtsz, p_flddtsz_end, NULL /*inblobs*/,
                             NULL /*outblobs*/, 0 /*maxblobs*/,
                             NULL /*tzname*/);
}

int stag_to_ctag_buf_tz(struct dbtable *table, const char *stag, const char *inbuf,
                        int len, const char *ctag, void *outbufp,
                        unsigned char *outnulls, int flags,
                        uint8_t **pp_flddtsz, const uint8_t *p_flddtsz_end,
                        const char *tzname)
{
    return _stag_to_ctag_buf(table, stag, inbuf, len, ctag, outbufp, outnulls,
                             flags, pp_flddtsz, p_flddtsz_end, NULL /*inblobs*/,
                             NULL /*outblobs*/, 0 /*maxblobs*/, tzname);
}

/* TODO this no longer does anything? just rename _stag_to_ctag_buf? */
int stag_to_ctag_buf_blobs_tz(struct dbtable *table, const char *stag,
                              const char *inbuf, int len, const char *ctag,
                              void *outbufp, unsigned char *outnulls, int flags,
                              uint8_t **pp_flddtsz,
                              const uint8_t *p_flddtsz_end,
                              blob_buffer_t *inblobs, blob_buffer_t *outblobs,
                              int maxblobs, const char *tzname)
{
    return _stag_to_ctag_buf(table, stag, inbuf, len, ctag, outbufp, outnulls,
                             flags, pp_flddtsz, p_flddtsz_end, inblobs,
                             outblobs, maxblobs, tzname);
}

struct dbrecord *allocate_db_record(struct dbtable *table, const char *tag)
{
    struct schema *s = find_tag_schema(table, tag);
    if (s == NULL)
        return NULL;

    struct dbrecord *db = calloc(1, sizeof(struct dbrecord));
    db->bufsize = get_size_of_schema(s);
    db->table = strdup(table->tablename);
    db->schema = s;
    db->recbuf = malloc(db->bufsize);
    db->tag = strdup(tag);

    return db;
}

void free_db_record(struct dbrecord *db)
{
    free(db->table);
    free(db->recbuf);
    free(db->tag);
    free(db);
}

int stag_to_stag_buf_schemas(const struct dbtable *tbl, struct schema *fromsch,
                             struct schema *tosch, const char *inbuf,
                             char *outbuf, const char *tzname)
{
    return _stag_to_stag_buf_flags_blobs(tbl, fromsch, tosch, inbuf, outbuf, 0,
                                         NULL, NULL, NULL, 0, tzname);
}

int stag_to_stag_buf_blobs(const struct dbtable *table, const char *fromtag,
                           const char *inbuf, const char *totag, char *outbuf,
                           struct convert_failure *reason, blob_buffer_t *blobs,
                           int maxblobs, int get_new_blobs)
{
    int rc;
    blob_buffer_t newblobs[MAXBLOBS];
    blob_buffer_t *p_newblobs;

    if (blobs) /* if we were given blobs */
    {
        /* make sure we are using the right MAXBLOBS number */
        if (maxblobs != MAXBLOBS) {
            logmsg(LOGMSG_ERROR, "stag_to_stag_buf_blobs with maxblobs=%d!\n",
                    maxblobs);
            return -1;
        }

        /* zero out newblobs for us to store the return blobs */
        bzero(newblobs, sizeof(newblobs));
        p_newblobs = newblobs;
    } else /* not using blobs */
    {
        p_newblobs = NULL;
        maxblobs = 0;
    }

    rc = _stag_to_stag_buf_flags_blobs(table, find_tag_schema(table, fromtag),
                                       find_tag_schema(table, totag), inbuf, outbuf,
                                       0 /*flags*/, reason, blobs, p_newblobs, maxblobs, NULL /*tzname*/);

    if (blobs && get_new_blobs) /* if we were given blobs */
    {
        /* free the blobs that came in */
        free_blob_buffers(blobs, maxblobs);

        /* and if successful copy over the return blobs */
        if (rc < 0)
            free_blob_buffers(newblobs, MAXBLOBS);
        else
            memcpy(blobs, newblobs, sizeof(newblobs));
    } else if (blobs)
        free_blob_buffers(newblobs, MAXBLOBS);

    return rc;
}

int stag_ondisk_to_ix(const struct dbtable *db, int ixnum, const char *inbuf, char *outbuf)
{
    return _stag_to_stag_buf_flags_blobs(db, get_schema(db, -1), get_schema(db, ixnum),
                                         inbuf, outbuf, 0, NULL, NULL, NULL,
                                         0, NULL);
}

int stag_ondisk_to_ix_blobs(const struct dbtable *db, int ixnum, const char *inbuf, char *outbuf, blob_buffer_t *blobs,
                            int maxblobs)
{
    int rc;
    blob_buffer_t newblobs[MAXBLOBS];
    blob_buffer_t *p_newblobs;

    if (blobs != NULL) { /* if we were given blobs */
        /* make sure we are using the right MAXBLOBS number */
        if (maxblobs != MAXBLOBS) {
            logmsg(LOGMSG_ERROR, "stag_ondisk_to_ix_blobs with maxblobs=%d!\n", maxblobs);
            return -1;
        }

        /* zero out newblobs for us to store the return blobs */
        bzero(newblobs, sizeof(newblobs));
        p_newblobs = newblobs;
    } else { /* not using blobs */
        p_newblobs = NULL;
        maxblobs = 0;
    }

    rc = _stag_to_stag_buf_flags_blobs(db, get_schema(db, -1), get_schema(db, ixnum),
                                       inbuf, outbuf, 0, NULL, blobs, p_newblobs,
                                       maxblobs, NULL);
    if (blobs)
        free_blob_buffers(newblobs, MAXBLOBS);

    return rc;
}

int stag_to_stag_buf(const struct dbtable *table, const char *fromtag, const char *inbuf,
                     const char *totag, char *outbuf,
                     struct convert_failure *reason)
{
    return _stag_to_stag_buf_flags_blobs(table, find_tag_schema(table, fromtag),
                                         find_tag_schema(table, totag), inbuf, outbuf,
                                         0, reason, NULL /*inblobs*/, NULL /*outblobs*/, 0 /*maxblobs*/, NULL);
}

int stag_to_stag_buf_update_tz(const struct dbtable *tbl, struct schema *from,
                               struct schema *to, const char *inbuf, char *outbuf,
                               struct convert_failure *reason, const char *tzname)
{
    return _stag_to_stag_buf_flags_blobs(tbl, from, to, inbuf, outbuf, CONVERT_UPDATE,
                                         reason, NULL /*inblobs*/,
                                         NULL /*outblobs*/, 0 /*maxblobs*/, tzname);
}

/*
 * Given an update columns structure, create an equivalent one for the incoming
 * tag used to create a valid updcols structure for schema change.  If a column
 * was updated, outcols will be updated to contain the corresponding input-
 * column number.  A value of -1 in outcols signifies that the column was not
 * changed.
 */
int remap_update_columns(struct dbtable *table, const char *intag,
                         const int *incols, const char *outtag, int *outcols)
{
    struct schema *insc, *outsc;
    int i, idx;

    insc = find_tag_schema(table, intag);
    if (NULL == insc) {
        return -1;
    }

    if (incols[0] != insc->nmembers) {
        return -2;
    }

    outsc = find_tag_schema(table, outtag);
    if (NULL == outsc) {
        return -3;
    }

    outcols[0] = outsc->nmembers;
    for (i = 0; i < outsc->nmembers; i++) {
        outcols[i + 1] = -1;
    }

    for (i = 0; i < insc->nmembers; i++) {
        struct field *f = &insc->member[i];

        if (-1 == incols[i + 1]) {
            continue;
        }
        idx = find_field_idx_in_tag(outsc, f->name);
        if (idx >= 0 && idx < outsc->nmembers) {
            outcols[idx + 1] = i;
        }
    }
    return 0;
}

/* fill the updCols array */
int describe_update_columns(const struct ireq *iq, const struct schema *chk, int *updCols)
{
    struct schema *ondisk;
    int i;

    ondisk = get_schema(iq->usedb, -1);
    if (ondisk == NULL) {
        return -1;
    }

    updCols[0] = ondisk->nmembers;

    for (i = 0; i < ondisk->nmembers; i++) {
        updCols[i + 1] = -1;
    }

    for (i = 0; i < chk->nmembers; i++) {
        struct field *chk_fld = &chk->member[i];
        int idx;
        if (ondisk == chk) {
            idx = i;
        } else {
            idx = find_field_idx_in_tag(ondisk, chk_fld->name);
        }

        if (idx < 0 || idx >= ondisk->nmembers) {
            return -3;
        }
        updCols[idx + 1] = i;
    }
    return 0;
}

int indexes_expressions_data(const struct dbtable *tbl, struct schema *sc,
                             const char *inbuf, char *outbuf,
                             blob_buffer_t *blobs, size_t maxblobs,
                             struct field *f,
                             struct convert_failure *fail_reason,
                             const char *tzname);
static int stag_to_stag_field(const struct dbtable *tbl, const char *inbuf,
                              char *outbuf, int flags,
                              struct convert_failure *fail_reason,
                              blob_buffer_t *inblobs, blob_buffer_t *outblobs,
                              int maxblobs, const char *tzname, int field_idx,
                              int field, struct schema *fromsch,
                              struct schema *tosch)
{
    int outdtsz = 0;
    blob_buffer_t *outblob = NULL;
    struct field *from_field = NULL;
    struct field *to_field = &tosch->member[field];
    int rec_srt_off = gbl_sort_nulls_correctly ? 0 : 1;
    int rc;

    int iflags = 0;
    int oflags = 0;
    if (fail_reason) {
        fail_reason->target_field_idx = field;
        fail_reason->source_field_idx = -1;
    }
    /* Just like ctag_to_stag_int with a small twist. If the old
       table had a seqno column, use it (no reason to change ids) - fall
       through to regular processing.  The column is a plain long long.
       If the old table didn't have the column, generate a new value.  */
    if (field_idx != -1)
        from_field = &fromsch->member[field_idx];

    if (gbl_replicate_local &&
        (strcasecmp(to_field->name, "comdb2_seqno") == 0) &&
        (field_idx == -1 || stype_is_null(inbuf + from_field->offset))) {
        unsigned long long val;
        int outsz;
        const struct field_conv_opts outopts = {0};

        val = get_unique_longlong(thedb);

        rc = CLIENT_to_SERVER(&val, sizeof(unsigned long long), CLIENT_INT, 0,
                              NULL, NULL, outbuf + to_field->offset,
                              to_field->len, to_field->type, flags, &outsz,
                              &outopts, NULL);
        if (rc) {
            if (fail_reason)
                fail_reason->reason = CONVERT_FAILED_SHOULDNT_HAPPEN_1;
            return -1;
        }
        return 0;
    }

    if (outblobs && to_field->blob_index >= 0) {
        if (to_field->blob_index >= maxblobs) {
            if (fail_reason)
                fail_reason->reason = CONVERT_FAILED_BAD_BLOB_PROGRAMMER;
            return -1;
        }
        outblob = &outblobs[to_field->blob_index];
    }

    if ((tosch->flags & SCHEMA_INDEX) && to_field->isExpr) {
        if (fromsch->flags & SCHEMA_INDEX) {
            if (fail_reason)
                fail_reason->reason = CONVERT_FAILED_INDEX_EXPRESSION;
            return -1;
        } else {
            rc = indexes_expressions_data(tbl, fromsch, inbuf, outbuf, inblobs,
                                          maxblobs, to_field, fail_reason,
                                          tzname);
            if (rc) {
                if (fail_reason)
                    fail_reason->reason = CONVERT_FAILED_INDEX_EXPRESSION;
                return -1;
            }
        }
        /* indexes_expressions_data took care of INDEX_DESCEND already */
        return 0;
    } else if (field_idx == -1) {
        if (flags & CONVERT_UPDATE)
            return 0;

        if (to_field->in_default == NULL ||
            stype_is_null(to_field->in_default)) {
            if ((to_field->flags & NO_NULL) &&
                !(flags & CONVERT_NULL_NO_ERROR)) {
                if (fail_reason)
                    fail_reason->reason = CONVERT_FAILED_NULL_CONSTRAINT_VIOLATION;
                return -1;
            }
            rc = NULL_to_SERVER(outbuf + to_field->offset, to_field->len,
                                to_field->type);
            if (rc) {
                if (fail_reason)
                    fail_reason->reason = CONVERT_FAILED_SHOULDNT_HAPPEN_1;
                return -1;
            }
        } else {
            if (fail_reason)
                fail_reason->source_field_idx = field_idx;
            /* Be sure to evalute a dbstore function for new records only.
               For existing records, use NULL */
            if (to_field->in_default_type == SERVER_FUNCTION)
                rc = NULL_to_SERVER(outbuf + to_field->offset, to_field->len, to_field->type);
            else
                rc = SERVER_to_SERVER(to_field->in_default, to_field->in_default_len, to_field->in_default_type,
                                      NULL /*convopts*/, NULL /*blob*/, 0, outbuf + to_field->offset, to_field->len,
                                      to_field->type, oflags, &outdtsz, &to_field->convopts, outblob /*blob*/
                );
            if (rc) {
                if (fail_reason)
                    fail_reason->reason = CONVERT_FAILED_INCOMPATIBLE_VALUES;
                return -1;
            }
        }
        if (to_field->flags & INDEX_DESCEND)
            xorbuf(((char *)outbuf) + to_field->offset + rec_srt_off,
                    to_field->len - rec_srt_off);
        return 0;
    }
    // else we have (field_idx != -1)

    blob_buffer_t *inblob = NULL;
    from_field = &fromsch->member[field_idx];

    if (inblobs && from_field->blob_index >= 0) {
        if (from_field->blob_index >= maxblobs) {
            if (fail_reason)
                fail_reason->reason = CONVERT_FAILED_BAD_BLOB_PROGRAMMER;
            return -1;
        }
        inblob = &inblobs[from_field->blob_index];

        /* we had a bug at some point which allowed null blobs to be added, when
           the user
           really meant 0-length blobs.  If the input blob is null, and we don't
           allow nulls
           and the fix flag is set (null_blob_fix in lrl file), convert it to a
           zero-length
           blob */
        if (gbl_disallow_null_blobs &&
            stype_is_null(inbuf + from_field->offset) && inblob->exists == 0 &&
            (from_field->flags & NO_NULL)) {
            extern int null_bit;
            /* unset null bit, blob exists */
            inblob->exists = 1;
            bclr(inbuf + from_field->offset, null_bit);
            bset(inbuf + from_field->offset, data_bit);
        }
    }

    /* Don't allow nulls to be copied across if the dest field does not
     * support them.  Otherwise schema changes can end up with broken
     * databases.. */
    if ((to_field->flags & NO_NULL) &&
        field_is_null(fromsch, from_field, inbuf)) {
        if (fail_reason) {
            fail_reason->reason = CONVERT_FAILED_NULL_CONSTRAINT_VIOLATION;
        }
        return -1;
    }

    if ((tosch->flags & SCHEMA_INDEX) && (to_field->flags & INDEX_DESCEND))
        oflags = INDEX_DESCEND;

    if ((fromsch->flags & SCHEMA_INDEX) && (from_field->flags & INDEX_DESCEND))
        iflags = INDEX_DESCEND;
    if (from_field->flags & INDEX_DESCEND)
        xorbuf(((char *)inbuf) + from_field->offset + rec_srt_off,
               from_field->len - rec_srt_off);
    if (tzname && tzname[0]) {

        struct field_conv_opts_tz tzopts;

        bzero(&tzopts, sizeof(tzopts));
        memcpy(&tzopts, &to_field->convopts, sizeof(struct field_conv_opts));
        tzopts.flags |= FLD_CONV_TZONE;
        strncpy0(tzopts.tzname, tzname, sizeof(tzopts.tzname));

        rc = SERVER_to_SERVER(inbuf + from_field->offset, from_field->len,
                              from_field->type, &from_field->convopts, inblob,
                              iflags, outbuf + to_field->offset, to_field->len,
                              to_field->type, oflags, &outdtsz,
                              (const struct field_conv_opts *)&tzopts, outblob);
    } else {
        rc = SERVER_to_SERVER(inbuf + from_field->offset, from_field->len,
                              from_field->type, &from_field->convopts, inblob,
                              iflags, outbuf + to_field->offset, to_field->len,
                              to_field->type, oflags, &outdtsz,
                              &to_field->convopts, outblob);
    }
#if 0
    printf("%s -> %s field %s rc %d\n", fromtag, totag, to_field->name, rc);
    printf("from type %d:\n", from_field->type);
    fsnapf(stdout, inbuf + from_field->offset, from_field->len);
    printf("to type %d:\n", to_field->type);
    fsnapf(stdout, outbuf + to_field->offset, to_field->len);
#endif
    if (rc) {
        if (fail_reason)
            fail_reason->reason = CONVERT_FAILED_INCOMPATIBLE_VALUES;
        return -1;
    }
    if (from_field->flags & INDEX_DESCEND)
        xorbuf(((char *)inbuf) + from_field->offset + rec_srt_off,
               from_field->len - rec_srt_off);
    if (to_field->flags & INDEX_DESCEND)
        xorbuf(((char *)outbuf) + to_field->offset + rec_srt_off,
               to_field->len - rec_srt_off);
    return 0;
}

int stag_set_key_null(struct dbtable *table, const char *tag, const char *inkey, const int keylen, char *outkey)
{
    struct schema *schema;
    schema = find_tag_schema(table, tag);
    if (schema == NULL)
        return -1;

    memcpy(outkey, inkey, keylen);
    for (int fieldno = 0; fieldno < schema->nmembers; fieldno++) {
        struct field *field = &schema->member[fieldno];
        if (field->flags & NO_NULL) {
            return ERR_NULL_CONSTRAINT;
        }
        int rc = NULL_to_SERVER(outkey + field->offset, field->len, field->type);
        if (rc)
            return rc;
    }
    return 0;
}

/*
 * On success only outblobs will be valid, there is no need to free up inblobs.
 * On failure the caller should free inblobs and outblobs.
 */
static int _stag_to_stag_buf_flags_blobs(const struct dbtable *tbl, struct schema *fromsch,
                                         struct schema *tosch, const char *inbuf,char *outbuf,
                                         int flags, struct convert_failure *fail_reason,
                                         blob_buffer_t *inblobs, blob_buffer_t *outblobs,
                                         int maxblobs, const char *tzname)
{
    if (fail_reason)
        init_convert_failure_reason(fail_reason);

    if (fromsch == NULL || tosch == NULL) {
        if (fail_reason)
            fail_reason->reason = CONVERT_FAILED_INVALID_INPUT_TAG;
        return -1;
    }

    if (fail_reason) {
        fail_reason->source_schema = fromsch;
        fail_reason->target_schema = tosch;
    }

    for (int field = 0; field < tosch->nmembers; field++) {
        int field_idx;

        if (fromsch == tosch) {
            field_idx = field;
        } else {
            field_idx = find_field_idx_in_tag(fromsch, tosch->member[field].name);
        }
        int rc = stag_to_stag_field(tbl, inbuf, outbuf, flags, fail_reason, inblobs,
                                    outblobs, maxblobs, tzname, field_idx,
                                    field, fromsch, tosch);
        if (rc)
            return rc;
    }
    return 0;
}

int *get_tag_mapping(struct schema *fromsch, struct schema *tosch)
{
    int *tagmap =
        malloc(tosch->nmembers * sizeof(int)); // clean this up only once

    for (int field = 0; field < tosch->nmembers; field++) {
        struct field *to_field = &tosch->member[field];
        tagmap[field] = find_field_idx_in_tag(fromsch, to_field->name);
    }
    return tagmap;
}

/* use cached from -> to mapping
 * no blobs for now
 */
int stag_to_stag_buf_cachedmap(const struct dbtable *tbl, int tagmap[],
                               struct schema *from, struct schema *to,
                               const char *inbuf, char *outbuf, int flags,
                               struct convert_failure *fail_reason,
                               blob_buffer_t *inblobs, int maxblobs)
{
    int rc = 0;

    if (fail_reason)
        init_convert_failure_reason(fail_reason);

    if (from == NULL) {
        if (fail_reason)
            fail_reason->reason = CONVERT_FAILED_INVALID_INPUT_TAG;
        return -1;
    }
    if (fail_reason)
        fail_reason->source_schema = from;

    if (to == NULL) {
        if (fail_reason)
            fail_reason->reason = CONVERT_FAILED_INVALID_OUTPUT_TAG;
        return -1;
    }
    if (fail_reason)
        fail_reason->target_schema = to;

    blob_buffer_t newblobs[MAXBLOBS];
    blob_buffer_t *p_newblobs;

    if (inblobs) /* if we were given blobs */
    {
        /* make sure we are using the right MAXBLOBS number */
        if (maxblobs != MAXBLOBS) {
            logmsg(LOGMSG_ERROR, "stag_to_stag_buf_blobs with maxblobs=%d!\n",
                   maxblobs);
            return -1;
        }

        /* zero out newblobs for us to store the return blobs */
        bzero(newblobs, sizeof(newblobs));
        p_newblobs = newblobs;
    } else /* not using blobs */
    {
        p_newblobs = NULL;
        maxblobs = 0;
    }

    for (int field = 0; field < to->nmembers; field++) {
        rc = stag_to_stag_field(tbl, inbuf, outbuf, flags, fail_reason, inblobs,
                                p_newblobs, maxblobs, NULL, tagmap[field],
                                field, from, to);

        if (rc)
            break;
    }

    if (inblobs) /* if we were given blobs */
    {
        /* free the blobs that came in */
        free_blob_buffers(inblobs, maxblobs);

        /* and if successful copy over the return blobs */
        if (rc < 0)
            free_blob_buffers(newblobs, MAXBLOBS);
        else
            memcpy(inblobs, newblobs, sizeof(newblobs));
    }

    return rc;
}

/*
 * Convert an ondisk format key for one table into the equivalent ondisk format
 * key in the other table.  This is used for foreign key constraint checking.
 * The number of columns in the two key tags should be the same.  We ignore
 * column names here - the 1st column in fromtag maps to the 1st column in
 * totag and so on.
 */
int stag_to_stag_buf_ckey(const struct dbtable *table, const char *fromtag,
                          const char *inbuf, const char *totable,
                          const char *totag, char *outbuf, int *nulls,
                          enum constraint_dir direction)
{
    struct schema *from, *to;
    int rc;
    int rec_srt_off = 1;

    if (gbl_sort_nulls_correctly)
        rec_srt_off = 0;

    from = find_tag_schema(table, fromtag);
    if (from == NULL)
        return -1;

    to = find_tag_schema(get_dbtable_by_name(totable), totag);
    if (to == NULL)
        return -1;

    int nmembers = to->nmembers;

    if (gbl_fk_allow_prefix_keys && gbl_fk_allow_superset_keys) {
        if (from->nmembers < to->nmembers)
            nmembers = from->nmembers;
    } else if (gbl_fk_allow_prefix_keys) {
        switch (direction) {
        case FK2PK:
            if (to->nmembers < from->nmembers) {
                return -1;
            }
            nmembers = from->nmembers;
            break;
        case PK2FK:
            if (to->nmembers > from->nmembers) {
                return -1;
            }
            break;
        default:
            return -1;
        }
    } else if (gbl_fk_allow_superset_keys) {
        switch (direction) {
        case FK2PK:
            if (to->nmembers > from->nmembers) {
                return -1;
            }
            break;
        case PK2FK:
            if (to->nmembers < from->nmembers) {
                return -1;
            }
            nmembers = from->nmembers;
            break;
        default:
            return -1;
        }
    } else if (to->nmembers != from->nmembers) {
        return -1;
    }

    if (nulls)
        *nulls = 0;

    struct field *to_field = NULL;
    for (int field = 0; field < nmembers; field++) {
        int outdtsz = 0;
        to_field = &to->member[field];
        struct field *from_field = &from->member[field];

        if (nulls && field_is_null(from, from_field, inbuf)) {
            *nulls = 1;
        }

        int oflags = ((to->flags & SCHEMA_INDEX) && (to_field->flags & INDEX_DESCEND)) ? INDEX_DESCEND : 0;
        int iflags = ((from->flags & SCHEMA_INDEX) && (from_field->flags & INDEX_DESCEND)) ? INDEX_DESCEND : 0;

        if (from_field->flags & INDEX_DESCEND)
            xorbuf(((char *)inbuf) + from_field->offset + rec_srt_off,
                   from_field->len - rec_srt_off);
        rc = SERVER_to_SERVER(inbuf + from_field->offset, from_field->len,
                              from_field->type, &from_field->convopts,
                              NULL /*blob*/, iflags, outbuf + to_field->offset,
                              to_field->len, to_field->type, oflags, &outdtsz,
                              &to_field->convopts, NULL /*blob*/);
        if (from_field->flags & INDEX_DESCEND)
            xorbuf(((char *)inbuf) + from_field->offset + rec_srt_off,
                   from_field->len - rec_srt_off);
        if (rc)
            return -1;
        if (to_field->flags & INDEX_DESCEND)
            xorbuf(((char *)outbuf) + to_field->offset + rec_srt_off,
                   to_field->len - rec_srt_off);
#if 0
      printf("%s -> %s field %s rc %d\n", fromtag, totag, to_field->name, rc);
      printf("from type %d:\n", from_field->type);
      fsnapf(stdout, inbuf + from_field->offset, from_field->len);
      printf("to type %d:\n", to_field->type);
      fsnapf(stdout, outbuf + to_field->offset, to_field->len);
#endif
    }

    return to_field->offset + to_field->len;
}

/*
 * Look at all tags of a table and see if there is any difference between
 * them and the .NEW. tags.
 */
struct cmp_tag_struct {
    int numoldtags;
    int numnewtags;

    int numtags;
    struct schema *tags[1024];
};

static int cmp_tag_callback(const struct schema *s, struct cmp_tag_struct *info)
{
    /* skip index tags */
    if (s->flags & SCHEMA_TABLE) {
        if (strncmp(s->tag, ".NEW.", 5) == 0)
            info->numnewtags++;
        else {
            info->numoldtags++;
            info->tags[info->numtags++] = (struct schema *)s;
        }
    }

    return 0;
}

/* NOTE: this is called in one place, with stderr, so any code
   it calls in turn assumes the output is to be logged. */
int compare_all_tags(const char *table, FILE *out)
{
    int rc;
    dbtable *db;

    rc = 0;

    lock_taglock();
    {
        struct dbtag *tag;
        struct cmp_tag_struct info;
        bzero(&info, sizeof(info));
        tag = hash_find_readonly(gbl_tag_hash, &table);
        if (tag == NULL) {
            unlock_taglock();
            return 0;
        }
        hash_for(tag->tags, (hashforfunc_t *)cmp_tag_callback, &info);
        if (info.numnewtags > info.numoldtags) {
            if (out) {
                logmsg(LOGMSG_USER, "number of tags has increased\n");
            }
            rc = 1;
        } else {
            int ii;
            for (ii = 0; ii < info.numtags; ii++) {
                struct schema *old = info.tags[ii];
                struct schema *new;
                char search[MAXTAGLEN + 6];
                snprintf(search, sizeof(search), ".NEW.%s", old->tag);
                /* find the corresponding .NEW. tag */
                LISTC_FOR_EACH(&tag->taglist, new, lnk)
                {
                    if (strcasecmp(search, new->tag) == 0)
                        break;
                }
                if (new == NULL) {
                    if (out) {
                        logmsg(LOGMSG_USER, "tag %s does not exist in new schema\n",
                                old->tag);
                    }
                    rc = 1;
                    break;
                }
                rc = compare_tag_int(old, new, out, 1 /*strict comparison*/);
                if (rc != 0)
                    break;
            }
        }
    }
    unlock_taglock();

    if (rc != 0)
        return rc;

    db = get_dbtable_by_name(table);
    if (db == NULL) {
        logmsg(LOGMSG_ERROR, "Invalid table %s\n", table);
        return 0;
    }

    rc = compare_indexes(table, out);
    if (rc != 0)
        return rc;

    return 0;
}

int compare_tag(const char *tblname, const char *tag, FILE *out)
{
    dbtable *db;
    struct schema *old, *new;
    char oldtag[MAXTAGLEN + 16];
    char newtag[MAXTAGLEN + 16];

    snprintf(oldtag, sizeof(oldtag), "%s", tag);
    snprintf(newtag, sizeof(newtag), ".NEW.%s", tag);

    db = get_dbtable_by_name(tblname);
    if (db == NULL) {
        logmsg(LOGMSG_ERROR, "Invalid table %s\n", tblname);
        return -1;
    }
    old = find_tag_schema(db, oldtag);
    if (old == NULL) {
        logmsg(LOGMSG_ERROR, "Can't find schema for old table %s\n", tblname);
        return -1;
    }
    new = find_tag_schema(db, newtag);
    if (new == NULL) {
        logmsg(LOGMSG_ERROR, "Can't find schema for new table %s\n", tblname);
        return -1;
    }

    return compare_tag_int(old, new, out, 0 /*non-strict comparison*/);
}

static int default_cmp(int oldlen, const void *oldptr, int newlen,
                       const void *newptr)
{
    if (!oldptr && !newptr)
        return 0;
    else if (oldptr && !newptr)
        return -1;
    else if (!oldptr && newptr)
        return -1;
    else if (oldlen != newlen)
        return -1;
    else
        return memcmp(oldptr, newptr, oldlen);
}

/* compare_tag_int() return codes
 * SC_TAG_CHANGE if :-
 *   1. new recsize is smaller
 *   2. field type has changed
 *   3. NULL attribute removed from field
 *   4. field size reduced
 *   5. field deleted
 *   6. enable system versioning
 * SC_BAD_NEW_FIELD: New field missing dbstore or null
 * SC_BAD_DBPAD: Byte array size changed and missing dbpad
 * SC_COLUMN_ADDED: If new column is added
 * SC_DBSTORE_CHANGE: Only change is dbstore of an existing field */
int compare_tag_int(struct schema *old, struct schema *new, FILE *out,
                    int strict)
{
    struct field *fnew = NULL, *fold;
    int rc = SC_NO_CHANGE;
    int change = SC_NO_CHANGE;
    int oidx, nidx;

    if (!old->periods[PERIOD_SYSTEM].enable &&
        new->periods[PERIOD_SYSTEM].enable)
        return SC_TAG_CHANGE;
    /* Find changes to old fields */
    for (oidx = 0; oidx < old->nmembers; ++oidx) {
        char buf[256] = "";
        int oldflags, newflags;
        int found;
        fold = &old->member[oidx];
        oldflags = fold->flags & INDEX_DESCEND;
        found = 0;
        for (nidx = 0; nidx < new->nmembers; ++nidx) {
            fnew = &new->member[nidx];
            newflags = fnew->flags & INDEX_DESCEND;
            if (strcasecmp(fnew->name, fold->name) == 0) {
                found = 1;
                if (fnew->type != fold->type) {
                    snprintf(buf, sizeof(buf), "data type was %d, now %d",
                             fold->type, fnew->type);
                    change = SC_TAG_CHANGE;
                } else if (!(fold->flags & NO_NULL) &&
                           (fnew->flags & NO_NULL)) {
                    snprintf(buf, sizeof(buf),
                             "field flags were 0x%x, now 0x%x", fold->flags,
                             fnew->flags);
                    change = SC_TAG_CHANGE;
                } else if (fnew->len != fold->len) {
                    snprintf(buf, sizeof(buf), "field size was %d, now %d",
                             fold->len, fnew->len);
                    if (fnew->type == SERVER_BYTEARRAY && !((&fnew->convopts)->flags & FLD_CONV_DBPAD)) {
                        if (out) {
                            logmsg(LOGMSG_INFO,
                                   "tag %s field %s changed byte array size from %d to %d and requires dbpad\n",
                                   old->tag, fold->name, fold->len, fnew->len);
                        }
                        return SC_BAD_DBPAD;
                    } else if (fnew->len < fold->len) {
                        change = SC_TAG_CHANGE;
                    } else if (new->nmembers == old->nmembers) {
                        if ((fnew->type == SERVER_BLOB2) ||
                            (fnew->type == SERVER_VUTF8)) {
                            /* Blob size might have increased, and can not fit
                             * in dta file. */
                            change = SC_TAG_CHANGE;
                        } else {
                            change = SC_COLUMN_ADDED;
                        }

                    }
                    /*
                       else if (fnew->len >  fold->len && fold->in_default)
                       {
                       change = SC_TAG_CHANGE;
                       }
                     */
                    else {
                        change = SC_COLUMN_ADDED;
                    }
                } else if (fnew->offset != fold->offset) {
                    snprintf(buf, sizeof(buf), "field offset was %d, now %d",
                             fold->offset, fnew->offset);
                    change = SC_COLUMN_ADDED;
                } else if (newflags != oldflags) {
                    snprintf(buf, sizeof(buf),
                             "field flags were 0x%x, now 0x%x", oldflags,
                             newflags);
                    change = SC_COLUMN_ADDED;
                }
                /* DBSTORE checks have to be last; its ok to have
                 * this change without increase in record size */
                else if (fold->in_default_len != fnew->in_default_len ||
                         fold->in_default_type != fnew->in_default_type) {
                    snprintf(buf, sizeof(buf), "dbstore");
                    change = SC_DBSTORE_CHANGE;
                    if (fnew->in_default_type == SERVER_FUNCTION && fold->in_default_type != SERVER_FUNCTION &&
                        (fnew->flags & NO_NULL)) {
                        if (out)
                            logmsg(LOGMSG_ERROR, "field %s must be nullable to set a default function\n", fold->name);
                        return SC_BAD_DBSTORE_FUNC_NOT_NULL;
                    }

                    if ((fnew->in_default_type == SERVER_DATETIME || fnew->in_default_type == SERVER_DATETIMEUS) &&
                        stype_is_null(fnew->in_default) && (fnew->flags & NO_NULL)) {
                        if (out)
                            logmsg(LOGMSG_ERROR, "field %s must be nullable to use CURRENT_TIMESTAMP\n", fold->name);
                        return SC_BAD_DBSTORE_FUNC_NOT_NULL;
                    }
                } else {
                    assert(fold->in_default_len == fnew->in_default_len);
                    int len = fold->in_default_len;
                    if (memcmp(fold->in_default, fnew->in_default, len)) {
                        snprintf(buf, sizeof(buf), "dbstore");
                        change = SC_DBSTORE_CHANGE;
                    }
                }
                break;
            }
        }

        /* if old column has been deleted */
        if (!found) {
            snprintf(buf, sizeof(buf), "field has been deleted");
            change = SC_TAG_CHANGE;
        }

        /* These kind of changes would require a rebuild if this were the
         * ondisk tag.  Note that we force a rebuild if we are REMOVING
         * the null attribute from a field; this is so that we can verify
         * that that constraint still holds. */
        /*
           if (fnew->type != fold->type) {
           snprintf(buf, sizeof(buf), "data type was %d, now %d",
           fold->type, fnew->type);
           change = SC_TAG_CHANGE;
           } else if (fnew->offset != fold->offset) {
           snprintf(buf, sizeof(buf), "field offset was %d, now %d",
           fold->offset, fnew->offset);
           change = SC_COLUMN_ADDED;
           } else if (fnew->len != fold->len) {
           snprintf(buf, sizeof(buf), "field size was %d, now %d",
           fold->len, fnew->len);
           change = SC_COLUMN_ADDED;
           } else if (newflags != oldflags) {
           snprintf(buf, sizeof(buf), "field flags were 0x%x, now 0x%x",
           oldflags, newflags);
           change = SC_COLUMN_ADDED;
           } else if (!(fold->flags & NO_NULL) && (fnew->flags & NO_NULL)) {
           snprintf(buf, sizeof(buf),
           "field flags were 0x%x, now 0x%x", fold->flags, fnew->flags);
           change = SC_COLUMN_ADDED;
           } else if (strcasecmp(fnew->name, fold->name) != 0) {
           snprintf(buf, sizeof(buf), "name was '%s', now '%s'",
           fold->name, fnew->name);
           change = SC_TAG_CHANGE;
           }
         */

        /* appears to be dead code -Akshat */
        if (!buf[0] && strict) {
            /* These kind of changes would not cause a rebuild on a schema
             * change
             * but they are schema changes. -- Sam J */
            if (fnew->convopts.flags != fold->convopts.flags) {
                snprintf(buf, sizeof(buf),
                         "conversion option flags were 0x%x, now 0x%x",
                         fold->convopts.flags, fnew->convopts.flags);
                change = SC_TAG_CHANGE;
            } else if ((fnew->convopts.flags & FLD_CONV_DBPAD) &&
                       fnew->convopts.dbpad != fold->convopts.dbpad) {
                snprintf(buf, sizeof(buf), "dbpad value was %d, now %d",
                         fold->convopts.dbpad, fnew->convopts.dbpad);
                change = SC_TAG_CHANGE;
            } else if (fnew->flags != fold->flags) { /* null allowed change */
                snprintf(buf, sizeof(buf), "field flags were 0x%x, now 0x%x",
                         fold->flags, fnew->flags);
                change = SC_TAG_CHANGE;
            } else if (fold->in_default_type != fnew->in_default_type ||
                       default_cmp(fold->in_default_len, fold->in_default,
                                   fnew->in_default_len,
                                   fnew->in_default) != 0) {
                snprintf(buf, sizeof(buf),
                         "dbstore changed (%d, %d, %p) -> (%d, "
                         "%d, %p)",
                         fold->in_default_type, fold->in_default_len,
                         fold->in_default, fnew->in_default_type,
                         fnew->in_default_len, fnew->in_default);
                change = SC_TAG_CHANGE;
            } else if (fold->out_default_type != fnew->out_default_type ||
                       default_cmp(fold->out_default_len, fold->out_default,
                                   fnew->out_default_len,
                                   fnew->out_default) != 0) {
                snprintf(buf, sizeof(buf),
                         "dbload changed (%d, %d, %p) -> (%d, "
                         "%d, %p)",
                         fold->out_default_type, fold->out_default_len,
                         fold->out_default, fnew->out_default_type,
                         fnew->out_default_len, fnew->out_default);
                change = SC_TAG_CHANGE;
            }
        }
        /* end - appears to be dead code */

        if (change == SC_TAG_CHANGE) {
            if (out) {
                logmsg(LOGMSG_INFO,
                       "tag %s field %d (named %s) has changed (%s)\n",
                       old->tag, oidx, fold->name, buf);
            }
            return change;
        }

        if (change == SC_COLUMN_ADDED || change == SC_DBSTORE_CHANGE) {
            if (out) {
                logmsg(LOGMSG_INFO,
                       "tag %s field %d (named %s) has changed (%s)\n",
                       old->tag, oidx, fold->name, buf);
            }
            /* don't overwrite COLUMN_ADDED with DBSTORE */
            if (rc != SC_COLUMN_ADDED)
                rc = change;
            change = SC_NO_CHANGE;
        }
    }

    /* Verify that new fields have one of dbstore or null */
    for (nidx = 0; nidx < new->nmembers; ++nidx) {
        int found;
        fnew = &new->member[nidx];
        found = 0;
        for (oidx = 0; oidx < old->nmembers; ++oidx) {
            fold = &old->member[oidx];
            if (strcasecmp(fnew->name, fold->name) == 0) {
                found = 1;
                break;
            }
        }
        if (!found) {
            int allow_null = !(fnew->flags & NO_NULL);
            int dbstore_type = fnew->in_default_type;
            int is_datetime_type = (dbstore_type == SERVER_DATETIME || dbstore_type == SERVER_DATETIMEUS);
            int is_current_timestamp = (is_datetime_type && stype_is_null(fnew->in_default));
            int is_function = (is_current_timestamp || dbstore_type == SERVER_FUNCTION);
            int requires_null = (is_function || dbstore_type == SERVER_SEQUENCE);

            if (SERVER_VUTF8 == fnew->type &&
                fnew->in_default_len > (fnew->len - 5)) {
                rc = SC_TAG_CHANGE;
                if (out) {
                    logmsg(LOGMSG_INFO, "tag %s has new field %d (named %s -- dbstore "
                                 "too large forcing rebuild)\n",
                            old->tag, nidx, fnew->name);
                }
                break;
            } else if (allow_null || (fnew->in_default && !requires_null)) {
                rc = SC_COLUMN_ADDED;
                if (out) {
                    logmsg(LOGMSG_INFO, "tag %s has new field %d (named %s)\n",
                            old->tag, nidx, fnew->name);
                }
            } else if (!allow_null && is_function) {
                rc = SC_BAD_DBSTORE_FUNC_NOT_NULL;
                if (out) {
                    logmsg(LOGMSG_INFO, "tag %s has new field %d (named %s)"
                           "that uses a dbstore function but isn't nullable\n",
                           old->tag, nidx, fnew->name);
                }
                break;
            } else {
                if (out) {
                    logmsg(LOGMSG_INFO, "tag %s has new field %d (named %s) without "
                            "dbstore or null\n",
                            old->tag, nidx, fnew->name);
                }
                rc = SC_BAD_NEW_FIELD;
                break;
            }
        }
    }

    /* Cannot do instant schema change if recsize does not
     * atleast increase by 2 bytes */
    if (rc == SC_COLUMN_ADDED) {
        if (new->recsize < old->recsize + 2) {
            if (out) {
                logmsg(LOGMSG_INFO, "tag %s recsize %d, was %d\n", old->tag,
                        new->recsize, old->recsize);
            }
            rc = SC_TAG_CHANGE;
        }
    }
    return rc;
}

int has_index_changed(struct dbtable *tbl, char *keynm, int ct_check, int newkey,
                      FILE *out, int accept_type_change)
{
    struct schema *old, *new;
    struct field *fnew, *fold;
    int fidx = 0;
    int ix = 0, rc = 0;
    char ixbuf[MAXTAGLEN * 2]; /* .NEW..ONDISK_ix_xxxx */
    char oldtag[MAXTAGLEN + 16];
    char newtag[MAXTAGLEN + 16];
    const char *tag = ".ONDISK";

    snprintf(oldtag, sizeof(oldtag), "%s", tag);
    snprintf(newtag, sizeof(newtag), ".NEW.%s", tag);

    rc = getidxnumbyname(tbl, keynm, &ix);
    if (rc != 0) {
        logmsg(LOGMSG_INFO, "No index %s in old schema\n", keynm);
        return 1;
    }
    snprintf(ixbuf, sizeof(ixbuf), ".NEW.%s", keynm);
    rc = getidxnumbyname(tbl, ixbuf, &fidx);
    if (rc != 0) {
        logmsg(LOGMSG_INFO, "No index %s in new schema\n", keynm);
        return 1;
    }
    if (fidx != ix) {
        logmsg(LOGMSG_INFO, "key %s maps from ix%d->ix%d\n", keynm, ix, fidx);
        return 1;
    }

    /*fprintf(stderr, "key %s ix %d\n", keynm, ix);*/

    snprintf(ixbuf, sizeof(ixbuf), "%s_ix_%d", tag, ix);
    old = find_tag_schema(tbl, ixbuf);
    if (old == NULL) {
        logmsg(LOGMSG_ERROR, "Can't find schema for old table %s index %d\n",
               tbl->tablename, ix);
        return 1;
    }
    snprintf(ixbuf, sizeof(ixbuf), ".NEW.%s_ix_%d", tag, fidx);
    new = find_tag_schema(tbl, ixbuf);
    if (new == NULL) {
        logmsg(LOGMSG_ERROR, "Can't find schema for new table %s index %d\n",
               tbl->tablename, fidx);
        return 1;
    }

    if (!ct_check &&
        (new->flags !=
         old->flags)) /* index properties changes..it is ok to skip that
                         for constraints */
    {
        if (out) {
            logmsg(LOGMSG_USER, "index %d properties have changed\n", ix);
        }
        return 1;
    }

    if (new->nmembers != old->nmembers) {
        /* number of members of index changed */
        if (out) {
            logmsg(LOGMSG_USER, "index %d number of members has changed\n", ix);
        }
        return 1;
    }
    for (fidx = 0; fidx < new->nmembers; fidx++) {
        fnew = &new->member[fidx];
        fold = &old->member[fidx];

        /* optionally we accept type changing */
        if (accept_type_change && fold->type != fnew->type) {
            if (debug_switch_allow_key_typechange()) {
                if (fold->type != SERVER_UINT || fold->len != 8 ||
                    fold->type != SERVER_BINT || fold->len != 8) {
                    if (out) {
                        logmsg(LOGMSG_USER, "index %d member %d (called %s) changed "
                                     "types, only allow ull->ll\n",
                                ix, fidx, fold->name);
                    }
                    return 1;
                }
            } else {
                /* we accept lossless conversion between types */
            }
        }

        if ((!accept_type_change && fnew->type != fold->type) /* type changed */
            || strcmp(fnew->name, fold->name) != 0 /* member changed */
            || fnew->offset != fold->offset        /* position changed */
            || fnew->len != fold->len              /* size changed */
            || fnew->flags != fold->flags /* flags (asc/desc) changed */
            ) {
            if (out) {
                logmsg(LOGMSG_USER, "index %d member %d (called %s) has changed\n", ix,
                        fidx, fold->name);
            }
            return 1;
        }
    }

    return 0;
}

/* Compare two indexes and summarise their changes.
 * Input: oldix, newix - two index schemas to compare.
 *        oldtbl, newtbl - the corresponding table schemas.
 * Output:
 *    *ondisk_chg - set if the ondisk structure of the indexes has changed.
 *    *schema_chg - set if anything has changed.
 *         descr, descrlen - buffer to place string description of change.
 */
int cmp_index_int(struct schema *oldix, struct schema *newix, char *descr,
                  size_t descrlen)
{
    int oldattr, newattr;

    /* First compare attributes */
    oldattr = oldix->flags & (SCHEMA_DUP | SCHEMA_RECNUM | SCHEMA_DATACOPY | SCHEMA_UNIQNULLS | SCHEMA_PARTIALDATACOPY);
    newattr = newix->flags & (SCHEMA_DUP | SCHEMA_RECNUM | SCHEMA_DATACOPY | SCHEMA_UNIQNULLS | SCHEMA_PARTIALDATACOPY);
    if (oldattr != newattr) {
        if (descr)
            snprintf(descr, descrlen, "properties have changed");
        return 1;
    } else if (newix->nmembers != oldix->nmembers) {
        if (descr)
            snprintf(descr, descrlen, "number of fields has changed");
        return 1;
    } else if ((oldix->where == NULL && newix->where != NULL) ||
               (oldix->where != NULL && newix->where == NULL) ||
               (oldix->where != NULL && newix->where != NULL &&
                strcmp(oldix->where, newix->where) != 0)) {
        if (descr)
            snprintf(descr, descrlen, "partial index has changed");
        return 1;
    } else {
        int fidx;
        for (fidx = 0; fidx < newix->nmembers; fidx++) {
            /* Check for differences to individual fields.  Note that we don't
             * care if the position of the field within the data record has
             * changed.  That can change with impunity.  We care a lot if the
             * size or shape of the field has changed, or if it is in fact a
             * different column (hence the name check). */
            struct field *oldfld = &oldix->member[fidx];
            struct field *newfld = &newix->member[fidx];
            if (oldfld->type != newfld->type /* type of field changed */
                ||
                oldfld->offset != newfld->offset  /* offset in index changed */
                || oldfld->len != newfld->len     /* length of field changed */
                || oldfld->flags != newfld->flags /* ASCEND/DESCEND changed */
                || 0 != strcmp(oldfld->name, newfld->name)) {
                if (descr)
                    snprintf(descr, descrlen, "field %d (%s) changed", fidx + 1,
                             oldfld->name);
                return 1;
            }
        }

        if (oldix->flags & newix->flags & SCHEMA_PARTIALDATACOPY) {
            struct schema *oldpd = oldix->partial_datacopy;
            struct schema *newpd = newix->partial_datacopy;
            char *descr_helper = descr ? calloc(descrlen, sizeof(char)) : descr;
            if (cmp_index_int(oldpd, newpd, descr_helper, descrlen)) {
                if (descr) {
                    snprintf(descr, descrlen, "%s in partial datacopy", descr_helper);
                    free(descr_helper);
                }
                return 1;
            }
            if (descr)
                free(descr_helper);
        }
    }
    return 0;
}

/* See if anything about the indexes has changed which would necessitate a
 * rebuild. */
int compare_indexes(const char *tblname, FILE *out)
{
    dbtable *tbl;
    struct schema *old, *new;
    int ix;
    char ixbuf[MAXTAGLEN * 2]; /* .NEW..ONDISK_ix_xxxx */
    char oldtag[MAXTAGLEN + 16];
    char newtag[MAXTAGLEN + 16];
    const char *tag = ".ONDISK";

    snprintf(oldtag, sizeof(oldtag), "%s", tag);
    snprintf(newtag, sizeof(newtag), ".NEW.%s", tag);

    tbl = get_dbtable_by_name(tblname);
    if (tbl == NULL) {
        logmsg(LOGMSG_INFO, "Invalid table %s\n", tblname);
        return 0;
    }
    old = find_tag_schema(tbl, oldtag);
    if (old == NULL) {
        logmsg(LOGMSG_INFO, "Can't find schema for old table %s\n", tblname);
        return 0;
    }
    new = find_tag_schema(tbl, newtag);
    if (new == NULL) {
        logmsg(LOGMSG_INFO, "Can't find schema for new table %s\n", tblname);
        return 0;
    }

    /* if number of indices changed, then ondisk operations are required */
    if (old->nix != new->nix) {
        if (out) {
            logmsg(LOGMSG_INFO, "indexes have been added or removed\n");
        }
        return 1;
    }

    /* go through indices */
    for (ix = 0; ix < tbl->nix; ix++) {
        char descr[128];
        snprintf(ixbuf, sizeof(ixbuf), "%s_ix_%d", tag, ix);
        old = find_tag_schema(tbl, ixbuf);
        if (old == NULL) {
            logmsg(LOGMSG_INFO, "Can't find schema for old table %s index %d\n", tblname, ix);
            return 0;
        }
        snprintf(ixbuf, sizeof(ixbuf), ".NEW.%s_ix_%d", tag, ix);
        new = find_tag_schema(tbl, ixbuf);
        if (new == NULL) {
            logmsg(LOGMSG_INFO, "Can't find schema for new table %s index %d\n", tblname, ix);
            return 0;
        }

        if (cmp_index_int(old, new, descr, sizeof(descr)) != 0) {
            if (out)
                logmsg(LOGMSG_INFO, "index %d (%s) changed: %s\n", ix + 1, old->csctag,
                        descr);
            return 1;
        }
    }

    return 0;
}

void printrecord(char *buf, struct schema *sc, int len)
{
    int field;
    struct field *f;
    struct field_conv_opts opts = {0};

    unsigned long long uival;
    long long ival;
    char *sval;
    double dval;
    int null = 0;
    int flen = 0;

#ifdef _LINUX_SOURCE
    opts.flags |= FLD_CONV_LENDIAN;
#endif

    logmsg(LOGMSG_USER, "[");
    for (field = 0; field < sc->nmembers; field++) {
        int outdtsz = 0;
        null = 0;
        f = &sc->member[field];
        flen = f->len;
        if (len) {
            if (f->offset + f->len >= len) {
                /* ignore partial integer/real fields - shouldn't happen */
                flen = len - f->offset;
                if (flen == 0)
                    break;
            }
        }
        switch (f->type) {
        case SERVER_UINT:
            SERVER_UINT_to_CLIENT_UINT(
                buf + f->offset, flen, &opts /*convopts*/, NULL /*blob*/,
                &uival, 8, &null, &outdtsz, &opts /*convopts*/, NULL /*blob*/);
            if (null)
                logmsg(LOGMSG_USER, "%s=NULL", f->name);
            else
                logmsg(LOGMSG_USER, "%s=%llu", f->name, uival);
            break;
        case SERVER_BINT:
            SERVER_BINT_to_CLIENT_INT(buf + f->offset, flen, &opts /*convopts*/,
                                      NULL /*blob*/, &ival, 8, &null, &outdtsz,
                                      &opts /*convopts*/, NULL /*blob*/);
            if (null)
                logmsg(LOGMSG_USER, "%s=NULL", f->name);
            else
                logmsg(LOGMSG_USER, "%s=%lld", f->name, ival);
            break;
        case SERVER_BREAL:
            SERVER_BREAL_to_CLIENT_REAL(
                buf + f->offset, flen, &opts /*convopts*/, NULL /*blob*/, &dval,
                8, &null, &outdtsz, &opts /*convopts*/, NULL /*blob*/);
            if (null)
                logmsg(LOGMSG_USER, "%s=NULL", f->name);
            else
                logmsg(LOGMSG_USER, "%s=%f", f->name, dval);
            break;
        case SERVER_BCSTR:
            sval = malloc(flen + 1);
            SERVER_BCSTR_to_CLIENT_CSTR(
                buf + f->offset, flen, &opts /*convopts*/, NULL /*blob*/, sval,
                flen + 1, &null, &outdtsz, &opts /*convopts*/, NULL /*blob*/);
            if (null)
                logmsg(LOGMSG_USER, "%s=NULL", f->name);
            else
                logmsg(LOGMSG_USER, "%s=\"%s\"", f->name, sval);
            free(sval);
            break;
        case SERVER_BYTEARRAY:
            sval = malloc(flen);
            SERVER_BYTEARRAY_to_CLIENT_BYTEARRAY(
                buf + f->offset, flen, &opts /*convopts*/, NULL /*blob*/, sval,
                flen, &null, &outdtsz, &opts /*convopts*/, NULL /*blob*/);
            if (null)
                logmsg(LOGMSG_USER, "%s=NULL", f->name);
            else {
                logmsg(LOGMSG_USER, "%s=", f->name);
                hexdump(LOGMSG_USER, (const char *)sval, flen);
            }
            free(sval);

            break;
        }

        if (field != sc->nmembers - 1)
            logmsg(LOGMSG_USER, " ");
    }
    logmsg(LOGMSG_USER, "] ");
}

int have_all_schemas(void)
{
    int tbl, ix;
    char namebuf[MAXTAGLEN + 1];
    int bad = 0;
    struct schema *sqlite_stat1 = NULL;
    struct field *table_field = NULL, *index_field = NULL;

    sqlite_stat1 = find_tag_schema(get_dbtable_by_name("sqlite_stat1"), ".ONDISK");
    if (sqlite_stat1) {
        int i;
        for (i = 0; i < sqlite_stat1->nmembers; i++) {
            if (strcmp(sqlite_stat1->member[i].name, "tbl") == 0)
                table_field = &sqlite_stat1->member[i];
            else if (strcmp(sqlite_stat1->member[i].name, "idx") == 0)
                index_field = &sqlite_stat1->member[i];
        }
    }

    for (tbl = 0; tbl < thedb->num_dbs; tbl++) {
        struct schema *s =
            find_tag_schema(thedb->dbs[tbl], ".ONDISK");
        if (s == NULL) {
            logmsg(LOGMSG_ERROR, "Missing schema: table %s tag .ONDISK\n",
                   thedb->dbs[tbl]->tablename);
            bad = 1;
        }

        if (sqlite_stat1 && table_field && index_field &&
            strcasecmp(thedb->dbs[tbl]->tablename, "sqlite_stat1")) {
            int nlen = strlen(thedb->dbs[tbl]->tablename);
            if (nlen >= table_field->len || nlen >= index_field->len) {
                logmsg(LOGMSG_WARN,
                       "WARNING: table name %s too long for analyze\n",
                       thedb->dbs[tbl]->tablename);
            }
        }

        for (ix = 0; s && ix < thedb->dbs[tbl]->nix; ix++) {
            snprintf(namebuf, sizeof(namebuf), ".ONDISK_ix_%d", ix);
            s = find_tag_schema(thedb->dbs[tbl], namebuf);
            if (s == NULL) {
                logmsg(LOGMSG_ERROR, "Missing schema: table %s tag %s\n",
                       thedb->dbs[tbl]->tablename, namebuf);
                bad = 1;
            }
        }
    }

    if (bad)
        return 0;
    else
        return 1;
}

int getdefaultkeysize(const dbtable *tbl, int ixnum)
{
    char tagbuf[MAXTAGLEN];

    snprintf(tagbuf, MAXTAGLEN, ".DEFAULT_ix_%d", ixnum);
    struct schema *s = find_tag_schema(tbl, tagbuf);
    if (s == NULL)
        return -1;
    return get_size_of_schema(s);
    /* return s->member[s->nmembers-1].offset + s->member[s->nmembers-1].len; */
}

int getdefaultdatsize(const dbtable *tbl)
{
    struct schema *s = find_tag_schema(tbl, ".DEFAULT");

    if (s == NULL)
        return -1;
    return get_size_of_schema(s);
    /* return s->member[s->nmembers-1].offset + s->member[s->nmembers-1].len; */
}

int getondiskclientdatsize(const dbtable *db)
{
    struct schema *s = find_tag_schema(db, ".ONDISK_CLIENT");

    if (s == NULL)
        return -1;
    return get_size_of_schema(s);
}

int getclientdatsize(const dbtable *db, char *sname)
{
    struct schema *s = find_tag_schema(db, sname);

    if (s == NULL)
        return -1;
    return get_size_of_schema(s);
}

/* fetch the indexnumber (offset in tag map) of index with name keytag
 * on table dbname. indexnumber is the offset of the index in the csc2
 * schema starting from 0.
 * Using similar name pattern as get_dbtable_by_name()
 * funcion getidxnumbyname() was formerly named maptag2ix */
int getidxnumbyname(const struct dbtable *table, const char *keytag, int *ixnum)
{
    struct schema *s = find_tag_schema(table, keytag);
    if (s == NULL || (s->flags & SCHEMA_INDEX) == 0)
        return -1;
    *ixnum = s->ixnum;
    return 0;
}

/* initialized in schema_init */
static char *get_unique_tag(void)
{
    struct thread_info *thd;
    char *tag;

    thd = pthread_getspecific(thd_info_key);
    if (thd == NULL)
        return NULL;

    tag = malloc(MAXTAGLEN);
    if (tag == NULL)
        return NULL;

    /* Worst case: sizeof ".TEMP_-9223372036854775808_-9223372036854775808" =
       48.
       MAXTAGLEN is 64 so we're good. */
    snprintf(tag, MAXTAGLEN, ".TEMP_%" PRIdPTR "_%lld",
             (intptr_t)pthread_self(), thd->uniquetag++);

    return tag;
}

static int csc2_type_to_client_type(int csc2_type)
{
    switch (csc2_type) {
    case COMDB2_INT:
    case COMDB2_SHORT:
    case COMDB2_LONGLONG:
        return CLIENT_INT;

    case COMDB2_UINT:
    case COMDB2_USHORT:
    case COMDB2_ULONGLONG:
        return CLIENT_UINT;

    case COMDB2_FLOAT:
    case COMDB2_DOUBLE:
        return CLIENT_REAL;

    case COMDB2_BYTE:
        return CLIENT_BYTEARRAY;

    case COMDB2_CSTR:
        return CLIENT_CSTR;

    case COMDB2_PSTR:
        return CLIENT_PSTR2;

    case COMDB2_BLOB:
        return CLIENT_BLOB;

    case COMDB2_VUTF8:
        return CLIENT_VUTF8;

    case COMDB2_DATETIME:
        return CLIENT_DATETIME;

    case COMDB2_DATETIMEUS:
        return CLIENT_DATETIMEUS;

    case COMDB2_INTERVALYM:
        return CLIENT_INTVYM;

    case COMDB2_INTERVALDS:
        return CLIENT_INTVDS;

    case COMDB2_INTERVALDSUS:
        return CLIENT_INTVDSUS;

    default:
        return -1;
    }
}

void free_dynamic_schema(const char *table, struct schema *dsc)
{
    if (dsc == NULL)
        return;

    del_tag_schema(table, dsc->tag);
}

struct schema *new_dynamic_schema(const char *s, int len, int trace)
{
    struct schema *sc;
    int toff = 6, tlen;
    char *tok;
    int reclen, nfields;
    int field;
    struct field *f;
    const char *fieldname;
    int fieldname_len;
    int ftype, flen, foffset, fattr, fresrv, fdsize;
    int maxoffset = -1;
    int maxoffsetix = -1;
    struct field tmpfld;
    static int once = 1;
    int *ptrs[] = {&ftype, &flen, &fdsize, &foffset, &fattr, &fresrv, NULL};
    int ii;

    const char *end = s + len;

    if (strncasecmp(s, ".DYNT.", 6))
        return NULL;

    /* record length */
    tok = segtokx((char *)s, len, &toff, &tlen, ".");
    if (tlen == 0)
        return NULL;
    reclen = toknum(tok, tlen);
    if (reclen < 0)
        return NULL;

    tok = segtokx((char *)s, len, &toff, &tlen, ":");
    if (tlen == 0)
        return NULL;
    nfields = toknum(tok, tlen);
    if (nfields <= 0)
        return NULL;
    if (nfields > MAXDYNTAGCOLUMNS)
        return NULL;

    /* NOTE: for bound parameters get_unique_tag returns NULL, not an error */
    sc = alloc_schema(get_unique_tag(), nfields, SCHEMA_TABLE | SCHEMA_DYNAMIC);
    if (!sc)
        return NULL;

    sc->nix = 0; /* note: we won't be adding ix schema records since
                    we never need to convert to client-form keys for
                    dynamic tags (all requests are new) */
    sc->ixnum = -1;
    s = &s[toff];
    for (field = 0; field < nfields; field++) {
        f = &sc->member[field];
        f->blob_index = -1;
        f->idx = -1;

        if (s >= end)
            goto backout;
        if (*s != '{')
            goto backout;
        s++;

        /* Extract the field name.  Accept [a-zA-Z0-9_] up until a semicolon. */
        fieldname = s;
        fieldname_len = 0;
        while (s < end && *s != ';') {
            if (!isalnum(*s) && *s != '_')
                goto backout;
            fieldname_len++;
            s++;
        }
        if (fieldname_len == 0)
            goto backout;
        if (s == end)
            goto backout;
        s++;

        /* Extract the numerical fields, of which there are 6.  Each field
         * should end in a semicolon except the last. */
        for (ii = 0; ptrs[ii]; ii++) {
            int n = 0;
            int is_negative = 0;
            while (s < end) {
                if (isdigit(*s)) {
                    n = (n * 10) + (*s - '0');
                } else if (*s == '-') {
                    is_negative = !is_negative;
                } else if (*s == ';') {
                    s++;
                    break;
                } else if (*s == '}' && !ptrs[ii + 1]) {
                    /* Only last int ends in a '}' */
                    break;
                } else {
                    goto backout;
                }
                s++;
            }
            if (s == end)
                goto backout;
            if (is_negative)
                n = -n;
            *ptrs[ii] = n;
        }

        if (flen < 0 && ftype != COMDB2_NULL_TYPE)
            goto backout;
        ftype = csc2_type_to_client_type(ftype);
        /* check/adjust length */
        switch (ftype) {
        case CLIENT_UINT:
            if (flen != 2 && flen != 4 && flen != 8)
                goto backout;
            break;

        case CLIENT_INT:
            if (flen != 2 && flen != 4 && flen != 8)
                goto backout;
            break;

        case CLIENT_REAL:
            if (flen != 4 && flen != 8)
                goto backout;
            break;

        case CLIENT_CSTR:
            break;

        case CLIENT_BYTEARRAY:
            break;

        case CLIENT_PSTR:
            break;
        case CLIENT_PSTR2:
            break;

        case CLIENT_BLOB:
        case CLIENT_BLOB2:
        case CLIENT_VUTF8:
            if (flen != CLIENT_BLOB_TYPE_LEN)
                goto backout;
            f->blob_index = sc->numblobs;
            sc->numblobs++;
            break;

        case CLIENT_DATETIME:
        case CLIENT_DATETIMEUS:
            if (flen != CLIENT_DATETIME_LEN && flen != CLIENT_DATETIME_EXT_LEN)
                goto backout;
            break;

        case CLIENT_INTVYM:
            if (flen != CLIENT_INTV_YM_LEN)
                goto backout;
            break;

        case CLIENT_INTVDS:
        case CLIENT_INTVDSUS:
            if (flen != CLIENT_INTV_DS_LEN)
                goto backout;
            break;

        case COMDB2_NULL_TYPE:
            flen = 0;
            break;

        default:
            goto backout;
        }
        f->type = ftype;
        f->offset = foffset;
        f->len = flen;
        /* this will be used when converting strings (pstrings mostly) for now
         */
        f->datalen = fdsize;
        f->name = malloc(fieldname_len + 1);
        memcpy(f->name, fieldname, fieldname_len);
        f->name[fieldname_len] = 0;
        f->idx = -1;
        f->flags = 0;

        /* Skip forward until we find the } and then go past that.
         * This gives the flexibility of adding more fields in the future that
         * older servers can silently ignore. */
        while (s < end && *s != '}')
            s++;
        if (s == end)
            goto backout;
        s++;

        /* If there are more fields expect a semicolon */
        if (field + 1 < nfields) {
            if (s == end)
                goto backout;
            if (*s != ':')
                goto backout;
            s++;
        }
        if (foffset >= maxoffset) {
            maxoffset = foffset;
            maxoffsetix = field;
        }
    }
    if (field != nfields)
        goto backout;

    /*
       size_of_schema always looks at the last field to determine
       schema size.  This works with static tags, but fields for
       dynamic tags are sometimes sent out of order.  Make the
       field with highest offset be last.
     */
    if (maxoffsetix != nfields - 1) {
        /*
           Paranoid - want to see databases where this is happening.
         */
        if (once) {
            logmsg(LOGMSG_ERROR, "flipped tag field order for fields %s <-> %s\n",
                   sc->member[nfields - 1].name, sc->member[maxoffsetix].name);
            once = 0;
        }

        tmpfld = sc->member[nfields - 1];
        sc->member[nfields - 1] = sc->member[maxoffsetix];
        sc->member[maxoffsetix] = tmpfld;
    }

    return sc;

backout:
    if (trace) {
        logmsg(LOGMSG_USER, "Error in parsing tag, current field: %d, total "
                        "fields: %d, location %s\n",
                field, nfields, s);
    }
    if (sc)
        free_tag_schema(sc);
    return NULL;
}

/* only pass this schemas created by new_dynamic_schema() */
void free_tag_schema(struct schema *sc)
{
    int i;
    if (sc->tag)
        free(sc->tag);
    if (sc->member) {
        for (i = 0; i < sc->nmembers; i++)
            free(sc->member[i].name);
        free(sc->member);
    }
    free(sc);
}

int get_schema_blob_count(struct dbtable *table, const char *ctag)
{
    struct schema *sc = find_tag_schema(table, ctag);
    if (sc == NULL)
        return -1;

    return sc->numblobs;
}

int get_numblobs(const dbtable *tbl)
{
    return tbl->schema->numblobs;
}

void free_blob_buffers(blob_buffer_t *blobs, int nblobs)
{
    int ii;
    blob_buffer_t *blob;
    for (ii = 0; ii < nblobs; ii++) {
        blob = blobs + ii;
        if (blob->exists) {
            /* If both `qblob' and `freeptr' are NULL, `data' is allocated by
               the types system and hence must be freed. */
            if (blob->qblob == NULL && blob->freeptr == NULL)
                free(blob->data);
            free(blob->qblob);
            free(blob->freeptr);
        }
    }
    bzero(blobs, sizeof(blob_buffer_t) * nblobs);
}

/* map blob index in one tag to blob index in another tag.  rather
 * inefficient.
 *
 * Returns:
 *  -1      schemas not found
 *  -2      blob not found in from schema
 *  -3      blob not found in to schema
 */
int blob_no_to_blob_no(struct dbtable *table, const char *from_tag, int from_blob,
                       const char *to_tag)
{
    return tbl_blob_no_to_tbl_blob_no(table, from_tag, from_blob, table,
                                      to_tag);
}

int tbl_blob_no_to_tbl_blob_no(struct dbtable *from_table, const char *from_tag,
                               int from_blob, struct dbtable *to_table,
                               const char *to_tag)
{
    struct schema *from, *to;
    int from_idx, from_blob_idx, to_idx, to_blob_idx;

    from = find_tag_schema(from_table, from_tag);
    if (from == NULL)
        return -1;

    to = find_tag_schema(to_table, to_tag);
    if (to == NULL)
        return -1;

    from_blob_idx = -1;
    for (from_idx = 0; from_idx < from->nmembers; from_idx++) {
        if (from->member[from_idx].type == CLIENT_BLOB ||
            from->member[from_idx].type == SERVER_BLOB ||
            from->member[from_idx].type == CLIENT_BLOB2 ||
            from->member[from_idx].type == SERVER_BLOB2 ||
            from->member[from_idx].type == CLIENT_VUTF8 ||
            from->member[from_idx].type == SERVER_VUTF8) {
            if (++from_blob_idx == from_blob) {
                /* found the blob - now match it in the other schema */
                to_blob_idx = -1;
                for (to_idx = 0; to_idx < to->nmembers; to_idx++) {
                    if (to->member[to_idx].type == CLIENT_BLOB ||
                        to->member[to_idx].type == SERVER_BLOB ||
                        to->member[to_idx].type == CLIENT_BLOB2 ||
                        to->member[to_idx].type == SERVER_BLOB2 ||
                        to->member[to_idx].type == CLIENT_VUTF8 ||
                        to->member[to_idx].type == SERVER_VUTF8) {
                        to_blob_idx++;
                        if (strcasecmp(to->member[to_idx].name,
                                       from->member[from_idx].name) == 0)
                            return to_blob_idx;
                    }
                }

                /* Blob not found in to_ schema */
                return -3;
            }
        }
    }

    /* Blob not found in from schema. */
    return -2;
}

/* This routine needs to support lookup of server and client blob fields.
 * It maps a blob number to a field index. */
int get_schema_blob_field_idx_sc(struct schema *sc, int blobno)
{
    if (sc == NULL)
        return -1;

    int idx;
    int blob_idx = -1;
    for (idx = 0; idx < sc->nmembers; idx++) {
        if (sc->member[idx].type == CLIENT_BLOB ||
            sc->member[idx].type == SERVER_BLOB ||
            sc->member[idx].type == CLIENT_BLOB2 ||
            sc->member[idx].type == SERVER_BLOB2 ||
            sc->member[idx].type == CLIENT_VUTF8 ||
            sc->member[idx].type == SERVER_VUTF8) {
            blob_idx++;
            if (blob_idx == blobno)
                return idx;
        }
    }

    return -1;
}

int get_schema_blob_field_idx(struct dbtable *table, const char *tag, int blobno)
{

    struct schema *sc = find_tag_schema(table, tag);
    if (sc == NULL)
        return -1;

    return get_schema_blob_field_idx_sc(sc, blobno);
}

/* This is the opposite - maps a field index to a blob number. */
int get_schema_field_blob_idx(struct dbtable *table, const char *tag, int fldindex)
{
    int idx, blob_idx;

    struct schema *sc = find_tag_schema(table, tag);
    if (sc == NULL)
        return -1;

    blob_idx = 0;
    for (idx = 0; idx < fldindex; idx++) {
        if (sc->member[idx].type == CLIENT_BLOB ||
            sc->member[idx].type == SERVER_BLOB ||
            sc->member[idx].type == CLIENT_BLOB2 ||
            sc->member[idx].type == SERVER_BLOB2 ||
            sc->member[idx].type == CLIENT_VUTF8 ||
            sc->member[idx].type == SERVER_VUTF8) {
            blob_idx++;
        }
    }

    return blob_idx;
}

void *get_field_ptr_in_buf(struct schema *sc, int idx, const void *buf)
{
    const char *cptr = buf;
    if (sc == NULL)
        return NULL;

    if (idx < 0 || idx >= sc->nmembers)
        return NULL;

    return (void *)(cptr + sc->member[idx].offset);
}

void reset_sc_stat();
static int backout_schemas_lockless(const char *tblname)
{
    struct dbtag *dbt;
    struct schema *sc, *tmp;

    dbt = hash_find_readonly(gbl_tag_hash, &tblname);
    if (dbt == NULL) {
        return -1;
    }
    for (sc = dbt->taglist.top; sc != NULL;) {
        tmp = sc->lnk.next;
        if (strncasecmp(sc->tag, ".NEW.", 5) == 0) {
            /* new addition? delete */
            _tags_free_schema(dbt, sc, tblname);
        }
        sc = tmp;
    }

    reset_sc_stat();
    return 0;
}

void backout_schemas(char *tblname)
{
    struct dbtag *dbt;
    int rc;

    lock_taglock();

    dbt = hash_find_readonly(gbl_tag_hash, &tblname);
    if (dbt == NULL) {
        unlock_taglock();
        return;
    }

    rc = backout_schemas_lockless(tblname);
    if (rc) {
        logmsg(LOGMSG_ERROR, "backout_schemas %s failed rc=%d\n", tblname, rc);
    }

    unlock_taglock();
}

/* datacopy_nmembers is -1 if cloning table schema,
 * else the number of members in the table if cloning index schema
 */
struct schema *clone_schema_index(struct schema *from, const char *tag,
                                  int datacopy_nmembers)
{
    struct schema *sc = NULL;
    int i;

    char *tmp = strdup(tag);
    if (tmp)
        sc = alloc_schema(tmp, from->nmembers, from->flags);
    if (!sc)
        goto err;
    sc->nix = from->nix;
    if (sc->nix) {
        sc->ix = calloc(from->nix, sizeof(struct schema *));
        if (!sc->ix)
            goto err;
    }

    for (i = 0; i < from->nmembers; i++) {
        sc->member[i] = from->member[i];
        sc->member[i].name = strdup(from->member[i].name);
        if (!sc->member[i].name)
            goto err;
        if (from->member[i].in_default) {
            sc->member[i].in_default = malloc(sc->member[i].in_default_len);
            if (!sc->member[i].in_default)
                goto err;
            memcpy(sc->member[i].in_default, from->member[i].in_default,
                   from->member[i].in_default_len);
        }
        if (from->member[i].out_default) {
            sc->member[i].out_default = malloc(sc->member[i].out_default_len);
            if (!sc->member[i].out_default)
                goto err;
            memcpy(sc->member[i].out_default, from->member[i].out_default,
                   from->member[i].out_default_len);
        }
    }

    for (i = 0; i < from->nix; i++) {
        if (from->ix && from->ix[i]) {
            sc->ix[i] = clone_schema_index(from->ix[i], from->ix[i]->tag,
                                           from->nmembers);
            if (!sc->ix[i])
                goto err;
        }
    }

    sc->ixnum = from->ixnum;
    sc->recsize = from->recsize;
    sc->numblobs = from->numblobs;

    if (from->csctag) {
        sc->csctag = strdup(from->csctag);
        if (!sc->csctag)
            goto err;
    }

    if (from->partial_datacopy) {
        sc->partial_datacopy = clone_schema_index(from->partial_datacopy,
                                                  from->partial_datacopy->tag,
                                                  -1);
        if (!sc->partial_datacopy)
            goto err;
        datacopy_nmembers = sc->partial_datacopy->nmembers; // update number of members in datacopy
    } 
    if (from->datacopy) {
        assert(datacopy_nmembers != -1);
        sc->datacopy = malloc(datacopy_nmembers * sizeof(int));
        if (!sc->datacopy)
            goto err;
        memcpy(sc->datacopy, from->datacopy, datacopy_nmembers * sizeof(int));
    }
    return sc;

err:
    freeschema(sc, 1);
    return NULL;
}



struct schema *clone_schema(struct schema *from) {
    return clone_schema_index(from, from->tag, -1);
}

/* note: threads are quiesced before this is called - no locks */
void commit_schemas(const char *tblname)
{
    struct dbtag *dbt;
    struct schema *sc;
    struct schema *tmp;
    dbtable *db = get_dbtable_by_name(tblname);

    LISTC_T(struct schema) to_be_freed;

    listc_init(&to_be_freed, offsetof(struct schema, lnk));

    lock_taglock();
    dbt = hash_find_readonly(gbl_tag_hash, &tblname);

    if (dbt == NULL) {
        unlock_taglock();
        return;
    }

    sc = dbt->taglist.top;
    hash_clear(dbt->tags);
    while (sc != NULL) {
        tmp = sc->lnk.next;
        if (strncasecmp(sc->tag, ".NEW.", 5) == 0) {
            /* .NEW..ONDISK: remove .NEW. prefix;
             *               save copy as .ONDISK.VER.NN.
             * .NEW.index and .NEW.blob: remove .NEW. prefix. */
            char *oldname, *newname;
            oldname = sc->tag;
            newname = strdup(sc->tag + 5);
            if (!newname) {
                logmsg(LOGMSG_FATAL, 
                        "%s: out of memory on strdup tag\n", __func__);
                exit(1);
            }
            sc->tag = newname;
            free(oldname);

            if (sc->csctag) {
                oldname = sc->csctag;
                newname = strdup(sc->csctag + 5);
                if (!newname) {
                    logmsg(LOGMSG_FATAL, 
                            "%s: out of memory on strdup tag\n", __func__);
                    exit(1);
                }
                free(oldname);
                sc->csctag = newname;
            }

            if (strcasecmp(sc->tag, ".ONDISK") == 0) {
                /* this is .ONDISK */
                if (db->odh && db->instant_schema_change) {
                    struct schema *ver_schema = NULL;
                    char *newname = malloc(gbl_ondisk_ver_len);
                    if (newname == NULL) {
                        logmsg(LOGMSG_FATAL, 
                                "%s: out of memory on malloc\n", __func__);
                        exit(1);
                    }
                    sprintf(newname, "%s%d", gbl_ondisk_ver,
                            db->schema_version);
                    ver_schema = clone_schema(sc);
                    free(ver_schema->tag);
                    ver_schema->tag = newname;
                    listc_atl(&dbt->taglist, ver_schema);
                }
            }
        } else if (strncasecmp(sc->tag, gbl_ondisk_ver,
                               sizeof(gbl_ondisk_ver) - 1)) {
            /* not .NEW. and not .ONDISK_VER. delete */
            listc_rfl(&dbt->taglist, sc);
            listc_abl(&to_be_freed, sc);
            sc = NULL;
        } else {
            /* .ONDISK.VER.nn
             * if nn >= current version drop it */
            int ver;
            sscanf(sc->tag, gbl_ondisk_ver_fmt, &ver);
            if (ver >= db->schema_version) {
                listc_rfl(&dbt->taglist, sc);
                listc_abl(&to_be_freed, sc);
                sc = NULL;
            }
        }
        sc = tmp;
    }

    /* populating the hash table */
    sc = dbt->taglist.top;
    while (sc != NULL) {
        tmp = sc->lnk.next;
        if (hash_find_readonly(dbt->tags, sc)) {
            listc_rfl(&dbt->taglist, sc);
            sc = NULL;
        } else {
            if (_dbg_tags)
                logmsg(LOGMSG_DEBUG, "3 Adding %s:%s\n", tblname, sc->tag);
            hash_add(dbt->tags, sc);
#if defined DEBUG_STACK_TAG_SCHEMA
            comdb2_cheapstack_sym(stderr, "%s:%d -> %s:%s ", __func__, __LINE__,
                                  tblname, sc->tag);
            sc->frames = backtrace(sc->buf, MAX_TAG_STACK_FRAMES);
            sc->tid = pthread_self();
#endif
        }
        sc = tmp;
    }

    /* Free outstanding schemas above.  Since they may reference other
     * schemas, the order in which we free them below is important.  This
     * is very ugly code.  Sorry. */
    if (1) {
        struct schema **s;
        int i = 0;
        int count = to_be_freed.count;

        s = calloc(sizeof(struct schema *), count);
        sc = listc_rtl(&to_be_freed);
        while (sc) {
            if (db->schema != sc)
                s[i++] = sc;
            else
                count--;
            sc = listc_rtl(&to_be_freed);
        }

        /* free the innards of schemas first, then the schemas themselves */
        /* free schemas with no indices defined first */
        for (i = 0; i < count; i++) {
            if (!s[i]->ix)
                freeschema_internals(s[i]);
        }
        /* free the index schemas */
        for (i = 0; i < count; i++) {
            if (s[i]->ix) {
                int j, k;
                /* .VER.# schemas have indices that aren't in hash - make sure
                 * we free those too.
                 * A few more ugly points. */
                for (j = 0; j < s[i]->nix; j++) {
                    /* if we find a schema that's (1) not found in the
                     * schemas hash, and (2) not in our list of schemas to free,
                     * free it */
                    for (k = 0; k < count; k++) {
                        if (s[i]->ix[j] == s[k])
                            break;
                    }
                    if (k == count) {
                        if (!hash_find_readonly(dbt->tags, &s[i]->ix[j]->tag))
                            freeschema(s[i]->ix[j], 0);
                    }
                }
                free(s[i]->ix);
            }
        }
        /* free the other schemas */
        for (i = 0; i < count; i++) {
            if (s[i]->ix)
                freeschema_internals(s[i]);
        }

        /* free schemas themselves */
        for (i = 0; i < count; i++)
            free(s[i]);
        free(s);
    }

    unlock_taglock();
}

int is_tag_ondisk_sc(struct schema *sc)
{
    int type;
    if (sc == NULL)
        return -1;

    type = sc->member[0].type;

    if (type > CLIENT_MINTYPE && type < CLIENT_MAXTYPE)
        return 0;

    if (type > SERVER_MINTYPE && type < SERVER_MAXTYPE)
        return 1;

    return -1;
}

int schema_num_fields(const struct schema *sc) { return sc->nmembers; }

const struct field *schema_get_field_n(const struct schema *sc, int index)
{
    return &sc->member[index];
}

const char *field_get_name(const struct field *fld) { return fld->name; }

const void *field_get_buf(const struct field *fld, const void *dta)
{
    return (const void *)(((const char *)dta) + fld->offset);
}

int field_get_type(const struct field *fld) { return fld->type; }

int field_get_length(const struct field *fld) { return fld->len; }

const struct field_conv_opts *field_get_conv_opts(const struct field *fld)
{
    return &fld->convopts;
}

int resolve_tag_name(struct ireq *iq, const char *tagdescr, size_t taglen,
                     struct schema **dynschema, char *tagname,
                     size_t tagnamelen)
{
    *dynschema = NULL;
    if (taglen >= 6 && strncasecmp(tagdescr, ".DYNT.", 6) == 0) {
        /* dynamic schema tag */
        *dynschema = new_dynamic_schema((char *)tagdescr, taglen, 0);
        if (!*dynschema) {
            if (iq->debug)
                reqprintf(
                    iq, "resolve_tag_name CAN'T ALLOCATE DYNAMIC SCHEMA '%.*s'",
                    (int) taglen, tagdescr);
            return -1;
        }
        add_tag_schema(iq->usedb->tablename, *dynschema);
        strncpy0(tagname, (*dynschema)->tag, tagnamelen);
    } else if (taglen > 0) {
        /* static tag name */
        if (taglen > tagnamelen - 1) {
            if (iq->debug)
                reqprintf(iq, "resolve_tag_name TAG NAME TOO LONG '%.*s'",
                          (int) taglen, tagdescr);
            return -1;
        }
        memcpy(tagname, tagdescr, taglen);
        bzero(tagname + taglen, tagnamelen - taglen);
    } else {
        /* use default tag */
        strncpy0(tagname, ".DEFAULT", tagnamelen);
    }

    return 0;
}

/* return a unique id associated with a record */
long long get_record_unique_id(dbtable *db, void *rec)
{
    int i;
    char *p = (char *)rec;
    long long id;
    int outnull;
    int sz;

    struct schema *s = find_tag_schema(db, ".ONDISK");
    for (i = 0; i < s->nmembers; i++) {
        if (strcasecmp(s->member[i].name, "comdb2_seqno") == 0) {
            SERVER_BINT_to_CLIENT_INT(p + s->member[i].offset, 9, NULL, NULL,
                                      &id, 8, &outnull, &sz, NULL, NULL);
            return id;
        }
    }
    return 0;
}

const char *get_keynm_from_db_idx(dbtable *db, int idx)
{
    const char *EMPTY = "";
    char *ptr;

    if (!db || !db->schema || (idx < 0) || (idx >= db->schema->nix)) {
        return EMPTY;
    }

    ptr = db->schema->ix[idx]->csctag;
    if (!ptr) {
        return EMPTY;
    }

    return ptr;
}

/* Load the null bitmap from the given source into the given destination.
 * The destination bitmap may be larger then the source, in which case we
 * zero pad.
 * It may be smaller than the source, in which case we print scary trace if
 * we are actually losing nulls.
 */
void loadnullbmp(void *destbmp, size_t destbmpsz, const void *srcbmp,
                 size_t srcbmpsz)
{
    if (destbmpsz <= srcbmpsz) {
        memcpy(destbmp, srcbmp, destbmpsz);
        /* Check for truncated NULLs, that would be bad */
        while (destbmpsz < srcbmpsz) {
            if (((const char *)srcbmp)[destbmpsz] != 0) {
                logmsg(LOGMSG_ERROR, "%s: I am losing NULLs in truncation!!\n",
                        __func__);
                cheap_stack_trace();
                return;
            }
            destbmpsz++;
        }

    } else {
        memcpy(destbmp, srcbmp, srcbmpsz);
        bzero(((char *)destbmp) + srcbmpsz, destbmpsz - srcbmpsz);
    }
}

/* Find position & struct field * of field in schema */
static struct field *get_field_position(struct schema *s, const char *name,
                                        int *position)
{
    struct field *field;
    for (int idx = 0; idx < s->nmembers; ++idx) {
        field = &s->member[idx];
        if (strcmp(field->name, name) == 0) {
            *position = idx;
            return field;
        }
    }
    *position = -1;
    return NULL;
}

static uint16_t *create_fld_hints(struct schema *schema) {
    int n = schema->nmembers;
    uint16_t *hints = malloc(sizeof(*hints) * (n + 1));
    for (int i = 0; i < n; ++i) {
        hints[i] = schema->member[i].len;
    }
    hints[n] = 0;
    return hints;
}

static void update_fld_hints(dbtable *tbl)
{
    struct schema *ondisk = tbl->schema;
    uint16_t *hints = create_fld_hints(ondisk);
    bdb_set_fld_hints(tbl->handle, hints);

    // update partial datacopy field hints
    struct schema *ixschema;
    struct schema *pd;
    for (int i = 0; i < ondisk->nix; ++i) {
        ixschema = ondisk->ix[i];
        if (ixschema->flags & SCHEMA_PARTIALDATACOPY) {
            pd = ixschema->partial_datacopy;
            hints = create_fld_hints(pd);
            bdb_set_fld_hints_pd(tbl->handle, hints, i);
        }
    }
}

void set_bdb_option_flags(dbtable *tbl, int odh, int ipu, int isc, int ver,
                          int compr, int blob_compr, int datacopy_odh)
{
    update_fld_hints(tbl);
    bdb_state_type *handle = tbl->handle;
    bdb_set_odh_options(handle, odh, compr, blob_compr);
    bdb_set_inplace_updates(handle, ipu);
    bdb_set_instant_schema_change(handle, isc);
    bdb_set_csc2_version(handle, ver);
    bdb_set_datacopy_odh(handle, datacopy_odh);
    bdb_set_key_compression(handle);
}

void set_bdb_queue_option_flags(dbtable *tbl, int odh, int compr, int persist)
{
    bdb_state_type *handle = tbl->handle;
    bdb_set_queue_odh_options(handle, odh, compr, persist);
}

int gbl_permissive_sequence_sc = 1;

int delete_table_sequences(tran_type *tran, struct dbtable *db)
{
    int rc = 0, bdberr = 0;
    for (int i = 0; i < db->schema->nmembers; i++) {
        struct field *f = &db->schema->member[i];
        if (f->in_default_type == SERVER_SEQUENCE) {
            if ((rc = bdb_del_sequence(tran, db->tablename, f->name, &bdberr) != 0)) {
                if (gbl_permissive_sequence_sc) {
                    logmsg(LOGMSG_INFO, "%s error %s %s rc=%d bdberr=%d, continuing.\n", __func__, db->tablename,
                           f->name, rc, bdberr);
                } else {
                    logmsg(LOGMSG_ERROR, "%s error deleting sequence %s %s rc=%d bdberr=%d\n", __func__, db->tablename,
                           f->name, rc, bdberr);
                    return rc;
                }
            }
        }
    }
    return 0;
}

static int try_fetch_highest_sequence_key(dbtable *db, int idx_in_table, tran_type *tran, char *tablename,
                                          char *columnname, struct schema **valid_idx, void *key, int keylen, int *len)
{
    // need to find existing highest non-null value of column to find where to start sequence from
    // find an index that is ascending on this column and is not partial
    *valid_idx = NULL;
    *len = 0;
    int ix_sequence = -1;
    int rc, bdberr;
    for (int ix = 0; ix < db->nix; ix++) {
        *valid_idx = db->ixschema[ix];
        struct field *first = &(*valid_idx)->member[0];
        if (!(*valid_idx)->where && first->idx == idx_in_table && !(first->flags & INDEX_DESCEND)) {
            ix_sequence = ix;
            break;
        }
        *valid_idx = NULL;
    }
    if (!*valid_idx) {
        return 0;
    }

    if ((rc = bdb_fetch_last_key_tran(db->handle, tran, 0, ix_sequence, keylen, key, len, &bdberr))) {
        logmsg(LOGMSG_ERROR,
               "%s error fetching last key on table %s for creating sequence on existing column %s rc=%d "
               "bdberr=%d\n",
               __func__, tablename, columnname, rc, bdberr);
        return rc;
    }
    return 0;
}

int gbl_permit_small_sequences = 0;

int alter_table_sequences(struct ireq *iq, tran_type *tran, dbtable *olddb, dbtable *newdb)
{
    int rc = 0, bdberr = 0;
    for (int i = 0; i < olddb->schema->nmembers; i++) {
        struct field *f = &olddb->schema->member[i];
        if (f->in_default_type == SERVER_SEQUENCE) {
            int fn = find_field_idx_in_tag(newdb->schema, f->name);
            if (fn == -1 || newdb->schema->member[fn].in_default_type != SERVER_SEQUENCE) {
                if ((rc = bdb_del_sequence(tran, olddb->tablename, f->name, &bdberr))) {
                    if (gbl_permissive_sequence_sc) {
                        logmsg(LOGMSG_INFO, "%s error %s %s rc=%d bdberr=%d, continuing.\n", __func__, olddb->tablename,
                               f->name, rc, bdberr);

                    } else {
                        logmsg(LOGMSG_ERROR, "%s error deleting sequence %s %s rc=%d bdberr=%d\n", __func__,
                               olddb->tablename, f->name, rc, bdberr);
                        return rc;
                    }
                }
            }
        }
    }

    for (int i = 0; i < newdb->schema->nmembers; i++) {
        struct field *f = &newdb->schema->member[i];
        if (f->in_default_type == SERVER_SEQUENCE) {
            if (f->type != SERVER_BINT) {
                logmsg(LOGMSG_ERROR, "%s sequences only supported for int types\n", __func__);
                if (iq) {
                    sc_client_error(iq->sc, "datatype invalid for sequences");
                }
                return -1;
            }
            if (!gbl_permit_small_sequences && f->len < 9) {
                logmsg(LOGMSG_ERROR, "%s failing sc, permit-small-sequences is disabled\n", __func__);
                if (iq) {
                    sc_client_error(iq->sc, "datatype invalid for sequences");
                }
                return -1;
            }

            int fn = find_field_idx_in_tag(olddb->schema, f->name);
            if (fn == -1) {
                if ((rc = bdb_set_sequence(tran, olddb->tablename, f->name, 0, &bdberr))) {
                    logmsg(LOGMSG_ERROR, "%s error creating sequence %s %s rc=%d bdberr=%d\n", __func__,
                           olddb->tablename, f->name, rc, bdberr);
                    return rc;
                }
            } else if (olddb->schema->member[fn].in_default_type != SERVER_SEQUENCE) {
                if (null_bit != null_bit_low) {
                    logmsg(LOGMSG_ERROR, "%s need nulls to be sorted low to use sequences on existing column\n",
                           __func__);
                    if (iq) {
                        sc_client_error(iq->sc, "need nulls to be sorted low to use sequences on existing column");
                    }
                    return -1;
                }
                // NOTE: with gbl_permit_small_sequences on, it is ok if olddb->schema->member[fn].len is less than 9
                // here since we will be converting it to len 9 in the new field
                if (olddb->schema->member[fn].type != SERVER_BINT) {
                    logmsg(LOGMSG_ERROR, "%s sequences only supported for existing int types\n", __func__);
                    if (iq) {
                        sc_client_error(iq->sc, "datatype invalid for sequences, convert column to int type first");
                    }
                    return -1;
                }

                struct schema *valid_idx = NULL;
                struct schema *valid_idx_new = NULL;
                unsigned char key[MAXKEYLEN];
                int len = 0;
                if ((rc = try_fetch_highest_sequence_key(olddb, fn, tran, olddb->tablename, f->name, &valid_idx, key,
                                                         sizeof(key), &len)))
                    return rc;
                if (len == 0 && (rc = try_fetch_highest_sequence_key(newdb, i, tran, olddb->tablename, f->name,
                                                                     &valid_idx_new, key, sizeof(key), &len)))
                    return rc;
                if (valid_idx_new) // else may have case with valid idx old schema and len == 0 (empty index)
                    valid_idx = valid_idx_new;

                if (!valid_idx) {
                    logmsg(LOGMSG_ERROR,
                           "%s need index that is ascending and not partial on same column for sequences\n", __func__);
                    if (iq) {
                        sc_client_error(iq->sc,
                                        "need index that is ascending and not partial on same column for sequences");
                    }
                    return -1;
                }

                int64_t highest = 0;
                if (len > 0) { // index is not empty
                    Mem data;
                    if (len < valid_idx->member[0].offset) {
                        logmsg(LOGMSG_ERROR,
                               "%s error key len %d smaller than col size %u on table %s for creating sequence on "
                               "existing column %s\n",
                               __func__, len, valid_idx->member[0].offset, olddb->tablename, f->name);
                        return -1;
                    }
                    if ((rc = get_data(NULL, valid_idx, key, 0, &data, 0, NULL))) {
                        logmsg(LOGMSG_ERROR,
                               "%s error fetching data on table %s for creating sequence on existing column %s rc=%d\n",
                               __func__, olddb->tablename, f->name, rc);
                        return rc;
                    }
                    if (data.flags == MEM_Int)
                        highest = data.u.i;
                    else if (data.flags != MEM_Null) {
                        logmsg(LOGMSG_ERROR,
                               "%s error got wrong data type on table %s for creating sequence on existing column %s\n",
                               __func__, olddb->tablename, f->name);
                        return -1;
                    }
                }
                if (gbl_debug_alter_sequences_sleep)
                    sleep(10);
                if ((rc = bdb_set_sequence(tran, olddb->tablename, f->name, highest, &bdberr))) {
                    logmsg(LOGMSG_ERROR, "%s error creating sequence %s for existing column %s rc=%d bdberr=%d\n",
                           __func__, olddb->tablename, f->name, rc, bdberr);
                    return rc;
                }
            }
        }
    }
    return 0;
}

int rename_table_sequences(tran_type *tran, dbtable *db, const char *newname)
{
    int rc = 0, bdberr = 0;
    for (int i = 0; i < db->schema->nmembers; i++) {
        struct field *f = &db->schema->member[i];
        if (f->in_default_type == SERVER_SEQUENCE) {
            int64_t s;
            if ((rc = bdb_get_sequence(tran, db->tablename, f->name, &s, &bdberr))) {
                if (gbl_permissive_sequence_sc) {
                    logmsg(LOGMSG_INFO, "%s error %s %s rc=%d bdberr=%d, continuing.\n", __func__, db->tablename,
                           f->name, rc, bdberr);
                    s = 0;
                } else {
                    logmsg(LOGMSG_ERROR, "%s error getting sequence %s %s rc=%d bdberr=%d\n", __func__, db->tablename,
                           f->name, rc, bdberr);
                    return rc;
                }
            }
            if ((rc = bdb_del_sequence(tran, db->tablename, f->name, &bdberr))) {
                if (gbl_permissive_sequence_sc) {
                    logmsg(LOGMSG_INFO, "%s error %s %s rc=%d bdberr=%d, continuing.\n", __func__, db->tablename,
                           f->name, rc, bdberr);
                } else {
                    logmsg(LOGMSG_ERROR, "%s error deleting sequence %s %s rc=%d bdberr=%d\n", __func__, db->tablename,
                           f->name, rc, bdberr);
                    return rc;
                }
            }
            if ((rc = bdb_set_sequence(tran, newname, f->name, s, &bdberr))) {
                logmsg(LOGMSG_ERROR, "%s error adding sequence %s %s rc=%d bdberr=%d\n", __func__, newname, f->name, rc,
                       bdberr);
                return rc;
            }
        }
    }
    return 0;
}

int init_table_sequences(struct ireq *iq, tran_type *tran, dbtable *db)
{
    int rc = 0, bdberr = 0;
    for (int i = 0; i < db->schema->nmembers; i++) {
        struct field *f = &db->schema->member[i];
        if (f->in_default_type == SERVER_SEQUENCE) {
            if (f->type != SERVER_BINT) {
                logmsg(LOGMSG_ERROR, "%s sequences only supported for int types\n", __func__);
                if (iq) {
                    sc_client_error(iq->sc, "datatype invalid for sequences");
                }
                return -1;
            }
            if (!gbl_permit_small_sequences && f->len < 9) {
                logmsg(LOGMSG_ERROR, "%s failing sc, permit-small-sequences is disabled\n", __func__);
                if (iq) {
                    sc_client_error(iq->sc, "datatype invalid for sequences");
                }
                return -1;
            }
            if ((rc = bdb_set_sequence(tran, db->tablename, f->name, 0, &bdberr))) {
                logmsg(LOGMSG_ERROR, "%s error adding sequence %s %s rc=%d bdberr=%d\n", __func__, db->tablename,
                       f->name, rc, bdberr);
                return rc;
            }
        }
    }
    return 0;
}

/* Compute map of dbstores used in vtag_to_ondisk */
void update_dbstore(dbtable *db)
{
    if (!db->instant_schema_change)
        return;

    struct schema *ondisk = db->schema;
    if (ondisk == NULL) {
        logmsg(LOGMSG_FATAL, "%s: .ONDISK not found!! PANIC!! %s() @ %d\n",
               db->tablename, __func__, __LINE__);
        cheap_stack_trace();
        exit(1);
    }

    for (int i = 0; i < sizeof db->dbstore / sizeof db->dbstore[0]; ++i) {
        if (db->dbstore[i].data) {
            free(db->dbstore[i].data);
        }
    }

    bzero(db->dbstore, sizeof db->dbstore);
    logmsg(LOGMSG_DEBUG, "%s table '%s' schema version %d\n", __func__,
           db->tablename, db->schema_version);

    for (int v = 1; v <= db->schema_version; ++v) {
        char tag[MAXTAGLEN];
        struct schema *ver;
        snprintf(tag, sizeof tag, gbl_ondisk_ver_fmt, v);
        ver = find_tag_schema(db, tag);
        if (ver == NULL) {
            logmsg(LOGMSG_FATAL, "%s: %s not found!! PANIC!! %s() @ %d\n",
                   db->tablename, tag, __func__, __LINE__);
            cheap_stack_trace();
            abort();
        }

        db->versmap[v] = (unsigned int *)get_tag_mapping(ver, ondisk);
        logmsg(LOGMSG_DEBUG, "%s set table '%s' schema version %d to %p\n",
               __func__, db->tablename, v, db->versmap[v]);
        db->vers_compat_ondisk[v] = 1;
        if (SC_TAG_CHANGE ==
            compare_tag_int(ver, ondisk, NULL, 0 /*non-strict compliance*/))
            db->vers_compat_ondisk[v] = 0;

        for (int i = 0; i < ver->nmembers; ++i) {
            struct field *from = &ver->member[i];
            struct field *to;
            int position;
            int outdtsz = 0;
            int rc;

            to = get_field_position(ondisk, from->name, &position);
            if (position < 0) {
                logmsg(LOGMSG_FATAL,
                       "%s: %s no such field: %s in .ONDISK. but in %s!! "
                       "PANIC!!\n",
                       db->tablename, __func__, from->name, tag);
                abort();
            }

            if (db->versmap[v][i] != i || from->type != to->type ||
                from->len != to->len)
                db->vers_compat_ondisk[v] = 0; // not compatible

            if (db->dbstore[position].ver == 0) {
                /* column not seen before */
                db->dbstore[position].ver = v;

                if (from->in_default_type == SERVER_FUNCTION)
                    continue;
                else if (from->in_default_len && from->in_default_type != SERVER_SEQUENCE) {
                    db->dbstore[position].len = to->len;
                    db->dbstore[position].data = calloc(1, to->len);
                    if (db->dbstore[position].data == NULL) {
                        logmsg(LOGMSG_FATAL,
                               "%s: %s() @ %d calloc failed!! PANIC!!\n",
                               db->tablename, __func__, __LINE__);
                        abort();
                    }
                    rc = SERVER_to_SERVER(
                        from->in_default, from->in_default_len,
                        from->in_default_type, &from->convopts, NULL,
                        0 /* flags are unused */, db->dbstore[position].data,
                        to->len, to->type, 0, /*flags are unused */ &outdtsz,
                        &to->convopts, NULL);
                    if (rc != 0) {
                        logmsg(LOGMSG_FATAL,
                               "%s: %s() @ %d: SERVER_to_SERVER failed!! "
                               "PANIC!!\n",
                               db->tablename, __func__, __LINE__);
                        abort();
                    }
                }
            }
        } /* end for each field */
    }     /* end for each version */
}

void replace_tag_schema(dbtable *db, struct schema *schema)
{
    struct schema *old_schema;
    struct schema *tmp;
    struct dbtag *dbt;

    lock_taglock();
    dbt = hash_find_readonly(gbl_tag_hash, &db->tablename);
    if (dbt == NULL) {
        unlock_taglock();
        return;
    }

    old_schema = dbt->taglist.top;
    while (old_schema) {
        tmp = old_schema->lnk.next;
        if (strcasecmp(old_schema->tag, schema->tag) == 0) {
            listc_rfl(&dbt->taglist, old_schema);
            freeschema(old_schema, 0);
        }
        old_schema = tmp;
    }
    listc_atl(&dbt->taglist, schema);
    unlock_taglock();
}

void delete_schema(const char *tblname)
{
    struct dbtag *dbt;
    lock_taglock();
    dbt = hash_find(gbl_tag_hash, &tblname);
#if defined DEBUG_STACK_TAG_SCHEMA
    comdb2_cheapstack_sym(stderr, "%s:%d -> %s", __func__, __LINE__, tblname);
#endif
    assert(dbt != NULL);
    hash_del(gbl_tag_hash, dbt);
    unlock_taglock();
    struct schema *schema = dbt->taglist.top;
    while (schema) {
        struct schema *tmp = schema;
        schema = schema->lnk.next;
        listc_rfl(&dbt->taglist, tmp);
        freeschema(tmp, 0);
    }
    if (dbt->tags) {
        hash_clear(dbt->tags);
        hash_free(dbt->tags);
        dbt->tags = NULL;
    }
    free(dbt->tblname);
    free(dbt);
}

void rename_schema(const char *oldname, char *newname)
{
    struct dbtag *dbt;
    lock_taglock();
    dbt = hash_find(gbl_tag_hash, &oldname);
#if defined DEBUG_STACK_TAG_SCHEMA
    comdb2_cheapstack_sym(stderr, "%s:%d rename %s to %s\n", __func__, __LINE__,
                          oldname, newname);
#endif
    hash_del(gbl_tag_hash, dbt);
    free(dbt->tblname);
    dbt->tblname = newname;
    hash_add(gbl_tag_hash, dbt);
    unlock_taglock();
}

static void freeschema_internals(struct schema *schema)
{
    int i;
    free(schema->tag);
    for (i = 0; i < schema->nmembers; i++) {
        free(schema->member[i].in_default);
        free(schema->member[i].out_default);
        free(schema->member[i].name);
    }
    free(schema->member);
    if (schema->datacopy) {
        free(schema->datacopy);
    }
    if (schema->csctag) {
        free(schema->csctag);
    }
    if (schema->sqlitetag) {
        free(schema->sqlitetag);
        schema->sqlitetag = NULL;
    }
    if (schema->partial_datacopy) {
        freeschema(schema->partial_datacopy, 0);
        schema->partial_datacopy = NULL;
    }
}

void freeschema(struct schema *schema, int free_ix)
{
    if (!schema)
        return;
    if (free_ix) {
        int i;
        for(i=0; i < schema->nix; i++)
            freeschema(schema->ix[i], 0);
    }
    freeschema_internals(schema);
    free(schema);
}

void freedb_int(dbtable *db, dbtable *replace)
{
    int i;
    int dbs_idx;
    char *sqlaliasname = db->sqlaliasname;
    const char *timepartition_name = db->timepartition_name;

    dbs_idx = db->dbs_idx;

    if (!replace) 
        free(sqlaliasname);
    free(db->lrlfname);
    free(db->tablename);
    /* who frees schema/ixschema? */
    free(db->sql);

    if (db->ixsql) {
        for (i = 0; i < db->nix; i++) {
            free(db->ixsql[i]);
            db->ixsql[i] = NULL;
        }
        free(db->ixsql);
    }

    if (db->rev_constraints) {
        free(db->rev_constraints);
        db->rev_constraints = NULL;
    }

    for (i = 0; (i < db->n_constraints); i++) {
        if (db->constraints == NULL)
            break;
        free(db->constraints[i].consname);
        free(db->constraints[i].lclkeyname);
        for (int j = 0; j < db->constraints[i].nrules; ++j) {
            free(db->constraints[i].table[j]);
            free(db->constraints[i].keynm[j]);
        }
        free(db->constraints[i].table);
        free(db->constraints[i].keynm);
    }

    if (db->constraints) {
        free(db->constraints);
        db->constraints = NULL;
    }

    for (i = 0; i < db->n_check_constraints; i++) {
        free(db->check_constraints[i].consname);
        free(db->check_constraints[i].expr);
    }

    free(db->ixuse);
    free(db->sqlixuse);
    free(db->csc2_schema);
    free(db->ixschema);
    if (db->sc_genids)
        free(db->sc_genids);

    if (db->instant_schema_change) {
        for (i = 0; i < sizeof db->dbstore / sizeof db->dbstore[0]; ++i) {
            if (db->dbstore[i].data) {
                free(db->dbstore[i].data);
                db->dbstore[i].data = NULL;
            }
        }
        for (int v = 1; v <= db->schema_version; ++v) {
            if (db->versmap[v]) {
                free(db->versmap[v]);
                db->versmap[v] = NULL;
            }
        }
    }

    if (replace) {
        memcpy(db, replace, sizeof(dbtable));
        db->dbs_idx = dbs_idx;
        db->sqlaliasname = sqlaliasname;
        db->timepartition_name = timepartition_name;
    }
}

void free_db_and_replace(dbtable *db, dbtable *newdb)
{
    freedb_int(db, newdb);
}

void freedb(dbtable *db)
{
    freedb_int(db, NULL);
}

static struct schema *create_version_schema(char *csc2, int version,
                                            struct dbenv *dbenv)
{
    struct schema *ver_schema = NULL;
    dbtable *ver_db;
    char *tag;
    struct errstat err = {0};

    Pthread_mutex_lock(&csc2_subsystem_mtx);

    ver_db = create_new_dbtable(
        thedb, NULL, csc2, 0 /* no altname */, 0 /* fake dbnum */,
        1 /* allow ull */, 1 /* no side effects */, &err);
    if (!ver_db) {
        logmsg(LOGMSG_ERROR, "%s\ncsc2: \"%s\"\n", err.errstr, csc2);
        goto done;
    }

    struct schema *s = find_tag_schema(ver_db, ".ONDISK");
    if (s == NULL) {
        logmsg(LOGMSG_ERROR, "find_tag_schema failed %s:%d\n", __FILE__, __LINE__);
        goto done;
    }

    tag = malloc(gbl_ondisk_ver_len);
    if (tag == NULL) {
        logmsg(LOGMSG_ERROR, "malloc failed %s:%d\n", __FILE__, __LINE__);
        goto done;
    }

    sprintf(tag, gbl_ondisk_ver_fmt, version);
    ver_schema = clone_schema(s);
    free(ver_schema->tag);
    ver_schema->tag = tag;

    /* get rid of temp schema */
    del_tag_schema(ver_db->tablename, s->tag);

    /* get rid of temp table */
    delete_schema(ver_db->tablename);
    freedb(ver_db);

done:
    Pthread_mutex_unlock(&csc2_subsystem_mtx);
    return ver_schema;
}

static void clear_existing_schemas(dbtable *db)
{
    char tag[64];
    int i;
    for (i = 1; i <= db->schema_version; ++i) {
        sprintf(tag, gbl_ondisk_ver_fmt, i);
        del_tag_schema(db->tablename, tag);
    }
    del_tag_schema(db->tablename, ".ONDISK");
}

/* this populates global schema hash (i.e. tags) for a table 
 * all versions of the schema are loaded
 */
int load_csc2_versions(dbtable *tbl, tran_type *tran)
{
    int isc;
    get_db_instant_schema_change_tran(tbl, &isc, tran);
    if (!isc)
        return 0;

    int i;
    int version = get_csc2_version_tran(tbl->tablename, tran);
    for (i = 1; i <= version; ++i) {
        char *csc2;
        int len;
        get_csc2_file_tran(tbl->tablename, i, &csc2, &len, tran);
        struct schema *schema = create_version_schema(csc2, i, tbl->dbenv);
        if (schema == NULL) {
            free(csc2);
            logmsg(LOGMSG_ERROR, "Could not create schema version: %d\n", i);
            return 1;
        }
        add_tag_schema(tbl->tablename, schema);
        free(csc2);
    }
    return 0;
}

static int load_new_ondisk(dbtable *db, tran_type *tran)
{
    int rc;
    int bdberr;
    int version = get_csc2_version_tran(db->tablename, tran);
    int len;
    void *old_bdb_handle, *new_bdb_handle;
    char *csc2 = NULL;

    Pthread_mutex_lock(&csc2_subsystem_mtx);
    rc = get_csc2_file_tran(db->tablename, version, &csc2, &len, tran);
    if (rc) {
        logmsg(LOGMSG_ERROR, "get_csc2_file failed %s:%d\n", __FILE__, __LINE__);
        logmsg(LOGMSG_ERROR, "rc: %d len: %d csc2:\n%s\n", rc, len, csc2);
        goto err;
    }

    struct errstat err = {0};
    dbtable *newdb = create_new_dbtable(thedb, db->tablename, csc2, db->dbnum,
                                        0, 1, 0, &err);
    if (!newdb) {
        logmsg(LOGMSG_ERROR, "%s (%s:%d)\n", err.errstr, __FILE__, __LINE__);
        goto err;
    }

    newdb->schema_version = version;
    newdb->dbnum = db->dbnum;
    newdb->meta = db->meta;
    newdb->dtastripe = gbl_dtastripe;

    extern int gbl_rowlocks;
    tran_type *arg_tran = gbl_rowlocks ? NULL : tran;

    /* Must use tran or this can cause deadlocks */
    newdb->handle = bdb_open_more_tran(
        db->tablename, thedb->basedir, newdb->lrl, newdb->nix,
        (short *)newdb->ix_keylen, newdb->ix_dupes, newdb->ix_recnums,
        newdb->ix_datacopy, newdb->ix_datacopylen, newdb->ix_collattr, newdb->ix_nullsallowed,
        newdb->numblobs + 1, thedb->bdb_env, arg_tran, 0, &bdberr);

    if (bdberr != 0 || newdb->handle == NULL) {
        logmsg(LOGMSG_ERROR, "reload_schema handle %p bdberr %d\n",
               newdb->handle, bdberr);
        cheap_stack_trace();
        goto err;
    }
    Pthread_mutex_unlock(&csc2_subsystem_mtx);

    old_bdb_handle = db->handle;
    new_bdb_handle = newdb->handle;

    set_odh_options_tran(newdb, tran);
    transfer_db_settings(db, newdb);
    restore_constraint_pointers(db, newdb);
    free_db_and_replace(db, newdb);

    bdb_close_only(old_bdb_handle, &bdberr);
    /* swap the handle in place */
    rc = bdb_free_and_replace(old_bdb_handle, new_bdb_handle, &bdberr);
    if (rc)
        logmsg(LOGMSG_ERROR, "%s:%d bdb_free rc %d %d\n", __FILE__, __LINE__, rc,
                bdberr);
    db->handle = old_bdb_handle;

    re_add_dbtable_to_thedb_dbs(db);
    fix_constraint_pointers(db, newdb);

    memset(newdb, 0xff, sizeof(dbtable));
    free(newdb);
    free(csc2);
    free(new_bdb_handle);
    return 0;

err:
    Pthread_mutex_unlock(&csc2_subsystem_mtx);
    free(csc2);
    return 1;
}

int reload_after_bulkimport(dbtable *db, tran_type *tran)
{
    clear_existing_schemas(db);
    if (load_new_ondisk(db, NULL)) {
        logmsg(LOGMSG_ERROR, "Failed to load new .ONDISK\n");
        return 1;
    }
    if (load_csc2_versions(db, NULL)) {
        logmsg(LOGMSG_ERROR, "Failed to load .ONDISK.VER.nn\n");
        return 1;
    }
    if (create_datacopy_array(db)) {
        logmsg(LOGMSG_ERROR, "Failed to create datacopy array for %s\n", db->tablename);
        return 1;
    }
    db->tableversion = table_version_select(db, NULL);
    update_dbstore(db);
    create_sqlmaster_records(tran);
    create_sqlite_master();
    return 0;
}

#include <bdb_int.h>

int reload_db_tran(dbtable *db, tran_type *tran)
{
    backout_schemas(db->tablename);
    clear_existing_schemas(db);
    if (load_new_ondisk(db, tran)) {
        logmsg(LOGMSG_ERROR, "Failed to load new .ONDISK\n");
        return 1;
    }
    if (load_csc2_versions(db, tran)) {
        logmsg(LOGMSG_ERROR, "Failed to load .ONDISK.VER.nn\n");
        return 1;
    }
    db->tableversion = table_version_select(db, tran);
    update_dbstore(db);
    create_sqlmaster_records(tran);
    create_sqlite_master();
    return 0;
}

short field_decimal_quantum(const dbtable *db, struct schema *s, int fnum,
                            char *ptail, int taillen, int *sign)

{
    int cnt;
    int i;
    short *tail = (short *)ptail;

    if (fnum > s->nmembers) {
        logmsg(LOGMSG_ERROR, "%s: wrong field fnum=%d schema has %d members\n",
                __func__, fnum, s->nmembers);
        return 0;
    }

    for (i = 0, cnt = 0; i < fnum; i++) {
        if (s->member[i].type == SERVER_DECIMAL) {
            cnt++;
        }
    }

    if (cnt >= taillen / 4) {
        static int once = 0;
        if (!once) {
            once = 1;
            logmsg(LOGMSG_ERROR, "we have old decimals\n");
        }

        if (sign)
            *sign = -1;

        return 0;
    }

    if (sign) {
        *sign = ntohs(tail[taillen / 4 + cnt]);
    }

    return ntohs(tail[cnt]);
}

/**
 * Given a normalized cohort value, make that value memcmp-able by zero-ing out
 *the quantum value
 * The quantum value is returned so that it can be stored out-of-band, and be
 *preserved for reads
 * In the case of indexes, this is stored as a column attribute in the data part
 *
 */
int extract_decimal_quantum(const dbtable *db, int ix, char *inbuf,
                            char *poutbuf, int outbuf_max, int *outlen)
{
    struct schema *s;
    int i;
    short ch;
    int sign;
    int decimals;
    short *outbuf = (short *)poutbuf;

    if (!db || ix < 0 || ix >= db->nix || db->ixschema == 0 ||
        db->ixschema[ix] == 0) {
        logmsg(LOGMSG_FATAL, "%s: bug, bug\n", __func__);
        abort();
    }

    if (outlen)
        *outlen = 0;

    s = db->ixschema[ix];

    decimals = 0;
    for (i = 0; i < s->nmembers; i++) {
        if (s->member[i].type == SERVER_DECIMAL)
            decimals++;
    }

    if (outbuf && outlen && (outbuf_max < 4 * decimals)) {
        logmsg(LOGMSG_ERROR, 
                "%s: wrong buffer size, cannot construct datacopy %d+1 >= %d \n",
                __func__, 4 * decimals, outbuf_max);
        return -1;
    }

    for (i = 0; i < s->nmembers; i++) {
        if (s->member[i].type == SERVER_DECIMAL) {
            if (bdb_attr_get(thedb->bdb_attr,
                             BDB_ATTR_REPORT_DECIMAL_CONVERSION)) {
                logmsg(LOGMSG_USER, "Dec extract IN:\n");
                hexdump(LOGMSG_USER, &inbuf[s->member[i].offset],
                        s->member[i].len);
                logmsg(LOGMSG_USER, "\n");
            }

            ch = decimal_quantum_get(&inbuf[s->member[i].offset],
                                     s->member[i].len, &sign);
            if (outbuf && outlen) {
                outbuf[*outlen] = htons(ch);
                outbuf[decimals + *outlen] = htons(sign);
                (*outlen)++;
            }

            if (bdb_attr_get(thedb->bdb_attr,
                             BDB_ATTR_REPORT_DECIMAL_CONVERSION)) {
                logmsg(LOGMSG_USER, "Dec extract OUT:\n");
                hexdump(LOGMSG_USER, &inbuf[s->member[i].offset],
                        s->member[i].len);
                logmsg(LOGMSG_USER, "\n");
            }
        }
    }

    if (outbuf && outlen) {
        *outlen += decimals; /*add signs*/
        *outlen *= 2;
    }

    return 0;
}

int create_key_from_schema(const struct dbtable *db, struct schema *schema, int ixnum, char **tail, int *taillen,
                           char *mangled_key, char *partial_datacopy_tail, const char *inbuf, int inbuflen,
                           char *outbuf, blob_buffer_t *inblobs, int maxblobs, const char *tzname)
{
    int rc = 0;
    struct schema *fromsch, *tosch;
    const char *fromtag, *totag;

    if (inblobs == NULL)
        maxblobs = 0;

    for (int i = 0; i != maxblobs; ++i) {
        rc = unodhfy_blob_buffer(db, inblobs + i, i);
        if (rc != 0)
            return rc;
    }

    fromsch = schema ? schema : get_schema(db, -1);
    tosch = get_schema(db, ixnum);

    rc = _stag_to_stag_buf_flags_blobs(db, fromsch, tosch, inbuf, outbuf, 0 /*flags*/,
                                       NULL, inblobs, NULL /*outblobs*/, maxblobs, tzname);
    if (rc)
        return rc;

    if (db->ix_datacopy[ixnum]) {
        assert(db->ix_collattr[ixnum] == 0);

        if (db->ix_datacopy[ixnum] > 1) {
            /* the key has decimals */
            rc = extract_decimal_quantum(db, ixnum, outbuf, mangled_key,
                                         4 * (db->ix_datacopy[ixnum] - 1),
                                         taillen);
        }
        if (rc) {
            if (tail) {
                *tail = NULL;
                *taillen = 0;
            }
            rc = -1; /* callers like -1 */
        } else if (tail) {
            fromtag = fromsch->tag;
            totag = tosch->tag;
            if ((strncmp(fromtag, ".ONDISK", 7) == 0 &&
                 strncmp(totag, ".NEW.", 5) == 0) ||
                (strncmp(fromtag, ".NEW.", 5) == 0 &&
                 strncmp(totag, ".NEW.", 5) != 0)) {
                /* Abort if new index uses old ondisk datacopy;
                 * or if old index uses new ondisk datacopy. */
                logmsg(LOGMSG_FATAL,
                       "%s: BUG! BUG! Converting from tag %s to %s\n", __func__,
                       fromtag, totag);
                abort();
            }
            if (partial_datacopy_tail && (tosch->flags & SCHEMA_PARTIALDATACOPY)) {
                rc = stag_to_stag_buf_schemas(db, fromsch, tosch->partial_datacopy,
                                              inbuf, partial_datacopy_tail, tzname);
                if (rc)
                    return rc;
                *tail = (char *)partial_datacopy_tail;
                *taillen = get_size_of_schema(tosch->partial_datacopy);
            } else {
                *tail = (char *)inbuf;
                *taillen = inbuflen;
            }
        }
    } else if (db->ix_collattr[ixnum]) {
        assert(db->ix_datacopy[ixnum] == 0);

        rc = extract_decimal_quantum(db, ixnum, outbuf, mangled_key,
                                     4 * db->ix_collattr[ixnum], taillen);
        if (rc) {
            if (tail) {
                *tail = NULL;
                *taillen = 0;
            }
            rc = -1; /* callers like -1 */
        } else {
            if (tail) {
                *tail = mangled_key;
            }
        }
    } else {
        if (tail) {
            *tail = NULL;
            *taillen = 0;
        }
    }

    return rc;
}

int create_key_from_ondisk(const struct dbtable *db, int ixnum, const char *inbuf, char *outbuf)
{
    return create_key_from_schema(db, NULL, ixnum, NULL, NULL, NULL, NULL, inbuf, 0, outbuf, NULL, 0, NULL);
}

int create_key_from_schema_simple(const struct dbtable *db, struct schema *schema, int ixnum, const char *inbuf,
                                  char *outbuf, blob_buffer_t *inblobs, int maxblobs)
{
    return create_key_from_schema(db, schema, ixnum, NULL, NULL, NULL, NULL, inbuf, 0, outbuf, inblobs, maxblobs, NULL);
}

int create_key_from_ireq(struct ireq *iq, int ixnum, int isDelete, char **tail,
                         int *taillen, char *mangled_key, char *partial_datacopy_tail,
                         const char *inbuf, int inbuflen, char *outbuf)
{
    int rc = 0;
    dbtable *db = iq->usedb;

    if (isDelete)
        memcpy(outbuf, iq->idxDelete[ixnum], db->ix_keylen[ixnum]);
    else
        memcpy(outbuf, iq->idxInsert[ixnum], db->ix_keylen[ixnum]);

    if (db->ix_datacopy[ixnum]) {
        assert(db->ix_collattr[ixnum] == 0);

        if (db->ix_datacopy[ixnum] > 1) {
            /* the key has decimals */
            rc = extract_decimal_quantum(db, ixnum, outbuf, mangled_key,
                                         4 * (db->ix_datacopy[ixnum] - 1),
                                         taillen);
        }
        if (rc) {
            if (tail) {
                *tail = NULL;
                *taillen = 0;
            }
            rc = -1; /* callers like -1 */
        } else if (tail) {
            struct schema *fromsch = get_schema(db, -1);
            struct schema *tosch = get_schema(db, ixnum);
            if (partial_datacopy_tail && (tosch->flags & SCHEMA_PARTIALDATACOPY)) {
                rc = stag_to_stag_buf_schemas(db, fromsch, tosch->partial_datacopy,
                                              inbuf, partial_datacopy_tail, iq->tzname);
                if (rc)
                    return rc;

                *tail = (char *)partial_datacopy_tail;
                *taillen = get_size_of_schema(tosch->partial_datacopy);
            } else {
                *tail = (char *)inbuf;
                *taillen = inbuflen;
            }
        }
    } else if (db->ix_collattr[ixnum]) {
        assert(db->ix_datacopy[ixnum] == 0);

        rc = extract_decimal_quantum(db, ixnum, outbuf, mangled_key,
                                     4 * db->ix_collattr[ixnum], taillen);
        if (rc) {
            if (tail) {
                *tail = NULL;
                *taillen = 0;
            }
            rc = -1; /* callers like -1 */
        } else {
            if (tail) {
                *tail = mangled_key;
            }
        }
    } else {
        if (tail) {
            *tail = NULL;
            *taillen = 0;
        }
    }

    return rc;
}

struct schema *get_schema(const struct dbtable *db, int ix)
{
    return (ix == -1) ? db->schema : db->ixschema[ix];
}
