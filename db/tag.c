/*
   Copyright 2015, 2017, Bloomberg Finance L.P.

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

#include "limit_fortify.h"
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

#include <str0.h>
#include <epochlib.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <plbitlib.h>
#include <segstr.h>
#include <fsnap.h>

#include <netinet/in.h>
#include "util.h"
#include <plhash.h>
#include "tag.h"
#include "types.h"
#include "comdb2.h"
#include "block_internal.h"
#include "prefault.h"

#include <dynschematypes.h>
#include <dynschemaload.h>

#include "sql.h"
#include <bdb_api.h>
#include <strbuf.h>

#include "utilmisc.h"
#include "views.h"
#include "debug_switches.h"
#include "logmsg.h"

extern struct dbenv *thedb;
extern void hexdump(const void *buf, int size);

pthread_key_t unique_tag_key;

struct dbtag {
    char *tblname;
    hash_t *tags;
    LISTC_T(struct schema) taglist;
};

int gbl_use_t2t = 0;

char gbl_ver_temp_table[] = ".COMDB2.TEMP.VER.";
char gbl_ondisk_ver[] = ".ONDISK.VER.";
char gbl_ondisk_ver_fmt[] = ".ONDISK.VER.%d";
const int gbl_ondisk_ver_len = sizeof ".ONDISK.VER.255xx";

#define TAGLOCK_RW_LOCK
#ifdef TAGLOCK_RW_LOCK
static pthread_rwlock_t taglock;
#else
static pthread_mutex_t taglock;
#endif
static hash_t *tags;

int compare_tag_int(struct schema *old, struct schema *new, FILE *out,
                    int strict);
int compare_indexes(const char *table, FILE *out);

static inline int lock_taglock_read(void)
{
#ifdef TAGLOCK_RW_LOCK
    return pthread_rwlock_rdlock(&taglock);
#else
    return pthread_mutex_lock(&taglock);
#endif
}

static inline int lock_taglock(void)
{
#ifdef TAGLOCK_RW_LOCK
    return pthread_rwlock_wrlock(&taglock);
#else
    return pthread_mutex_lock(&taglock);
#endif
}

static inline int unlock_taglock(void)
{
#ifdef TAGLOCK_RW_LOCK
    return pthread_rwlock_unlock(&taglock);
#else
    return pthread_mutex_unlock(&taglock);
#endif
}

static inline int init_taglock(void)
{
#ifdef TAGLOCK_RW_LOCK
    return pthread_rwlock_init(&taglock, NULL);
#else
    return pthread_mutex_init(&taglock, NULL);
#endif
}

/* set dbstore (or null) value for a column
 * returns 0 if there was no valid dbstore */
static inline int set_dbstore(struct dbtable *db, int col, void *field)
{
    struct dbstore *dbstore = db->dbstore;
    if (dbstore[col].len) {
        memcpy(field, dbstore[col].data, dbstore[col].len);
    } else {
        set_null(field, 0);
    }
    return dbstore[col].len;
}

static int ctag_to_stag_int(const char *table, const char *ctag,
                            const char *inbuf, int len,
                            const unsigned char *innulls, const char *stag,
                            void *outbufp, int flags, int ondisk_lim,
                            struct convert_failure *fail_reason,
                            blob_buffer_t *inblobs, blob_buffer_t *outblobs,
                            int maxblobs, const char *tzname);

int schema_init(void)
{
    int rc;

    rc = init_taglock();
    if (rc) {
        logmsg(LOGMSG_ERROR, "schema_init:pthread_rwlock_init failed rc=%d\n", rc);
        return -1;
    }
    tags = hash_init_user((hashfunc_t *)strhashfunc, (cmpfunc_t *)strcmpfunc,
                          offsetof(struct dbtag, tblname), 0);
    rc = pthread_key_create(&unique_tag_key, free);
    if (rc) {
        logmsg(LOGMSG_ERROR, "schema_init:pthread_key_create failed rc=%d\n", rc);
        return -1;
    }

    logmsg(LOGMSG_INFO, "Schema module init ok\n");
    return 0;
}

void add_tag_schema(const char *table, struct schema *schema)
{
    struct dbtag *tag;
    int rc;

    rc = lock_taglock();
    if (rc != 0) {
        logmsg(LOGMSG_FATAL, "pthread_rwlock_wrlock(&taglock) failed\n");
        exit(1);
    }

    tag = hash_find_readonly(tags, &table);
    if (tag == NULL) {
        tag = malloc(sizeof(struct dbtag));
        if (tag == NULL) {
            logmsg(LOGMSG_FATAL, "malloc failed\n");
            exit(1);
        }

        tag->tblname = strdup(table);
        tag->tags =
            hash_init_user((hashfunc_t *)strhashfunc, (cmpfunc_t *)strcmpfunc,
                           offsetof(struct schema, tag), 0);
        hash_add(tags, tag);
        listc_init(&tag->taglist, offsetof(struct schema, lnk));
    }
    hash_add(tag->tags, schema);
    listc_abl(&tag->taglist, schema);

    rc = unlock_taglock();
    if (rc != 0) {
        logmsg(LOGMSG_FATAL, "pthread_rwlock_unlock(&taglock) failed\n");
        exit(1);
    }
}

void del_tag_schema(const char *table, const char *tagname)
{
    int rc = lock_taglock();
    if (rc != 0) {
        logmsg(LOGMSG_FATAL, "pthread_rwlock_rwlock(&taglock) failed\n");
        exit(1);
    }

    struct dbtag *tag = hash_find_readonly(tags, &table);
    if (tag == NULL) {
        unlock_taglock();
        return; /* doesn't exist */
    }
    struct schema *sc = hash_find(tag->tags, &tagname);
    if (sc) {
        hash_del(tag->tags, sc);
        listc_rfl(&tag->taglist, sc);
        if (sc->datacopy) {
            free(sc->datacopy);
            sc->datacopy = NULL;
        }
    }
    /* doesn't exist? */
    rc = unlock_taglock();
    if (rc != 0) {
        logmsg(LOGMSG_FATAL, "pthread_rwlock_unlock(&taglock) failed\n");
        exit(1);
    }
}

struct schema *find_tag_schema(const char *table, const char *tagname)
{
    int rc = lock_taglock_read();
    if (unlikely(rc != 0)) {
        logmsg(LOGMSG_FATAL, "pthread_rwlock_rdlock(&taglock) failed\n");
        exit(1);
    }

    struct dbtag *tag = hash_find_readonly(tags, &table);
    if (unlikely(tag == NULL)) {
        rc = unlock_taglock();
        if (rc != 0) {
            logmsg(LOGMSG_FATAL, "pthread_rwlock_unlock(&taglock) failed\n");
            exit(1);
        }
        return NULL;
    }
    struct schema *s = hash_find_readonly(tag->tags, &tagname);

    rc = unlock_taglock();
    if (unlikely(rc != 0)) {
        logmsg(LOGMSG_FATAL, "pthread_rwlock_unlock(&taglock) failed\n");
        exit(1);
    }

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

int find_field_idx(const char *table, const char *tagname, const char *field)
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
    default:
        return type;
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
    int idx;
    int maxalign = 0;

    struct field *last_field = &sc->member[sc->nmembers - 1];
    sz = last_field->offset + last_field->len;
    /* if it's a table, fiddle with size so alignment rules match cmacc */
    if (sc->flags & SCHEMA_TABLE) {
        /* actually, we might have this information from csc2lib, in which case
         * just believe him. -- Sam J */
        if (sc->recsize > 0) {
            sz = sc->recsize;
        } else {
            /* we wind up in here for dynamic schemas */
            for (idx = 0; idx < sc->nmembers; idx++) {
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
        }
    }

    if (maxalign) {
        if (sz % maxalign)
            sz += (maxalign - sz % maxalign);
    }

    return sz;
}

int get_size_of_schema_by_name(const char *table, const char *schema)
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
    tag = hash_find_readonly(tags, &tblname);
    if (tag == NULL) {
        unlock_taglock();
        return;
    }
    hash_for(tag->tags, (hashforfunc_t *)dump_tag, NULL);
    unlock_taglock();
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
            slen = pstr2lenlim(buf, len);
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
            client_datetime_get(&dt, buf, buf + sizeof(cdb2_client_datetime_t));
            logmsg(LOGMSG_USER, "%4.4u-%2.2u-%2.2uT%2.2u%2.2u%2.2u.%3.3u %s", dt.tm.tm_year,
                   dt.tm.tm_mon, dt.tm.tm_mday, dt.tm.tm_hour, dt.tm.tm_min,
                   dt.tm.tm_sec, dt.msec, dt.tzname);
            break;
        }

        case CLIENT_DATETIMEUS: {
            cdb2_client_datetimeus_t dt;
            client_datetimeus_get(&dt, buf,
                                  buf + sizeof(cdb2_client_datetimeus_t));
            logmsg(LOGMSG_USER, "%4.4u-%2.2u-%2.2uT%2.2u%2.2u%2.2u.%6.6u %s", dt.tm.tm_year,
                   dt.tm.tm_mon, dt.tm.tm_mday, dt.tm.tm_hour, dt.tm.tm_min,
                   dt.tm.tm_sec, dt.usec, dt.tzname);
            break;
        }

        case CLIENT_INTVYM: {
            cdb2_client_intv_ym_t ym;
            client_intv_ym_get(&ym, buf, buf + sizeof(cdb2_client_intv_ym_t));
            logmsg(LOGMSG_USER, "%s%u-%u", (ym.sign < 0) ? "- " : "", ym.years, ym.months);
            break;
        }

        case CLIENT_INTVDS: {
            cdb2_client_intv_ds_t ds;
            client_intv_ds_get(&ds, buf, buf + sizeof(cdb2_client_intv_ds_t));
            logmsg(LOGMSG_USER, "%s%u %u:%u:%u.%u", (ds.sign < 0) ? "- " : "", ds.days,
                   ds.hours, ds.mins, ds.sec, ds.msec);
            break;
        }

        case CLIENT_INTVDSUS: {
            cdb2_client_intv_dsus_t ds;
            client_intv_dsus_get(&ds, buf,
                                 buf + sizeof(cdb2_client_intv_dsus_t));
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
            server_datetime_get(&dt, buf, buf + sizeof(server_datetime_t));
            logmsg(LOGMSG_USER, "%llu.%hu", dt.sec, dt.msec);
            break;
        }
        case SERVER_DATETIMEUS: {
            server_datetimeus_t dt;
            server_datetimeus_get(&dt, buf, buf + sizeof(server_datetimeus_t));
            logmsg(LOGMSG_USER, "%llu.%hu", dt.sec, dt.usec);
            break;
        }
        case SERVER_INTVYM: {
            server_intv_ym_t si;
            server_intv_ym_get(&si, buf, buf + sizeof(server_intv_ym_t));
            logmsg(LOGMSG_USER, "%d", si.months);
            break;
        }
        case SERVER_INTVDS: {
            server_intv_ds_t ds;
            server_intv_ds_get(&ds, buf, buf + sizeof(server_intv_ds_t));
            logmsg(LOGMSG_USER, "%lld.%hu", ds.sec, ds.msec);
            break;
        }
        case SERVER_INTVDSUS: {
            server_intv_dsus_t ds;
            server_intv_dsus_get(&ds, buf, buf + sizeof(server_intv_dsus_t));
            logmsg(LOGMSG_USER, "%lld.%hu", ds.sec, ds.usec);
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
        return "bbytearray";
    case SERVER_BLOB:
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

void dump_tagged_buf(const char *table, const char *tag,
                     const unsigned char *buf)
{
    struct schema *sc = find_tag_schema(table, tag);
    if (sc == NULL)
        return;
    dump_tagged_buf_with_schema(sc, buf);
}

/* assume schema has been added */
void add_tag_alias(const char *table, struct schema *s, char *name)
{
    struct dbtag *tag;
    struct schema *sc;
    struct schema *old;

    lock_taglock();
    tag = hash_find_readonly(tags, &table);
    if (tag == NULL) {
        tag = malloc(sizeof(struct dbtag));
        tag->tblname = strdup(table);
        tag->tags =
            hash_init_user((hashfunc_t *)strhashfunc, (cmpfunc_t *)strcmpfunc,
                           offsetof(struct schema, tag), 0);
        listc_init(&tag->taglist, offsetof(struct schema, lnk));
        hash_add(tags, tag);
    }
    sc = clone_schema(s);
    free(sc->tag);
    sc->tag = strdup(name);
    if (s->csctag == NULL)
        s->csctag = strdup(sc->tag);
    listc_abl(&tag->taglist, sc);

    /* Don't add dupes! */
    old = hash_find(tag->tags, &sc->tag);
    if (old) {
        listc_rfl(&tag->taglist, old);
        hash_del(tag->tags, old);
        freeschema(old);
    }

    hash_add(tag->tags, sc);
    unlock_taglock();
}

/* used to clone ONDISK to ONDISK_CLIENT */
int clone_server_to_client_tag(const char *table, const char *fromtag,
                               const char *newtag)
{
    struct schema *from, *to;
    void *typebuf;
    int field, offset, sz;
    struct field *from_field, *to_field;
    int field_idx;
    int rc;

    from = find_tag_schema(table, fromtag);
    if (from == NULL)
        return -1;

    to = calloc(1, sizeof(struct schema));
    to->tag = strdup(newtag);
    add_tag_schema(table, to);
    to->nmembers = from->nmembers;
    to->member = calloc(to->nmembers, sizeof(struct field));
    to->flags = from->flags;
    to->ix = NULL; /* don't need to copy indx yet */
    to->nix = 0;
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
            int i;

            logmsg(LOGMSG_ERROR, 
                   "clone_server_to_client_tag: severe error - returning\n");
            for (i = 0; i <= field; i++) {
                free(to->member[i].name);
                if (to->member[i].in_default != NULL) {
                    free(to->member[i].in_default);
                    to->member[i].in_default = NULL;
                    to->member[i].in_default_type = 0;
                    to->member[i].in_default_len = 0;
                }
            }
            del_tag_schema(table, to->tag);
            free(to->tag);
            free(to);
            return -1;
        }
        to->member[field].offset = offset;
        offset += to->member[field].len;

        to_field->blob_index = from_field->blob_index;

        /* do not clone out_default/in_default - those are only used for
         * .ONDISK tag itself */
    }
    return 0;
}

char *indexes_expressions_unescape(char *expr);
extern int gbl_new_indexes;
/* create keys for each schema */
static int create_key_schema(struct dbtable *db, struct schema *schema, int alt)
{
    char buf[MAXCOLNAME + 1];
    int ix;
    int piece;
    struct field *m;
    int npieces;
    int offset;
    int flags;
    char altname[MAXTAGLEN];
    char tmptagname[MAXTAGLEN];
    char *where;
    char *expr;
    int rc;
    int ascdesc;
    char *dbname = db->dbname;
    struct schema *s;

    /* keys not reqd for ver temp table; just ondisk tag */
    if (strncasecmp(dbname, gbl_ver_temp_table, strlen(gbl_ver_temp_table)) ==
        0)
        return 0;

    schema->nix = dyns_get_idx_count();
    schema->ix = calloc(schema->nix, sizeof(struct schema *));

    for (ix = 0; ix < schema->nix; ix++) {
        snprintf(buf, sizeof(buf), "%s_ix_%d", schema->tag, ix);
        s = schema->ix[ix] = calloc(1, sizeof(struct schema));

        s->tag = strdup(buf);
        s->nmembers = dyns_get_idx_piece_count(ix);
        s->member = calloc(schema->ix[ix]->nmembers, sizeof(struct field));

        s->flags = SCHEMA_INDEX;

        if (dyns_is_idx_dup(ix))
            s->flags |= SCHEMA_DUP;

        if (dyns_is_idx_recnum(ix))
            s->flags |= SCHEMA_RECNUM;

        if (dyns_is_idx_datacopy(ix))
            s->flags |= SCHEMA_DATACOPY;

        s->nix = 0;
        s->ix = NULL;
        s->ixnum = ix;
        if ((strcasecmp(schema->tag, ".ONDISK") == 0 ||
             strcasecmp(schema->tag, ".NEW..ONDISK") == 0) &&
            (db->ixschema && db->ixschema[ix] == NULL))
            db->ixschema[ix] = s;
        npieces = s->nmembers;
        offset = 0;
        for (piece = 0; piece < s->nmembers; piece++) {
            m = &s->member[piece];
            m->flags = 0;
            expr = NULL;
            m->isExpr = 0;
            dyns_get_idx_piece(ix, piece, buf, sizeof(buf), &m->type,
                               (int *)&m->offset, (int *)&m->len, &ascdesc,
                               &expr);
            if (expr) {
                expr = indexes_expressions_unescape(expr);
                m->name = expr;
                m->isExpr = 1;
                m->idx = -1;
                if (expr == NULL) {
                    logmsg(LOGMSG_ERROR, "Indexes on expressions error: failed to"
                                    "unescape expression string\n");
                    rc = 1;
                    goto errout;
                }
                switch (m->type) {
                case 0:
                    abort();
                case SERVER_BLOB:
                case SERVER_VUTF8:
                case SERVER_BLOB2:
                    logmsg(LOGMSG_ERROR, "Indexes on expressions error: index %d "
                                    "blob is not supported\n",
                            ix);
                    if (db->iq)
                        reqerrstr(db->iq, ERR_SC,
                                  "blob index is not supported.");
                    rc = 1;
                    goto errout;
                case SERVER_BCSTR:
                    if (m->len < 2) {
                        logmsg(LOGMSG_ERROR, "Indexes on expressions error: index "
                                        "%d string must be at least 2 bytes in "
                                        "length\n",
                                ix);
                        if (db->iq)
                            reqerrstr(db->iq, ERR_SC, "string must be at least "
                                                      "2 bytes in in length.");
                        rc = 1;
                        goto errout;
                    }
                    break;
                case SERVER_DATETIME:
                    /* server CLIENT_DATETIME is a server_datetime_t */
                    m->len = sizeof(server_datetime_t);
                    break;
                case SERVER_DATETIMEUS:
                    /* server CLIENT_DATETIMEUS is a server_datetimeus_t */
                    m->len = sizeof(server_datetimeus_t);
                    break;
                case SERVER_INTVYM:
                    /* server CLIENT_INTVYM is a server_intv_ym_t */
                    m->len = sizeof(server_intv_ym_t);
                    break;
                case SERVER_INTVDS:
                    /* server CLIENT_INTVDS is a server_intv_ds_t */
                    m->len = sizeof(server_intv_ds_t);
                    break;
                case SERVER_INTVDSUS:
                    /* server CLIENT_INTVDSUS is a server_intv_dsus_t */
                    m->len = sizeof(server_intv_dsus_t);
                    break;
                case SERVER_DECIMAL:
                    switch (m->len) {
                    case 14:
                        m->len = sizeof(server_decimal32_t);
                        break;
                    case 24:
                        m->len = sizeof(server_decimal64_t);
                        break;
                    case 43:
                        m->len = sizeof(server_decimal128_t);
                        break;
                    default:
                        abort();
                    }
                    break;
                default:
                    /* other types just add one for the flag byte */
                    m->len++;
                    break;
                }
                if (offset + m->len > MAXKEYLEN) {
                    logmsg(LOGMSG_ERROR, 
                           "Indexes on expressions error: index %d is too large\n",
                           ix);
                    if (db->iq)
                        reqerrstr(db->iq, ERR_SC, "index %d is too large.", ix);
                    rc = 1;
                    goto errout;
                }
                db->ix_expr = 1;
            } else {
                m->name = strdup(buf);
                m->idx = find_field_idx(dbname, schema->tag, m->name);
                if (m->idx == -1) {
                    rc = -ix - 1;
                    goto errout;
                }
                m->type = schema->member[m->idx].type;
                m->len = schema->member[m->idx].len;

                /* the dbstore default is still needed during schema change, so
                 * populate that */
                if (schema->member[m->idx].in_default) {
                    m->in_default =
                        malloc(schema->member[m->idx].in_default_len);
                    m->in_default_len = schema->member[m->idx].in_default_len;
                    m->in_default_type = schema->member[m->idx].in_default_type;
                    memcpy(m->in_default, schema->member[m->idx].in_default,
                           m->in_default_len);
                }
                memcpy(&m->convopts, &schema->member[m->idx].convopts,
                       sizeof(struct field_conv_opts));
                if (gbl_new_indexes && strncmp(dbname, "sqlite_stat", 11))
                    m->isExpr = 1;
            }
            if (ascdesc)
                m->flags |= INDEX_DESCEND;
            /* we could check here if there are decimals in the index, and mark
             * it "datacopy" special */
            if (db->ix_datacopy[ix] == 0 && m->type == SERVER_DECIMAL) {
                db->ix_collattr[ix]++; /* special tail */
            } else if (m->type == SERVER_DECIMAL) {
                db->ix_datacopy[ix]++; /* special tail */
            }
            m->offset = offset;
            offset += m->len;
        }
        /* rest of fields irrelevant for indexes */
        add_tag_schema(dbname, s);
        rc = dyns_get_idx_tag(ix, altname, MAXTAGLEN, &where);
        if (rc == 0 && (strcasecmp(schema->tag, ".ONDISK") == 0 ||
                        strcasecmp(schema->tag, ".NEW..ONDISK") == 0)) {
            if (alt == 0) {
                char *tmp_altname = strdup(altname);
                add_tag_alias(dbname, s, tmp_altname);
            } else {
                snprintf(tmptagname, sizeof(tmptagname), ".NEW.%s", altname);
                add_tag_alias(dbname, s, tmptagname);
            }
            if (where) {
                s->where = strdup(where);
                db->ix_partial = 1;
            }
        }
    }
    if (gbl_new_indexes && strncmp(dbname, "sqlite_stat", 11)) {
        db->ix_partial = 1;
        db->ix_expr = 1;
    }
    return 0;

errout:
    freeschema(s);
    free(schema->ix);
    schema->ix = NULL;
    schema->nix = 0;
    return rc;
}

int fixup_verified_record(const char *dbname, const char *from, char *to)
{
    struct schema *ondisk, *def;
    int field;
    int fidx;
    struct field *f;
    ondisk = find_tag_schema(dbname, ".ONDISK");
    def = find_tag_schema(dbname, ".DEFAULT");

    /* go through default schema.  copy corresponding fields from->to */
    for (field = 0; field < ondisk->nmembers; field++) {
        fidx = find_field_idx_in_tag(def, ondisk->member[field].name);
        if (fidx == -1) {
            f = &ondisk->member[field];
            memcpy(to + f->offset, from + f->offset, f->len);
        }
    }
    return 0;
}

/* Given a partial string, find the length of a key.
   Partial string MUST be a field prefix. */
int partial_key_length(const char *dbname, const char *keyname,
                       const char *pstring, int len)
{
    int tlen, toff = 0;
    char *tok;
    int fldnum = 0;
    int klen = 0;
    char *s;
    int plen;
    int is_last = 0;
    int space_err = 0;

    struct schema *sc = find_tag_schema(dbname, keyname);
    if (sc == NULL)
        return -1;

    if (len > 0 && strnchr(pstring, len, ' '))
        space_err = 1;

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

int client_keylen_to_server_keylen(const char *table, const char *tag,
                                   int ixnum, int keylen)
{
    char skeytag[MAXTAGLEN];
    char ckeytag[MAXTAGLEN];
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

int ctag_to_stag_buf(const char *table, const char *ctag, const char *inbuf,
                     int len, const unsigned char *innulls, const char *stag,
                     void *outbufp, int flags,
                     struct convert_failure *fail_reason)
{
    return ctag_to_stag_int(table, ctag, inbuf, len, innulls, stag, outbufp,
                            flags, WHOLE_BUFFER, fail_reason, NULL, NULL, 0,
                            NULL);
}

int ctag_to_stag_buf_tz(const char *table, const char *ctag, const char *inbuf,
                        int len, const unsigned char *innulls, const char *stag,
                        void *outbufp, int flags,
                        struct convert_failure *fail_reason, const char *tzname)
{
    return ctag_to_stag_int(table, ctag, inbuf, len, innulls, stag, outbufp,
                            flags, WHOLE_BUFFER, fail_reason, NULL, NULL, 0,
                            tzname);
}

static int _ctag_to_stag_blobs(const char *table, const char *ctag,
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

int ctag_to_stag_blobs(const char *table, const char *ctag, const char *inbuf,
                       int len, const unsigned char *innulls, const char *stag,
                       void *outbufp, int flags,
                       struct convert_failure *fail_reason,
                       blob_buffer_t *blobs, int maxblobs)
{

    return _ctag_to_stag_blobs(table, ctag, inbuf, len, innulls, stag, outbufp,
                               flags, fail_reason, blobs, maxblobs, NULL);
}

int ctag_to_stag_blobs_tz(const char *table, const char *ctag,
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

int ctag_to_stag_buf_p(const char *table, const char *ctag, const char *inbuf,
                       int len, const unsigned char *innulls, const char *stag,
                       void *outbufp, int flags, int ondisk_lim,
                       struct convert_failure *fail_reason)
{
    return ctag_to_stag_int(table, ctag, inbuf, len, innulls, stag, outbufp,
                            flags, ondisk_lim, fail_reason, NULL, NULL, 0,
                            NULL);
}

int ctag_to_stag_buf_p_tz(const char *table, const char *ctag,
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
    struct schema *from;
    struct schema *to;
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

    convert_failure_reason_str(fail_reason, iq->usedb->dbname, fromtag, totag,
                               str, sizeof(str));
    reqprintf(iq, "convert: %s\n", str);
    return;
}

/* Plan how to convert from one schema to another.  The input plan should be
 * large enough to hold all the relevant fields. */
static void populate_t2t_plan(const struct schema *from,
                              const struct schema *to, struct t2t_plan *plan)
{
    const struct field *to_field;
    int to_field_idx;

    plan->from = from;
    plan->to = to;
    init_convert_failure_reason(&plan->fail_reason);
    plan->fail_reason.source_schema = from;
    plan->fail_reason.target_schema = to;

    plan->max_from_len = get_size_of_schema(from);
    plan->max_to_len = get_size_of_schema(to);

    for (to_field_idx = 0, to_field = to->member; to_field_idx < to->nmembers;
         to_field_idx++, to_field++) {

        int from_field_idx;

        from_field_idx = find_field_idx_in_tag(from, to_field->name);

        if (gbl_replicate_local &&
            strcasecmp(to_field->name, "comdb2_seqno") == 0 &&
            IS_SERVER_TYPE(to_field->type)) {
            if (from_field_idx < 0 ||
                IS_SERVER_TYPE(from->member[from_field_idx].type)) {
                /* If the input tag doesn't have this seqno column, of if the
                 * input tag is client format, then generate a seqno.  If the
                 * input tag is server format and has this column then we
                 * want to use the value that it already has. */
                plan->fields[to_field_idx].from_field_idx =
                    T2T_PLAN_COMDB2_SEQNO;
                continue;
            }
        }

        if (from_field_idx < 0) {
            /* Field doesn't exist in source tag.  If we have an in_default
             * defined then we can use that.  Otherwise we'll be storing null.
             */
            if (to_field->in_default) {
                plan->fields[to_field_idx].from_field_idx = T2T_PLAN_IN_DEFAULT;
                continue;
            } else {
                if (to_field->flags & NO_NULL) {
                    /* Dest tag doesn't allow null here, so this conversion
                     * could
                     * never work UNLESS we're doing an update, so record
                     * T2T_PLAN_NULL anyway. */
                    plan->fail_reason.reason =
                        CONVERT_FAILED_NULL_CONSTRAINT_VIOLATION;
                    plan->fail_reason.target_field_idx = to_field_idx;
                    plan->fail_reason.source_field_idx = -1;
                }
                plan->fields[to_field_idx].from_field_idx = T2T_PLAN_NULL;
            }
        } else {
            plan->fields[to_field_idx].from_field_idx = from_field_idx;
        }
    }
}

static void dump_t2t_plan(FILE *out, const struct t2t_plan *plan)
{
    logmsg(LOGMSG_USER, "Plan to convert from tag %s -> tag %s\n", plan->from->tag,
            plan->to->tag);
    if (plan->fail_reason.reason != CONVERT_OK) {
        char str[256];
        convert_failure_reason_str(&plan->fail_reason, "unused",
                                   plan->from->tag, plan->to->tag, str,
                                   sizeof(str));
        logmsg(LOGMSG_USER, "  Conversion impossible due to: %s\n", str);
    } else {
        int to_field_idx;
        const struct field *to_field;

        for (to_field_idx = 0, to_field = plan->to->member;
             to_field_idx < plan->to->nmembers; to_field_idx++, to_field++) {

            int from_field_idx;

            logmsg(LOGMSG_USER, "  Field %s(%s) = ", to_field->name,
                    strtype(to_field->type));

            from_field_idx = plan->fields[to_field_idx].from_field_idx;
            if (T2T_PLAN_NULL == from_field_idx) {
                logmsg(LOGMSG_USER, "NULL");
            } else if (T2T_PLAN_IN_DEFAULT == from_field_idx) {
                logmsg(LOGMSG_USER, "dbstore value");
            } else if (T2T_PLAN_COMDB2_SEQNO == from_field_idx) {
                logmsg(LOGMSG_USER, "generate seq number");
            } else {
                const struct field *from_field;
                from_field = &plan->from->member[from_field_idx];
                logmsg(LOGMSG_USER, "%s(%s)", from_field->name,
                        strtype(from_field->type));
            }

            logmsg(LOGMSG_USER, "\n");
        }
    }
}

/* XXX is this used?  can we delete it? */
/* Do a pre-planned conversion from one tag format to another. */
static int t2t_with_plan(const struct t2t_plan *plan, const void *from_buf,
                         int from_len, const unsigned char *from_nulls,
                         blob_buffer_t *from_blobs, void *to_buf, int to_len,
                         unsigned char *to_nulls, blob_buffer_t *to_blobs,
                         int maxblobs, int flags, const char *tzname,
                         struct convert_failure *fail_reason)
{
    int to_field_idx;
    struct field *to_field;
    struct field_conv_opts_tz tzopts1;
    struct field_conv_opts_tz tzopts2;
    char *temp_buf = NULL;
    int server_sort_off;
    unsigned char *to_bufptr = to_buf;
    int to_bufpos = 0;
    int out_len = 0;

    /* See if we're doomed before we even start.  The only reason this can
     * happen right now is that we're missing a required column.  This is ok
     * if we're doing an update. */
    if ((plan->fail_reason.reason != CONVERT_OK) && !(flags & CONVERT_UPDATE)) {
        if (fail_reason) {
            memcpy(fail_reason, &plan->fail_reason, sizeof(fail_reason));
        }
        return -1;
    }

    /* for server type fields that have INDEX_DESCEND set we shouldn't invert
     * the first byte so that nulls will sort low. */
    if (gbl_sort_nulls_correctly) {
        server_sort_off = 0;
    } else {
        server_sort_off = 1;
    }

    if (tzname) {
        if (tzname[0]) {
            strncpy(tzopts1.tzname, tzname, sizeof(tzopts1.tzname));
            strncpy(tzopts2.tzname, tzname, sizeof(tzopts2.tzname));
        } else {
            tzname = NULL;
        }
    }

    if (from_len < 0) {
        from_len = plan->max_from_len;
    }
    if (to_len < 0) {
        to_len = plan->max_to_len;
    }

    for (to_field_idx = 0, to_field = plan->to->member;
         to_field_idx < plan->to->nmembers; to_field_idx++, to_field++) {

        const char *from_fieldbuf;
        int from_fieldlen, from_field_idx;
        struct field_conv_opts convopts;
        blob_buffer_t *from_blob;
        blob_buffer_t *to_blob;
        int from_type;
        const struct field_conv_opts *from_convopts;
        const struct field_conv_opts *to_convopts;
        unsigned long long seqno;
        struct field_conv_opts seqno_convopts;
        int to_fieldlen;
        int exclude_len = 0;

        if (to_blobs && to_field->blob_index >= 0) {
            if (to_field->blob_index >= maxblobs) {
                if (fail_reason) {
                    memcpy(fail_reason, &plan->fail_reason,
                           sizeof(fail_reason));
                    fail_reason->target_field_idx = to_field_idx;
                    fail_reason->reason = CONVERT_FAILED_BAD_BLOB_PROGRAMMER;
                }
                return -1;
            }
            to_blob = &to_blobs[to_field->blob_index];
        } else {
            to_blob = NULL;
        }

        to_fieldlen = to_field->len;
        to_convopts = &to_field->convopts;

        /**
         ** Populate the from_ field variables
         **/
        from_field_idx = plan->fields[to_field_idx].from_field_idx;

        if (from_field_idx == T2T_PLAN_COMDB2_SEQNO) {
            /* Generate a sequence number for the comdb2_seqno field */
            seqno = get_unique_longlong(thedb);
            bzero(&seqno_convopts, sizeof(seqno_convopts));
            from_fieldbuf = (const void *)&seqno;
            from_fieldlen = sizeof(seqno);
            from_blob = NULL;
            from_type = CLIENT_INT;
            from_convopts = &seqno_convopts;
        } else if (from_field_idx == T2T_PLAN_IN_DEFAULT) {
            /* If this is an update then do not overwrite destination schema in
             * this situation. */
            if (flags & CONVERT_UPDATE)
                continue;

            from_fieldbuf = to_field->in_default;
            from_fieldlen = to_field->in_default_len;
            from_type = to_field->in_default_type;
            from_convopts = &to_field->convopts;
        } else if (from_field_idx == T2T_PLAN_NULL) {
            /* If this is an update then do not overwrite destination schema in
             * this situation. */
            if (flags & CONVERT_UPDATE)
                continue;
            from_fieldbuf = NULL;
        } else {
            const struct field *from_field;

            from_field = &plan->from->member[from_field_idx];
            from_fieldlen = from_field->len;
            from_type = from_field->type;
            from_convopts = &from_field->convopts;

            if (from_nulls && btst(from_nulls, from_field_idx)) {
                from_fieldbuf = NULL;
            } else {

                if (from_blobs && from_field->blob_index >= 0) {
                    if (from_field->blob_index >= maxblobs) {
                        if (fail_reason) {
                            memcpy(fail_reason, &plan->fail_reason,
                                   sizeof(fail_reason));
                            fail_reason->target_field_idx = to_field_idx;
                            fail_reason->source_field_idx = from_field_idx;
                            fail_reason->reason =
                                CONVERT_FAILED_BAD_BLOB_PROGRAMMER;
                        }
                        return -1;
                    }
                    from_blob = &from_blobs[from_field->blob_index];
                    /* There is a day one bug/feature here - the blob may be
                     * null,
                     * in which case we bypass the null checks.  One day I will
                     * fix this since schema change does enforce these checks.
                     */
                } else {
                    from_blob = NULL;
                }

                from_fieldbuf = ((const char *)from_buf) + from_field->offset;

                /* For client -> server conversion we have special code to
                 * handle partial input fields.  They must be either string
                 * fields or byte arrays. */
                if (from_field->offset + from_fieldlen > from_len) {
                    exclude_len =
                        (from_field->offset + from_fieldlen) - from_len;
                    logmsg(LOGMSG_ERROR, "1 exclude_len=%d\n", exclude_len);
                }
                if (to_field->len > to_len) {
                    int len = to_field->len - to_len;
                    if (len > exclude_len) {
                        exclude_len = len;
                    }
                }
                if (exclude_len) {
                    if (!CLIENT_TYPE_CAN_BE_PARTIAL(from_field->type)) {
                        if (fail_reason) {
                            memcpy(fail_reason, &plan->fail_reason,
                                   sizeof(fail_reason));
                            fail_reason->target_field_idx = to_field_idx;
                            fail_reason->source_field_idx = from_field_idx;
                            fail_reason->reason =
                                CONVERT_FAILED_INVALID_PARTIAL_TYPE;
                        }
                        return -1;
                    }
                    from_fieldlen -= exclude_len;
                    to_fieldlen -= exclude_len;
                }

                /* If converting from a descending index field then we must
                 * create a
                 * flipped version so that the conversion routines can read it
                 * correctly.  This could happen for example when SQL reads from
                 * an
                 * index. */
                if (from_field->flags & INDEX_DESCEND) {
                    if (!temp_buf) {
                        temp_buf = alloca(from_len);
                    }
                    if (server_sort_off == 1) {
                        temp_buf[0] = from_fieldbuf[0];
                    }
                    xorbufcpy(temp_buf + server_sort_off,
                              from_fieldbuf + server_sort_off,
                              from_fieldlen - server_sort_off);
                    from_fieldbuf = temp_buf;
                }
            }

            /* If the from field data is null then check if the from_field has
             * an
             * out_default set for clients that can't handle nulls.  If it has
             * then use that as the data to copy across but make sure this still
             * gets recorded as null in the nulls bitmap. */
            if (from_fieldbuf && IS_SERVER_TYPE(from_type) &&
                stype_is_null(from_fieldbuf)) {
                from_fieldbuf = from_field->out_default;
                from_fieldlen = from_field->out_default_len;
                from_type = from_field->out_default_type;
                from_blob = NULL;
                if (to_nulls) {
                    bset(to_nulls, to_field_idx);
                }
            }
        }

        /**
         ** Now we know where we're getting the data from, convert/copy
         ** into the to_ field
         **/

        if (!from_fieldbuf) {
            /* Output a NULL */
            if (to_field->flags & NO_NULL) {
                if (fail_reason) {
                    memcpy(fail_reason, &plan->fail_reason,
                           sizeof(fail_reason));
                    fail_reason->target_field_idx = to_field_idx;
                    fail_reason->source_field_idx = from_field_idx;
                    fail_reason->reason =
                        CONVERT_FAILED_NULL_CONSTRAINT_VIOLATION;
                }
                return -1;
            }

            if (IS_CLIENT_TYPE(to_field->type)) {
                bzero(to_bufptr, to_fieldlen);
                if (to_nulls) {
                    bset(to_nulls, to_field_idx);
                }
            } else {
                NULL_to_SERVER(to_bufptr, to_fieldlen, to_field->type);
            }
        } else {
            /* Write a non-null value into the buffer */
            int rc, outdtsz;

            /* Client type fields need timezone information.  Server type fields
             * don't since time in the server format is absolute. */
            if (tzname) {
                if (IS_CLIENT_TYPE(from_type)) {
                    memcpy(&tzopts1, from_convopts, sizeof(*from_convopts));
                    tzopts1.flags |= FLD_CONV_TZONE;
                    from_convopts = (const struct field_conv_opts *)&tzopts1;
                }
                if (IS_CLIENT_TYPE(to_field->type)) {
                    memcpy(&tzopts2, from_convopts, sizeof(*from_convopts));
                    tzopts2.flags |= FLD_CONV_TZONE;
                    from_convopts = (const struct field_conv_opts *)&tzopts2;
                }
            }

            if (IS_CLIENT_TYPE(from_type)) {
                if (IS_CLIENT_TYPE(to_field->type)) {
                    /* client -> client */
                    rc = CLIENT_to_CLIENT(
                        from_fieldbuf, from_fieldlen, from_type, from_convopts,
                        from_blob, to_bufptr, to_fieldlen, to_field->type,
                        &outdtsz, to_convopts, to_blob);
                } else if (exclude_len > 0) {
                    /* partial field client -> server */
                    /* This has special handling primarily for C strings;
                     * without
                     * this the traditional conversion functions fail because
                     * the
                     * C string isn't null terminated. */
                    rc = copy_partial_client_buf(from_fieldbuf, from_fieldlen,
                                                 0, /* it isn't null */
                                                 (char *)to_bufptr);
                } else {
                    /* client -> server */
                    rc = CLIENT_to_SERVER(
                        from_fieldbuf, from_fieldlen, from_type,
                        0 /*not null, we already know this*/, from_convopts,
                        from_blob, to_bufptr, to_fieldlen, to_field->type,
                        0, /*flags are unused */
                        &outdtsz, to_convopts, to_blob);
                }
            } else {
                if (IS_CLIENT_TYPE(to_field->type)) {
                    /* server -> client */
                    int isnull;
                    rc = SERVER_to_CLIENT(
                        from_fieldbuf, from_fieldlen, from_type, from_convopts,
                        from_blob, 0, /* flags are unused */
                        to_bufptr, to_fieldlen, to_field->type, &isnull,
                        &outdtsz, to_convopts, to_blob);
                } else {
                    /* server -> server */
                    rc = SERVER_to_SERVER(
                        from_fieldbuf, from_fieldlen, from_type, from_convopts,
                        from_blob, 0, /* flags are unused */
                        to_bufptr, to_fieldlen, to_field->type,
                        0, /*flags are unused */
                        &outdtsz, to_convopts, to_blob);
                }
            }
            if (rc != 0) {
                if (fail_reason) {
                    memcpy(fail_reason, &plan->fail_reason,
                           sizeof(fail_reason));
                    fail_reason->target_field_idx = to_field_idx;
                    fail_reason->source_field_idx = from_field_idx;
                    fail_reason->reason = CONVERT_FAILED_INCOMPATIBLE_VALUES;
                }
                return rc;
            }
        }

        /* Bitwise invert this field to force it to sort in the opposite
         * direction.  Only server fields have INDEX_DESCEND set. */
        if (to_field->flags & INDEX_DESCEND) {
            if (IS_SERVER_TYPE(to_field->type)) {
                xorbuf(to_bufptr + server_sort_off,
                       to_fieldlen - server_sort_off);
            } else {
                xorbuf(to_bufptr, to_fieldlen);
            }
        }

        to_bufptr += to_field->len;
        to_len -= to_field->len;
        out_len += to_field->len;

        if (to_len <= 0) {
            break;
        }
    }

    return out_len;
}

static int t2t_without_plan(const struct schema *from, const void *from_buf,
                            int from_len, const unsigned char *from_nulls,
                            blob_buffer_t *from_blobs, const struct schema *to,
                            void *to_buf, int to_len, unsigned char *to_nulls,
                            blob_buffer_t *to_blobs, int maxblobs, int flags,
                            const char *tzname,
                            struct convert_failure *fail_reason)
{
    struct t2t_plan *plan;

    plan = alloca(offsetof(struct t2t_plan, fields) +
                  sizeof(struct t2t_field) * to->nmembers);
    populate_t2t_plan(from, to, plan);
    return t2t_with_plan(plan, from_buf, from_len, from_nulls, from_blobs,
                         to_buf, to_len, to_nulls, to_blobs, maxblobs, flags,
                         tzname, fail_reason);
}

/*
 * Detect which blobs appear in this static tag, and set them to 'exists', and
 * the length to 0.  Previously static tag api differentiated between NULL and
 * 0-length blobs via the 'ptr' element in the client-side blob (a NULL ptr
 * signified a NULL blob).  This is a special case which we want to eliminate.
 */
int static_tag_blob_conversion(const char *table, const char *ctag,
                               void *record, blob_buffer_t *blobs,
                               size_t maxblobs)
{
    struct schema *scm;
    struct field *fld;
    blob_buffer_t *blob;
    client_blob_tp *clb;
    int ii;

    /* Return immediately if we don't have both lrl blob-fix options. */
    if (!gbl_disallow_null_blobs || !gbl_force_notnull_static_tag_blobs) {
        return 0;
    }

    /* If we can't find the tag, it is probably a not-yet-added dynamic tag. */
    if (NULL == (scm = find_tag_schema(table, ctag))) {
        return 0;
    }

    /* If this is dynamic the blob descriptor is correct already. */
    if (scm->flags & SCHEMA_DYNAMIC) {
        return 0;
    }

    /* If this is an ondisk then we are in an sql statement.  */
    if (0 == strcmp(ctag, ".ONDISK")) {
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
static int ctag_to_stag_int(const char *table, const char *ctag,
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

    /* Sam's been refactoring again.  May the Lord save us. */
    if (gbl_use_t2t) {
        return t2t_without_plan(from, inbuf, len, innulls, inblobs, to, outbufp,
                                ondisk_lim, NULL, outblobs, maxblobs, flags,
                                tzname, fail_reason);
    }

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
            if (to_field->in_default == NULL ||
                stype_is_null(to_field->in_default)) {
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
                /* if converting FROM a flipped index */
                rc = SERVER_to_SERVER(
                    to_field->in_default, to_field->in_default_len,
                    to_field->in_default_type, NULL, /*convopts*/
                    NULL,                            /*blob*/
                    0, outbuf + to_field->offset, to_field->len, to_field->type,
                    fflags, &outdtsz, NULL, /*convopts*/
                    outblob                 /*blob*/
                    );
                if (rc) {
                    if (fail_reason)
                        fail_reason->reason =
                            CONVERT_FAILED_INCOMPATIBLE_VALUES;
                    return -1;
                }
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
                    strncpy(tzopts.tzname, tzname, sizeof(tzopts.tzname));

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
int vtag_to_ondisk_vermap(struct dbtable *db, uint8_t *rec, int *len, uint8_t ver)
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
        logmsg(LOGMSG_FATAL, "%s:%d %s() %s %d -> %d\n", __FILE__, __LINE__, __func__,
               db->dbname, ver, db->version);
        cheap_stack_trace();
        exit(1);
    }

    if (ver == db->version) {
        goto done;
    }

    /* fix dbstore values */
    to_schema = db->schema;
    if (to_schema == NULL) {
        logmsg(LOGMSG_FATAL, "could not get to_schema for .ONDISK in %s\n", db->dbname);
        exit(1);
    }

    if (db->versmap[ver] == NULL) { // not possible
        logmsg(LOGMSG_FATAL, "vtag_to_ondisk_vermap: db->versmap[%d] should NOT be null\n",
               ver);
        cheap_stack_trace();
        abort();
    }

    if (!db->vers_compat_ondisk[ver]) { /* need to convert buffer */
        /* old version will necessarily be smaller in size.
         * it would make more sense to make a copy of the
         * smaller of the two buffers */
        /* find schema for older version */
        snprintf(ver_tag, sizeof ver_tag, "%s%d", gbl_ondisk_ver, ver);
        from_schema = find_tag_schema(db->dbname, ver_tag);
        if (unlikely(from_schema == NULL)) {
            logmsg(LOGMSG_FATAL, "%s:%d %s() %s %d -> %d\n", __FILE__, __LINE__, __func__,
                   db->dbname, ver, db->version);
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
        rc = stag_to_stag_buf_cachedmap(
            db->versmap[ver], from_schema, to_schema, inbuf, rec,
            CONVERT_NULL_NO_ERROR, &reason, NULL, 0);

        if (rc) {
            char err[1024];
            convert_failure_reason_str(&reason, db->dbname, ver_tag, ".ONDISK",
                                       err, sizeof(err));
            logmsg(LOGMSG_ERROR, "%s: %s -> %s failed: %s\n", __func__, ver_tag,
                    ".ONDISK", err);
            return 0;
        }

        /* This fill the dbstore for all versions up to curr */
        for (int i = 0; i < to_schema->nmembers; ++i) {
            if (db->dbstore[i].ver > ver) {
                unsigned int offset = to_schema->member[i].offset;
                set_dbstore(db, i, &rec[offset]);
            }
        }
    } else {
        // same ordering of fields between ver and ondisk
        // so we can loop from the end until when we hit .ver < ver
        int i = to_schema->nmembers - 1;
        while (i > 0 && db->dbstore[i].ver > ver) {
            unsigned int offset = to_schema->member[i].offset;
            set_dbstore(db, i, &rec[offset]);
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
int vtag_to_ondisk(struct dbtable *db, uint8_t *rec, int *len, uint8_t ver,
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
    bdb_state_type *bdb_state;

    if (!db || !db->instant_schema_change || !rec)
        return 0;

    /* version sanity check */
    if (unlikely(ver == 0)) {
        logmsg(LOGMSG_FATAL, "%s:%d %s() %s %d -> %d\n", __FILE__, __LINE__, __func__,
               db->dbname, ver, db->version);
        cheap_stack_trace();
        exit(1);
    }

    if (ver == db->version) {
        goto done;
    }

    extern int offload_comm_send_upgrade_records(struct dbtable *,
                                                 unsigned long long);

    if (gbl_num_record_upgrades > 0 && genid != 0)
        offload_comm_send_upgrade_records(db, genid);

    if (BDB_ATTR_GET(thedb->bdb_attr, USE_VTAG_ONDISK_VERMAP))
        return vtag_to_ondisk_vermap(db, rec, len, ver);

    /* find schema for older version */
    snprintf(ver_tag, sizeof ver_tag, "%s%d", gbl_ondisk_ver, ver);
    from_schema = find_tag_schema(db->dbname, ver_tag);
    if (unlikely(from_schema == NULL)) {
        logmsg(LOGMSG_FATAL, "%s:%d %s() %s %d -> %d\n", __FILE__, __LINE__, __func__,
               db->dbname, ver, db->version);
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

    // rc = stag_to_stag_buf(db->dbname, ver_tag, from, ".ONDISK", (char *)rec,
    // &reason);
    rc = stag_to_stag_buf_flags(db->dbname, ver_tag, from, db->dbname,
                                ".ONDISK", rec, CONVERT_NULL_NO_ERROR, &reason);
    if (rc) {
        char err[1024];
        convert_failure_reason_str(&reason, db->dbname, ver_tag, ".ONDISK", err,
                                   sizeof(err));
        logmsg(LOGMSG_ERROR, "%s: %s -> %s failed: %s\n", __func__, ver_tag,
                ".ONDISK", err);
        return 0;
    }

    /* fix dbstore values */
    to_schema = db->schema;
    if (to_schema == NULL) {
        logmsg(LOGMSG_FATAL, "could not get to_schema for .ONDISK in %s\n", db->dbname);
        exit(1);
    }
    for (int i = 0; i < to_schema->nmembers; ++i) {
        field = &to_schema->member[i];
        offset = field->offset;
        if (db->dbstore[i].ver > ver) {
            set_dbstore(db, i, &rec[offset]);
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

static int _stag_to_ctag_buf(const char *table, const char *stag,
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
    int ext_tm_offset_adj = 0, add_offset_adj = 0;
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
                        strncpy(tzopts.tzname, tzname, DB_MAX_TZNAMEDB);

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
                    strncpy(tzopts.tzname, tzname, DB_MAX_TZNAMEDB);

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
            if (rc)
                return -1;
        }
    }

    /* if we were given a place to put them, pack the flddtaszs */
    if (pp_flddtsz &&
        !(*pp_flddtsz = flddtasizes_put(&flddtasz, *pp_flddtsz, p_flddtsz_end)))
        return -1;

    return tlen;
}

/*
 * Populate a record buffer with server format nulls or default values.
 * This record does not necessarily satisfy null constraints for the table.
 */
void *create_blank_record(struct dbtable *db, size_t *length)
{
    int nfield;
    const struct schema *schema;
    char *record = malloc(db->lrl);
    if (!record) {
        logmsg(LOGMSG_ERROR, "%s: out of memory for lrl %d\n", __func__, db->lrl);
        return NULL;
    }
    schema = find_tag_schema(db->dbname, ".ONDISK");
    if (!schema) {
        logmsg(LOGMSG_ERROR, "%s: cannot find .ONDISK schema for table %s\n",
                __func__, db->dbname);
        free(record);
        return NULL;
    }
    for (nfield = 0; nfield < schema->nmembers; nfield++) {
        const struct field *field = &schema->member[nfield];
        if (field->offset + field->len > db->lrl) {
            logmsg(LOGMSG_ERROR, "%s: buffer size %u too small for tag %s\n",
                    __func__, db->lrl, schema->tag);
            free(record);
            return NULL;
        }
        /* TODO we go to null here for vutf8 fields because they need to be
         * converted from a SERVER_BCSTR to SERVER_VUTF8 this should really be
         * fixed in a cleaner way */
        if (field->in_default && field->type != CLIENT_VUTF8 &&
            field->type != SERVER_VUTF8) {
            memcpy(record + field->offset, field->in_default, field->len);
        } else {
            set_null(record + field->offset, field->len);
        }
    }
    if (length)
        *length = db->lrl;
    return record;
}

/*
 * Scan a server format record and make sure that all fields validate for
 * null constraints.
 */
int validate_server_record(const void *record, size_t reclen,
                           const struct schema *schema,
                           struct convert_failure *fail_reason)
{
    int nfield;
    const char *crec = record;
    if (fail_reason) {
        bzero(fail_reason, sizeof(*fail_reason));
    }
    for (nfield = 0; nfield < schema->nmembers; nfield++) {
        const struct field *field = &schema->member[nfield];
        if (field->flags & NO_NULL) {
            if (stype_is_null(crec + field->offset)) {
                /* field can't be NULL */
                if (fail_reason) {
                    fail_reason->reason =
                        CONVERT_FAILED_NULL_CONSTRAINT_VIOLATION;
                    fail_reason->source_schema = schema;
                    fail_reason->target_schema = schema;
                    fail_reason->source_field_idx = nfield;
                    fail_reason->target_field_idx = nfield;
                }
                return -1;
            }
        }
    }
    return 0;
}

int stag_to_ctag_buf(const char *table, const char *stag, const char *inbuf,
                     int len, const char *ctag, void *outbufp,
                     unsigned char *outnulls, int flags, uint8_t **pp_flddtsz,
                     const uint8_t *p_flddtsz_end)
{
    return _stag_to_ctag_buf(table, stag, inbuf, len, ctag, outbufp, outnulls,
                             flags, pp_flddtsz, p_flddtsz_end, NULL /*inblobs*/,
                             NULL /*outblobs*/, 0 /*maxblobs*/,
                             NULL /*tzname*/);
}

int stag_to_ctag_buf_tz(const char *table, const char *stag, const char *inbuf,
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
int stag_to_ctag_buf_blobs_tz(const char *table, const char *stag,
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
#if 0
int stag_to_ctag_buf_blobs_tz(const char *table, const char *stag, char *inbuf,
        int len, const char *ctag, void *outbufp, unsigned char *outnulls, 
        int flags, void * flddtsz, int * flddtlen,
        blob_buffer_t *blobs, int maxblobs, const char *tzname)
{
   int rc;
   blob_buffer_t newblobs[MAXBLOBS];
   if(!blobs || maxblobs != MAXBLOBS)
   {
      fprintf(stderr, "%s: with no blobs maxblobs=%d!\n", __func__, maxblobs);
      return -1;
   }
   bzero(newblobs, sizeof(newblobs));
    rc = _stag_to_ctag_buf(table, stag, inbuf, len, ctag, outbufp, outnulls, 
            flags, pp_flddtsz, p_flddtsz_end, blobs, newblobs, maxblobs, 
            tzname);
   free_blob_buffers(blobs, maxblobs);
   if(rc < 0)
      free_blob_buffers(newblobs, MAXBLOBS);
   else
      memcpy(blobs, newblobs, sizeof(newblobs));
   return rc;
}
#endif

struct dbrecord *allocate_db_record(const char *table, const char *tag)
{
    struct schema *s = find_tag_schema(table, tag);
    if (s == NULL)
        return NULL;

    struct dbrecord *db = calloc(1, sizeof(struct dbrecord));
    db->bufsize = get_size_of_schema(s);
    db->table = strdup(table);
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

void *stag_to_ctag(const char *table, const char *stag, const char *inbuf,
                   const char *ctag, unsigned char *outnulls, int flags)
{
    struct schema *from, *to;
    char *outbuf;
    int rc;

    from = find_tag_schema(table, ctag);
    if (from == NULL)
        return NULL;
    to = find_tag_schema(table, stag);
    if (to == NULL)
        return NULL;
    outbuf = malloc(get_size_of_schema(to));
    rc = stag_to_ctag_buf(table, stag, inbuf, -1, ctag, outbuf, outnulls, flags,
                          NULL, NULL);
    if (rc != -1)
        return outbuf;

    free(outbuf);
    return NULL;
}

/* forward */
static int _stag_to_stag_buf_flags_blobs(
    struct schema *fromsch, const char *fromtag, const char *inbuf,
    const char *totable, const char *totag, char *outbuf, int flags,
    struct convert_failure *fail_reason, blob_buffer_t *inblobs,
    blob_buffer_t *outblobs, int maxblobs, const char *tzname);

int stag_to_stag_buf_blobs(const char *table, const char *fromtag,
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

    struct schema *fromsch = find_tag_schema(table, fromtag);
    rc = _stag_to_stag_buf_flags_blobs(fromsch, fromtag, inbuf, table, totag,
                                       outbuf, 0 /*flags*/, reason, blobs,
                                       p_newblobs, maxblobs, NULL /*tzname*/);

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

int stag_to_stag_buf(const char *table, const char *fromtag, const char *inbuf,
                     const char *totag, char *outbuf,
                     struct convert_failure *reason)
{
    return stag_to_stag_buf_flags(table, fromtag, inbuf, table, totag, outbuf,
                                  0, reason);
}

int stag_to_stag_buf_tz(struct schema *fromsch, const char *table,
                        const char *fromtag, const char *inbuf,
                        const char *totag, char *outbuf,
                        struct convert_failure *reason, const char *tzname)
{
    return _stag_to_stag_buf_flags_blobs(
        fromsch, fromtag, inbuf, table, totag, outbuf, 0 /*flags*/, reason,
        NULL /*inblobs*/, NULL /*outblobs*/, 0 /*maxblobs*/, tzname);
}

int stag_to_stag_buf_update(const char *table, const char *fromtag,
                            const char *inbuf, const char *totag, char *outbuf,
                            struct convert_failure *reason)
{
    return stag_to_stag_buf_flags(table, fromtag, inbuf, table, totag, outbuf,
                                  CONVERT_UPDATE, reason);
}

int stag_to_stag_buf_update_tz(const char *table, const char *fromtag,
                               const char *inbuf, const char *totag,
                               char *outbuf, struct convert_failure *reason,
                               const char *tzname)
{
    struct schema *fromsch = find_tag_schema(table, fromtag);
    return _stag_to_stag_buf_flags_blobs(
        fromsch, fromtag, inbuf, table, totag, outbuf, CONVERT_UPDATE, reason,
        NULL /*inblobs*/, NULL /*outblobs*/, 0 /*maxblobs*/, tzname);
}

/*
 * Given an update columns structure, create an equivalent one for the incoming
 * tag used to create a valid updcols structure for schema change.  If a column
 * was updated, outcols will be updated to contain the corresponding input-
 * column number.  A value of -1 in outcols signifies that the column was not
 * changed.
 */
int remap_update_columns(const char *table, const char *intag,
                         const int *incols, const char *outtag, int *outcols)
{
    struct schema *insc, *outsc;
    int field, i, idx;

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
int describe_update_columns(const char *table, const char *tag, int *updCols)
{
    struct schema *ondisk, *chk;
    int field, i;
    int same_tag = 0;

    ondisk = find_tag_schema(table, ".ONDISK");
    if (ondisk == NULL) {
        return -1;
    }

    chk = find_tag_schema(table, tag);
    if (chk == NULL) {
        return -2;
    }

    if (strcmp(tag, ".ONDISK") == 0)
        same_tag = 1;

    updCols[0] = ondisk->nmembers;

    for (i = 0; i < ondisk->nmembers; i++) {
        updCols[i + 1] = -1;
    }

    for (i = 0; i < chk->nmembers; i++) {
        struct field *chk_fld = &chk->member[i];
        int idx;
        if (same_tag) {
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

int indexes_expressions_data(struct schema *sc, const char *inbuf, char *outbuf,
                             blob_buffer_t *blobs, size_t maxblobs,
                             struct field *f,
                             struct convert_failure *fail_reason,
                             const char *tzname);
static int stag_to_stag_field(const char *inbuf, char *outbuf, int flags,
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
    char *exprdta = NULL;

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
            rc = indexes_expressions_data(fromsch, inbuf, outbuf, inblobs,
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
            if (fail_reason)
                fail_reason->source_field_idx = field_idx;
            rc = SERVER_to_SERVER(
                to_field->in_default, to_field->in_default_len,
                to_field->in_default_type, NULL /*convopts*/, NULL /*blob*/, 0,
                outbuf + to_field->offset, to_field->len, to_field->type,
                oflags, &outdtsz, &to_field->convopts, outblob /*blob*/
                );
            if (rc) {
                if (fail_reason)
                    fail_reason->reason = CONVERT_FAILED_INCOMPATIBLE_VALUES;
                return -1;
            }
            if (to_field->flags & INDEX_DESCEND)
                xorbuf(((char *)outbuf) + to_field->offset + rec_srt_off,
                       to_field->len - rec_srt_off);
        }
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
        strncpy(tzopts.tzname, tzname, sizeof(tzopts.tzname));

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

/*
 * On success only outblobs will be valid, there is no need to free up inblobs.
 * On failure the caller should free inblobs and outblobs.
 */
static int _stag_to_stag_buf_flags_blobs(
    struct schema *fromsch, const char *fromtag, const char *inbuf,
    const char *totable, const char *totag, char *outbuf, int flags,
    struct convert_failure *fail_reason, blob_buffer_t *inblobs,
    blob_buffer_t *outblobs, int maxblobs, const char *tzname)
{
    struct schema *tosch;
    int same_tag = 0;

    if (fail_reason)
        init_convert_failure_reason(fail_reason);

    if (fromsch == NULL) {
        if (fail_reason)
            fail_reason->reason = CONVERT_FAILED_INVALID_INPUT_TAG;
        return -1;
    }
    if (fail_reason)
        fail_reason->source_schema = fromsch;

    tosch = find_tag_schema(totable, totag);
    if (tosch == NULL) {
        if (fail_reason)
            fail_reason->reason = CONVERT_FAILED_INVALID_OUTPUT_TAG;
        return -1;
    }
    if (fail_reason)
        fail_reason->target_schema = tosch;

    if (strcmp(fromtag, totag) == 0)
        same_tag = 1;

    for (int field = 0; field < tosch->nmembers; field++) {
        int field_idx;

        if (same_tag) {
            field_idx = field;
        } else {
            field_idx =
                find_field_idx_in_tag(fromsch, tosch->member[field].name);
        }
        int rc = stag_to_stag_field(inbuf, outbuf, flags, fail_reason, inblobs,
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
int stag_to_stag_buf_cachedmap(int tagmap[], struct schema *from,
                               struct schema *to, const char *inbuf,
                               char *outbuf, int flags,
                               struct convert_failure *fail_reason,
                               blob_buffer_t *inblobs, int maxblobs)
{
    int same_tag = 0;
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
            fprintf(stderr, "stag_to_stag_buf_blobs with maxblobs=%d!\n",
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
        rc = stag_to_stag_field(inbuf, outbuf, flags, fail_reason, inblobs,
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

int stag_to_stag_buf_flags(const char *table, const char *fromtag,
                           const char *inbuf, const char *totable,
                           const char *totag, char *outbuf, int flags,
                           struct convert_failure *fail_reason)
{
    struct schema *fromsch = find_tag_schema(table, fromtag);
    return _stag_to_stag_buf_flags_blobs(
        fromsch, fromtag, inbuf, totable, totag, outbuf, flags, fail_reason,
        NULL /*inblobs*/, NULL /*outblobs*/, 0 /*maxblobs*/, NULL);
}

/*
 * Convert an ondisk format key for one table into the equivalent ondisk format
 * key in the other table.  This is used for foreign key constraint checking.
 * The number of columns in the two key tags should be the same.  We ignore
 * column names here - the 1st column in fromtag maps to the 1st column in
 * totag and so on.
 */
int stag_to_stag_buf_ckey(const char *table, const char *fromtag,
                          const char *inbuf, const char *totable,
                          const char *totag, char *outbuf, int *nulls,
                          enum constraint_dir direction)
{
    struct schema *from, *to;
    int field;
    struct field *from_field, *to_field;
    int field_idx;
    int rc;
    int iflags, oflags;
    int rec_srt_off = 1;
    int nmembers;
    int ixlen;

    if (gbl_sort_nulls_correctly)
        rec_srt_off = 0;

    from = find_tag_schema(table, fromtag);
    if (from == NULL)
        return -1;

    to = find_tag_schema(totable, totag);
    if (to == NULL)
        return -1;

    nmembers = to->nmembers;

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

    for (field = 0; field < nmembers; field++) {
        int outdtsz = 0;
        to_field = &to->member[field];
        field_idx = field; /* find_field_idx_in_tag(from, to_field->name);*/
        from_field = &from->member[field_idx];

        if (nulls && stype_is_null(inbuf + from_field->offset)) {
            *nulls = 1;
        }

        if ((to->flags & SCHEMA_INDEX) && (to_field->flags & INDEX_DESCEND))
            oflags = INDEX_DESCEND;
        else
            oflags = 0;
        if ((from->flags & SCHEMA_INDEX) && (from_field->flags & INDEX_DESCEND))
            iflags = INDEX_DESCEND;
        else
            iflags = 0;
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
    struct dbtable *db;

    rc = 0;

    lock_taglock();
    {
        struct dbtag *tag;
        struct cmp_tag_struct info;
        bzero(&info, sizeof(info));
        tag = hash_find_readonly(tags, &table);
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

int compare_tag(const char *table, const char *tag, FILE *out)
{
    struct dbtable *db;
    struct schema *old, *new;
    struct field *fnew, *fold;
    int fidx;
    char oldtag[MAXTAGLEN + 16];
    char newtag[MAXTAGLEN + 16];

    snprintf(oldtag, sizeof(oldtag), "%s", tag);
    snprintf(newtag, sizeof(newtag), ".NEW.%s", tag);

    db = get_dbtable_by_name(table);
    if (db == NULL) {
        logmsg(LOGMSG_ERROR, "Invalid table %s\n", table);
        return -1;
    }
    old = find_tag_schema(table, oldtag);
    if (old == NULL) {
        logmsg(LOGMSG_ERROR, "Can't find schema for old table %s\n", table);
        return -1;
    }
    new = find_tag_schema(table, newtag);
    if (new == NULL) {
        logmsg(LOGMSG_ERROR, "Can't find schema for new table %s\n", table);
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
 * SC_BAD_NEW_FIELD: New field missing dbstore or null
 * SC_COLUMN_ADDED: If new column is added
 * SC_DBSTORE_CHANGE: Only change is dbstore of an existing field */
int compare_tag_int(struct schema *old, struct schema *new, FILE *out,
                    int strict)
{
    struct field *fnew, *fold;
    int rc = SC_NO_CHANGE;
    int change = SC_NO_CHANGE;
    int oidx, nidx;

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
                    if (fnew->len < fold->len) {
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
                logmsg(LOGMSG_USER,
                       "tag %s field %d (named %s) has changed (%s)\n",
                       old->tag, oidx, fold->name, buf);
            }
            return change;
        }

        if (change == SC_COLUMN_ADDED || change == SC_DBSTORE_CHANGE) {
            if (out) {
                logmsg(LOGMSG_USER,
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
        char buf[256] = "";
        int oldflags, newflags;
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
            if (SERVER_VUTF8 == fnew->type &&
                fnew->in_default_len >= (fnew->len - 5)) {
                rc = SC_TAG_CHANGE;
                if (out) {
                    logmsg(LOGMSG_USER, "tag %s has new field %d (named %s -- dbstore "
                                 "too large forcing rebuild)\n",
                            old->tag, nidx, fnew->name);
                }
            } else if (fnew->in_default || allow_null) {
                rc = SC_COLUMN_ADDED;
                if (out) {
                    logmsg(LOGMSG_USER, "tag %s has new field %d (named %s)\n",
                            old->tag, nidx, fnew->name);
                }
            } else {
                if (out) {
                    logmsg(LOGMSG_USER, "tag %s has new field %d (named %s) without "
                            "dbstore or null\n",
                            old->tag, nidx, fnew->name);
                }
                rc = SC_BAD_NEW_FIELD;
            }
        }
    }

    /* Cannot do instant schema change if recsize does not
     * atleast increase by 2 bytes */
    if (rc == SC_COLUMN_ADDED) {
        if (new->recsize < old->recsize + 2) {
            if (out) {
                logmsg(LOGMSG_USER, "tag %s recsize %d, was %d\n", old->tag,
                        new->recsize, old->recsize);
            }
            rc = SC_TAG_CHANGE;
        }
    }
    return rc;
}

int has_index_changed(struct dbtable *db, char *keynm, int ct_check, int newkey,
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
    const char *table = NULL;

    snprintf(oldtag, sizeof(oldtag), "%s", tag);
    snprintf(newtag, sizeof(newtag), ".NEW.%s", tag);

    table = db->dbname;

    if (table == NULL) {
        logmsg(LOGMSG_ERROR, "Invalid table\n");
        return -1;
    }
    rc = getidxnumbyname(table, keynm, &ix);
    if (rc != 0) {
        logmsg(LOGMSG_USER, "No index %s in old schema\n", keynm);
        return 1;
    }
    snprintf(ixbuf, sizeof(ixbuf), ".NEW.%s", keynm);
    rc = getidxnumbyname(table, ixbuf, &fidx);
    if (rc != 0) {
        logmsg(LOGMSG_USER, "No index %s in new schema\n", keynm);
        return 1;
    }
    if (fidx != ix) {
        logmsg(LOGMSG_USER, "key %s maps from ix%d->ix%d\n", keynm, ix, fidx);
        return 1;
    }

    /*fprintf(stderr, "key %s ix %d\n", keynm, ix);*/

    snprintf(ixbuf, sizeof(ixbuf), "%s_ix_%d", tag, ix);
    old = find_tag_schema(table, ixbuf);
    if (old == NULL) {
        logmsg(LOGMSG_ERROR, "Can't find schema for old table %s index %d\n", table, ix);
        return 1;
    }
    snprintf(ixbuf, sizeof(ixbuf), ".NEW.%s_ix_%d", tag, fidx);
    new = find_tag_schema(table, ixbuf);
    if (new == NULL) {
        logmsg(LOGMSG_ERROR, "Can't find schema for new table %s index %d\n", table, fidx);
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
    oldattr = oldix->flags & (SCHEMA_DUP | SCHEMA_RECNUM | SCHEMA_DATACOPY);
    newattr = newix->flags & (SCHEMA_DUP | SCHEMA_RECNUM | SCHEMA_DATACOPY);
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
    }
    return 0;
}

/* See if anything about the indexes has changed which would necessitate a
 * rebuild. */
int compare_indexes(const char *table, FILE *out)
{
    struct dbtable *db;
    struct schema *old, *new;
    struct field *fnew, *fold;
    int fidx;
    int ix;
    char ixbuf[MAXTAGLEN * 2]; /* .NEW..ONDISK_ix_xxxx */
    char oldtag[MAXTAGLEN + 16];
    char newtag[MAXTAGLEN + 16];
    const char *tag = ".ONDISK";

    snprintf(oldtag, sizeof(oldtag), "%s", tag);
    snprintf(newtag, sizeof(newtag), ".NEW.%s", tag);

    db = get_dbtable_by_name(table);
    if (db == NULL) {
        logmsg(LOGMSG_ERROR, "Invalid table %s\n", table);
        return 0;
    }
    old = find_tag_schema(table, oldtag);
    if (old == NULL) {
        logmsg(LOGMSG_ERROR, "Can't find schema for old table %s\n", table);
        return 0;
    }
    new = find_tag_schema(table, newtag);
    if (new == NULL) {
        logmsg(LOGMSG_ERROR, "Can't find schema for new table %s\n", table);
        return 0;
    }

    /* if number of indices changed, then ondisk operations are required */
    if (old->nix != new->nix) {
        if (out) {
            logmsg(LOGMSG_USER, "indexes have been added or removed\n");
        }
        return 1;
    }

    /* go through indices */
    for (ix = 0; ix < db->nix; ix++) {
        char descr[128];
        snprintf(ixbuf, sizeof(ixbuf), "%s_ix_%d", tag, ix);
        old = find_tag_schema(table, ixbuf);
        if (old == NULL) {
            logmsg(LOGMSG_ERROR, "Can't find schema for old table %s index %d\n", table, ix);
            return 0;
        }
        snprintf(ixbuf, sizeof(ixbuf), ".NEW.%s_ix_%d", tag, ix);
        new = find_tag_schema(table, ixbuf);
        if (new == NULL) {
            logmsg(LOGMSG_ERROR, "Can't find schema for new table %s index %d\n", table, ix);
            return 0;
        }

        if (cmp_index_int(old, new, descr, sizeof(descr)) != 0) {
            if (out)
                logmsg(LOGMSG_USER, "index %d (%s) changed: %s\n", ix + 1, old->csctag,
                        descr);
            return 1;
        }
    }

    return 0;
}

void hexdumpdta(unsigned char *p, int len)
{
    for (int i = 0; i < len; i++)
        logmsg(LOGMSG_USER, "%02x", p[i]);
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
    int rc;

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
            rc = SERVER_UINT_to_CLIENT_UINT(
                buf + f->offset, flen, &opts /*convopts*/, NULL /*blob*/,
                &uival, 8, &null, &outdtsz, &opts /*convopts*/, NULL /*blob*/);
            if (null)
                logmsg(LOGMSG_USER, "%s=NULL", f->name);
            else
                logmsg(LOGMSG_USER, "%s=%llu", f->name, uival);
            break;
        case SERVER_BINT:
            rc = SERVER_BINT_to_CLIENT_INT(
                buf + f->offset, flen, &opts /*convopts*/, NULL /*blob*/, &ival,
                8, &null, &outdtsz, &opts /*convopts*/, NULL /*blob*/);
            if (null)
                logmsg(LOGMSG_USER, "%s=NULL", f->name);
            else
                logmsg(LOGMSG_USER, "%s=%lld", f->name, ival);
            break;
        case SERVER_BREAL:
            rc = SERVER_BREAL_to_CLIENT_REAL(
                buf + f->offset, flen, &opts /*convopts*/, NULL /*blob*/, &dval,
                8, &null, &outdtsz, &opts /*convopts*/, NULL /*blob*/);
            if (null)
                logmsg(LOGMSG_USER, "%s=NULL", f->name);
            else
                logmsg(LOGMSG_USER, "%s=%f", f->name, dval);
            break;
        case SERVER_BCSTR:
            sval = malloc(flen + 1);
            rc = SERVER_BCSTR_to_CLIENT_CSTR(
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
            rc = SERVER_BYTEARRAY_to_CLIENT_BYTEARRAY(
                buf + f->offset, flen, &opts /*convopts*/, NULL /*blob*/, sval,
                flen, &null, &outdtsz, &opts /*convopts*/, NULL /*blob*/);
            if (null)
                logmsg(LOGMSG_USER, "%s=NULL", f->name);
            else {
                logmsg(LOGMSG_USER, "%s=", f->name);
                hexdumpdta((void *)sval, flen);
            }
            free(sval);

            break;
        }

        if (field != sc->nmembers - 1)
            logmsg(LOGMSG_USER, " ");
    }
    logmsg(LOGMSG_USER, "] ");
}

/* Setup the dbload/dbstore values for a field.  Only do this for the
 * ondisk tag.  The in/out default values are stored in server format
 * but with the same type as the dbload/dbstore value uses in the csc2
 * (why? - why not just convert it here so later on we can memcpy?)
 */
static int init_default_value(struct field *fld, int fldn, int loadstore)
{
    const char *name;
    void **p_default;
    int *p_default_len;
    int *p_default_type;
    int mastersz;
    void *typebuf = NULL;
    int opttype = 0;
    int optsz;
    int rc, outrc = 0;

    switch (loadstore) {
    case FLDOPT_DBSTORE:
        name = "dbstore";
        p_default = &fld->in_default;
        p_default_len = &fld->in_default_len;
        p_default_type = &fld->in_default_type;
        break;

    case FLDOPT_DBLOAD:
        name = "dbload";
        p_default = &fld->out_default;
        p_default_len = &fld->out_default_len;
        p_default_type = &fld->out_default_type;
        break;

    default:
        logmsg(LOGMSG_FATAL, "init_default_value: loadstore=%d?!\n", loadstore);
        exit(1);
    }

    if (fld->type == SERVER_VUTF8) {
        mastersz = 16 * 1024;
    } else if (fld->type == SERVER_DATETIME || fld->type == SERVER_DATETIMEUS) {
        mastersz =
            CLIENT_DATETIME_EXT_LEN; /* We want to get back cstring here. */
    } else {
        mastersz = max_type_size(fld->type, fld->len);
    }

    if (mastersz > 0)
        typebuf = calloc(1, mastersz);
    rc = dyns_get_table_field_option(".ONDISK", fldn, loadstore, &opttype,
                                     &optsz, typebuf, mastersz);

    *p_default = NULL;
    *p_default_len = 0;

    if (rc == -1 && opttype != CLIENT_MINTYPE) {
        /* csc2 doesn't like its default value data, or our buffer is too small
         */
        logmsg(LOGMSG_ERROR, "init_default_value: %s for %s is invalid\n", name,
                fld->name);
        outrc = -1;
    } else if (rc == 0) {
        int outdtsz;
        int is_null = 0;

        /* if we are dealing with a default for vutf8 we must be sure to save it
         * a
         * as a cstring, we don't want to have to hold on to a "default blob" */
        if (opttype == CLIENT_VUTF8) {
            opttype = CLIENT_CSTR;
            *p_default_len = optsz + 1 /* csc2lib doesn't count NUL byte*/;
        } else
            *p_default_len = fld->len;

        *p_default_type = client_type_to_server_type(opttype);
        *p_default = calloc(1, *p_default_len);

        if (opttype == CLIENT_DATETIME || opttype == CLIENT_DATETIMEUS) {
            opttype = CLIENT_CSTR;
            if (strncasecmp(typebuf, "CURRENT_TIMESTAMP", 17) == 0)
                is_null = 1; /* use isnull flag for current timestamp since
                                null=yes is used for dbstore null */
        }
        if (*p_default == NULL) {
            logmsg(LOGMSG_ERROR, "init_default_value: out of memory\n");
            outrc = -1;
        } else {
            /* csc2lib doesn't count the \0 in the size for cstrings - but type
             * system does and will balk if no \0 is found. */
            if (opttype == CLIENT_CSTR)
                optsz++;
            rc = CLIENT_to_SERVER(typebuf, optsz, opttype, is_null /*isnull*/,
                                  NULL /*convopts*/, NULL /*blob*/, *p_default,
                                  *p_default_len, *p_default_type, 0, &outdtsz,
                                  &fld->convopts, NULL /*blob*/);
            if (rc == -1) {
                logmsg(LOGMSG_ERROR, "%s initialisation failed for field %s\n", name,
                        fld->name);
                free(*p_default);
                *p_default = NULL;
                *p_default_len = 0;
                *p_default_type = 0;
                outrc = -1;
            }
            /*
            else
            {
               printf("%s opt for %s is:\n", name, fld->name);
               fsnapf(stdout, *p_default, outdtsz);
            }
            */
        }
    }

    if (typebuf)
        free(typebuf);

    return outrc;
}

/* have a cmacc parsed and sanity-checked csc schema.
   convert to sql statement, execute, save schema in db
   structure and sanity check again (sqlite schema must
   match cmacc).  libcmacc2 puts results into globals.
   process them here after each .csc file is read
 */
static int add_cmacc_stmt_int(struct dbtable *db, int alt, int side_effects)
{
    /* loaded from csc2 at this point */
    int field;
    int nfields;
    int rc;
    int flags;
    int piece, npieces;
    struct schema *schema;
    struct field *m;
    char buf[MAXCOLNAME + 1] = {0}; /* scratch space buffer */
    int offset;
    int ntags;
    int itag;
    char *tag = NULL;
    int isnull;
    int is_disk_schema = 0;
    char tmptagname[MAXTAGLEN] = {0};
    char *rtag = NULL;
    int have_comdb2_seqno_field;

    /* save cmacc structures into db structs */

    /* table */
    ntags = dyns_get_table_count();
    for (itag = 0; itag < ntags; itag++) {
        have_comdb2_seqno_field = 0;
        if (rtag) {
            free(rtag);
            rtag = NULL;
        }

        if (alt == 0) {
            tag = strdup(dyns_get_table_tag(itag));
            rtag = strdup(tag);
        } else {
            rtag = strdup(dyns_get_table_tag(itag));
            snprintf(tmptagname, sizeof(tmptagname), ".NEW.%s", rtag);
            tag = strdup(tmptagname);
        }
        schema = calloc(1, sizeof(struct schema));
        schema->tag = tag;

        add_tag_schema(db->dbname, schema);

        schema->nmembers = dyns_get_table_field_count(rtag);
        if ((gbl_morecolumns && schema->nmembers > MAXCOLUMNS) ||
            (!gbl_morecolumns && schema->nmembers > MAXDYNTAGCOLUMNS)) {
            logmsg(LOGMSG_ERROR, "%s: tag '%s' has too many columns %d (max soft "
                            "limit %d, hard limit %d)\n",
                    __func__, tag, schema->nmembers, gbl_max_columns_soft_limit,
                    MAXCOLUMNS);
            return -1;
        }
        schema->member = calloc(schema->nmembers, sizeof(struct field));
        schema->flags = SCHEMA_TABLE;
        schema->recsize = dyns_get_table_tag_size(rtag); /* wrong for .ONDISK */
        /* we generate these later */
        schema->ix = NULL;
        schema->nix = 0;
        offset = 0;
        is_disk_schema = 0;
        if (strncasecmp(rtag, ".ONDISK", 7) == 0) {
            is_disk_schema = 1;
        } else {
            is_disk_schema = 0;
        }

        for (field = 0; field < schema->nmembers; field++) {
            int outdtsz = 0;
            int padval;
            int type;
            int sz;

            dyns_get_table_field_info(
                rtag, field, buf, sizeof(buf),
                (int *)&schema->member[field].type,
                (int *)&schema->member[field].offset, NULL,
                (int *)&schema->member[field].len, /* want fullsize, not size */
                NULL, strncasecmp(rtag, ".ONDISK", 7) == 0);
            schema->member[field].idx = -1;
            schema->member[field].name = strdup(buf);
            schema->member[field].in_default = NULL;
            schema->member[field].in_default_len = 0;
            schema->member[field].in_default_type = 0;
            schema->member[field].flags |= NO_NULL;

            extern int gbl_forbid_ulonglong;
            if (gbl_forbid_ulonglong &&
                schema->member[field].type == CLIENT_UINT &&
                schema->member[field].len == sizeof(unsigned long long) &&
                strncasecmp(db->dbname, gbl_ver_temp_table,
                            strlen(gbl_ver_temp_table)) != 0) {
                logmsg(LOGMSG_ERROR, 
                        "Error in table %s: u_longlong is unsupported\n",
                        db->dbname);
                return -1;
            }

            /* count the blobs */
            if (schema->member[field].type == CLIENT_BLOB ||
                schema->member[field].type == CLIENT_VUTF8 ||
                schema->member[field].type == CLIENT_BLOB2 ||
                schema->member[field].type == SERVER_BLOB ||
                schema->member[field].type == SERVER_BLOB2 ||
                schema->member[field].type == SERVER_VUTF8) {
                schema->member[field].blob_index = schema->numblobs;
                schema->numblobs++;
            } else {
                schema->member[field].blob_index = -1;
            }

            /* for special on-disk schema, change types to be
               special on-disk types */
            if (is_disk_schema) {
                if (strcasecmp(schema->member[field].name, "comdb2_seqno") == 0)
                    have_comdb2_seqno_field = 1;

                /* cheat: change type to be ondisk type */
                switch (schema->member[field].type) {
                case SERVER_BLOB:
                    /* TODO use the enums that are used in types.c */
                    /* blobs are stored as flag byte + 32 bit length */
                    schema->member[field].len = 5;
                    break;
                case SERVER_VUTF8:
                case SERVER_BLOB2:
                    /* TODO use the enums that are used in types.c */
                    /* vutf8s are stored as flag byte + 32 bit length, plus
                     * optionally strings up to a certain length can be stored
                     * in the record itself */
                    if (schema->member[field].len == -1)
                        schema->member[field].len = 5;
                    else
                        schema->member[field].len += 5;
                    break;
                case SERVER_BCSTR: {
                    int clnt_type = 0;
                    int clnt_offset = 0;
                    int clnt_len = 0;

                    dyns_get_table_field_info(rtag, field, buf, sizeof(buf),
                                              &clnt_type, &clnt_offset, NULL,
                                              &clnt_len, NULL, 0);

                    if (clnt_type == CLIENT_PSTR || clnt_type == CLIENT_PSTR2) {
                        schema->member[field].len++;
                    } else {
                        /* no change needed from client length */
                    }
                } break;
                case SERVER_DATETIME:
                    /* server CLIENT_DATETIME is a server_datetime_t */
                    schema->member[field].len = sizeof(server_datetime_t);
                    break;
                case SERVER_DATETIMEUS:
                    /* server CLIENT_DATETIMEUS is a server_datetimeus_t */
                    schema->member[field].len = sizeof(server_datetimeus_t);
                    break;
                case SERVER_INTVYM:
                    /* server CLIENT_INTVYM is a server_intv_ym_t */
                    schema->member[field].len = sizeof(server_intv_ym_t);
                    break;
                case SERVER_INTVDS:
                    /* server CLIENT_INTVDS is a server_intv_ds_t */
                    schema->member[field].len = sizeof(server_intv_ds_t);
                    break;
                case SERVER_INTVDSUS:
                    /* server CLIENT_INTVDSUS is a server_intv_dsus_t */
                    schema->member[field].len = sizeof(server_intv_dsus_t);
                    break;
                case SERVER_DECIMAL:
                    switch (schema->member[field].len) {
                    case 14:
                        schema->member[field].len = sizeof(server_decimal32_t);
                        break;
                    case 24:
                        schema->member[field].len = sizeof(server_decimal64_t);
                        break;
                    case 43:
                        schema->member[field].len = sizeof(server_decimal128_t);
                        break;
                    default:
                        abort();
                    }
                    break;
                default:
                    /* other types just add one for the flag byte */
                    schema->member[field].len++;
                    break;
                }
                schema->member[field].offset = offset;
                offset += schema->member[field].len;

                /* correct recsize */
                if (field == schema->nmembers - 1) {
                    /* we are on the last field */
                    schema->recsize = offset;
                }
#if 0
              schema->member[field].type = 
                  client_type_to_server_type(schema->member[field].type);
#endif
                if (side_effects) {
                    db->schema = schema;
                    if (db->ixschema)
                        free(db->ixschema);
                    db->ixschema = calloc(sizeof(struct schema *), db->nix);

                    db->do_local_replication = have_comdb2_seqno_field;
                }
            }

            if (strcmp(rtag, ".ONDISK") == 0) {
                /* field allowed to be null? */
                rc = dyns_get_table_field_option(
                    rtag, field, FLDOPT_NULL, &type, &sz, &isnull, sizeof(int));
                if (rc == 0 && isnull)
                    schema->member[field].flags &= ~NO_NULL;

                /* dbpad value (used for byte array conversions).  If it is
                 * present
                 * then type must be integer. */
                rc = dyns_get_table_field_option(rtag, field, FLDOPT_PADDING,
                                                 &type, &sz, &padval,
                                                 sizeof(padval));
                if (rc == 0 && CLIENT_INT == type) {
                    schema->member[field].convopts.flags |= FLD_CONV_DBPAD;
                    schema->member[field].convopts.dbpad = padval;
                }

                /* input default */
                rc = init_default_value(&schema->member[field], field,
                                        FLDOPT_DBSTORE);
                if (rc != 0) {
                    if (rtag)
                        free(rtag);
                    return -1;
                }

                /* output default  */
                rc = init_default_value(&schema->member[field], field,
                                        FLDOPT_DBLOAD);
                if (rc != 0) {
                    if (rtag)
                        free(rtag);
                    return -1;
                }
            }
        }
        if (create_key_schema(db, schema, alt) > 0)
            return -1;
        if (is_disk_schema) {
            int i, rc;
            /* csc2 doesn't have the correct recsize for ondisk schema - use
             * our value instead. */
            schema->recsize = offset;
            if (!alt) {
                if (side_effects)
                    db->lrl = get_size_of_schema_by_name(db->dbname, ".ONDISK");
                rc = clone_server_to_client_tag(db->dbname, ".ONDISK",
                                                ".ONDISK_CLIENT");
            } else {
                if (side_effects)
                    db->lrl =
                        get_size_of_schema_by_name(db->dbname, ".NEW..ONDISK");
                rc = clone_server_to_client_tag(db->dbname, ".NEW..ONDISK",
                                                ".NEW..ONDISK_CLIENT");
            }

            if (side_effects) {
                char sname_buf[MAXTAGLEN], cname_buf[MAXTAGLEN];
                db->nix = dyns_get_idx_count();
                for (i = 0; i < db->nix; i++) {
                    if (!alt)
                        snprintf(tmptagname, sizeof(tmptagname),
                                 ".ONDISK_ix_%d", i);
                    else
                        snprintf(tmptagname, sizeof(tmptagname),
                                 ".NEW..ONDISK_ix_%d", i);
                    db->ix_keylen[i] =
                        get_size_of_schema_by_name(db->dbname, tmptagname);

                    if (!alt) {
                        snprintf(sname_buf, sizeof(sname_buf), ".ONDISK_IX_%d",
                                 i);
                        snprintf(cname_buf, sizeof(cname_buf),
                                 ".ONDISK_CLIENT_IX_%d", i);
                    } else {
                        snprintf(sname_buf, sizeof(sname_buf),
                                 ".NEW..ONDISK_IX_%d", i);
                        snprintf(cname_buf, sizeof(cname_buf),
                                 ".NEW..ONDISK_CLIENT_IX_%d", i);
                    }

                    clone_server_to_client_tag(db->dbname, sname_buf,
                                               cname_buf);
                }

                db->numblobs = schema->numblobs;
            }
        }
    }
    if (rtag)
        free(rtag);
    return 0;
}

int add_cmacc_stmt(struct dbtable *db, int alt)
{
    return add_cmacc_stmt_int(db, alt, 1);
}
int add_cmacc_stmt_no_side_effects(struct dbtable *db, int alt)
{
    return add_cmacc_stmt_int(db, alt, 0);
}

/* this routine is called from comdb2 when all is well
   (we checked that all schemas are defined, etc.) */
void fix_lrl_ixlen_tran(tran_type *tran)
{
    int tbl, ix;
    char namebuf[MAXTAGLEN + 1];
    struct dbtable *db;
    int nix;
    int ver;
    int rc;

    for (tbl = 0; tbl < thedb->num_dbs; tbl++) {
        db = thedb->dbs[tbl];
        struct schema *s = find_tag_schema(db->dbname, ".ONDISK");
        db->lrl = get_size_of_schema(s);
        nix = s->nix;
        for (ix = 0; ix < nix; ix++) {
            snprintf(namebuf, sizeof(namebuf), ".ONDISK_ix_%d", ix);
            s = find_tag_schema(db->dbname, namebuf);
            db->ix_keylen[ix] = get_size_of_schema(s);
        }

        if (db->csc2_schema) {
            free(db->csc2_schema);
            db->csc2_schema = NULL;
        }

        if (bdb_have_llmeta()) {
            int ver;
            ver = get_csc2_version_tran(db->dbname, tran);
            if (ver > 0) {
                get_csc2_file_tran(db->dbname, ver, &db->csc2_schema,
                                   &db->csc2_schema_len, tran);
            }
        } else {
            if (!db->csc2_schema)  
                db->csc2_schema = load_text_file(db->lrlfname);
            if (db->csc2_schema)
                db->csc2_schema_len = strlen(db->csc2_schema);
        }
    }
    /* TODO: schema information for foreign tables */
}

void fix_lrl_ixlen()
{
    fix_lrl_ixlen_tran(NULL);
}

static int tablecount = 0;
static int tagcount = 0;

int have_all_schemas(void)
{
    int tbl, ix;
    char namebuf[MAXTAGLEN + 1];
    int bad = 0;
    struct schema *sqlite_stat1 = NULL;
    struct field *table_field = NULL, *index_field = NULL;
    int tbl_len = 0;

    sqlite_stat1 = find_tag_schema("sqlite_stat1", ".ONDISK");
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
        struct schema *s = find_tag_schema(thedb->dbs[tbl]->dbname, ".ONDISK");
        if (s == NULL) {
            logmsg(LOGMSG_ERROR, "Missing schema: table %s tag .ONDISK\n",
                    thedb->dbs[tbl]->dbname);
            bad = 1;
        }

        if (sqlite_stat1 && table_field && index_field &&
            strcasecmp(thedb->dbs[tbl]->dbname, "sqlite_stat1")) {
            if (strlen(thedb->dbs[tbl]->dbname) >= table_field->len ||
                strlen(thedb->dbs[tbl]->dbname) >= index_field->len) {
                logmsg(LOGMSG_WARN, "WARNING: table name %s too long for analyze\n",
                       thedb->dbs[tbl]->dbname);
            }
        }

        thedb->dbs[tbl]->schema = s;

        for (ix = 0; s && ix < thedb->dbs[tbl]->nix; ix++) {
            snprintf(namebuf, sizeof(namebuf), ".ONDISK_ix_%d", ix);
            s = find_tag_schema(thedb->dbs[tbl]->dbname, namebuf);
            if (s == NULL) {
                logmsg(LOGMSG_ERROR, "Missing schema: table %s tag %s\n",
                        thedb->dbs[tbl]->dbname, namebuf);
                bad = 1;
            }
        }
    }

    if (bad)
        return 0;
    else
        return 1;
}

int getdefaultkeysize(const struct dbtable *db, int ixnum)
{
    char tagbuf[MAXTAGLEN];

    snprintf(tagbuf, MAXTAGLEN, ".DEFAULT_ix_%d", ixnum);
    struct schema *s = find_tag_schema(db->dbname, tagbuf);
    if (s == NULL)
        return -1;
    return get_size_of_schema(s);
    /* return s->member[s->nmembers-1].offset + s->member[s->nmembers-1].len; */
}

int getdefaultdatsize(const struct dbtable *db)
{
    struct schema *s = find_tag_schema(db->dbname, ".DEFAULT");

    if (s == NULL)
        return -1;
    return get_size_of_schema(s);
    /* return s->member[s->nmembers-1].offset + s->member[s->nmembers-1].len; */
}

int getondiskclientdatsize(const struct dbtable *db)
{
    struct schema *s = find_tag_schema(db->dbname, ".ONDISK_CLIENT");

    if (s == NULL)
        return -1;
    return get_size_of_schema(s);
}

int getclientdatsize(const struct dbtable *db, char *sname)
{
    struct schema *s = find_tag_schema(db->dbname, sname);

    if (s == NULL)
        return -1;
    return get_size_of_schema(s);
}

/* fetch the indexnumber (offset in tag map) of index with name keytag
 * on table dbname. indexnumber is the offset of the index in the csc2
 * schema starting from 0.
 * Using similar name pattern as get_dbtable_by_name()
 * funcion getidxnumbyname() was formerly named maptag2ix */
int getidxnumbyname(const char *dbname, const char *keytag, int *ixnum)
{
    struct schema *s = find_tag_schema(dbname, keytag);
    if (s == NULL || (s->flags & SCHEMA_INDEX) == 0)
        return -1;
    *ixnum = s->ixnum;
    return 0;
}

/* initialized in schema_init */
static char *get_unique_tag(void)
{
    struct thread_info *thd;
    char tag[MAXTAGLEN];

    thd = pthread_getspecific(unique_tag_key);
    if (thd == NULL)
        return NULL;

    snprintf(tag, sizeof(tag), ".TEMP_%d_%lld", (int)pthread_self(),
             thd->uniquetag++);

    /*fprintf(stderr, "get_unique_tag returning <%s>\n", tag);*/
    return strdup(tag);
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
    free_tag_schema(dsc);
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
    int rc;
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

    sc = calloc(1, sizeof(struct schema));
    sc->recsize = 0;
    sc->tag = get_unique_tag();
    sc->nmembers = nfields;
    sc->flags = SCHEMA_TABLE | SCHEMA_DYNAMIC;
    sc->nix = 0; /* note: we won't be adding ix schema records since
                    we never need to convert to client-form keys for
                    dynamic tags (all requests are new) */
    sc->ix = NULL;
    sc->ixnum = -1;
    sc->member = calloc(sc->nmembers, sizeof(struct field));
    sc->numblobs = 0;
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

int get_schema_blob_count(const char *table, const char *ctag)
{
    struct schema *sc = find_tag_schema(table, ctag);
    if (sc == NULL)
        return -1;

    return sc->numblobs;
}

void free_blob_buffers(blob_buffer_t *blobs, int nblobs)
{
    int ii;
    for (ii = 0; ii < nblobs; ii++) {
        if (blobs[ii].exists && blobs[ii].data)
            free(blobs[ii].data);
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
int blob_no_to_blob_no(const char *table, const char *from_tag, int from_blob,
                       const char *to_tag)
{
    return tbl_blob_no_to_tbl_blob_no(table, from_tag, from_blob, table,
                                      to_tag);
}

int tbl_blob_no_to_tbl_blob_no(const char *from_table, const char *from_tag,
                               int from_blob, const char *to_table,
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

/* re-write the 16 byte client blob fields in a buffer */
int rewrite_client_blob_field(const char *table, const char *tag, int blobno,
                              void *client_record, int blob_isnull,
                              unsigned blob_length)
{
    int idx, blob_idx;

    struct schema *sc = find_tag_schema(table, tag);
    if (sc == NULL)
        return -1;

    blob_idx = -1;
    for (idx = 0; idx < sc->nmembers; idx++) {
        /* obviously we are not looking for server blobs right now.. */
        if (sc->member[idx].type == CLIENT_BLOB) {
            blob_idx++;
            if (blob_idx == blobno) {
                char *cptr = client_record;
                client_blob_tp *blob =
                    (client_blob_tp *)(cptr + sc->member[idx].offset);
                blob->notnull = blob_isnull ? 0 : htonl(1);
                blob->length = blob_isnull ? 0 : htonl(blob_length);
                blob->padding0 = 0;
                blob->padding1 = 0;
                return 0;
            }
        }
    }

    return -1;
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

int get_schema_blob_field_idx(const char *table, const char *tag, int blobno)
{

    struct schema *sc = find_tag_schema(table, tag);
    if (sc == NULL)
        return -1;

    return get_schema_blob_field_idx_sc(sc, blobno);
}

/* This is the opposite - maps a field index to a blob number. */
int get_schema_field_blob_idx(const char *table, const char *tag, int fldindex)
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

static int backout_schemas_lockless(const char *tblname)
{
    struct dbtag *dbt;
    struct schema *sc, *tmp;

    dbt = hash_find_readonly(tags, &tblname);
    if (dbt == NULL) {
        return -1;
    }
    for (sc = dbt->taglist.top; sc != NULL;) {
        tmp = sc->lnk.next;
        if (strncasecmp(sc->tag, ".NEW.", 5) == 0) {
            /* new addition? delete */
            listc_rfl(&dbt->taglist, sc);
            hash_del(dbt->tags, sc);
            freeschema(sc);
        }
        sc = tmp;
    }

    return 0;
}

void backout_schemas(char *tblname)
{
    struct dbtag *dbt;
    int rc;

    lock_taglock();

    dbt = hash_find_readonly(tags, &tblname);
    if (dbt == NULL) {
        if (likely(timepart_is_timepart(tblname, 1))) {
            timepart_for_each_shard(tblname, backout_schemas_lockless);
        }

        unlock_taglock();
        return;
    }

    rc = backout_schemas_lockless(tblname);
    if (rc) {
        logmsg(LOGMSG_ERROR, "backout_schemas %s failed rc=%d\n", tblname, rc);
    }

    unlock_taglock();
}

struct schema *clone_schema(struct schema *from)
{
    int i;

    struct schema *sc = calloc(1, sizeof(struct schema));
    sc->tag = strdup(from->tag);
    sc->nmembers = from->nmembers;
    sc->member = malloc(from->nmembers * sizeof(struct field));
    sc->flags = from->flags;
    sc->nix = from->nix;
    if (sc->nix)
        sc->ix = calloc(from->nix, sizeof(struct schema *));

    for (i = 0; i < from->nmembers; i++) {
        sc->member[i] = from->member[i];
        sc->member[i].name = strdup(from->member[i].name);
        if (from->member[i].in_default) {
            sc->member[i].in_default = malloc(sc->member[i].in_default_len);
            memcpy(sc->member[i].in_default, from->member[i].in_default,
                   from->member[i].in_default_len);
        }
        if (from->member[i].out_default) {
            sc->member[i].out_default = malloc(sc->member[i].out_default_len);
            memcpy(sc->member[i].out_default, from->member[i].out_default,
                   from->member[i].out_default_len);
        }
    }

    for (i = 0; i < from->nix; i++) {
        if (from->ix && from->ix[i])
            sc->ix[i] = clone_schema(from->ix[i]);
    }

    sc->ixnum = from->ixnum;
    sc->recsize = from->recsize;
    sc->numblobs = from->numblobs;

    if (from->csctag)
        sc->csctag = strdup(from->csctag);

    if (from->datacopy) {
        sc->datacopy = malloc(from->nmembers * sizeof(int));
        memcpy(sc->datacopy, from->datacopy, from->nmembers * sizeof(int));
    }
    return sc;
}

/* note: threads are quiesced before this is called - no locks */
void commit_schemas(const char *tblname)
{
    struct dbtag *dbt;
    struct schema *sc;
    struct schema *tmp;
    struct dbtable *db = get_dbtable_by_name(tblname);

    LISTC_T(struct schema) to_be_freed;

    listc_init(&to_be_freed, offsetof(struct schema, lnk));

    lock_taglock();
    dbt = hash_find_readonly(tags, &tblname);

    if (dbt == NULL) {
        unlock_taglock();
        return;
    }

    sc = dbt->taglist.top;
    while (sc != NULL) {
        tmp = sc->lnk.next;
        /* printf("]]]]]] %p %s\n", sc, sc->tag); */
        sc = tmp;
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
                        "commit_schemas: out of memory on strdup tag\n");
                exit(1);
            }
            sc->tag = newname;
            free(oldname);

            if (sc->csctag) {
                oldname = sc->csctag;
                newname = strdup(sc->csctag + 5);
                if (!newname) {
                    logmsg(LOGMSG_FATAL, 
                            "commit_schemas: out of memory on strdup tag\n");
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
                                "commit_schemas: out of memory on malloc\n");
                        exit(1);
                    }
                    sprintf(newname, "%s%d", gbl_ondisk_ver, db->version);
                    ver_schema = clone_schema(sc);
                    free(ver_schema->tag);
                    ver_schema->tag = newname;
                    listc_atl(&dbt->taglist, ver_schema);
                }
            }
        } else if (strncasecmp(sc->tag, gbl_ondisk_ver,
                               strlen(gbl_ondisk_ver))) {
            /* not .NEW. and not .ONDISK_VER. delete */
            listc_rfl(&dbt->taglist, sc);
            listc_abl(&to_be_freed, sc);
            sc = NULL;
        } else {
            /* .ONDISK.VER.nn
             * if nn >= current version drop it */
            int ver;
            sscanf(sc->tag, gbl_ondisk_ver_fmt, &ver);
            if (ver >= db->version) {
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
            hash_add(dbt->tags, sc);
        }
        sc = tmp;
    }

    /* Free outstanding schemas above.  Since they may reference other
     * schemas, the order in which we free them below is important.  This
     * is very ugly code.  Sorry. */
    if (1) {
        struct schema **s;
        int i = 0, j;
        int count = to_be_freed.count;

        s = malloc(sizeof(struct schema *) * count);
        sc = listc_rtl(&to_be_freed);
        while (sc) {
            if (db->schema != sc)
                s[i++] = sc;
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
                            freeschema(s[i]->ix[j]);
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

/*
 * Returns 1 if this is ondisk tag, 0 if it is not, -1 if the
 * tag is not found.
 */
int is_tag_ondisk(const char *table, const char *tag)
{
    struct schema *sc = find_tag_schema(table, tag);
    if (sc == NULL)
        return -1;
    int rc = is_tag_ondisk_sc(sc);
    if (rc < 0)
        logmsg(LOGMSG_ERROR, 
                "is_tag_ondisk: table %s tag %s has illegal type %d member 0\n",
                table, tag, sc->member[0].type);
    return rc;
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
                    taglen, tagdescr);
            return -1;
        }
        add_tag_schema(iq->usedb->dbname, *dynschema);
        strncpy0(tagname, (*dynschema)->tag, tagnamelen);
    } else if (taglen > 0) {
        /* static tag name */
        if (taglen > tagnamelen - 1) {
            if (iq->debug)
                reqprintf(iq, "resolve_tag_name TAG NAME TOO LONG '%.*s'",
                          taglen, tagdescr);
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

int ondisk_type_is_vutf8(struct dbtable *db, const char *fieldname,
                         size_t fieldname_len)
{
    int i;
    struct schema *s = find_tag_schema(db->dbname, ".ONDISK");
    for (i = 0; i < s->nmembers; i++) {
        if (strncmp(s->member[i].name, fieldname, fieldname_len) == 0)
            /* TODO delme */
            /*return s->member[i].type == CLIENT_VUTF8;*/
            return s->member[i].type == SERVER_VUTF8;
    }

    /* error? */
    return 0;
}

/* return a unique id associated with a record */
long long get_record_unique_id(struct dbtable *db, void *rec)
{
    int i;
    char *p = (char *)rec;
    long long id;
    int outnull;
    int sz;

    struct schema *s = find_tag_schema(db->dbname, ".ONDISK");
    for (i = 0; i < s->nmembers; i++) {
        if (strcasecmp(s->member[i].name, "comdb2_seqno") == 0) {
            SERVER_BINT_to_CLIENT_INT(p + s->member[i].offset, 9, NULL, NULL,
                                      &id, 8, &outnull, &sz, NULL, NULL);
            return id;
        }
    }
    return 0;
}

const char *get_keynm_from_db_idx(struct dbtable *db, int idx)
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

static void update_fld_hints(struct dbtable *db)
{
    struct schema *ondisk = db->schema;
    int n = ondisk->nmembers;
    uint16_t *hints = malloc(sizeof(*hints) * (n + 1));
    for (int i = 0; i < n; ++i) {
        hints[i] = ondisk->member[i].len;
    }
    hints[n] = 0;
    bdb_set_fld_hints(db->handle, hints);
}

void set_bdb_option_flags(struct dbtable *db, int odh, int ipu, int isc, int ver,
                          int compr, int blob_compr, int datacopy_odh)
{
    update_fld_hints(db);
    bdb_state_type *handle = db->handle;
    bdb_set_odh_options(handle, odh, compr, blob_compr);
    bdb_set_inplace_updates(handle, ipu);
    bdb_set_instant_schema_change(handle, isc);
    bdb_set_csc2_version(handle, ver);
    bdb_set_datacopy_odh(handle, datacopy_odh);
    bdb_set_key_compression(handle);
}

/* Compute map of dbstores used in vtag_to_ondisk */
void update_dbstore(struct dbtable *db)
{
    if (!db->instant_schema_change)
        return;

    struct schema *ondisk = db->schema;
    if (ondisk == NULL) {
        logmsg(LOGMSG_FATAL, "%s: .ONDISK not found!! PANIC!! %s() @ %d\n", db->dbname,
               __func__, __LINE__);
        cheap_stack_trace();
        exit(1);
    }

    for (int i = 0; i < sizeof db->dbstore / sizeof db->dbstore[0]; ++i) {
        if (db->dbstore[i].data) {
            free(db->dbstore[i].data);
        }
    }

    bzero(db->dbstore, sizeof db->dbstore);

    for (int v = 1; v <= db->version; ++v) {
        char tag[MAXTAGLEN];
        struct schema *ver;
        int position;
        snprintf(tag, sizeof tag, gbl_ondisk_ver_fmt, v);
        ver = find_tag_schema(db->dbname, tag);
        if (ver == NULL) {
            logmsg(LOGMSG_FATAL, "%s: %s not found!! PANIC!! %s() @ %d\n", db->dbname, tag,
                   __func__, __LINE__);
            /* FIXME */
            cheap_stack_trace();
            abort();
        }

        db->versmap[v] = get_tag_mapping(ver, ondisk);
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
                logmsg(LOGMSG_FATAL, "%s: %s no such field: %s in .ONDISK. but in %s!! "
                       "PANIC!!\n",
                       db->dbname, __func__, from->name, tag);
                /* FIXME */
                exit(1);
            }

            if (db->versmap[v][i] != i || from->type != to->type ||
                from->len != to->len)
                db->vers_compat_ondisk[v] = 0; // not compatible

            if (db->dbstore[position].ver == 0) {
                /* column not seen before */
                db->dbstore[position].ver = v;

                if (from->in_default_len) {
                    db->dbstore[position].len = to->len;
                    db->dbstore[position].data = calloc(1, to->len);
                    if (db->dbstore[position].data == NULL) {
                        logmsg(LOGMSG_FATAL, "%s: %s() @ %d calloc failed!! PANIC!!\n",
                               db->dbname, __func__, __LINE__);
                        /* FIXME */
                        exit(1);
                    }
                    rc = SERVER_to_SERVER(
                        from->in_default, from->in_default_len,
                        from->in_default_type, &from->convopts, NULL,
                        0 /* flags are unused */, db->dbstore[position].data,
                        to->len, to->type, 0, /*flags are unused */ &outdtsz,
                        &to->convopts, NULL);
                    if (rc != 0) {
                        logmsg(LOGMSG_FATAL, "%s: %s() @ %d: SERVER_to_SERVER failed!! "
                               "PANIC!!\n",
                               db->dbname, __func__, __LINE__);
                        /* FIXME */
                        exit(1);
                    }
                }
            }
        } /* end for each field */
    }     /* end for each version */
}

void replace_tag_schema(struct dbtable *db, struct schema *schema)
{
    struct schema *old_schema;
    struct schema *tmp;
    struct dbtag *dbt;

    lock_taglock();
    dbt = hash_find_readonly(tags, &db->dbname);
    if (dbt == NULL) {
        unlock_taglock();
        return;
    }

    old_schema = dbt->taglist.top;
    while (old_schema) {
        tmp = old_schema->lnk.next;
        if (strcasecmp(old_schema->tag, schema->tag) == 0) {
            listc_rfl(&dbt->taglist, old_schema);
            freeschema(old_schema);
        }
        old_schema = tmp;
    }
    listc_atl(&dbt->taglist, schema);
    unlock_taglock();
}

void delete_schema(const char *dbname)
{
    struct dbtag *dbt;
    lock_taglock();
    dbt = hash_find(tags, &dbname);
    hash_del(tags, dbt);
    unlock_taglock();
    struct schema *schema = dbt->taglist.top;
    while (schema) {
        struct schema *tmp = schema;
        schema = schema->lnk.next;
        listc_rfl(&dbt->taglist, tmp);
        freeschema(tmp);
    }
    if (dbt->tags)
        hash_free(dbt->tags);
    free(dbt->tblname);
    free(dbt);
}

void freeschema_internals(struct schema *schema)
{
    int i;
    char *blank = "";

    free(schema->tag);
    for (i = 0; i < schema->nmembers; i++) {
        if (schema->member[i].in_default) {
            free(schema->member[i].in_default);
        }
        if (schema->member[i].out_default) {
            free(schema->member[i].out_default);
        }
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
}

void freeschema(struct schema *schema)
{
    if (!schema)
        return;
    freeschema_internals(schema);
    free(schema);
}

void freedb_int(struct dbtable *db, struct dbtable *replace)
{
    int i;

    free(db->lrlfname);
    free(db->dbname);
    /* who frees schema/ixschema? */
    free(db->sql);

    if (db->ixsql) {
        for (i = 0; i < db->nix; i++) {
            free(db->ixsql[i]);
            db->ixsql[i] = NULL;
        }
        free(db->ixsql);
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
        for (int v = 1; v <= db->version; ++v) {
            if (db->versmap[v]) {
                free(db->versmap[v]);
                db->versmap[v] = NULL;
            }
        }
    }

    if (replace)
        memcpy(db, replace, sizeof(struct dbtable));
    else
        free(db);
}

void free_db_and_replace(struct dbtable *db, struct dbtable *newdb)
{
    freedb_int(db, newdb);
}

void freedb(struct dbtable *db) { freedb_int(db, NULL); }

struct schema *create_version_schema(char *csc2, int version,
                                     struct dbenv *dbenv)
{
    struct dbtable *ver_db;
    char *tag;
    int rc;

    rc = dyns_load_schema_string(csc2, dbenv->envname, gbl_ver_temp_table);
    if (rc) {
        logmsg(LOGMSG_ERROR, "dyns_load_schema_string failed %s:%d\n", __FILE__,
                __LINE__);
        goto err;
    }

    ver_db = newdb_from_schema(dbenv, gbl_ver_temp_table, NULL, 0, 0, 0);
    if (ver_db == NULL) {
        logmsg(LOGMSG_ERROR, "newdb_from_schema failed %s:%d\n", __FILE__, __LINE__);
        goto err;
    }

    rc = add_cmacc_stmt_no_side_effects(ver_db, 0);
    if (rc) {
        logmsg(LOGMSG_ERROR, "add_cmacc_stmt failed %s:%d\n", __FILE__, __LINE__);
        goto err;
    }

    struct schema *s = find_tag_schema(gbl_ver_temp_table, ".ONDISK");
    if (s == NULL) {
        logmsg(LOGMSG_ERROR, "find_tag_schema failed %s:%d\n", __FILE__, __LINE__);
        goto err;
    }

    tag = malloc(gbl_ondisk_ver_len);
    if (tag == NULL) {
        logmsg(LOGMSG_ERROR, "malloc failed %s:%d\n", __FILE__, __LINE__);
        goto err;
    }

    sprintf(tag, gbl_ondisk_ver_fmt, version);
    struct schema *ver_schema = clone_schema(s);
    free(ver_schema->tag);
    ver_schema->tag = tag;

    /* get rid of temp schema */
    del_tag_schema(ver_db->dbname, s->tag);
    freeschema(s);

    /* get rid of temp table */
    delete_schema(ver_db->dbname);
    freedb(ver_db);

    return ver_schema;

err:
    return NULL;
}

static void clear_existing_schemas(struct dbtable *db)
{
    struct schema *schema;
    char tag[64];
    int i;
    for (i = 1; i <= db->version; ++i) {
        sprintf(tag, gbl_ondisk_ver_fmt, i);
        schema = find_tag_schema(db->dbname, tag);
        del_tag_schema(db->dbname, tag);
        freeschema(schema);
    }

    schema = find_tag_schema(db->dbname, ".ONDISK");
    del_tag_schema(db->dbname, ".ONDISK");
    freeschema(schema);
}

static int load_new_versions(struct dbtable *db, tran_type *tran)
{
    int isc;
    get_db_instant_schema_change_tran(db, &isc, tran);
    if (!isc)
        return 0;

    int i;
    int version = get_csc2_version_tran(db->dbname, tran);
    for (i = 1; i <= version; ++i) {
        char *csc2;
        int len;
        get_csc2_file_tran(db->dbname, i, &csc2, &len, tran);
        struct schema *schema = create_version_schema(csc2, i, db->dbenv);
        if (schema == NULL) {
            logmsg(LOGMSG_ERROR, "Could not create schema version: %d\n", i);
            return 1;
        }
        add_tag_schema(db->dbname, schema);
        free(csc2);
    }
    return 0;
}

static int load_new_ondisk(struct dbtable *db, tran_type *tran)
{
    int rc;
    int bdberr;
    int foundix = db->dbs_idx;
    int version = get_csc2_version_tran(db->dbname, tran);
    int len;
    char *csc2 = NULL;

    rc = get_csc2_file_tran(db->dbname, version, &csc2, &len, tran);
    if (rc) {
        logmsg(LOGMSG_ERROR, "get_csc2_file failed %s:%d\n", __FILE__, __LINE__);
        logmsg(LOGMSG_ERROR, "rc: %d len: %d csc2:\n%s\n", rc, len, csc2);
        goto err;
    }

    rc = dyns_load_schema_string(csc2, db->dbenv->envname, db->dbname);
    if (rc) {
        logmsg(LOGMSG_ERROR, "dyns_load_schema_string failed %s:%d\n", __FILE__,
                __LINE__);
        logmsg(LOGMSG_ERROR, "rc: %d len: %d csc2:\n%s\n", rc, len, csc2);
        goto err;
    }

    struct dbtable *newdb =
        newdb_from_schema(db->dbenv, db->dbname, NULL, db->dbnum, foundix, 0);
    if (newdb == NULL) {
        logmsg(LOGMSG_ERROR, "newdb_from_schema failed %s:%d\n", __FILE__, __LINE__);
        goto err;
    }
    newdb->version = version;
    newdb->dbnum = db->dbnum;
    rc = add_cmacc_stmt(newdb, 0);
    if (rc) {
        logmsg(LOGMSG_ERROR, "add_cmacc_stmt failed %s:%d\n", __FILE__, __LINE__);
        goto err;
    }
    newdb->meta = db->meta;
    newdb->dtastripe = gbl_dtastripe;

    /* reopen db */
    newdb->handle = bdb_open_more_tran(
        db->dbname, thedb->basedir, newdb->lrl, newdb->nix, newdb->ix_keylen,
        newdb->ix_dupes, newdb->ix_recnums, newdb->ix_datacopy,
        newdb->ix_collattr, newdb->ix_nullsallowed, newdb->numblobs + 1,
        thedb->bdb_env, tran, &bdberr);

    if (bdberr != 0 || newdb->handle == NULL) {
        logmsg(LOGMSG_ERROR, "reload_schema handle %08x bdberr %d\n", newdb->handle, bdberr);
        cheap_stack_trace();
        goto err;
    }

    set_odh_options_tran(newdb, tran);
    transfer_db_settings(db, newdb);
    restore_constraint_pointers(db, newdb);
    bdb_close_only(db->handle, &bdberr);
    rc = bdb_free_and_replace(db->handle, newdb->handle, &bdberr);
    if (rc)
        logmsg(LOGMSG_ERROR, "%s:%d bdb_free rc %d %d\n", __FILE__, __LINE__, rc,
                bdberr);
    free_db_and_replace(db, newdb);
    fix_constraint_pointers(db, newdb);
    memset(newdb, 0xff, sizeof(struct dbtable));
    free(newdb);
    replace_db_idx(db, foundix);
    fix_lrl_ixlen_tran(tran);
    free(csc2);
    return 0;

err:
    free(csc2);
    return 1;
}

int reload_after_bulkimport(struct dbtable *db, tran_type *tran)
{
    clear_existing_schemas(db);
    if (load_new_ondisk(db, NULL)) {
        logmsg(LOGMSG_ERROR, "Failed to load new .ONDISK\n");
        return 1;
    }
    if (load_new_versions(db, NULL)) {
        logmsg(LOGMSG_ERROR, "Failed to load .ONDISK.VER.nn\n");
        return 1;
    }
    db->tableversion = table_version_select(db, NULL);
    update_dbstore(db);
    create_sqlmaster_records(tran);
    create_master_tables();
    return 0;
}

int reload_db_tran(struct dbtable *db, tran_type *tran)
{
    clear_existing_schemas(db);
    if (load_new_ondisk(db, tran)) {
        logmsg(LOGMSG_ERROR, "Failed to load new .ONDISK\n");
        return 1;
    }
    if (load_new_versions(db, tran)) {
        logmsg(LOGMSG_ERROR, "Failed to load .ONDISK.VER.nn\n");
        return 1;
    }
    db->tableversion = table_version_select(db, tran);
    update_dbstore(db);
    create_sqlmaster_records(tran);
    create_master_tables();
    return 0;
}

extern int null_bit;
void err_print_rec(strbuf *buf, void *rec, char *table, char *tag)
{
    struct field *f;
    int field;
    int rc;
    int isnull;
    int outsz = 0;
    struct field_conv_opts outopts = {0};
    struct field_conv_opts inopts = {0};

    struct schema *s = find_tag_schema(table, tag);
    if (s == NULL)
        return;

/* grumble grumble */
#ifdef _LINUX_SOURCE
    outopts.flags = FLD_CONV_LENDIAN;
#endif

    strbuf_append(buf, " key=[");

    for (field = 0; field < s->nmembers; field++) {
        f = &s->member[field];

        strbuf_appendf(buf, "%s=", f->name);

        switch (f->type) {
        case CLIENT_UINT: {
            uint64_t uint;
            memcpy(&uint, (uint8_t *)rec + f->offset, sizeof(uint));
            strbuf_appendf(buf, "%u", uint);
            break;
        }
        case CLIENT_INT: {
            int64_t sint;
            memcpy(&sint, (uint8_t *)rec + f->offset, sizeof(sint));
            strbuf_appendf(buf, "%lld", sint);
            break;
        }
        case CLIENT_REAL: {
            double d;
            memcpy(&d, (uint8_t *)rec + f->offset, sizeof(double));
            strbuf_appendf(buf, "%f", d);
            break;
        }
        case CLIENT_CSTR: {
            char *s;
            s = (char *)(((uint8_t *)rec) + f->offset);
            strbuf_appendf(buf, "\"%s\"", s);
            break;
        }
        case CLIENT_PSTR:
        case CLIENT_PSTR2: {
            char *s;
            s = (char *)(((uint8_t *)rec) + f->offset);
            strbuf_appendf(buf, "\"%.*s\"", f->len, s);
            break;
        }
        case CLIENT_BYTEARRAY: {
            uint8_t *b;
            b = (uint8_t *)rec + f->offset;
            strbuf_appendf(buf, "x'");
            for (int i = 0; i < f->len; f++)
                strbuf_appendf(buf, "%02x", b[i]);
            strbuf_appendf(buf, "'");
            break;
        }
        case CLIENT_BLOB:
        case CLIENT_BLOB2:
        case CLIENT_DATETIME:
        case CLIENT_DATETIMEUS:
        case CLIENT_INTVYM:
        case CLIENT_INTVDS:
        case CLIENT_INTVDSUS:
        case CLIENT_VUTF8:
            strbuf_appendf(buf, "x'?'");
            break;

        case SERVER_UINT: {
            uint64_t uint;
            rc = SERVER_UINT_to_CLIENT_UINT((uint8_t *)rec + f->offset, f->len,
                                            &inopts, NULL, &uint, sizeof(uint),
                                            &isnull, &outsz, &outopts, NULL);
            if (rc) {
                strbuf_appendf(buf, "x'?'");
            } else {
                if (isnull)
                    strbuf_appendf(buf, "null");
                else
                    strbuf_appendf(buf, "%u", uint);
            }
            break;
        }
        case SERVER_BINT: {
            int64_t sint;
            rc = SERVER_BINT_to_CLIENT_INT((uint8_t *)rec + f->offset, f->len,
                                           &inopts, NULL, &sint, sizeof(sint),
                                           &isnull, &outsz, &outopts, NULL);
            if (rc) {
                strbuf_appendf(buf, "x'?'");
            } else {
                if (isnull)
                    strbuf_appendf(buf, "null");
                else
                    strbuf_appendf(buf, "%lld", sint);
            }
            break;
        }
        case SERVER_BREAL: {
            double d;
            rc = SERVER_BREAL_to_CLIENT_REAL((uint8_t *)rec + f->offset, f->len,
                                             &inopts, NULL, &d, sizeof(d),
                                             &isnull, &outsz, &outopts, NULL);
            if (rc) {
                strbuf_appendf(buf, "x'?'");
            } else {
                if (isnull)
                    strbuf_appendf(buf, "null");
                else
                    strbuf_appendf(buf, "%f", d);
            }
            break;
        }
        case SERVER_BCSTR: {
            char *s;
            s = (char *)((uint8_t *)rec + f->offset);
            isnull = btst(s, null_bit);
            if (isnull)
                strbuf_appendf(buf, "null");
            else
                strbuf_appendf(buf, "\"%.*s\"", f->len - 1, s + 1);
            break;
        }
        case SERVER_BYTEARRAY: {
            uint8_t *b;
            b = (char *)((uint8_t *)rec + f->offset);
            isnull = btst(b, null_bit);
            if (isnull)
                strbuf_appendf(buf, "null");
            else {
                strbuf_appendf(buf, "x'");
                b++;
                for (int i = 0; i < f->len - 1; i++)
                    strbuf_appendf(buf, "%02x", b[i]);
                strbuf_appendf(buf, "'");
                break;
            }
        }

        case SERVER_DATETIME: {
            break;
        }
        case SERVER_DATETIMEUS: {
            break;
        }
        case SERVER_INTVYM: {
            break;
        }
        case SERVER_INTVDS: {
            break;
        }
        case SERVER_INTVDSUS: {
            break;
        }
        case SERVER_DECIMAL: {
            break;
        }
        default:
            break;
        }
        if (field != s->nmembers - 1)
            strbuf_appendf(buf, ", ");
    }

    strbuf_appendf(buf, "]");
}

short field_decimal_quantum(struct dbtable *db, struct schema *s, int fnum,
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
int extract_decimal_quantum(struct dbtable *db, int ix, char *inbuf, char *poutbuf,
                            int outbuf_max, int *outlen)
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
                hexdump(&inbuf[s->member[i].offset], s->member[i].len);
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
                hexdump(&inbuf[s->member[i].offset], s->member[i].len);
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

int create_key_from_ondisk_sch_blobs(
    struct dbtable *db, struct schema *fromsch, int ixnum, char **tail, int *taillen,
    char *mangled_key, const char *fromtag, const char *inbuf, int inbuflen,
    const char *totag, char *outbuf, struct convert_failure *reason,
    blob_buffer_t *inblobs, int maxblobs, const char *tzname)
{
    int rc = 0;

    rc = _stag_to_stag_buf_flags_blobs(
        fromsch, fromtag, inbuf, db->dbname, totag, outbuf, 0 /*flags*/, reason,
        inblobs, NULL /*outblobs*/, maxblobs, tzname);
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
            *tail = (char *)inbuf;
            *taillen = inbuflen;
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

int create_key_from_ondisk_sch(struct dbtable *db, struct schema *fromsch, int ixnum,
                               char **tail, int *taillen, char *mangled_key,
                               const char *fromtag, const char *inbuf,
                               int inbuflen, const char *totag, char *outbuf,
                               struct convert_failure *reason,
                               const char *tzname)
{
    return create_key_from_ondisk_sch_blobs(
        db, fromsch, ixnum, tail, taillen, mangled_key, fromtag, inbuf,
        inbuflen, totag, outbuf, reason, NULL /*inblobs*/, 0 /*maxblobs*/,
        tzname);
}

inline int create_key_from_ondisk(struct dbtable *db, int ixnum, char **tail,
                                  int *taillen, char *mangled_key,
                                  const char *fromtag, const char *inbuf,
                                  int inbuflen, const char *totag, char *outbuf,
                                  struct convert_failure *reason,
                                  const char *tzname)
{
    struct schema *fromsch = find_tag_schema(db->dbname, fromtag);

    return create_key_from_ondisk_sch(db, fromsch, ixnum, tail, taillen,
                                      mangled_key, fromtag, inbuf, inbuflen,
                                      totag, outbuf, reason, tzname);
}

inline int create_key_from_ondisk_blobs(
    struct dbtable *db, int ixnum, char **tail, int *taillen, char *mangled_key,
    const char *fromtag, const char *inbuf, int inbuflen, const char *totag,
    char *outbuf, struct convert_failure *reason, blob_buffer_t *inblobs,
    int maxblobs, const char *tzname)
{
    struct schema *fromsch = find_tag_schema(db->dbname, fromtag);

    return create_key_from_ondisk_sch_blobs(
        db, fromsch, ixnum, tail, taillen, mangled_key, fromtag, inbuf,
        inbuflen, totag, outbuf, reason, inblobs, maxblobs, tzname);
}

int create_key_from_ireq(struct ireq *iq, int ixnum, int isDelete, char **tail,
                         int *taillen, char *mangled_key, const char *inbuf,
                         int inbuflen, char *outbuf)
{
    int rc = 0;
    struct dbtable *db = iq->usedb;

    if (isDelete)
        memcpy(outbuf, iq->idxDelete[ixnum], db->ix_keylen[ixnum]);
    else
        memcpy(outbuf, iq->idxInsert[ixnum], db->ix_keylen[ixnum]);

    if (db->ix_datacopy[ixnum]) {
        assert(db->ix_collattr[ixnum] == 0);

        if (tail) {
            *tail = (char *)inbuf;
            *taillen = inbuflen;
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
