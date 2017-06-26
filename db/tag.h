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

#ifndef __SCHEMA_H
#define __SCHEMA_H

#include <list.h>
#include "types.h"
#include "strbuf.h"
#include <pthread.h>

struct ireq;
struct db;

/* libcmacc2 populates these structures.
   Schema records are added from upon parsing a "csc" directive.
   After all files are read, create_sqlmaster_records is called at
   init time to seed SQL code with create statements for tables/indices */
struct field {
    int type;
    unsigned int offset;  /* byte offset in record */
    unsigned int len;     /* byte length in record */
    unsigned int datalen; /* for dyntags; length of client supplied buffer */
    char *name;           /* name of field */
    int isExpr;
    int idx;   /* only meaningful for indices: index of column in table */
    int flags; /* field specific flags */

    /* in/out defaults are only set for the .ONDISK tag and are stored in
     * server format. */
    int in_default_type;
    void *in_default;
    int in_default_len;
    int out_default_type;
    void *out_default;
    int out_default_len;
    struct field_conv_opts convopts;

    int blob_index; /* index of this blob, -1 for non blobs */
};

/* A schema for a tag or index.  The schema for the .ONDISK tag will have
 * an array of ondisk index schemas too. */
struct schema {
    char *tag;
    int nmembers;
    struct field *member;
    int flags;
    int nix;
    struct schema **ix;
    int ixnum;   /* for indices, number of index in struct db */
    int ix_blob; /* set to 1 if blobs are involved in indexes */
    int recsize; /* for tables, gives the length of the record structure */
    int numblobs;
    char *
        csctag; /* this is valid for indices, name of the index listed in csc
                   file */
    char *sqlitetag;
    int *datacopy;
    char *where;
    uint8_t disableskipscan;
    LINKC_T(struct schema) lnk;
};

/* sql_record.flags */
enum {
    SCHEMA_TABLE = 1,
    SCHEMA_INDEX = 2,
    SCHEMA_DUP = 4 /* dup flag set */
    ,
    SCHEMA_RECNUM = 8 /* recnum flag set */
    ,
    SCHEMA_DYNAMIC = 16,
    SCHEMA_DATACOPY = 32 /* datacopy flag set on index */
};

/* sql_record_member.flags */
enum {
    INDEX_DESCEND = 1 /* only set for index members; data is inverted to
                         reverse ondisk sort */
    ,
    NO_NULL = 2 /* do not allow nulls */
};

/* flags for schema conversion */
enum {
    /* conversion for update: skip fields missing in source buffer */
    CONVERT_UPDATE = 1,
    CONVERT_LITTLE_ENDIAN_CLIENT = 2,
    CONVERT_NULL_NO_ERROR = 4 // support instant sc for dbstore
};

typedef enum convert_errcode {
    CONVERT_OK,
    CONVERT_FAILED_INVALID_INPUT_TAG,
    CONVERT_FAILED_INVALID_OUTPUT_TAG,
    CONVERT_FAILED_INVALID_LENGTH,
    CONVERT_FAILED_NULL_CONSTRAINT_VIOLATION,
    CONVERT_FAILED_SHOULDNT_HAPPEN_1,
    CONVERT_FAILED_INCOMPATIBLE_VALUES, /* the most common condition */
    CONVERT_FAILED_SHOULDNT_HAPPEN_2,
    CONVERT_FAILED_INVALID_PARTIAL_TYPE,
    CONVERT_FAILED_BAD_BLOB_PROGRAMMER,
    CONVERT_FAILED_INPUT_TAG_HAS_INVALID_FIELD,
    CONVERT_FAILED_INDEX_EXPRESSION
} convert_errcode;

struct convert_failure {
    convert_errcode reason; /* the reason conversion failed */
    const struct schema *source_schema;
    int source_field_idx; /* index of source tag that we failed to convert stuff
                             from */
    const struct schema *target_schema;
    int target_field_idx; /* index of target tag that we failed to convert stuff
                             to */

    int source_sql_field_flags;
    union {
        long long ival;
        double rval;
        size_t slen;
        size_t blen;
    } source_sql_field_info;
};

enum {
    T2T_PLAN_NULL = -1,
    T2T_PLAN_COMDB2_SEQNO = -2,
    T2T_PLAN_IN_DEFAULT = -3,
};

struct t2t_field {
    /* Index of field in from tag to use to populate field in to tag.
     * T2T_PLAN_NULL           => store null
     * T2T_PLAN_COMDB2_SEQNO   => generate and store a sequence number
     * T2T_PLAN_IN_DEFAULT     => store the to field's dbstore value
     */
    int from_field_idx;
};

struct t2t_plan {
    const struct schema *from;
    const struct schema *to;
    int max_from_len;
    int max_to_len;
    struct convert_failure fail_reason;
    struct t2t_field fields[1];
};

enum {
    /* good rcodes */
    SC_NO_CHANGE = 0,
    SC_TAG_CHANGE,
    SC_KEY_CHANGE,
    SC_CONSTRAINT_CHANGE,
    SC_COLUMN_ADDED,
    SC_DBSTORE_CHANGE,

    /* bad rcodes */
    SC_BAD_NEW_FIELD = -3,
    SC_BAD_INDEX_CHANGE = -4,
    SC_BAD_INDEX_NAME = -5
};

extern char gbl_ver_temp_table[];
extern char gbl_ondisk_ver[];
extern const int gbl_ondisk_ver_len;
extern char gbl_ondisk_ver_fmt[];

extern int gbl_use_t2t;

int tag_init(void);
void add_tag_schema(const char *table, struct schema *);
int get_size_of_schema(const struct schema *sc);
int get_size_of_schema_by_name(const char *table, const char *schema);
void *ctag_to_stag(const char *table, const char *ctag, const char *inbuf,
                   int len, const unsigned char *innulls, const char *stag,
                   int flags);
struct schema *find_tag_schema(const char *table, const char *tagname);
int stag_to_ctag_buf(const char *table, const char *stag, const char *inbuf,
                     int len, const char *ctag, void *outbufp,
                     unsigned char *outnulls, int flags, uint8_t **pp_flddtsz,
                     const uint8_t *p_flddtsz_end);
int stag_to_ctag_buf_tz(const char *table, const char *stag, const char *inbuf,
                        int len, const char *ctag, void *outbufp,
                        unsigned char *outnulls, int flags,
                        uint8_t **pp_flddtsz, const uint8_t *p_flddtsz_end,
                        const char *tzname);
void *stag_to_ctag(const char *table, const char *stag, const char *inbuf,
                   const char *ctag, unsigned char *outnulls, int flags);
int stag_to_ctag_buf_blobs_tz(const char *table, const char *stag,
                              const char *inbuf, int len, const char *ctag,
                              void *outbufp, unsigned char *outnulls, int flags,
                              uint8_t **pp_flddtsz,
                              const uint8_t *p_flddtsz_end,
                              blob_buffer_t *inblobs, blob_buffer_t *outblobs,
                              int maxblobs, const char *tzname);

int stag_to_stag_buf_blobs(const char *table, const char *fromtag,
                           const char *inbuf, const char *totag, char *outbuf,
                           struct convert_failure *reason, blob_buffer_t *blobs,
                           int maxblobs, int get_new_blobs);
int ctag_to_stag_buf(const char *table, const char *ctag, const char *inbuf,
                     int len, const unsigned char *innulls, const char *stag,
                     void *outbufp, int flags, struct convert_failure *reason);

int *get_tag_mapping(struct schema *fromsch, struct schema *tosch);

int stag_to_stag_buf_cachedmap(int tagmap[], struct schema *from,
                               struct schema *to, const char *inbuf,
                               char *outbuf, int flags,
                               struct convert_failure *fail_reason,
                               blob_buffer_t *inblobs, int maxblobs);

int ctag_to_stag_buf_tz(const char *table, const char *ctag, const char *inbuf,
                        int len, const unsigned char *innulls, const char *stag,
                        void *outbufp, int flags,
                        struct convert_failure *reason, const char *tzname);
int ctag_to_stag_buf_p(const char *table, const char *ctag, const char *inbuf,
                       int len, const unsigned char *innulls, const char *stag,
                       void *outbufp, int flags, int ondisk_lim,
                       struct convert_failure *reason);
int ctag_to_stag_buf_p_tz(const char *table, const char *ctag,
                          const char *inbuf, int len,
                          const unsigned char *innulls, const char *stag,
                          void *outbufp, int flags, int ondisk_lim,
                          struct convert_failure *reason, const char *tzname);
int ctag_to_stag_blobs(const char *table, const char *ctag, const char *inbuf,
                       int len, const unsigned char *innulls, const char *stag,
                       void *outbufp, int flags,
                       struct convert_failure *fail_reason,
                       blob_buffer_t *blobs, int maxblobs);
int ctag_to_stag_blobs_tz(const char *table, const char *ctag,
                          const char *inbuf, int len,
                          const unsigned char *innulls, const char *stag,
                          void *outbufp, int flags,
                          struct convert_failure *fail_reason,
                          blob_buffer_t *blobs, int maxblobs,
                          const char *tzname);
int ctag_to_ctag_buf(const char *table, const char *ftag, void *inbufp,
                     const char *ttag, void *outbufp);
void add_tag_alias(const char *table, struct schema *s, char *name);
void del_tag_schema(const char *table, const char *tagname);
void replace_tag_schema(struct db *db, struct schema *schema);
char *sqltype(struct field *f, char *buf, int len);
char *csc2type(struct field *f);
void debug_dump_schemas(void);
void debug_dump_tags(const char *tblname);
void fix_lrl_ixlen(void);
struct tran_tag;
void fix_lrl_ixlen_tran(struct tran_tag *);
int max_type_size(int type, int len);
int getidxnumbyname(const char *dbname, const char *tagname, int *ixnum);
int partial_key_length(const char *dbname, const char *keyname,
                       const char *pstring, int len);
int client_keylen_to_server_keylen(const char *table, const char *tag,
                                   int ixnum, int keylen);
void free_tag_schema(struct schema *s);
int get_schema_blob_count(const char *table, const char *ctag);
int blob_no_to_blob_no(const char *table, const char *from_tag, int from_blob,
                       const char *to_tag);
int tbl_blob_no_to_tbl_blob_no(const char *from_table, const char *from_tag,
                               int from_blob, const char *to_table,
                               const char *to_tag);
int rewrite_client_blob_field(const char *table, const char *tag, int blobno,
                              void *client_record, int blob_isnull,
                              unsigned blob_length);
int get_schema_blob_field_idx_sc(struct schema *sc, int blobno);
int get_schema_blob_field_idx(const char *table, const char *tag, int blobno);
int get_schema_field_blob_idx(const char *table, const char *tag, int fldindex);
void *get_field_ptr_in_buf(struct schema *sc, int idx, const void *buf);
int is_tag_ondisk_sc(struct schema *sc);
int is_tag_ondisk(const char *table, const char *tag);
int fixup_verified_record(const char *dbname, const char *from, char *to);
void backout_schemas(char *tblname);
int broadcast_resume_threads(void);
int have_all_schemas(void);
const char *strtype(int type);
int resolve_tag_name(struct ireq *iq, const char *tagdescr, size_t taglen,
                     struct schema **dynschema, char *tagname,
                     size_t tagnamelen);
void printrecord(char *buf, struct schema *sc, int len);

void *create_blank_record(struct db *db, size_t *length);
int validate_server_record(const void *record, size_t reclen,
                           const struct schema *schema,
                           struct convert_failure *reason);
void init_convert_failure_reason(struct convert_failure *fail_reason);

/* I'm putting these functions in so that javasp.c code can query schema stuff
 * without needing to know the internals of how we store schemas. */
int schema_num_fields(const struct schema *sc);
int find_field_idx_in_tag(const struct schema *tag, const char *field);
const struct field *schema_get_field_n(const struct schema *sc, int index);
const char *field_get_name(const struct field *fld);
int field_get_type(const struct field *fld);
int field_get_length(const struct field *fld);
const void *field_get_buf(const struct field *fld, const void *dta);
const struct field_conv_opts *field_get_conv_opts(const struct field *fld);

/* misc conversion routine flags */
enum { FLAGS_FLIP_ENDIAN, FLAGS_DONT_INVERSE_DESCENDING };

/* Allocated allocate_db_record - used by bdblib for db conversion.
   Call free_db_record to free. */
struct dbrecord {
    char *table;
    int bufsize;
    void *recbuf;

    /* for library's use */
    char *tag;
    struct schema *schema;
};

/* if tag is NULL, assume .ONDISK_new */
struct dbrecord *allocate_db_record(const char *table, const char *tag);
int stag_to_stag_buf(const char *table, const char *fromtag, const char *inbuf,
                     const char *totag, char *tobuf,
                     struct convert_failure *reason);

int stag_to_stag_buf_tz(struct schema *fromsch, const char *table,
                        const char *fromtag, const char *inbuf,
                        const char *totag, char *tobuf,
                        struct convert_failure *reason, const char *tzname);

int stag_to_stag_buf_update(const char *table, const char *fromtag,
                            const char *inbuf, const char *totag, char *tobuf,
                            struct convert_failure *reason);

int stag_to_stag_buf_update_tz(const char *table, const char *fromtag,
                               const char *inbuf, const char *totag,
                               char *tobuf, struct convert_failure *reason,
                               const char *tzname);

int stag_to_stag_buf_flags(const char *table, const char *fromtag,
                           const char *inbuf, const char *totable,
                           const char *totag, char *outbuf, int flags,
                           struct convert_failure *reason);

/* Primary key to foreign key enums */
enum constraint_dir {
    PK2FK, /* Primary key to foreign key: for cascade operations */
    FK2PK  /* Foreign key to primary key: for insert operations */
};

int stag_to_stag_buf_ckey(const char *table, const char *fromtag,
                          const char *inbuf, const char *totable,
                          const char *totag, char *outbuf, int *nulls,
                          enum constraint_dir direction);

int server_type_to_csc2_type_len(int type, int inlen, int *csc2type,
                                 int *csc2len);
int client_type_to_csc2_type(int type, int inlen, int *csc2type);

int describe_update_columns(const char *table, const char *tag, int *updCols);
int remap_update_columns(const char *table, const char *intag,
                         const int *incols, const char *outtag, int *outcols);
void free_blob_buffers(blob_buffer_t *blobs, int nblobs);
const char *get_keynm_from_db_idx(struct db *db, int idx);
int client_type_to_server_type(int);

void loadnullbmp(void *destbmp, size_t destbmpsz, const void *srcbmp,
                 size_t srcbmpsz);

void update_dbstore(struct db *db);

int static_tag_blob_conversion(const char *table, const char *ctag,
                               void *record, blob_buffer_t *blobs,
                               size_t maxblobs);

int compare_indexes(const char *table, FILE *out);

struct dbenv;
void free_db_record(struct dbrecord *db);

void delete_schema(const char *dbname);

void freeschema(struct schema *schema);
void freeschema_internals(struct schema *schema);

struct schema *clone_schema(struct schema *from);

void free_db_and_replace(struct db *db, struct db *newdb);

void err_print_rec(strbuf *buf, void *rec, char *table, char *tag);

int create_key_from_ondisk(struct db *db, int ixnum, char **tail, int *taillen,
                           char *mangled_key, const char *fromtag,
                           const char *inbuf, int inbuflen, const char *totag,
                           char *outbuf, struct convert_failure *reason,
                           const char *tzname);

int create_key_from_ondisk_blobs(struct db *db, int ixnum, char **tail,
                                 int *taillen, char *mangled_key,
                                 const char *fromtag, const char *inbuf,
                                 int inbuflen, const char *totag, char *outbuf,
                                 struct convert_failure *reason,
                                 blob_buffer_t *inblobs, int maxblobs,
                                 const char *tzname);

int create_key_from_ondisk_sch(struct db *db, struct schema *fromsch, int ixnum,
                               char **tail, int *taillen, char *mangled_key,
                               const char *fromtag, const char *inbuf,
                               int inbuflen, const char *totag, char *outbuf,
                               struct convert_failure *reason,
                               const char *tzname);

int create_key_from_ondisk_sch_blobs(
    struct db *db, struct schema *fromsch, int ixnum, char **tail, int *taillen,
    char *mangled_key, const char *fromtag, const char *inbuf, int inbuflen,
    const char *totag, char *outbuf, struct convert_failure *reason,
    blob_buffer_t *inblobs, int maxblobs, const char *tzname);

int create_key_from_ireq(struct ireq *iq, int ixnum, int isDelete, char **tail,
                         int *taillen, char *mangled_key, const char *inbuf,
                         int inbuflen, char *outbuf);

extern pthread_rwlock_t schema_lk;
char* typestr(int type, int len);

#endif
