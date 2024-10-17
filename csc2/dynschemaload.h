#ifndef __DYNSCHEMALOAD_H__
#define __DYNSCHEMALOAD_H__

#include <stdlib.h>
#include <stdio.h>

struct partial_datacopy {
    char *field;
    struct partial_datacopy *next;
};

enum fieldopttypes {
    FLDOPT_DBSTORE = 0,
    FLDOPT_DBLOAD = 1,
    FLDOPT_NULL = 2,
    FLDOPT_PADDING = 3,
    FLDOPT_MAX
};

enum fielddpthtype {
    FLDDPTH_STRUCT = 1,
    FLDDPTH_UNION = 2,
};

typedef struct dpthinfo {
    int struct_type;
    int struct_number;
    char struct_name[64]; /* currently not used */
    int reserved[16];
} dpth_t;

enum dyns_cnst {
    MAX_TAG_LEN = 32,
    MAXTBLS = 32,
};

char *dyns_field_option_text(int option);
void dyns_init_globals();
void dyns_cleanup_globals();
int dyns_load_schema_string(char *schematxt, char *dbname, char *tablename);
int dyns_load_schema(char *filename, char *dbname, char *tblname);
int dyns_form_key(int index, char *record, int recsz, char *key, int keysize);
int dyns_is_idx_dup(int index);
int dyns_is_idx_recnum(int index);
int dyns_is_idx_primary(int index);
int dyns_is_idx_datacopy(int index);
int dyns_is_idx_partial_datacopy(int index);
int dyns_is_idx_uniqnulls(int index);
int dyns_get_idx_count(void);
int dyns_get_idx_size(int index);
int dyns_get_idx_piece(int index, int piece, char *sname, int slen, int *type,
                       int *offset, int *plen, int *descend, char **pexpr);
int dyns_get_idx_piece_count(int index);
int dyns_get_db_num(void);
int dyns_get_dtadir(char *dir, int len);
int dyns_get_db_name(char *name, int len);
int dyns_get_db_tag(char *tag, int len);
int dyns_get_db_table_size(void);
int dyns_get_field_count(void);
int dyns_get_field_info(int fidx, char *name, int namelen, int *type,
                        int *offset, int *elsize, int *fullsize);
int dyns_field_depth(int fidx, dpth_t *dpthinfo, int ndpthsinfo, int *ndpthout);
int dyns_field_type(int fidx);
int dyns_is_field_array(int fidx);
int dyns_get_field_arr_dims(int fidx, int *dims, int ndims, int *nodims);
int dyns_get_idx_partial_datacopy(int index, struct partial_datacopy **partial_datacopy);
int dyns_get_idx_tag(int index, char *tag, int tlen, char **where);

/* calls to work with multiple tables */
char *dyns_get_table_tag(int tidx);
int dyns_is_table_field_array(char *tabletag, int fidx);
int dyns_get_table_field_arr_dims(char *tabletag, int fidx, int *dims,
                                  int ndims, int *nodims);
int dyns_get_table_field_info(char *tabletag, int fidx, char *name, int namelen,
                              int *type, int *offset, int *elsize,
                              int *fullsize, int *arr, int use_server_types);
int dyns_get_table_field_option(char *tag, int fidx, int option,
                                int *value_type, int *value_sz, void *valuebuf,
                                int vbsz, char **func_st);
int dyns_table_field_depth(char *tabletag, int fidx, dpth_t *dpthinfo,
                           int ndpthsinfo, int *ndpthout);
int dyns_get_table_count(void);
int dyns_get_table_tag_size(char *tabletag);
int dyns_get_table_field_count(char *tabletag);

/* period accessors */
int dyns_get_period(int period, int *start, int *end);

/* constraint accessors */
int dyns_get_constraint_count(void);
int dyns_get_constraint_at(int idx, char **consname, char **keyname,
                           int *rulecnt, int *flags);
int dyns_get_constraint_rule(int cidx, int ridx, char **tblname, char **keynm);
int dyns_get_check_constraint_count(void);
int dyns_get_check_constraint_at(int idx, char **consname, char **expr);

/* misc */
void dyns_allow_bools(void);
void dyns_disallow_bools(void);
int dyns_used_bools(void);

void csc2_free_all(void);
void *csc2_malloc(size_t sz);
char *csc2_strdup(char *);
char *csc2_get_errors(void);
char *csc2_get_syntax_errors(void);

/* In addition to stderr, pass any errors to user-defined callback.  Error
 * message is freed when callback returns. */
void dyns_register_error_message_callback(void *opaque,
                                          void (*callback)(void *, char *));

#endif
