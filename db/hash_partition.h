#ifndef _HASH_PARTITION_H_
#define _HASH_PARTITION_H_
#include "errstat.h"
#include <comdb2.h>
typedef struct hash_partition hash_partition_t;
typedef struct hash_view hash_view_t;
typedef struct hash_views hash_views_t;

typedef struct systable_hashpartitions {
    char *name;
    char *shardname;
    int64_t minKey;
    int64_t maxKey;
    int recno;
} systable_hashpartition_t;

hash_view_t *create_hash_view(const char *viewname, const char *tablename, uint32_t num_columns,
                            char columns[][MAXCOLNAME], uint32_t num_partitions,
                            char partitions[][MAXPARTITIONLEN], struct errstat *err);
int hash_create_inmem_view(hash_view_t *);
int hash_destroy_inmem_view(hash_view_t *);
const char *hash_view_get_viewname(struct hash_view *view);
const char *hash_view_get_tablename(struct hash_view *view);
char **hash_view_get_keynames(struct hash_view *view);
int hash_view_get_num_partitions(struct hash_view *view);
int hash_view_get_num_keys(struct hash_view *view);
char** hash_view_get_partitions(struct hash_view *view);
int hash_view_get_sqlite_view_version(struct hash_view *view);
int hash_partition_get_hash_val(struct hash_partition *partition);
const char *hash_partition_get_dbname(struct hash_partition *partition);
int hash_partition_llmeta_write(void *tran, hash_view_t *view, struct errstat *err);
int hash_partition_llmeta_erase(void *tran, hash_view_t *view, struct errstat *err);
hash_t *hash_create_all_views();
int hash_views_update_replicant(void *tran, const char *name);
int hash_get_inmem_view(const char *name, hash_view_t **oView);
int is_hash_partition(const char *name);
int is_hash_partition_table(const char *tablename, hash_view_t **oView);
unsigned long long hash_view_get_version(const char *name);
int hash_systable_collect(void **data, int *nrecords);
void hash_systable_free(void *data, int nrecords);
#endif

