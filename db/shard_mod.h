#ifndef _SHARD_MOD_H_
#define _SHARD_MOD_H_
#include "errstat.h"
#include <comdb2.h>
typedef struct mod_shard mod_shard_t;
typedef struct mod_view mod_view_t;
typedef struct mod_views mod_views_t;


mod_view_t *create_mod_view(const char *viewname, const char *tablename, uint32_t num_columns, char columns[][MAXCOLNAME], uint32_t num_shards, uint32_t keys[], char shards[][MAXTABLELEN], struct errstat *err); 
int mod_create_inmem_view(mod_view_t *);
int mod_destroy_inmem_view(mod_view_t *);
const char *mod_view_get_viewname(struct mod_view *view);
const char *mod_view_get_tablename(struct mod_view *view);
char **mod_view_get_keynames(struct mod_view *view);
int mod_view_get_num_shards(struct mod_view *view);
int mod_view_get_num_keys(struct mod_view *view);
int mod_view_get_sqlite_view_version(struct mod_view *view);
hash_t *mod_view_get_shards(struct mod_view *view);
int mod_shard_get_mod_val(struct mod_shard *shard);
const char *mod_shard_get_dbname(struct mod_shard *shard);
int mod_shard_llmeta_write(void *tran, mod_view_t *view, struct errstat *err);
int mod_partition_llmeta_erase(void *tran, mod_view_t *view, struct errstat *err);
hash_t *mod_create_all_views();
int mod_views_update_replicant(void *tran, const char *name);
int mod_get_inmem_view(const char *name, mod_view_t **oView);
int is_mod_partition(const char *name);
unsigned long long mod_view_get_version(const char *name);
#endif
