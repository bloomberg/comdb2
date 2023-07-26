#ifndef _SHARD_MOD_H_
#define _SHARD_MOD_H_
#include "errstat.h"
#include <comdb2.h>
typedef struct mod_shard mod_shard_t;
typedef struct mod_view mod_view_t;
typedef struct mod_views mod_views_t;


mod_view_t *create_mod_view(const char *viewname, const char *tablename, char **columns, const char *keyname, uint32_t num_shards, uint32_t num_columns, int32_t keys[], char *shards[], struct errstat *err); 
void free_mod_view(mod_view_t *mView);
int mod_create_inmem_view(mod_view_t *);
int mod_destroy_inmem_view(mod_view_t *);
const char *mod_view_get_viewname(struct mod_view *view);
const char *mod_view_get_tablename(struct mod_view *view);
const char *mod_view_get_keyname(struct mod_view *view);
int mod_view_get_num_shards(struct mod_view *view);
int mod_view_get_num_columns(struct mod_view *view);
char **mod_view_get_columns(struct mod_view *view);
hash_t *mod_view_get_shards(struct mod_view *view);
int mod_shard_get_mod_val(struct mod_shard *shard);
const char *mod_shard_get_dbname(struct mod_shard *shard);
int mod_shard_llmeta_write(void *tran, mod_view_t *view, struct errstat *err);
int mod_shard_llmeta_erase(void *tran, mod_view_t *view, struct errstat *err);
hash_t *mod_create_all_views();
int mod_views_update_replicant(void *tran, const char *name);
char *mod_views_describe_row(mod_view_t *view, const char *prefix, struct errstat *err);
mod_view_t *mod_get_view_by_name(char *name);
#endif
