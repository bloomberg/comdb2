#ifndef _SHARD_MOD_H_
#define _SHARD_MOD_H_
#include "errstat.h"
#include <comdb2.h>
typedef struct mod_shard mod_shard_t;
typedef struct mod_view mod_view_t;
typedef struct mod_views mod_views_t;


mod_view_t *create_mod_view(const char *viewname, const char *tablename, const char *keyname, uint32_t num_shards, uint32_t keys[], char shards[][MAX_DBNAME_LENGTH], struct errstat *err);
int mod_create_inmem_view(mod_view_t *);
int mod_destroy_inmem_view(mod_view_t *);
const char *mod_view_get_viewname(struct mod_view *view);
const char *mod_view_get_tablename(struct mod_view *view);
const char *mod_view_get_keyname(struct mod_view *view);
int mod_view_get_num_shards(struct mod_view *view);
hash_t *mod_view_get_shards(struct mod_view *view);
int mod_shard_get_mod_val(struct mod_shard *shard);
const char *mod_shard_get_dbname(struct mod_shard *shard);
int mod_shard_llmeta_write(void *tran, mod_view_t *view, struct errstat *err);
hash_t *mod_create_all_views();
#endif
