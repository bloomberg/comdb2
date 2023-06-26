#ifndef _SHARD_MOD_H_
#define _SHARD_MOD_H_
#include "errstat.h"
#include <comdb2.h>
typedef struct mod_shard mod_shard_t;
typedef struct mod_view mod_view_t;
typedef struct mod_views mod_views_t;


mod_view_t *create_mod_view(char *name, char *keyname, uint32_t num_shards, uint32_t keys[], char shards[][MAX_DBNAME_LENGTH], struct errstat *err);
int mod_create_inmem_view(mod_view_t *);
int mod_destroy_inmem_view(mod_view_t *);
const char *mod_view_get_name(struct mod_view *shard);
#endif
