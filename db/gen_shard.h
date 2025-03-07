#ifndef _GENERIC_SHARD_H
#define _GENERIC_SHARD_H
int gen_shard_llmeta_add(tran_type *tran, char *tablename, uint32_t numdbs, char **dbnames, 
		uint32_t numcols, char **columns, char **shardnames, struct errstat *);
int gen_shard_update_inmem_db(void *tran, struct dbtable *db, const char *name);
int gen_shard_llmeta_remove(tran_type *tran, char *tablename, struct errstat *err);
#endif
