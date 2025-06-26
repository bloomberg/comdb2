#ifndef _GENERIC_SHARD_H
#define _GENERIC_SHARD_H
int gen_shard_add(tran_type *tran, struct dbtable *tbl,
                  char *tablename, uint32_t numdbs, char **dbnames, char **shardnames, 
                  uint32_t numcols, char **columns, struct errstat *);
int gen_shard_rem(tran_type *tran, char *tablename, struct errstat *err);

int gen_shard_add_inmem_tbl(void *tran, struct dbtable *tbl, const char *name);
void gen_shard_rem_inmem_tbl(struct dbtable *tbl);

int gen_shard_update_sqlite(sqlite3 *db, struct errstat *err);
int is_gen_shard(const char *tablename);

int gen_shard_deserialize_shard(char **genshard_name, uint32_t *numdbs, char ***dbnames, char ***shardnames,
                                uint32_t *numcols, char ***columns, char *serializedStr);
#endif
