#ifndef ALIAS_H
#define ALIAS_H
typedef struct table_alias table_alias_t;
void load_aliases_from_llmeta();
void reload_aliases();
int add_alias(const char *, const char *);
void remove_alias(const char *);
char *get_alias(const char *);
char *get_tablename(const char *);
void dump_alias_info();
#endif 
