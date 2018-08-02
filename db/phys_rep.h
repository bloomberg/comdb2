#ifndef PHYS_REP_H
#define PHYS_REP_H

// extern variable defn
int gbl_is_physical_replicant;

int set_repl_db_name(char* host_db);
int add_replicant_host(char *hostname);
int remove_replicant_host(char *hostname);
void cleanup_hosts();
const char* start_replication();

/* unescapable function meant to keep local replicant in sync */
void* keep_in_sync(void* args);
void stop_sync();

/* expose as a hook for apply_log */
int apply_log_procedure(unsigned int file, unsigned int offset,
        void* blob, int blob_len, int newfile);

#endif
