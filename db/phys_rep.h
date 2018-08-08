#ifndef PHYS_REP_H
#define PHYS_REP_H

// extern variable defn
int gbl_is_physical_replicant;
unsigned int gbl_deferred_phys_update;

int set_repl_db_name(char* host_db);
int add_replicant_host(char *hostname);
int remove_replicant_host(char *hostname);
void cleanup_hosts();
const int start_replication();

void stop_replication();

/* expose as a hook for apply_log */
int apply_log_procedure(unsigned int file, unsigned int offset,
        void* blob, int blob_len, int newfile);

#endif
