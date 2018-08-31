#ifndef PHYS_REP_H
#define PHYS_REP_H
#include <stdlib.h>

// extern variable defn
int gbl_is_physical_replicant;
unsigned int gbl_deferred_phys_update;
extern int gbl_deferred_phys_flag;

int add_replicant_host(char *hostname, char *dbname, size_t tier);
int remove_replicant_host(char *hostname);
void cleanup_hosts();
int start_replication();

int stop_replication();

/* expose as a hook for apply_log */
int apply_log_procedure(unsigned int file, unsigned int offset,
        void* blob, int blob_len, int newfile);

#endif
