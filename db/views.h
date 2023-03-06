#ifndef _VIEWS_H_
#define _VIEWS_H_

/**
 * Initial prototype to support multi-table views, and support for sharding
 *
 * The scheme used will be time based
 *
 */

#include "comdb2.h"
#include <sqlite3.h>
#include "errstat.h"
#include "cron.h"

enum view_type { VIEW_TIME_PARTITION };

enum TIMEPART_ROLLOUT_TYPE {
    TIMEPART_ROLLOUT_ADDDROP = 0,
    TIMEPART_ROLLOUT_TRUNCATE = 1
};

enum view_partition_period {
    VIEW_PARTITION_INVALID = 0,
    VIEW_PARTITION_DAILY,
    VIEW_PARTITION_WEEKLY,
    VIEW_PARTITION_MONTHLY,
    VIEW_PARTITION_YEARLY,
    VIEW_PARTITION_TEST2MIN,
    VIEW_PARTITION_MANUAL
};

#define IS_TIMEPARTITION(p)                                                    \
    ((p) == VIEW_PARTITION_DAILY || (p) == VIEW_PARTITION_WEEKLY ||            \
     (p) == VIEW_PARTITION_MONTHLY || (p) == VIEW_PARTITION_YEARLY ||          \
     (p) == VIEW_PARTITION_TEST2MIN)

enum view_partition_errors {
    VIEW_NOERR = 0 /* no error */
    ,
    VIEW_ERR_GENERIC = -1 /* generic error, please avoid */
    ,
    VIEW_ERR_MALLOC = -2 /* malloc error */
    ,
    VIEW_ERR_EXIST = -3 /* exist when insert new, not exist when update */
    ,
    VIEW_ERR_UNIMPLEMENTED = -4 /* functionality not implemented */
    ,
    VIEW_ERR_PARAM = -5 /* setting wrong parameter */
    ,
    VIEW_ERR_BUG = -6 /* bug in code */
    ,
    VIEW_ERR_LLMETA = -7 /* error in I/O with llmeta backend */
    ,
    VIEW_ERR_SQLITE = -8 /* error in sqlite module while processing views */
    ,
    VIEW_ERR_PURGE = -9 /* error with removing oldest shards */
    ,
    VIEW_ERR_SC = -10 /* error with schema change */
    ,
    VIEW_ERR_CREATE = -11 /* error with pthread create */
};

typedef struct timepart_view timepart_view_t;

#define COMDB2_UPDATEABLE_VIEWS 1

enum views_trigger_op {
    VIEWS_TRIGGER_QUERY = 0,
    VIEWS_TRIGGER_INSERT = 1,
    VIEWS_TRIGGER_DELETE = 2,
    VIEWS_TRIGGER_UPDATE = 3
};

typedef struct timepart_sc_arg {
    struct schema_change_type *s;
    const char *view_name;
    int indx;
    int nshards;
    int rc;
    void *tran; /*remove?*/
} timepart_sc_arg_t;

extern int gbl_partitioned_table_enabled;
extern int gbl_merge_table_enabled;

/**
 * Initialize the views
 *
 */
timepart_views_t *timepart_views_init(struct dbenv *dbenv);

/**
 * Process a cson command
 *
 * Format:
 * {
 *    "COMMAND"   : "CREATE|DESTROY",
 *    "VIEWNAME"  : "a_name",
 *    "TYPE"      : "TIME BASED",
 *    "RETENTION" : n,
 *    "TABLE0"    : "an_existing_table_name"
 * }
 *
 * This is done under WRLOCK(schema_lk) !!!
 *
 */
int views_do_partition(void *tran, timepart_views_t *views, const char *name,
                       const char *cmd, struct errstat *err);

/**
 * Add timepart view, this is a schema change driven event
 *
 */
int timepart_add_view(void *tran, timepart_views_t *views,
                      timepart_view_t *view, struct errstat *err);

/**
 * Deserialize a CSON string into a in-memory view
 *
 */
timepart_view_t *timepart_deserialize_view(const char *str,
                                           struct errstat *err);

/**
 * Serialize an in-memory view into a CSON string
 *
 * NOTE: this appends to *out string;  Please set it to NULL
 * for new strings
 */
int timepart_serialize_view(timepart_view_t *view, int *plen, char **out,
                            int user_friendly);

/**
 *  Write a CSON representation overriding the current llmeta
 *
 *  NOTE: writing a NULL or 0 length string deletes existing entry if any
 *
 */
int views_write(const char *str);

/**
 *  Write a CSON representation of a view
 *  The view is internally saved as a parameter "viewname" for the table
 *  "sys_views".
 *  "override" set to 1 allows the function to behave like an upsert (if
 *  old view exists, it is overwritten).  Otherwise, an error is returned.
 *
 *  NOTE: writing a NULL or 0 length string deletes existing entry if any
 */
int views_write_view(void *tran, const char *viewname, const char *str,
                     int override);

/**
 * Read a CSON representation from llmeta
 *
 */
int views_read(char **pstr);

/**
 * Read a view CSON representation from llmeta
 *
 */
int views_read_view(void *tran, const char *name, char **pstr);

/**
 * Read the llmeta cson representation of all the views
 *
 */
char *views_read_all_views(void);

/**
 * Create in-memory view set corresponding to the llmeta saved value
 *
 */
timepart_views_t *views_create_all_views(void);

/**
 * Serialize in-memory view repository into a CSON string
 *
 */
int timepart_serialize(timepart_views_t *views, char **out, int user_friendly);

/**
 * Handle views change on replicants
 * - tran has the replication gbl_rep_lockid lockerid.
 *
 * NOTE: this is done from scdone_callback, so we have WRLOCK(schema_lk)
 */
int views_handle_replicant_reload(void *tran, const char *name);

/**
 * Deserialize a CSON string into a in-memory view repository
 *
 */
timepart_views_t *timepart_deserialize(const char *str, struct errstat *err);

/**
 * Create a sqlite view
 *
 */
int views_sqlite_add_view(timepart_view_t *view, sqlite3 *db,
                          struct errstat *err);

/**
 * Delete a sqlite view
 *
 *
 */
int views_sqlite_del_view(timepart_view_t *view, sqlite3 *db,
                          struct errstat *err);

/**
 * Populate an sqlite db with views
 *
 */
int views_sqlite_update(timepart_views_t *views, sqlite3 *db,
                        struct errstat *err, int lock);

/**
 * Create a new shard;
 * If too many shards, we trim the oldest one
 *
 */
int views_rollout(timepart_views_t *views, timepart_view_t *view,
                  struct errstat *err);

/**
 * Manual partition roll
 *
 */
int views_do_rollout(timepart_views_t *views, const char *name);

/**
 * Manual partition purge, if any
 *
 */
int views_do_purge(timepart_views_t *views, const char *name);

/**
 * Signal looping workers of views of db event like exiting
 *
 */
void views_signal(timepart_views_t *views);

/**
 * Returns various information about a partition, based on option
 * Things like CSON representation for the partition, list of tables ...
 *
 * Return, if any, is malloc-ed!
 */
char *comdb2_partition_info(const char *partition_name, const char *option);

enum view_partition_period name_to_period(const char *str);

const char *period_to_name(enum view_partition_period period);

int convert_from_start_string(enum view_partition_period period,
                              const char *str);

char *convert_to_start_string(enum view_partition_period period, int value,
                              char *buf, int buflen);

char *build_createcmd_json(char **out, int *len, const char *name,
                           const char *tablename, uint32_t period,
                           int retention, uint64_t starttime);

char *build_dropcmd_json(char **out, int *len, const char *name);

/**
 * List all partitions currently configured 
 *
 */
void comdb2_partition_info_all(const char *option);

/**
 * Check if the name already exists as a table or as a shard!
 *
 */
int comdb2_partition_check_name_reuse(const char *tblname, char **partname, int *indx);

/**
 * Run "func" for each shard, starting with "first_shard".
 * Callback receives the name of the shard and argument struct
 * NOTE: first_shard == -1 means include the next shard if
 * already created
 *
 */
int timepart_foreach_shard(const char *view_name,
                           int func(const char *, timepart_sc_arg_t *),
                           timepart_sc_arg_t *arg, int first_shard);

/**
 * Run "func" for each shard of a partition
 * If partition is visible to the rest of the world, it is locked by caller.
 * Otherwise, this can run unlocked (temp object generated by sc).
 *
 */
int timepart_foreach_shard_lockless(timepart_view_t *view,
                                    int func(const char *, timepart_sc_arg_t *),
                                    timepart_sc_arg_t *arg);

/**
 * Queue up the necessary events to rollout time partitions 
 * Done during restart and master swing 
 *
 */
int views_cron_restart(timepart_views_t *views);


/**
 * Update the retention of the existing partition
 *
 */
int timepart_update_retention(void *tran, const char *name, int value, struct errstat *err);

/**
 * Locking the views subsystem, needed for ordering locks with schema
 *
 */
void views_lock(void);
void views_unlock(void);

/**
 * Dump the timepartition json configuration
 * Used for schema copy only
 * Returns: 0 - no tps; 1 - has tps
 *
 */
int timepart_dump_timepartitions(FILE *dest);

/**
 * Create timepartition llmeta entries based on file configuration
 *
 */
int timepart_apply_file(const char *filename);

/**
 * Get number of shards
 *
 */
int timepart_get_num_shards(const char *view_name);

/**
 * Return a description of the timepart scheduler
 *
 */
char *timepart_describe(sched_if_t *impl);

/**
 * Timepart event description
 *
 */
char *timepart_event_describe(sched_if_t *impl, cron_event_t *event);

/**
 * Timepartition access routines
 *
 */
int timepart_shards_grant_access(bdb_state_type *bdb_state, void *tran, char
                                 *name, char *user, int access_type);
int timepart_shards_revoke_access(bdb_state_type *bdb_state, void *tran, char
                                  *name, char *user, int access_type);

/**
 * Check if name is time partition
 * NOTE: partition repository is temporary locked; answer can
 * change afterwards
 *
 */
int timepart_is_partition(const char *name);

/**
 * Check if a table name is the next shard for a time partition
 * and if so, returns the pointer to the partition name
 * NOTE: this is expensive, so only use it in schema resume
 * or recovery operations that lack information about the underlying
 * table (for regular operations, pass down this information from caller
 * instead of calling this function). Recovery includes sc_callbacks
 * NOTE2: it grabs views repository
 *
 */
const char *timepart_is_next_shard(const char *shardname,
                                   unsigned long long *version);

/**
 * Create a view object with the specified parameters
 * Called also internally when loading from llmeta
 * NOTE: returns allocated name of the partition in
 * "partitition_name", so we can point to it from dbtable objects
 *
 */
timepart_view_t *timepart_new_partition(const char *name, int period,
                                        int retention, long long start,
                                        uuid_t *uuid,
                                        enum TIMEPART_ROLLOUT_TYPE rolltype,
                                        const char **partition_name,
                                        struct errstat *err);

/**
 * Populates shard information based on preset number of shards
 * view->retention
 *
 */
int timepart_populate_shards(timepart_view_t *view, struct errstat *err);

/**
 * Create partition llmeta entry
 *
 */
int partition_llmeta_write(void *tran, timepart_view_t *view, int override,
                           struct errstat *err);

/**
 * Delete partition llmeta entry
 *
 */
int partition_llmeta_delete(void *tran, const char *name, struct errstat *err);

/**
 * Free a time partition object
 *
 */
void timepart_free_view(timepart_view_t *view);

/**
 * Link/unlink the time partition object in the global tpt repository
 *
 */
int timepart_create_inmem_view(timepart_view_t *view);
int timepart_destroy_inmem_view(const char *name);

/**
 * Get number of partitions
 *
 */
int timepart_num_views(void);

/**
 * Get name of view "i"
 *
 */
const char *timepart_name(int i);

/**
 * Get the malloced name of shard "i" for partition "p" if exists
 * NULL otherwise
 * If version provided, and shard exists, return its schema version
 *
 */
char *timepart_shard_name(const char *p, int i, int aliased,
                          unsigned long long *version);

/**
 * Alias the existing table so we can create a partition with
 * same name
 *
 */
void timepart_alias_table(timepart_view_t *view, struct dbtable *db);

/**
 * Create llmeta entries for the new shard for user access rights
 * and table version
 *
 */
int timepart_clone_access_version(tran_type *tran,
                                  const char *timepartition_name,
                                  const char *tablename,
                                  unsigned long long version);

/**
 * Get the malloc-ed name of shard "i" for partition "p" if it exists
 * NULL otherwise
 * If version is provided, and shard exists, return its schema version
 *
 */
char *timepart_shard_name(const char *p, int i, int aliased,
                          unsigned long long *version);

/**
 * Make partition visible (create in-mem structure and
 * notify replicants)
 */
int partition_publish(tran_type *tran, struct schema_change_type *sc);

/**
 * Revert the results of partition publish (for cases when transaction fails
 */
void partition_unpublish(struct schema_change_type *sc);


/* called for a truncate rollout before finalize commits the tran */
int partition_truncate_callback(tran_type *tran, struct schema_change_type *s);

/* return the default value for a manual partition */
int logical_partition_next_rollout(const char *name);

#endif
