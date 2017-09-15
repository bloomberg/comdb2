#ifndef _VIEWS_H_
#define _VIEWS_H_

/**
 * Initial prototype to support multi-table views, and support for sharding
 *
 * The scheme used will be time based
 *
 */

#include <sqlite3.h>
#include "comdb2.h"
#include "errstat.h"
#include "views_cron.h"

enum view_type { VIEW_TIME_PARTITION };

enum view_timepart_period {
    VIEW_TIMEPART_INVALID = 0,
    VIEW_TIMEPART_DAILY,
    VIEW_TIMEPART_WEEKLY,
    VIEW_TIMEPART_MONTHLY,
    VIEW_TIMEPART_YEARLY,
    VIEW_TIMEPART_TEST2MIN
};

enum view_timepart_errors {
    VIEW_NOERR = 0,        /* no error */
    VIEW_ERR_GENERIC = -1, /* generic error, please avoid */
    VIEW_ERR_MALLOC = -2,  /* malloc error */
    VIEW_ERR_EXIST = -3,   /* exist when insert new, not exist when update */
    VIEW_ERR_UNIMPLEMENTED = -4, /* functionality not implemented */
    VIEW_ERR_PARAM = -5,         /* setting wrong parameter */
    VIEW_ERR_BUG = -6,           /* bug in code */
    VIEW_ERR_LLMETA = -7,        /* error in I/O with llmeta backend */
    VIEW_ERR_SQLITE = -8,  /* error in sqlite module while processing views */
    VIEW_ERR_PURGE = -9,   /* error with removing oldest shards */
    VIEW_ERR_SC = -10,     /* error with schema change */
    VIEW_ERR_CREATE = -11, /* error with pthread create */
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
int views_write(const const char *str);

/**
 *  Write a CSON representation of a view
 *  The view is internally saved as a parameter "viewname" for the table
 *"sys_views"
 *
 *  NOTE: writing a NULL or 0 length string deletes existing entry if any
 *
 */
int views_write_view(void *tran, const char *viewname, const char *str);

/**
 * Read a CSON representation from llmeta
 *
 */
int views_read(char **pstr);

/**
 * Read a view CSON representation from llmeta
 *
 */
int views_read_view(const char *name, char **pstr);

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
 *
 * NOTE: this is done from scdone_callback, which gives us a big
 *WRLOCK(schema_lk)
 */
int views_handle_replicant_reload(const char *name);

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

enum view_timepart_period name_to_period(const char *str);

const char *period_to_name(enum view_timepart_period period);

int convert_time_string_to_epoch(const char *time_str);

char *convert_epoch_to_time_string(int epoch, char *buf, int buflen);

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
 * Best effort to validate a view;  are the table present?
 * Is there another partition colliding with it?
 *
 */
int views_validate_view(timepart_views_t *views, timepart_view_t *view, struct errstat *err);

/** 
 * Check if a name is a shard 
 *
 */
int timepart_is_shard(const char *name, int lock);

/** 
 * Check if a name is a timepart
 *
 */
int timepart_is_timepart(const char *name, int lock);

/**
 * Time partition schema change resume
 *
 */
int timepart_resume_schemachange(int check_llmeta(const char *));

/**
 * During resume, we need to check at if the interrupted alter made any
 * progress, and continue with that shard
 *
 */
int timepart_schemachange_get_shard_in_progress(const char *view_name,
                                                int check_llmeta(const char *));

/**
 * Run "func" for each shard, starting with "first_shard".
 * Callback receives the name of the shard and argument struct
 *
 */
int timepart_foreach_shard(const char *view_name,
                           int func(const char *, timepart_sc_arg_t *),
                           timepart_sc_arg_t *arg, int first_shard);

/**
 * Under views lock, call a function for each shard
 * 
 */
int timepart_for_each_shard(const char *name,
      int (*func)(const char *shardname));


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
 * Get the name of the newest shard, and optionally its version
 *
 */
char *timepart_newest_shard(const char *view_name, unsigned long long *version);

#endif

