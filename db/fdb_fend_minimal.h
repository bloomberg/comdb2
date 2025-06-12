/*
 * fdb_fend_minimal.h
 *
 * This header provides only the subset of declarations from `db/fdb_fend.h`.
 * This limits compile-time depenedencies, since fdb_fend.h depends on sqlite.
 */

#ifndef FDB_FEND_MINIMAL_H
#define FDB_FEND_MINIMAL_H

struct fdb;

/**
 * Get the comdb2 semantic version that an fdb is running.
 *
 * On success, *version points to a malloc’d string which the caller
 * must free.
 */
int fdb_get_server_semver(const struct fdb * const fdb, const char **version);

/**
 * Retrieve the “dbname” portion of a foreign‐db object.
 */
const char *fdb_dbname_name(const struct fdb * const fdb);

/**
 * Retrieve the routing‐class name for a foreign‐db object.
 */
const char *fdb_dbname_class_routing(const struct fdb * const fdb);

/**
 * Check if this fdb is local to the running node.
 */
int is_local(const struct fdb *fdb);

/**
 * Create a transient fdb object (not linked into global cache).
 * Caller is responsible for later calling destroy_local_fdb().
 */
int create_local_fdb(const char *fdb_name, struct fdb **p_fdb);

/**
 * Destroy a transient fdb object created via create_local_fdb().
 */
void destroy_local_fdb(struct fdb *fdb);

enum fdb_errors {
    FDB_NOERR = 0 /* NO ERROR */
    ,
    FDB_ERR_GENERIC = -1 /* failure, generic */
    ,
    FDB_ERR_WRITE_IO = -2 /* failure writing on a socket */
    ,
    FDB_ERR_READ_IO = -3 /* failure reading from a socket */
    ,
    FDB_ERR_MALLOC = -4 /* failed allocation */
    ,
    FDB_ERR_UNSUPPORTED = -5 /* recognizing a protocol option */
    ,
    FDB_ERR_TRAN_IO =
        -6 /* failed communication with a node involved in a transaction*/
    ,
    FDB_ERR_FDB_VERSION =
        -7 /* sent by FDB_VER_CODE_VERSION and above to downgrade protocol */
    ,
    FDB_ERR_FDB_NOTFOUND = -8 /* fdb name not found */
    ,
    FDB_ERR_FDB_TBL_NOTFOUND = -9 /* fdb found but table not existing */
    ,
    FDB_ERR_CLASS_UNKNOWN = -10 /* explicit class in fdb name, unrecognized */
    ,
    FDB_ERR_CLASS_DENIED =
        -11 /* denied access to the class, either implicit or explicit */
    ,
    FDB_ERR_REGISTER_NOTFOUND = -12 /* unable to access comdb2db */
    ,
    FDB_ERR_REGISTER_NODB = -13 /* no information about fdb name in comdb2db */
    ,
    FDB_ERR_REGISTER_NONODES = -14 /* no nodes available for the fdb */
    ,
    FDB_ERR_REGISTER_IO = -15 /* failure talking to comdb2db */
    ,
    FDB_ERR_REGISTER_NORESCPU = -16 /* no known node is rescpu */
    ,
    FDB_ERR_PTHR_LOCK = -17 /* pthread locks are mashed */
    ,
    FDB_ERR_CONNECT = -18 /* failed to connect to db */
    ,
    FDB_ERR_CONNECT_CLUSTER = -19 /* failed to connect to cluster */
    ,
    FDB_ERR_BUG = -20 /* bug in the code, should not see this */
    ,
    FDB_ERR_SOCKPOOL = -21 /* sockpool return error */
    ,
    FDB_ERR_PI_DISABLED = -22 /* foreign table has partial indexes but the
                                 feature is disabled locally */
    ,
    FDB_ERR_EXPRIDX_DISABLED =
    -23 /* foreign table has expressions indexes but the feature is
                   disabled locally */
    ,
    FDB_ERR_INDEX_DESCRIBE = -24 /* failed to describe index */
    ,
    FDB_ERR_SSL = -25 /* SSL configuration error */
    ,
    FDB_ERR_ACCESS = -26 /* Access error */
    ,
    FDB_ERR_TRANSIENT_IO = -27 /* Temporary IO failure */
};

#endif /* FDB_FEND_MINIMAL_H */
