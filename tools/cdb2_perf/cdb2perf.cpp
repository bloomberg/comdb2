#include <iostream>
#include <fstream>
#include <zlib.h>
#include <cson_amalgamation_core.h>
#include <cstdio>
#include <string>
#include <cdb2api.h>
#include <sstream>
#include <time.h>
#include <cstring>
#include <set>
#include <map>
#include <uuid/uuid.h>
#include <limits.h>


// Try to record the streams of logged database queries efficiently. There's too much data to log and 
// index everything.  So we log the bulk unparsed data as a block with a unique blockid. We remember all 
// the unique values of important fields that occured, and index those, with a pointer to the block where 
// they occur.

// Take 1 - just read and log a single block.  Corresponding module in db in db/eventlog.c.
// The finished version should poll database log files, and occasionally (based on time/size) tell the 
// db to roll the current file.  Rolled files can be processed (block per file), and deleted.
// Not sure whether to keep the raw data in a blob, or in a file (blobs replicate for free, files
// are more efficient).

// query events:
// {
//    "context" : [
//       "cdb2sql"
//    ],
//    "perf" : {
//       "runtime" : 139
//    },
//    "type" : "sql",
//    "rows" : 1,
//    "time" : 1497140758224992,
//    "fingerprint" : "71ddfd74d14d1adc93603142a36c5e0c",
//    "host" : "xps"
// }


struct dbstream {
    cdb2_hndl_tp *dbconn;

    std::string source;
    std::string dbname;

    // remember fingerprints we saw since start
    std::set<std::string> known_fingerprints;

    // stats since the last time the block was flushed
    int64_t mintime, maxtime;
    int64_t count;
    std::map<std::string, int64_t> fingerprints;  // known_fingerprints don't reset at each block, this does
    std::map<std::string, int64_t> contexts;

    dbstream(std::string _dbname, std::string s) : 
        source(s), 
        dbname(_dbname),
        mintime(LLONG_MAX),
        maxtime(0) {}
};


// TODO: these are stolen from cdb2_sqlreplay. Should have a single source
//       for cson convenience routines.
static const char *get_strprop(const cson_value *objval, const char *key) {
    cson_object *obj;
    cson_value_fetch_object(objval, &obj);
    cson_value *propval = cson_object_get(obj, key);
    if (propval == nullptr || !cson_value_is_string(propval))
        return nullptr;
    cson_string *cstr;
    cson_value_fetch_string(propval, &cstr);
    return cson_string_cstr(cstr);
}

bool get_intprop(const cson_value *objval, const char *key, int64_t *val) {
    cson_object *obj;
    cson_value_fetch_object(objval, &obj);
    cson_value *propval = cson_object_get(obj, key);
    if (propval == nullptr || !cson_value_is_integer(propval))
        return true;
    int rc = cson_value_fetch_integer(propval, (cson_int_t*) val);
    return rc != 0;
}

bool have_json_key(cson_value *objval, const char *key) {
    cson_object *obj;
    cson_value_fetch_object(objval, &obj);
    cson_value *propval = cson_object_get(obj, key);
    return propval != nullptr;
}

// Does the database know about this fingerprint for this db?
bool db_knows_fingerprint(dbstream &db, std::string &fingerprint) {
    std::string sql = "select 1 from querytypes where dbname=@dbname and fingerprint=@fingerprint";
    bool knows = false;

    cdb2_clearbindings(db.dbconn);
    cdb2_bind_param(db.dbconn, "dbname", CDB2_CSTRING, db.dbname.c_str(), db.dbname.size());
    cdb2_bind_param(db.dbconn, "fingerprint", CDB2_CSTRING, fingerprint.c_str(), fingerprint.size());
    int rc = cdb2_run_statement(db.dbconn, sql.c_str());
    // Be conservative - if we get an error, assume we don't know about it.
    if (rc) 
        return false;
    rc = cdb2_next_record(db.dbconn);
    if (rc == CDB2_OK)
        knows = true;
    while (rc == CDB2_OK)
        rc = cdb2_next_record(db.dbconn);
    if (rc != CDB2_OK_DONE)
        knows = false;

    return knows;
}

bool fingerprint_new_to_me(dbstream &db, std::string &fingerprint) {
    if (db.known_fingerprints.find(fingerprint) != db.known_fingerprints.end()) {
        return false;
    }
    if (db_knows_fingerprint(db, fingerprint)) {
        db.known_fingerprints.insert(fingerprint);
        return false;
    }
    return true;
}

cdb2_client_datetimeus_t totimestamp(int64_t timestamp) {
    struct tm t;
    time_t tval = timestamp / 1000000;
    cdb2_client_datetimeus_t out{0};

    gmtime_r(&tval, &t);

    out.tm.tm_year = t.tm_year;
    out.tm.tm_mon = t.tm_mon;
    out.tm.tm_mday = t.tm_mday;
    out.tm.tm_hour = t.tm_hour;
    out.tm.tm_min = t.tm_min;
    out.tm.tm_sec = t.tm_sec;
    out.usec = timestamp % 1000000;
    strcpy(out.tzname, "Etc/UTC");
    return out;
}

void record_new_fingerprint(dbstream &db, int64_t timestamp, std::string &fingerprint, std::string &sqlstr) {
    cdb2_client_datetimeus_t t = totimestamp(timestamp);
    std::string sql = "insert into querytypes(fingerprint, dbname, first_seen, sql) values(@fingerprint, @dbname, @first_seen, @sql)";
    // std::cout << "timestamp " << timestamp << " fingerprint " << fingerprint << " sql " << sql << std::endl;

    cdb2_clearbindings(db.dbconn);
    cdb2_bind_param(db.dbconn, "dbname", CDB2_CSTRING, db.dbname.c_str(), db.dbname.size());
    cdb2_bind_param(db.dbconn, "fingerprint", CDB2_CSTRING, fingerprint.c_str(), fingerprint.size());
    cdb2_bind_param(db.dbconn, "first_seen", CDB2_DATETIMEUS, &t, sizeof(t));
    cdb2_bind_param(db.dbconn, "sql", CDB2_CSTRING, sqlstr.c_str(), sqlstr.size());

    int rc = cdb2_run_statement(db.dbconn, sql.c_str());
    /* ignore duplicates */
    if (rc == CDB2ERR_DUPLICATE)
        rc = 0;
    if (rc)
        std::cerr << "record_new_fingerprint " << fingerprint << " rc " << rc << " " << cdb2_errstr(db.dbconn) << std::endl;
}

void handle_newsql(dbstream &db, const std::string &blockid, const cson_value *v) {
    int64_t timestamp;
    const char *s;

    s = get_strprop(v, "fingerprint");
    if (s == nullptr)
        return;
    std::string fingerprint(s);
    s = get_strprop(v, "sql");
    if (s == NULL)
        return;
    std::string sql(s);
    if (get_intprop(v, "time", &timestamp))
        return;

    if (fingerprint_new_to_me(db, fingerprint))
        record_new_fingerprint(db, timestamp, fingerprint, sql);
}

// Just collect information from the event, and record a summary.
void handle_sql(dbstream &db, const std::string &blockid, const cson_value *v) {
    int64_t t;
    if (get_intprop(v, "time", &t))
        return;
    if (t > db.maxtime)
        db.maxtime = t;
    if (t < db.mintime)
        db.mintime = t;

    const char *s = get_strprop(v, "fingerprint");
    if (s == nullptr)
        return;
    std::string fp(s);
    auto it = db.fingerprints.find(fp);
    if (it == db.fingerprints.end()) {
        db.fingerprints.insert(std::pair<std::string, int>(fp, 1));
    }
    else {
        it->second++;
    }

    const cson_object *obj = cson_value_get_object(v);
    const cson_value *contextv = cson_object_get(obj, "context");
    if (contextv == nullptr || !cson_value_is_array(contextv))
        return;
    cson_array *arr = cson_value_get_array(contextv);
    unsigned int len;
    if (cson_array_length_fetch(arr, &len))
        return;
    for (unsigned int i = 0; i < len; i++) {
        cson_value *c = cson_array_get(arr, i);
        if (c != nullptr && cson_value_is_string(c)) {
            cson_string *cs;
            cson_value_fetch_string(c, &cs);
            const char *s = cson_string_cstr(cs);
            std::string context(s);
            auto it = db.contexts.find(context);
            if (it == db.contexts.end())
                db.contexts.insert(std::pair<std::string, int>(context, 1));
            else
                it->second++;
        }
    }
}

void process_event(dbstream &db, const std::string &blockid, const std::string &json) {
    int rc;
    cson_parse_info info;
    cson_parse_opt opt = { .maxDepth = 32, .allowComments = 0 };
    cson_value *v;
    rc = cson_parse_string(&v, json.c_str(), json.size(), &opt, &info);
    if (rc)
        return;

    const char *s = get_strprop(v, "type");
    if (s == nullptr)
        return;
    std::string type(s);

    if (type == "newsql") {
        handle_newsql(db, blockid, v);
    }
    else if (type == "sql") {
        handle_sql(db, blockid, v);
    }
}

std::ostream& operator<<(std::ostream &os, const std::pair<std::string, int> &obj) {
    os << obj.first << " -> " << obj.second;
    return os;
}

void dump(dbstream &db, const std::string &blockid) {
    std::cout << "block: " << blockid << std::endl;
    std::cout << "contexts:" << std::endl;
    for (auto it = db.contexts.begin(); it != db.contexts.end(); ++it) {
        std::cout << *it << std::endl;
    }
    std::cout << "fingerprints:" << std::endl;
    for (auto it = db.fingerprints.begin(); it != db.fingerprints.end(); ++it) {
        std::cout << *it << std::endl;
    }
}

bool storeblock(const dbstream &db, const std::string &blockid, std::string block) {
    const char *sql = "insert into blocks(id, start, end, dbname, block) values(@id, @start, @end, @dbname, @block)";
    cdb2_client_datetimeus_t start, end;
    start = totimestamp(db.mintime);
    end = totimestamp(db.maxtime);
    cdb2_clearbindings(db.dbconn);
    cdb2_bind_param(db.dbconn, "id", CDB2_CSTRING, blockid.c_str(), blockid.size());
    cdb2_bind_param(db.dbconn, "start", CDB2_DATETIMEUS, &start, sizeof(start));
    cdb2_bind_param(db.dbconn, "end", CDB2_DATETIMEUS, &end, sizeof(end));
    cdb2_bind_param(db.dbconn, "dbname", CDB2_CSTRING, db.dbname.c_str(), db.dbname.size());
    cdb2_bind_param(db.dbconn, "block", CDB2_BLOB, block.c_str(), block.size());
    int rc = cdb2_run_statement(db.dbconn, sql);
    if (rc) {
        std::cerr << "write block rc " << rc << " " << cdb2_errstr(db.dbconn) << std::endl;
        return false;
    }
    return true;
}

bool storequeries(const dbstream &db, const std::string &blockid) {
    for (auto it : db.fingerprints) {
        const char *sql = "insert into queries(fingerprint, fingerprint_count, dbname, blockid, start, end) values(@fingerprint, @fingerprint_count, @dbname, @blockid, @start, @end)";

        cdb2_client_datetimeus_t start = totimestamp(db.mintime);
        cdb2_client_datetimeus_t end = totimestamp(db.maxtime);
        
        cdb2_clearbindings(db.dbconn);
        cdb2_bind_param(db.dbconn, "fingerprint", CDB2_CSTRING, it.first.c_str(), it.first.size());
        cdb2_bind_param(db.dbconn, "fingerprint_count", CDB2_INTEGER, &it.second, sizeof(it.second));
        cdb2_bind_param(db.dbconn, "dbname", CDB2_CSTRING, db.dbname.c_str(), db.dbname.size());
        cdb2_bind_param(db.dbconn, "blockid", CDB2_CSTRING, blockid.c_str(), blockid.size());
        cdb2_bind_param(db.dbconn, "start", CDB2_DATETIMEUS, &start, sizeof(start));
        cdb2_bind_param(db.dbconn, "end", CDB2_DATETIMEUS, &end, sizeof(end));
        int rc = cdb2_run_statement(db.dbconn, sql);
        if (rc) {
            std::cerr << "write fingerprint rc " << rc << " " << cdb2_errstr(db.dbconn) << std::endl;
            return false;
        }
    }

    for (auto it : db.contexts) {
        const char *sql = "insert into contexts(context, context_count, dbname, blockid, start, end) values(@context, @context_count, @dbname, @blockid, @start, @end)";

        cdb2_client_datetimeus_t start = totimestamp(db.mintime);
        cdb2_client_datetimeus_t end = totimestamp(db.maxtime);
        
        cdb2_clearbindings(db.dbconn);
        cdb2_bind_param(db.dbconn, "context", CDB2_CSTRING, it.first.c_str(), it.first.size());
        cdb2_bind_param(db.dbconn, "context_count", CDB2_INTEGER, &it.second, sizeof(it.second));
        cdb2_bind_param(db.dbconn, "dbname", CDB2_CSTRING, db.dbname.c_str(), db.dbname.size());
        cdb2_bind_param(db.dbconn, "blockid", CDB2_CSTRING, blockid.c_str(), blockid.size());
        cdb2_bind_param(db.dbconn, "start", CDB2_DATETIMEUS, &start, sizeof(start));
        cdb2_bind_param(db.dbconn, "end", CDB2_DATETIMEUS, &end, sizeof(end));
        int rc = cdb2_run_statement(db.dbconn, sql);
        if (rc) {
            std::cerr << "write fingerprint rc " << rc << " " << cdb2_errstr(db.dbconn) << std::endl;
            return false;
        }
    }
    return true;
}

// TODO: exception on error
void store(const dbstream &db, const std::string &blockid, std::string block) {
    cdb2_clearbindings(db.dbconn);
    int rc = cdb2_run_statement(db.dbconn, "begin");
    if (rc) {
        std::cerr << "begin rc " << rc << " " << cdb2_errstr(db.dbconn) << std::endl;
    }
    if (!storeblock(db, blockid, block))
        goto abort;
    if (!storequeries(db, blockid))
        goto abort;
    rc = cdb2_run_statement(db.dbconn, "commit");
    if (rc) {
        std::cerr << "commit rc " << rc << " " << cdb2_errstr(db.dbconn) << std::endl;
    }
    return;

abort:
    cdb2_run_statement(db.dbconn, "abort");
}

int main(int argc, char *argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: file" << std::endl;
        return 1;
    }
    char *fname = argv[1];

    dbstream db("mikedb", fname);
    gzFile ingz = gzopen(fname, "r");

    char buf[1024];
    int bytes = 0;

    std::stringstream block;

    while ((bytes = gzread(ingz, buf, sizeof(buf))) > 0) {
        block.write(buf, bytes);
    }
    gzclose(ingz);

    int rc = cdb2_open(&db.dbconn, "comdb2perfdb", "local" /* TODO ??? */, 0);
    if (rc) {
        std::cerr << "can't connect to the db" << std::endl;
        return 1;
    }

    std::string s;
    // Generate id for this block of data
    uuid_t blockid_uuid;
    uuid_generate(blockid_uuid);
    char blockid[37];  // aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee
    uuid_unparse(blockid_uuid, blockid);
    // std::cout << "uuid len " << strlen(blockid) << " " << blockid << std::endl;
    do {
        getline(block, s);
        process_event(db, std::string(blockid), s);
    } while (!block.eof());
    // record what queries we gathered
    // dump them first 
    dump(db, blockid);
    store(db, blockid, block.str());
    return 0;
}
