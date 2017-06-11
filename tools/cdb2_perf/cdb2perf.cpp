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

struct dbstream {
    gzFile ingz;
    std::ofstream out;
    std::ifstream in;
    cdb2_hndl_tp *dbconn;

    std::string source;
    std::string dbname;

    std::set<std::string> known_fingerprints;

    dbstream(std::string _dbname, std::string s) : source(s), dbname(_dbname) {}
};


/* TODO: these are stolen from cdb2_sqlreplay. Should have a single source
         for cson convenience routines. */
static const char *get_strprop(cson_value *objval, const char *key) {
    cson_object *obj;
    cson_value_fetch_object(objval, &obj);
    cson_value *propval = cson_object_get(obj, key);
    if (propval == nullptr || !cson_value_is_string(propval))
        return nullptr;
    cson_string *cstr;
    cson_value_fetch_string(propval, &cstr);
    return cson_string_cstr(cstr);
}

bool get_intprop(cson_value *objval, const char *key, int64_t *val) {
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
        std::cout << fingerprint << " seen before" << std::endl;
        return false;
    }
    if (db_knows_fingerprint(db, fingerprint)) {
        db.known_fingerprints.insert(fingerprint);
        std::cout << fingerprint << " seen in db" << std::endl;
        return false;
    }
    std::cout << fingerprint << " brand new" << std::endl;
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

void process_event(dbstream &db, std::string &json) {
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
        int64_t timestamp;
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

        // TODO: cache in process so I don't keep hitting the db for this
        if (fingerprint_new_to_me(db, fingerprint)) {
            record_new_fingerprint(db, timestamp, fingerprint, sql);
        }
    }
    else if (type == "sql") {
    }
}

int main(int argc, char *argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: file" << std::endl;
        return 1;
    }
    char *fname = argv[1];

    dbstream db("mikedb", fname);
    db.ingz = gzopen(fname, "r");

    db.out.open("tmp", std::ios::out | std::ios::binary);
    if (!db.out.is_open()) {
        std::cerr << "can't open temp output file" << std::endl;
        return 1;
    }
    char buf[1024];
    int bytes = 0;
    while ((bytes = gzread(db.ingz, buf, sizeof(buf))) > 0) {
        db.out.write(buf, bytes);
    }
    db.out.close();
    gzclose(db.ingz);

    int rc = cdb2_open(&db.dbconn, "comdb2perfdb", "local" /* TODO ??? */, 0);
    if (rc) {
        std::cerr << "can't connect to the db" << std::endl;
        return 1;
    }

    db.in.open("tmp", std::ios::in | std::ios::binary);
    if (!db.in.is_open()) {
        std::cerr << "can't open temp input file" << std::endl;
        return 1;
    }
    std::string s;
    for (;;) {
        getline(db.in, s);
        if (db.in.eof())
            break;
        process_event(db, s);
    }
    return 0;
}
