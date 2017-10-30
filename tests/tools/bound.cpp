#include <string>
#include <vector>
#include <limits.h>
#include <string.h>
#include <arpa/inet.h>
#include <cdb2api.h>

using namespace std;

static string quote("");
static string comma(",");
static string space(" ");
static string at("@");

int runtag(cdb2_hndl_tp *h, string &sql, vector<int> &types)
{
    int rc =
        cdb2_run_statement_typed(h, sql.c_str(), types.size(), types.data());
    if (rc != 0) {
        fprintf(stderr, "cdb2_run_statement_typed failed: %d %s\n", rc,
                cdb2_errstr(h));
        return rc;
    }

    rc = cdb2_next_record(h);
    while (rc == CDB2_OK) {
        int n = cdb2_numcolumns(h);
        int i;
        const char *c = "";
        for (i = 0; i < n; ++i) {
            printf("%s%s", c, (char *)cdb2_column_value(h, i));
            c = ", ";
        }
        puts("");
        rc = cdb2_next_record(h);
    }

    if (rc == CDB2_OK_DONE)
        rc = 0;
    else
        fprintf(stderr, "cdb2_next_record failed: %d %s\n", rc, cdb2_errstr(h));
    return rc;
}

void add_param(string &sp, string &sql, vector<int> &types, string name,
               string extra1 = "", string extra2 = "")
{
    sp += comma + space + quote + at + name + quote;
    sql += comma + space + extra1 + at + name + extra2;
    types.push_back(CDB2_CSTRING);
}

int main(int argc, char *argv[])
{
    cdb2_hndl_tp *db1, *db2;
    if(argc < 3) {
        fprintf(stderr, "not enough parameters\n");
        return 1;
    }
    cdb2_set_comdb2db_config(argv[1]);
    cdb2_open(&db1, argv[2], "default", 0);
    cdb2_open(&db2, argv[2], "default", 0);

    string sp("exec procedure bound(");
    string sql("select ");
    vector<int> types;

    int i = INT_MAX;
    cdb2_bind_param(db1, "i", CDB2_INTEGER, &i, sizeof(i));
    cdb2_bind_param(db2, "i", CDB2_INTEGER, &i, sizeof(i));

    sp += quote + "@i" + quote;
    sql += "@i";
    types.push_back(CDB2_CSTRING);

    unsigned u = UINT_MAX;
    cdb2_bind_param(db1, "u", CDB2_INTEGER, &u, sizeof(u));
    cdb2_bind_param(db2, "u", CDB2_INTEGER, &u, sizeof(u));
    add_param(sp, sql, types, "u");

    long long ll = LLONG_MAX;
    cdb2_bind_param(db1, "ll", CDB2_INTEGER, &ll, sizeof(ll));
    cdb2_bind_param(db2, "ll", CDB2_INTEGER, &ll, sizeof(ll));
    add_param(sp, sql, types, "ll");

    unsigned long long ull = ULLONG_MAX;
    cdb2_bind_param(db1, "ull", CDB2_INTEGER, &ull, sizeof(ull));
    cdb2_bind_param(db2, "ull", CDB2_INTEGER, &ull, sizeof(ull));
    add_param(sp, sql, types, "ull");

    short s = SHRT_MAX;
    cdb2_bind_param(db1, "s", CDB2_INTEGER, &s, sizeof(s));
    cdb2_bind_param(db2, "s", CDB2_INTEGER, &s, sizeof(s));
    add_param(sp, sql, types, "s");

    unsigned short us = USHRT_MAX;
    cdb2_bind_param(db1, "us", CDB2_INTEGER, &us, sizeof(us));
    cdb2_bind_param(db2, "us", CDB2_INTEGER, &us, sizeof(us));
    add_param(sp, sql, types, "us");

    float f = 3.14159;
    cdb2_bind_param(db1, "f", CDB2_REAL, &f, sizeof(f));
    cdb2_bind_param(db2, "f", CDB2_REAL, &f, sizeof(f));
    add_param(sp, sql, types, "f");

    double d = 3.14159;
    cdb2_bind_param(db1, "d", CDB2_REAL, &d, sizeof(d));
    cdb2_bind_param(db2, "d", CDB2_REAL, &d, sizeof(d));
    add_param(sp, sql, types, "d");

    time_t t = 1356998400;
    cdb2_client_datetime_t datetime = {0};
    gmtime_r(&t, (struct tm *)&datetime.tm);
    cdb2_bind_param(db1, "dt", CDB2_DATETIME, &datetime, sizeof(datetime));
    cdb2_bind_param(db2, "dt", CDB2_DATETIME, &datetime, sizeof(datetime));
    add_param(sp, sql, types, "dt");

    char cstr[] = "Hello, World!";
    cdb2_bind_param(db1, "cstr", CDB2_CSTRING, cstr, strlen(cstr));
    cdb2_bind_param(db2, "cstr", CDB2_CSTRING, cstr, strlen(cstr));
    add_param(sp, sql, types, "cstr");

    int ba = htonl(0xdbdbdbdb);
    cdb2_bind_param(db1, "ba", CDB2_BLOB, &ba, sizeof(ba));
    cdb2_bind_param(db2, "ba", CDB2_BLOB, &ba, sizeof(ba));
    add_param(sp, sql, types, "ba", "hex(", ")");

    uint32_t forblob[] = {htonl(0xdeadbeef), htonl(0xcafebabe),
                          htonl(0xffffffff)};
    cdb2_bind_param(db1, "blob", CDB2_BLOB, forblob, sizeof(forblob));
    cdb2_bind_param(db2, "blob", CDB2_BLOB, forblob, sizeof(forblob));
    add_param(sp, sql, types, "blob", "hex(", ")");

    char forvutf8[] =
        "Lorem ipsum dolor sit amet, consectetur "
        "adipisicing elit, sed do eiusmod tempor incididunt ut labore et "
        "dolore magna aliqua.";
    cdb2_bind_param(db1, "vutf8", CDB2_CSTRING, forvutf8, strlen(forvutf8));
    cdb2_bind_param(db2, "vutf8", CDB2_CSTRING, forvutf8, strlen(forvutf8));
    add_param(sp, sql, types, "vutf8");

    sp += ")";
    sql += ";";

    int rc = 0;
    printf("SQL: %s\n", sql.c_str());
    rc |= runtag(db1, sql, types);
    printf("SP: %s\n", sp.c_str());
    rc |= runtag(db2, sp, types);

    cdb2_close(db1);
    cdb2_close(db2);
    return rc;
}
