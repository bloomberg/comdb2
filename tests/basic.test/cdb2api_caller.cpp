#include <string>
#include <vector>
#include <limits.h>
#include <string.h>
#include <arpa/inet.h>
#include <cdb2api.h>
#include <time.h>
#include <sstream>
#include <iostream>

static std::string quote("");
static std::string comma(",");
static std::string space(" ");
static std::string at("@");

int runsql(cdb2_hndl_tp *h, std::string &sql)
{
    int rc = cdb2_run_statement(h, sql.c_str());
    if (rc != 0) {
        fprintf(stderr, "cdb2_run_statement failed: %d %s\n", rc,
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

int runtag(cdb2_hndl_tp *h, std::string &sql, std::vector<int> &types)
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

void add_param(std::string &sp, std::string &sql, std::vector<int> &types,
               std::string name, std::string extra1 = "",
               std::string extra2 = "")
{
    sp += comma + space + quote + at + name + quote;
    sql += comma + space + extra1 + at + name + extra2;
    types.push_back(CDB2_CSTRING);
}

int main(int argc, char *argv[])
{
    cdb2_hndl_tp *db;
    if (argc < 2) {
        fprintf(stderr, "not enough parameters\n");
        return 1;
    }
    char *conf = getenv("CDB2_CONFIG");
    if (conf) cdb2_set_comdb2db_config(conf);

    int rc = cdb2_open(&db, argv[1], "default", 0);
    if (rc != 0) {
        fprintf(stderr, "cdb2_open failed: %d %s\n", rc, cdb2_errstr(db));
        return rc;
    }

    // insert records
    for (int j = 1000; j < 2000; j++) {
        std::ostringstream ss;
        time_t rawtime;
        struct tm *timeinfo;
        char buffer[80];

        time(&rawtime);
        timeinfo = localtime(&rawtime);

        strftime(buffer, 80, "'%FT%T'", timeinfo);

        ss << "insert into t1(alltypes_short, alltypes_u_short, alltypes_int, "
              "alltypes_u_int, alltypes_longlong, alltypes_float, "
              "alltypes_double, alltypes_byte, alltypes_cstring, "
              "alltypes_pstring, alltypes_blob, alltypes_datetime, "
              "alltypes_datetimeus, alltypes_vutf8, alltypes_intervalym, "
              "alltypes_intervalds, alltypes_decimal32, alltypes_decimal64, "
              "alltypes_decimal128) values ( "
           << ((1 - 2 * (j % 2))) << j << ", " << j << ", "
           << ((1 - 2 * (j % 2))) << "0000" << j << ", "
           << "10000" << j << ", " << ((1 - 2 * (j % 2))) << "000000000" << j
           << ", " << ((1 - 2 * (j % 2))) << "00.00" << j << ", "
           << ((1 - 2 * (j % 2))) << "0000" << j << ".0000" << j << ", "
           << "x'" << (j % 2) << (j % 3) << (j % 4) << (j % 5) << (j % 2)
           << (j % 3) << (j % 4) << (j % 5) << (j % 2) << (j % 3) << (j % 4)
           << (j % 5) << (j % 2) << (j % 3) << (j % 4) << (j % 5)
           << "0000000000000000'"
           << ", "
           << "'mycstring" << j << "', "
           << "'mypstring" << j << "', "
           << "x'" << (j % 2) << (j % 3) << (j % 4) << (j % 5) << "', "
           << buffer << ", " << buffer << ", "
           << "'myvutf8" << j << "', " << (1 - 2 * (j % 2)) << j << ", "
           << (1 - 2 * (j % 2)) << "0000" << j << ", " << (1 - 2 * (j % 2))
           << "0000" << j << ", " << (1 - 2 * (j % 2)) << "00000000" << j
           << ", " << (1 - 2 * (j % 2)) << "00000000000000" << j << ")";
        // std::cout << "sql " << ss.str() << std::endl;
        std::string s = ss.str();
        runsql(db, s);
    }

    // update records
    for (int j = 1000; j < 2000; j++) {
        std::ostringstream ss;
        time_t rawtime;
        struct tm *timeinfo;
        char buffer[80];

        time(&rawtime);
        timeinfo = localtime(&rawtime);

        strftime(buffer, 80, "'%FT%T'", timeinfo);

        ss << "update t1 set "
           << "alltypes_short = " << ((1 - 2 * (j % 2))) << j << ", "
           << "alltypes_u_short = " << j << ", "
           << "alltypes_int = " << ((1 - 2 * (j % 2))) << "0000" << j << ", "
           << "alltypes_u_int = "
           << "10000" << j << ", "
           << "alltypes_longlong = " << ((1 - 2 * (j % 2))) << "000000000" << j
           << ", "
           << "alltypes_float = " << ((1 - 2 * (j % 2))) << "00.00" << j << ", "
           << "alltypes_double = " << ((1 - 2 * (j % 2))) << "0000" << j
           << ".0000" << j << ", "
           << "alltypes_byte = "
           << "x'" << (j % 2) << (j % 3) << (j % 4) << (j % 5) << (j % 2)
           << (j % 3) << (j % 4) << (j % 5) << (j % 2) << (j % 3) << (j % 4)
           << (j % 5) << (j % 2) << (j % 3) << (j % 4) << (j % 5)
           << "0000000000000000'"
           << ", "
           << "alltypes_cstring = "
           << "'mycstring" << j << "', "
           << "alltypes_pstring = "
           << "'mypstring" << j << "', "
           << "alltypes_blob = "
           << "x'" << (j % 2) << (j % 3) << (j % 4) << (j % 5) << "', "
           << "alltypes_datetime = " << buffer << ", "
           << "alltypes_datetimeus = " << buffer << ", "
           << "alltypes_vutf8 = "
           << "'myvutf8" << j << "', "
           << "alltypes_intervalym = " << (1 - 2 * (j % 2)) << j << ", "
           << "alltypes_intervalds = " << (1 - 2 * (j % 2)) << "0000" << j
           << ", "
           << "alltypes_decimal32 = " << (1 - 2 * (j % 2)) << "0000" << j
           << ", "
           << "alltypes_decimal64 = " << (1 - 2 * (j % 2)) << "00000000" << j
           << ", "
           << "alltypes_decimal128 = " << (1 - 2 * (j % 2)) << "00000000000000"
           << j << " "
           << "where alltypes_u_short = " << j;
        std::string s = ss.str();
        runsql(db, s);
    }

    // delete records
    for (int j = 1500; j < 2000; j++) {
        std::ostringstream ss;
        time_t rawtime;
        struct tm *timeinfo;
        char buffer[80];

        time(&rawtime);
        timeinfo = localtime(&rawtime);

        strftime(buffer, 80, "'%FT%T'", timeinfo);

        ss << "delete from t1 where alltypes_u_short = " << j;
        std::string s = ss.str();
        runsql(db, s);
    }

    // insert records with bind params
    for (int j = 2000; j < 3000; j++) {
        std::ostringstream ss;
        time_t rawtime;
        struct tm *timeinfo;
        char buffer[80];

        time(&rawtime);
        timeinfo = localtime(&rawtime);
        std::vector<int> types;

        strftime(buffer, 80, "'%FT%T'", timeinfo);

        short palltypes_short = (1 - 2 * (j % 2)) * j;
        unsigned short palltypes_u_short = j;
        float palltypes_float =
            (1 - 2 * (j % 2)) * (100.0 + ((float)j) / 1000.0);
        double palltypes_double =
            (1 - 2 * (j % 2)) * (100000.0 + ((double)j) / 1000000.0);
        unsigned char palltypes_byte[17] = "1234567890123456";
        unsigned char palltypes_blob[1024];
        {
            for (unsigned int i = 0; i < sizeof(palltypes_blob); i++)
                palltypes_blob[i] = 'a' + (i % 26);
        }

        if (cdb2_bind_param(db, "palltypes_short", CDB2_INTEGER,
                            &palltypes_short, sizeof(palltypes_short)))
            fprintf(stderr, "Error binding palltypes_short.\n");
        if (cdb2_bind_param(db, "palltypes_u_short", CDB2_INTEGER,
                            &palltypes_u_short, sizeof(palltypes_u_short)))
            fprintf(stderr, "Error binding palltypes_short.\n");
        if (cdb2_bind_param(db, "palltypes_float", CDB2_REAL, &palltypes_float,
                            sizeof(palltypes_float)))
            fprintf(stderr, "Error binding palltypes_float.\n");
        if (cdb2_bind_param(db, "palltypes_double", CDB2_REAL,
                            &palltypes_double, sizeof(palltypes_double)))
            fprintf(stderr, "Error binding palltypes_double.\n");
        if (cdb2_bind_param(db, "palltypes_byte", CDB2_BLOB, palltypes_byte,
                            sizeof(palltypes_byte) - 1))
            fprintf(stderr, "Error binding palltypes_byte.\n");
        if (cdb2_bind_param(db, "palltypes_blob", CDB2_BLOB, palltypes_blob,
                            sizeof(palltypes_blob)))
            fprintf(stderr, "Error binding palltypes_blob.\n");
        types.push_back(CDB2_INTEGER);

        std::string s = "insert into t1(alltypes_short, alltypes_u_short, "
                        "alltypes_float, alltypes_double, alltypes_byte, "
                        "alltypes_blob) values (@palltypes_short, "
                        "@palltypes_u_short, @palltypes_float, "
                        "@palltypes_double, @palltypes_byte, @palltypes_blob)";
        //, alltypes_u_short, alltypes_int, alltypes_u_int, alltypes_longlong,
        //alltypes_float, alltypes_double, alltypes_byte, alltypes_cstring,
        //alltypes_pstring, alltypes_blob, alltypes_datetime,
        //alltypes_datetimeus, alltypes_vutf8, alltypes_intervalym,
        //alltypes_intervalds, alltypes_decimal32, alltypes_decimal64,
        //alltypes_decimal128) values ( "
        /*
        << ((1-2*(j%2))) << j
        << ", " << j
        << ", " << ((1-2*(j%2))) << "0000" << j
        << ", " << "10000" << j
        << ", " << ((1-2*(j%2))) << "000000000" << j
        << ", " << ((1-2*(j%2))) << "00.00" << j
        << ", " << ((1-2*(j%2))) << "0000" << j << ".0000" << j
        << ", " << "x'" << (j%2) << (j%3) << (j%4) << (j%5) << (j%2) << (j%3) <<
        (j%4) << (j%5) << (j%2) << (j%3) << (j%4) << (j%5) << (j%2) << (j%3) <<
        (j%4) << (j%5) << "0000000000000000'" << ", "
        << "'mycstring" << j << "', "
        << "'mypstring" << j << "', "
        << "x'" << (j%2) << (j%3) << (j%4) << (j%5) << "', "
        << buffer << ", "
        << buffer << ", "
        << "'myvutf8" << j << "', "
        << (1-2*(j%2)) << j << ", "
        << (1-2*(j%2)) << "0000" << j << ", "
        << (1-2*(j%2)) << "0000" << j << ", "
        << (1-2*(j%2)) << "00000000" << j << ", "
        << (1-2*(j%2)) << "00000000000000" << j  << ")";
        */
        // this works too?? runsql(db, s);
        printf("float param: %f\n", palltypes_float);
        printf("double param: %lf\n", palltypes_double);
        runtag(db, s, types);
        cdb2_clearbindings(db);
    }

    /*
    std::vector<int> types;

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

    int forblob[] = {htonl(0xdeadbeef), htonl(0xcafebabe), htonl(0xffffffff)};
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

    cdb2_close(db2);
    */
    cdb2_close(db);
    return 0;
}
