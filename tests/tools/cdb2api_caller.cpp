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
            if(cdb2_column_type(h, i) == CDB2_CSTRING) {
                char * v = (char *)cdb2_column_value(h, i);
                printf("%s%s", c, v);
                c = ", ";
            }
        }
        if(*c) printf("\n");
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

void add_param(std::string &sp, std::string &sql, std::vector<int> &types, std::string name,
               std::string extra1 = "", std::string extra2 = "")
{
    sp += comma + space + quote + at + name + quote;
    sql += comma + space + extra1 + at + name + extra2;
    types.push_back(CDB2_CSTRING);
}

int main(int argc, char *argv[])
{
    cdb2_hndl_tp *db;
    if(argc < 2) {
        fprintf(stderr, "not enough parameters\n");
        return 1;
    }
    char *conf = getenv("CDB2_CONFIG");
    if (conf)
        cdb2_set_comdb2db_config(conf);
    char *host = getenv("CDB2_HOST");

    int rc;
    if (host)
        rc = cdb2_open(&db, argv[1], host, CDB2_DIRECT_CPU);
    else
        rc = cdb2_open(&db, argv[1], "default", 0);
    if (rc != 0) {
        fprintf(stderr, "cdb2_open failed: %d %s\n", rc, cdb2_errstr(db));
        return rc;
    }

    const char *table = "t1";
    if (argc > 2) 
        table = argv[2];

    // insert records
    for (int j = 1000; j < 2000; j++) {
        std::ostringstream ss;
        time_t rawtime;
        struct tm * timeinfo;
        char buffer [80];

        time (&rawtime);
        timeinfo = localtime (&rawtime);

        strftime (buffer,80,"'%FT%T'",timeinfo);

        ss << "insert into " << table << "(alltypes_short, alltypes_u_short, alltypes_int, alltypes_u_int, alltypes_longlong, alltypes_float, alltypes_double, alltypes_byte, alltypes_cstring, alltypes_pstring, alltypes_blob, alltypes_datetime, alltypes_datetimeus, alltypes_vutf8, alltypes_intervalym, alltypes_intervalds, alltypes_intervaldsus, alltypes_decimal32, alltypes_decimal64, alltypes_decimal128) values ( "
           << ((1-2*(j%2))) << j << ", " 
           << j << ", " 
           << ((1-2*(j%2))) << "0000" << j << ", " 
           << "10000" << j << ", " 
           << ((1-2*(j%2))) << "000000000" << j << ", " 
           << ((1-2*(j%2))) << "00.00" << j << ", " 
           << ((1-2*(j%2))) << "0000" << j << ".0000" << j << ", " 
           << "x'" << (j%2) << (j%3) << (j%4) << (j%5) << (j%2) << (j%3) << (j%4) << (j%5) << (j%2) << (j%3) << (j%4) << (j%5) << (j%2) << (j%3) << (j%4) << (j%5) << "0000000000000000'" << ", " 
           << "'mycstring" << j << "', " 
           << "'mypstring" << j << "', " 
           << "x'" << (j%2) << (j%3) << (j%4) << (j%5) << "', " 
           << buffer << ", " 
           << buffer << ", " 
           << "'myvutf8" << j << "', " 
           << (1-2*(j%2)) << j << ", " 
           << (1-2*(j%2)) << j << ", " 
           << "cast(" << (1-2*(j%2)) << "0000" << j << " as secs) , " 
           << (1-2*(j%2)) << "0000" << j << ", " 
           << (1-2*(j%2)) << "00000000" << j << ", " 
           << (1-2*(j%2)) << "00000000000000" << j  << ")"; 
        //std::cout << "sql " << ss.str() << std::endl;
        std::string s = ss.str();
        runsql(db, s);
    }

    // update records
    for (int j = 1000; j < 2000; j++) {
        std::ostringstream ss;
        time_t rawtime;
        struct tm * timeinfo;
        char buffer [80];

        time (&rawtime);
        timeinfo = localtime (&rawtime);

        strftime (buffer,80,"'%FT%T'",timeinfo);

        ss << "update " << table << " set "
           << "alltypes_short = " << ((1-2*(j%2))) << j << ", " 
           << "alltypes_u_short = " << j << ", " 
           << "alltypes_int = " << ((1-2*(j%2))) << "0000" << j << ", " 
           << "alltypes_u_int = " << "10000" << j << ", " 
           << "alltypes_longlong = " << ((1-2*(j%2))) << "000000000" << j << ", " 
           << "alltypes_float = " << ((1-2*(j%2))) << "00.00" << j << ", " 
           << "alltypes_double = " << ((1-2*(j%2))) << "0000" << j << ".0000" << j << ", " 
           << "alltypes_byte = " << "x'" << (j%2) << (j%3) << (j%4) << (j%5) << (j%2) << (j%3) << (j%4) << (j%5) << (j%2) << (j%3) << (j%4) << (j%5) << (j%2) << (j%3) << (j%4) << (j%5) << "0000000000000000'" << ", " 
           << "alltypes_cstring = " << "'mycstring" << j << "', " 
           << "alltypes_pstring = " << "'mypstring" << j << "', " 
           << "alltypes_blob = " << "x'" << (j%2) << (j%3) << (j%4) << (j%5) << "', " 
           << "alltypes_datetime = " << buffer << ", " 
           << "alltypes_datetimeus = " << buffer << ", " 
           << "alltypes_vutf8 = " << "'myvutf8" << j << "', " 
           << "alltypes_intervalym = " << (1-2*(j%2)) << j << ", " 
           << "alltypes_intervalds = " << (1-2*(j%2)) << "0000" << j << ", " 
           << "alltypes_intervaldsus = cast(" << (1-2*(j%2)) << "0000" << j << " as secs), " 
           << "alltypes_decimal32 = " << (1-2*(j%2)) << "0000" << j << ", " 
           << "alltypes_decimal64 = " << (1-2*(j%2)) << "00000000" << j << ", " 
           << "alltypes_decimal128 = " << (1-2*(j%2)) << "00000000000000" << j  << " "
           << "where alltypes_u_short = " << j; 
        std::string s = ss.str();
        runsql(db, s);
    }

    // delete records
    for (int j = 1500; j < 2000; j++) {
        std::ostringstream ss;
        time_t rawtime;
        struct tm * timeinfo;
        char buffer [80];

        time (&rawtime);
        timeinfo = localtime (&rawtime);

        strftime (buffer,80,"'%FT%T'",timeinfo);

        ss << "delete from "<< table << " where alltypes_u_short = " << j; 
        std::string s = ss.str();
        runsql(db, s);
    }

    // insert records with bind params
    for (int j = 2000; j < 3002; j++) {
        std::ostringstream ss;
        time_t rawtime;
        struct tm * timeinfo;
        char buffer [80];

        time (&rawtime);
        timeinfo = localtime (&rawtime);
        std::vector<int> types;

        strftime (buffer,80,"'%FT%T'",timeinfo);

        short palltypes_short = (1-2*(j%2))*j;
        int palltypes_int = (1-2*(j%2))*j;
        unsigned short palltypes_u_short = j;
        float palltypes_float = (1-2*(j%2)) * (100.0 + ((float) j)/1000.0);
        double palltypes_double = (1-2*(j%2)) * (100000.0 + ((double) j)/1000000.0);
        unsigned char palltypes_byte[17] = "1234567890123456";
        unsigned char palltypes_cstring[16] = "123456789012345";
        unsigned char palltypes_pstring[16] = "123456789012345";
        unsigned char palltypes_blob[1025];
        {
            for(unsigned int i = 0 ; i < sizeof(palltypes_blob) - 1; i++)
                palltypes_blob[i] = 'a' + (i % 26);
             palltypes_blob[sizeof(palltypes_blob)] = '\0';
        }
        time_t t = 1386783296; //2013-12-11T123456
        cdb2_client_datetime_t datetime = {0};
        gmtime_r(&t, (struct tm *)&datetime.tm); //corrupts memory past the tm member
        datetime.msec = 123; // should not be larger than 999
        datetime.tzname[0] = '\0';

        cdb2_client_datetimeus_t datetimeus = {0};
        gmtime_r(&t, (struct tm *)&datetimeus.tm);
        datetimeus.usec = 123456; // should not be larger than 999999
        datetimeus.tzname[0] = '\0'; // gmtime_r corrupts past member tm
        cdb2_client_intv_ym_t ym = {1, 5, 2};

        cdb2_client_intv_ds_t ci = {1, 12, 23, 34, 45, 456 };
        
        cdb2_client_intv_dsus_t cidsus = {1, 12, 23, 34, 45, 456789 };

        if(cdb2_bind_param(db, "palltypes_short", CDB2_INTEGER, &palltypes_short, sizeof(palltypes_short)) )
            fprintf(stderr, "Error binding palltypes_short.\n");
        if(cdb2_bind_param(db, "palltypes_u_short", CDB2_INTEGER, &palltypes_u_short, sizeof(palltypes_u_short)) )
            fprintf(stderr, "Error binding palltypes_short.\n");
        if(cdb2_bind_param(db, "palltypes_int", CDB2_INTEGER, &palltypes_int, sizeof(palltypes_int)) )
            fprintf(stderr, "Error binding palltypes_short.\n");
        if(cdb2_bind_param(db, "palltypes_float", CDB2_REAL, &palltypes_float, sizeof(palltypes_float)) )
            fprintf(stderr, "Error binding palltypes_float.\n");
        if(cdb2_bind_param(db, "palltypes_double", CDB2_REAL, &palltypes_double, sizeof(palltypes_double)) )
            fprintf(stderr, "Error binding palltypes_double.\n");
        if(cdb2_bind_param(db, "palltypes_byte", CDB2_BLOB, palltypes_byte, sizeof(palltypes_byte)-1))
            fprintf(stderr, "Error binding palltypes_byte.\n");
        if(j == 2000) {
          if(cdb2_bind_param(db, "palltypes_blob", CDB2_INTEGER, NULL, 0))
            fprintf(stderr, "Error binding palltypes_blob.\n");
        }
        else if(cdb2_bind_param(db, "palltypes_blob", CDB2_BLOB, palltypes_blob, sizeof(palltypes_blob) - 1))
            fprintf(stderr, "Error binding palltypes_blob.\n");
        if(cdb2_bind_param(db, "palltypes_cstring", CDB2_CSTRING, palltypes_cstring, sizeof(palltypes_cstring) - 1))
            fprintf(stderr, "Error binding palltypes_cstring.\n");
        if(cdb2_bind_param(db, "palltypes_pstring", CDB2_CSTRING, palltypes_pstring, sizeof(palltypes_pstring) - 1))
            fprintf(stderr, "Error binding palltypes_cstring.\n");
        if(cdb2_bind_param(db, "palltypes_datetime", CDB2_DATETIME, &datetime, sizeof(datetime)))
            fprintf(stderr, "Error binding palltypes_datetime.\n");
        if(cdb2_bind_param(db, "palltypes_datetimeus", CDB2_DATETIMEUS, &datetimeus, sizeof(datetimeus)))
            fprintf(stderr, "Error binding palltypes_datetimeus.\n");
        if(cdb2_bind_param(db, "palltypes_intervalym", CDB2_INTERVALYM, &ym, sizeof(ym)))
            fprintf(stderr, "Error binding p alltypes_intervalym.\n");
        if(cdb2_bind_param(db, "palltypes_intervalds", CDB2_INTERVALDS, &ci, sizeof(ci)))
            fprintf(stderr, "Error binding p alltypes_intervalds.\n");
        if(cdb2_bind_param(db, "palltypes_intervaldsus", CDB2_INTERVALDSUS, &cidsus, sizeof(cidsus)))
            fprintf(stderr, "Error binding p alltypes_intervaldsus.\n");

        ss << "insert into " << table 
           << "(  alltypes_short,   alltypes_u_short,   alltypes_int,   alltypes_float,   alltypes_double,   alltypes_byte,   alltypes_cstring,   alltypes_pstring,   alltypes_blob,   alltypes_datetime,   alltypes_datetimeus,   alltypes_intervalym,   alltypes_intervalds,   alltypes_intervaldsus) values "
              "(@palltypes_short, @palltypes_u_short, @palltypes_int, @palltypes_float, @palltypes_double, @palltypes_byte, @palltypes_cstring, @palltypes_pstring, @palltypes_blob, @palltypes_datetime, @palltypes_datetimeus, @palltypes_intervalym, @palltypes_intervalds, @palltypes_intervaldsus)" ;
        
        //this works too?? runsql(db, s);
        printf("float param: %f\n", palltypes_float);
        printf("double param: %lf\n", palltypes_double);
        std::string s = ss.str();
        if(runtag(db, s, types) != 0) {
            exit(1);
        }
        cdb2_clearbindings(db);
    }

    cdb2_close(db);
    return 0;
}
