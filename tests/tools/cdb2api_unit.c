/*
   Copyright 2017 Bloomberg Finance L.P.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */

/*
 * Unit Tests for cdb2api.h/c
 */

#include <bb_oscompat.h>
#include <cdb2api.c>



void test_is_sql_read()
{
    assert(is_sql_read(NULL) == -1);
    assert(is_sql_read("select blah") == 1);
    assert(is_sql_read("  SELECT blah") == 1);
    assert(is_sql_read("explain blah") == 1);
    assert(is_sql_read("  EXPLAIN blah") == 1);
    assert(is_sql_read("with blah") == 1);
    assert(is_sql_read(" WITH blah") == 1);
    assert(is_sql_read("  get blah") == 1);
    assert(is_sql_read("GET blah") == 1);
    assert(is_sql_read("EXEC blah") == 1);
    assert(is_sql_read("  EXEC blah") == 1);
    assert(is_sql_read("  insert blah") == 0);
    assert(is_sql_read("INSERT blah") == 0);
    assert(is_sql_read("UPDATE blah") == 0);
    assert(is_sql_read(" update blah") == 0);
    assert(is_sql_read(" anything else ") == 0);
}

void test_do_init_once()
{
    assert(log_calls == 0);
    assert(strcmp(CDB2DBCONFIG_NOBBENV, "/opt/bb/etc/cdb2/config/comdb2db.cfg") == 0);
    setenv("CDB2_LOG_CALLS", "1", 1);
    setenv("CDB2_CONFIG_FILE", "abracadabra01234567891234567890123456789012345678912345678901234567890123456789123456789012345678901234567891234567890123456789012345678912345678901234567890123456789123456789012345678901234567891234567890123456789012345678912345678901234567890123456789123456789012345678901234567891234567890123456789012345678912345678901234567890123456789123456789012345678901234567891234567890123456789012345678912345678901234567890123456789123456789012345678901234567891234567890123456789012345678912345678901234567890123456789123456789012345678901234567891234567890123456789", 1);
    do_init_once();
    assert(log_calls == 1);
    assert(strcmp(CDB2DBCONFIG_NOBBENV, "abracadabra01234567891234567890123456789012345678912345678901234567890123456789123456789012345678901234567891234567890123456789012345678912345678901234567890123456789123456789012345678901234567891234567890123456789012345678912345678901234567890123456789123456789012345678901234567891234567890123456789012345678912345678901234567890123456789123456789012345678901234567891234567890123456789012345678912345678901234567890123456789123456789012345678901234567891234567890123456789012345678912345678901234567890123456") == 0);
    assert( _PID == getpid() );
    assert( _MACHINE_ID == gethostid() );
}


void test_cdb2_set_min_retries()
{
    cdb2_set_min_retries(20);
    assert(MIN_RETRIES == 20);

    cdb2_set_min_retries(-30);
    assert(MIN_RETRIES == 20);
}

void test_cdb2_set_max_retries()
{
    cdb2_set_max_retries(20);
    assert(MAX_RETRIES == 20);

    cdb2_set_max_retries(-30);
    assert(MAX_RETRIES == 20);
}

void test_cdb2_hndl_set_min_retries()
{
    cdb2_hndl_tp hndl;
    cdb2_hndl_set_min_retries(&hndl, 20);
    assert(hndl.min_retries == 20);

    cdb2_hndl_set_min_retries(&hndl, -30);
    assert(hndl.min_retries == 20);
}

void test_cdb2_hndl_set_max_retries()
{
    cdb2_hndl_tp hndl;
    cdb2_hndl_set_max_retries(&hndl, 20);
    assert(hndl.max_retries == 20);

    cdb2_hndl_set_max_retries(&hndl, -30);
    assert(hndl.max_retries == 20);
}

void test_cdb2_set_comdb2db_config()
{
    cdb2_set_comdb2db_config("anotherconfigfile");
    assert(strcmp(CDB2DBCONFIG_NOBBENV, "anotherconfigfile") == 0);
}


void test_read_comdb2db_cfg()
{
    cdb2_hndl_tp hndl;
    char comdb2db_hosts[10][64];
    char db_hosts[10][64];
    FILE *fp = NULL;
    char *comdb2db_name = NULL;
    int num_hosts = 0;
    int comdb2db_num = 0;
    char *dbname = "mydb";
    int num_db_hosts = 0;
    int dbnum = 0;
    int dbname_found = 0;
    int comdb2db_found = 0;
    int stack_at_open = 0;

    const char *buf = 
"\
   \
   \
";


/*
static void read_comdb2db_cfg(cdb2_hndl_tp *hndl, FILE *fp, char *comdb2db_name, 
                              const char *buf, char comdb2db_hosts[][64],
                              int *num_hosts, int *comdb2db_num, char *dbname,
                              char db_hosts[][64], int *num_db_hosts,
                              int *dbnum, int *dbname_found,
                              int *comdb2db_found, int *stack_at_open)
                              */


    read_comdb2db_cfg(&hndl, fp, comdb2db_name,
                      buf, comdb2db_hosts,
                      &num_hosts, &comdb2db_num, dbname,
                      db_hosts, &num_db_hosts,
                      &dbnum, &dbname_found,
                      &comdb2db_found, &stack_at_open);

    assert(num_hosts == 0);
    assert(comdb2db_num == 0);
    assert(num_db_hosts == 0);
    assert(dbnum == 0);
    assert(dbname_found == 0);
    assert(comdb2db_found == 0);

    const char *buf2 = 
"\n\
  comdb2dbnm:a,b,c:d:e   \n\
  mydb:n1,n2,n3:n4:n5,n6 \n\
  comdb2_config:default_type=testsuite   \n\
  comdb2_config:portmuxport=12345         \n\
  comdb2_config:allow_pmux_route:true       \
";

    read_comdb2db_cfg(&hndl, fp, "comdb2dbnm",
                      buf2, comdb2db_hosts,
                      &num_hosts, &comdb2db_num, dbname,
                      db_hosts, &num_db_hosts,
                      &dbnum, &dbname_found,
                      &comdb2db_found, &stack_at_open);

    assert(num_hosts == 5);
    assert(comdb2db_found == 1);
    assert(comdb2db_num == 0);
    assert(strcmp(comdb2db_hosts[0], "a") == 0);
    assert(strcmp(comdb2db_hosts[1], "b") == 0);
    assert(strcmp(comdb2db_hosts[2], "c") == 0);
    assert(strcmp(comdb2db_hosts[3], "d") == 0);
    assert(strcmp(comdb2db_hosts[4], "e") == 0);

    assert(num_db_hosts == 6);
    assert(strcmp(db_hosts[0], "n1") == 0);
    assert(strcmp(db_hosts[1], "n2") == 0);
    assert(strcmp(db_hosts[2], "n3") == 0);
    assert(strcmp(db_hosts[3], "n4") == 0);
    assert(strcmp(db_hosts[4], "n5") == 0);
    assert(strcmp(db_hosts[5], "n6") == 0);

    //TODO: this is not set: assert(strcmp(hndl.cluster, "testsuite") == 0);

    assert(dbnum == 0);
    assert(dbname_found == 1);
}


void test_get_config_file()
{
    char shortname[16];
    int rc = get_config_file("mydb", shortname, sizeof(shortname));
    assert(rc == -1); //does not fit

    char filename[PATH_MAX];
    rc = get_config_file(NULL, filename, sizeof(filename));
    assert(rc == 0);

    setenv("COMDB2_ROOT", "myroot", 1);
    rc = get_config_file("mydb", filename, sizeof(filename));
    assert(rc == 0);
    assert(strcmp(filename, "myroot/etc/cdb2/config.d/mydb.cfg") == 0);
}

int main(int argc, char *argv[])
{
    int rc = 0;
    printf("starting unit test\n");
    assert(1 == 1);

    test_is_sql_read();
    test_do_init_once();

    test_cdb2_set_min_retries();
    test_cdb2_set_max_retries();

    test_cdb2_hndl_set_min_retries();
    test_cdb2_hndl_set_max_retries();

    test_cdb2_set_comdb2db_config();

    test_read_comdb2db_cfg();
    test_get_config_file();

    printf("finished succesfully\n");
    return rc;
}
