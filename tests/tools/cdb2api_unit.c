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

#undef NDEBUG

#include <assert.h>
#include <bb_oscompat.h>
#include <cdb2api.c>

#define CDB2HOSTNAME_LEN 128

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

    cdb2_set_min_retries(0);
    assert(MIN_RETRIES == 20);

    cdb2_set_min_retries(1);
    assert(MIN_RETRIES == 1);
}

void test_cdb2_set_max_retries()
{
    cdb2_set_max_retries(20);
    assert(MAX_RETRIES == 20);

    cdb2_set_max_retries(-30);
    assert(MAX_RETRIES == 20);

    cdb2_set_max_retries(0);
    assert(MAX_RETRIES == 20);

    cdb2_set_max_retries(1);
    assert(MAX_RETRIES == 1);
}

void test_cdb2_hndl_set_min_retries()
{
    cdb2_hndl_tp hndl;
    cdb2_hndl_set_min_retries(&hndl, 20);
    assert(hndl.min_retries == 20);

    cdb2_hndl_set_min_retries(&hndl, -30);
    assert(hndl.min_retries == 20);

    cdb2_hndl_set_min_retries(&hndl, 0);
    assert(hndl.min_retries == 20);

    cdb2_hndl_set_min_retries(&hndl, 1);
    assert(hndl.min_retries == 1);
}

void test_cdb2_hndl_set_max_retries()
{
    cdb2_hndl_tp hndl;
    cdb2_hndl_set_max_retries(&hndl, 20);
    assert(hndl.max_retries == 20);

    cdb2_hndl_set_max_retries(&hndl, -30);
    assert(hndl.max_retries == 20);

    cdb2_hndl_set_max_retries(&hndl, 0);
    assert(hndl.max_retries == 20);

    cdb2_hndl_set_max_retries(&hndl, 1);
    assert(hndl.max_retries == 1);
}


void test_reset_sockpool()
{
    assert(sockpool_fail_time == SOCKPOOL_FAIL_TIME_DEFAULT);
    assert(sockpool_generation > 0);
    /* Assume single threaded unit test */
    for (int i = 0; i < sockpool_fd_count; i++) {
        struct sockpool_fd_list *sp = &sockpool_fds[i];
        assert(sp->in_use == 0);
        assert(sp->sockpool_fd == -1);
    }
    assert(sockpool_enabled == SOCKPOOL_ENABLED_DEFAULT);
}


void test_cdb2_set_comdb2db_config()
{
    cdb2_set_comdb2db_config("anotherconfigfile");
    assert(strcmp(CDB2DBCONFIG_NOBBENV, "anotherconfigfile") == 0);

    cdb2_set_comdb2db_config(NULL);
}



void test_read_comdb2db_cfg()
{
    cdb2_hndl_tp hndl;
    char comdb2db_hosts[10][CDB2HOSTNAME_LEN];
    char db_hosts[10][CDB2HOSTNAME_LEN];
    SBUF2 *s = NULL;
    char *comdb2db_name = NULL;
    int num_hosts = 0;
    int comdb2db_num = 0;
    char *dbname = "mydb";
    int num_db_hosts = 0;
    int dbnum = 0;
    int stack_at_open = 0;
    char shards[10][DBNAME_LEN];
    int num_shards = 0;

    const char *buf = 
"\
    \
    \
";


    read_comdb2db_cfg(&hndl, s, comdb2db_name,
                      buf, comdb2db_hosts,
                      &num_hosts, &comdb2db_num, dbname,
                      db_hosts, &num_db_hosts,
                      &dbnum, &stack_at_open,
                      shards, &num_shards);

    assert(num_hosts == 0);
    assert(comdb2db_num == 0);
    assert(num_db_hosts == 0);
    assert(dbnum == 0);
    assert(num_shards == 0);

    const char *buf2 = 
"\n\
  comdb2dbnm:a,b,c:d:e  \n\
  mydb:n1,n2,n3:n4:n5,n6    \n\
  partition mydb:mydb1,mydb2,mydb3,mydb4    \n\
  comdb2_config:default_type:testsuite  \n\
  comdb2_config:portmuxport=12345   \n\
  comdb2_config:allow_pmux_route:true   \
";

    read_comdb2db_cfg(&hndl, s, "comdb2dbnm",
                      buf2, comdb2db_hosts,
                      &num_hosts, &comdb2db_num, dbname,
                      db_hosts, &num_db_hosts,
                      &dbnum, &stack_at_open,
                      shards, &num_shards);

    assert(num_hosts == 5);
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

    assert(dbnum == 0);
    assert(12345 == CDB2_PORTMUXPORT);

    assert(num_shards == 4);
    assert(strcmp(shards[0], "mydb1") == 0);
    assert(strcmp(shards[1], "mydb2") == 0);
    assert(strcmp(shards[2], "mydb3") == 0);
    assert(strcmp(shards[3], "mydb4") == 0);

    // test with buf3 which provokes buffer overflow in cdb2api
    // make sure cannot use mydb as shard name (will be ignored)
    num_hosts = 0;
    num_db_hosts = 0;
    num_shards = 0;
    const char *buf3 = "\
  comdb2dbnm:test_short_hostname,test_long_hostname_xf00fxf00fxf00fxf00fxf00fxf00fxf00fxf00fxf00f,test_overflow_hostname_extra_text_is_truncatedtest_overflow_hostname_extra_text_is_truncatedtest_overflow_hostname_extra_text_is_truncated   \n\
  mydb:test_short_hostname,test_long_hostname_xf00fxf00fxf00fxf00fxf00fxf00fxf00fxf00fxf00f,test_overflow_hostname_extra_text_is_truncatedtest_overflow_hostname_extra_text_is_truncatedtest_overflow_hostname_extra_text_is_truncated   \n\
  partition mydb:test_short_shard,mydb,test_long_shard_xf00fxf00fxf00f,test_overflow_shard_extra_text_is_truncated    \n\
  comdb2_config:default_type:test_overflow_when_assigning_the_default_type_extra_text_is_truncated   \n\
  comdb2_config:room:test_overflow_when_assigning_the_room_extra_text_is_truncated   \n\
  comdb2_config:comdb2dbname:test_overflow_when_assigning_the_dbname_extra_text_is_truncated   \n\
  comdb2_config:dnssuffix:test_overflow_when_assigning_the_dnssuffix_extra_text_is_truncatedtest_overflow_when_assigning_the_dnssuffix_extra_text_is_truncatedtest_overflow_when_assigning_the_dnssuffix_extra_text_is_truncatedtest_overflow_when_assigning_the_dnssuffix_extra_text_is_truncated  \n\
";
    read_comdb2db_cfg(NULL, s, "comdb2dbnm",
                      buf3, comdb2db_hosts,
                      &num_hosts, &comdb2db_num, dbname,
                      db_hosts, &num_db_hosts,
                      &dbnum, &stack_at_open,
                      shards, &num_shards);

    assert(num_db_hosts == 3);
    assert(num_hosts == 3);
    assert(strcmp(comdb2db_hosts[0], "test_short_hostname") == 0);
    assert(strcmp(comdb2db_hosts[1], "test_long_hostname_xf00fxf00fxf00fxf00fxf00fxf00fxf00fxf00fxf00f") == 0);
    assert(strcmp(comdb2db_hosts[2], "test_overflow_hostname_extra_text_is_truncatedtest_overflow_hostname_extra_text_is_truncatedtest_overflow_hostname_extra_text_i") == 0);
    assert(strcmp(db_hosts[0], "test_short_hostname") == 0);
    assert(strcmp(db_hosts[1], "test_long_hostname_xf00fxf00fxf00fxf00fxf00fxf00fxf00fxf00fxf00f") == 0);
    assert(strcmp(db_hosts[2], "test_overflow_hostname_extra_text_is_truncatedtest_overflow_hostname_extra_text_is_truncatedtest_overflow_hostname_extra_text_i") == 0);
    assert(strcmp(cdb2_default_cluster, "test_overflow_when_assigning_the_default_type_extra_text_is_tru") == 0);
    assert(strcmp(cdb2_machine_room, "test_overflow_w") == 0);
    assert(strcmp(cdb2_comdb2dbname, "test_overflow_when_assigning_th") == 0);
    assert(strcmp(cdb2_dnssuffix, "test_overflow_when_assigning_the_dnssuffix_extra_text_is_truncatedtest_overflow_when_assigning_the_dnssuffix_extra_text_is_truncatedtest_overflow_when_assigning_the_dnssuffix_extra_text_is_truncatedtest_overflow_when_assigning_the_dnssuffix_extra_text_is") == 0);

    assert(num_shards == 3);
    assert(strcmp(shards[0], "test_short_shard") == 0);
    assert(strcmp(shards[1], "test_long_shard_xf00fxf00fxf00f") == 0);
    assert(strcmp(shards[2], "test_overflow_shard_extra_text_is_truncated") == 0);
}


void test_get_config_file()
{
    char shortname[16];
    int rc = get_config_file("mydb", shortname, sizeof(shortname));
    assert(rc == -1); //does not fit

    char filename[PATH_MAX];
    rc = get_config_file(NULL, filename, sizeof(filename));
    /* NULL dbname is no longer permitted. */
    assert(rc == -1);

    setenv("COMDB2_ROOT", "myroot", 1);
    rc = get_config_file("mydb", filename, sizeof(filename));
    assert(rc == 0);
    assert(strcmp(filename, "myroot/etc/cdb2/config.d/mydb.cfg") == 0);
}


void test_cdb2_string_escape()
{
    const char *emptyStr = "";
    char *testEmptyStr = cdb2_string_escape(NULL, emptyStr);
    assert(strcmp(testEmptyStr, "''") == 0);
    free(testEmptyStr);
    
    const char *simpleStr = "Hello world!";
    char *testSimpleStr = cdb2_string_escape(NULL, simpleStr);
    assert(strcmp(testSimpleStr, "'Hello world!'") == 0);
    free(testSimpleStr);
    
    const char* complexStr = "'As quirky joke, chefs won't pay devil magic zebra tax.''";
    char *testComplexStr = cdb2_string_escape(NULL, complexStr);
    assert(strcmp(testComplexStr, "'''As quirky joke, chefs won''t pay devil magic zebra tax.'''''") == 0);
    free(testComplexStr);
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

    test_cdb2_string_escape();

    printf("finished succesfully\n");
    return rc;
}
