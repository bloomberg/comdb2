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

    printf("finished succesfully\n");
    return rc;
}
