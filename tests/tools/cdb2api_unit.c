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

int main(int argc, char *argv[])
{
    int rc = 0;
    printf("starting unit test\n");
    assert(1 == 1);

    test_is_sql_read();

    printf("finished succesfully\n");
    return rc;
}
