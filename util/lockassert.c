/*
   Copyright 2015 Bloomberg Finance L.P.

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

#include <lockassert.h>

#include <safestrerror.h>

#include <assert.h>
#include <stdio.h>

#include <logmsg.h>

int assert_pthread_lock_func(int rc, const char *pthread_func_name, int lineno,
                             const char *filename, const char *funcname)
{
    char errbuf[STRERROR_BUFLEN];

    if (funcname) {
        logmsg(LOGMSG_ERROR, "%s:%d:%s() %s failed rcode %s\n", filename, lineno,
                funcname, pthread_func_name, strerror_m(rc, errbuf));
    } else {
        logmsg(LOGMSG_ERROR, "%s:%d: %s failed rcode %s\n", filename, lineno,
                pthread_func_name, strerror_m(rc, errbuf));
    }

    assert(0);

    return rc;
}

#ifdef TEST_LOCKASSERT

#include <errno.h>

int main(int argc, char *argv[])
{
    pthread_mutex_t mutex;

    assert_pthread_mutex_init(&mutex, NULL);

    assert_pthread_mutex_lock(&mutex);
    assert(EBUSY == assert_pthread_mutex_trylock(&mutex));
    assert_pthread_mutex_unlock(&mutex);

    assert_pthread_lock_func(ENOMEM, "test", __LINE__, __FILE__, __func__);

    return 0;
}

#endif
