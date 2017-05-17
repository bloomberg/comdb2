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

#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#include "sockpool.h"
#include <syslog.h>


int pthread_create_attrs(pthread_t *tid, int detachstate,
        size_t stacksize, void *(*start_routine)(void*), void *arg)
{
    int rc;
    pthread_attr_t attr;
    pthread_t local_tid;
    if(!tid) tid = &local_tid;
    rc = pthread_attr_init(&attr);
    if(rc != 0) {
        syslog(LOG_ERR, "%s:pthread_attr_init: %d %s\n",
                __func__, rc, strerror(rc));
        return -1;
    }
    if(stacksize > 0) {
        rc = pthread_attr_setstacksize(&attr, stacksize);
        if(rc != 0) {
            syslog(LOG_ERR, "%s:pthread_attr_getstacksize: %d %s\n",
                    __func__, rc, strerror(rc));
            pthread_attr_destroy(&attr);
            return -1;
        }
    }
    rc = pthread_attr_setdetachstate(&attr, detachstate);
    if(rc != 0) {
        syslog(LOG_ERR, "%s:pthread_attr_setdetachstate: %d %s\n",
                __func__, rc, strerror(rc));
        pthread_attr_destroy(&attr);
        return -1;
    }
    rc = pthread_create(tid, &attr, start_routine, arg);
    if(rc != 0) {
        syslog(LOG_ERR, "%s:pthread_create: %d %s\n",
                __func__, rc, strerror(rc));
        pthread_attr_destroy(&attr);
        return -1;
    }
    pthread_attr_destroy(&attr);
    return 0;
}
