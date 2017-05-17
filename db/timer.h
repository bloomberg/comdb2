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

#ifndef _INCLUDED_TIMER_H__
#define _INCLUDED_TIMER_H__

enum TIMER_EVENTS {
    /* this re-enables log-delete mode, after it has been disabled */
    TMEV_ENABLE_LOG_DELETE = 1000,
    /* periodically age out old blkseq nums */
    TMEV_PURGE_OLD_BLKSEQ = 1001,

    /* periodically purge old transactions older than 5min */
    TMEV_PURGE_OLD_LONGTRN = 1002,

    /* post stats to comdb2 area ever 5 seconds */
    TMEV_GLM_STATS = 1004,

    /* hook to make the timer thread stop */
    TMEV_EXIT = 1005,
};

#endif
