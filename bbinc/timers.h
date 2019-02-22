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

#ifndef DEFINED_TIMERS_H
#define DEFINED_TIMERS_H

struct timer_parm /*sent from timserv */
    {
    int parm; /* parm you registered */
    unsigned int epoch;
    unsigned int epochms;
};

int comdb2_cantim(int parm);
int comdb2_timer(int ms, int parm);
int comdb2_timprm(int ms, int parm);

void timer_init(void (*func)(struct timer_parm *));

#endif
