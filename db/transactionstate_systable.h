/*
   Copyright 2022 Bloomberg Finance L.P.

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

#ifndef __TRANSACTIONSTATE_SYSTABLE_H_
#define __TRANSACTIONSTATE_SYSTABLE_H_

typedef struct thd_info {
    char *state;
    double time;
    char *machine;
    char *opcode;
    char *function;
    int64_t isIdle;
} thd_info;

int get_thd_info(thd_info **data, int *npoints);
void free_thd_info(thd_info *data, int npoints);

#endif
