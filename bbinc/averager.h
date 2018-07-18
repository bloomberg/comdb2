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

#ifndef INCLUDED_AVERAGER_H
#define INCLUDED_AVERAGER_H

#include <time.h>

struct averager;
struct averager *averager_new(int limit, int maxpoints);
void averager_add(struct averager *avg, int value, int now);
double averager_avg(struct averager *avg);
int averager_max(struct averager *avg);
int averager_min(struct averager *avg);
void averager_destroy(struct averager *avg);
int averager_depth(struct averager *avg);
void averager_purge_old(struct averager *avg, int now);

struct point {
    time_t time_added;
    int value;
};

int averager_get_points(struct averager *agv, struct point **values, int *nvalues);

#endif
