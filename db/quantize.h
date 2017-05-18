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

#ifndef __QUANTIZE_H_INCLUDED__
#define __QUANTIZE_H_INCLUDED__
/* module to generate distribution and count reports */

/* DISTRIBUTION TABLE */

/* allocate new quantize struct */
struct quantize *quantize_new(int step, int qmax, char *units_name);

/* release resources */
void quantize_free(struct quantize *q);

/* count a value */
void quantize(struct quantize *q, int val);

/* consolidate counts from source in to dest, and reset source */
int quantize_consolidate(struct quantize *dest, struct quantize *source);

/* dump distribution table */
int quantize_dump(struct quantize *q, FILE *ff);

void quantize_clear(struct quantize *q);
int quantize_ctrace(struct quantize *q, char *title);

/* OBJECT COUNTS */

/* allocate new counting struct, to track counts by object */
struct qcount *qcount_new(int objlen);

/* add value to count for an object */
void qcount_count(struct qcount *q, void *obj, int val);

/* how many objects are tracked */
int qcount_numobjs(struct qcount *q);

typedef void qcount_callback_func(void *callbackarg, void *obj, int numvals,
                                  long long sumval, int minval, int maxval);
/*iterate objects and call back with various counts */
void qcount_foreach(struct qcount *q, qcount_callback_func *callback,
                    void *callbackarg);

#endif
