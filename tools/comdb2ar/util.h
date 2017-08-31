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

#ifndef __UTIL_H__
#define __UTIL_H__

#include <sys/types.h>
#include <inttypes.h>

#define YESNO(x) ((x) ? "yes" : "no")

extern int gbl_myroom;

void perror_errnum(const char *s, int errnum);
int strcmpfunc(char **a, char **b, int len);
u_int strhashfunc(u_char **keyp, int len);
int gooddir(char *d);

void xorbufcpy(char *dest, const char *src, size_t len);
#define xorbuf(p,len) xorbufcpy((void*)(p), (const void *)(p), (len))

/* load a text file.  returns NULL and prints an error if it fails.
 * caller is responsible for free()ing the returned memory. */
char *load_text_file(const char *filename);

/* Load the given lrl file and change or add a line for the given table. */
int rewrite_lrl_table(const char *lrlname, const char *tablename,
        const char *csc2path);
/* Create an external lrl file with the db's table defs */
int rewrite_lrl_un_llmeta(const char *p_lrl_fname_in,
        const char *p_lrl_fname_out, char *p_table_names[],
        char *p_csc2_paths[], int table_nums[], size_t num_tables,
        char *out_lrl_dir, int has_sp);
/* Remove all table definitions from the lrl file and append use_llmeta */
int rewrite_lrl_remove_tables(const char *lrlname );

char *fmt_size(char *buf, size_t bufsz, uint64_t bytes);

int cpu_usage(double *user_time, double *system_time);

void timeval_diff(struct timeval *before, struct timeval *after, struct timeval *diff);
int getroom_callback(void *dummy, int node);
#endif
