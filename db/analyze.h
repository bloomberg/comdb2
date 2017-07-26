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

#ifndef INCLUDED_ANALYZE_H
#define INCLUDED_ANALYZE_H

/**
 * Returns 1 if this is an analyze-thread which has a sampled (compressed) btree
 * for the
 * table passed in as an argument.  Returns a 0 otherwise.
 */
int analyze_is_sampled(struct sqlclntstate *client, char *table, int idx);

/**
 * Allows the user to set the sampling (compression)-threshold for the analyze
 * module.  If
 * the tablesize is less than thresh, run analyze against the actual btree
 * rather than the sampled (compressed) btree.
 */
int analyze_set_sampling_threshold(void *context, void *thresh);

/**
 * Returns the current sampling (compression) threshold for chosing sampled
 * (compressed) tables.
 */
long long analyze_get_sampling_threshold(void);

/**
 * Returns a pointer to a sampled (compressed) temptable version of the table if
 * it
 * exists, or a NULL otherwise.
 */
struct temp_table *analyze_get_sampled_temptable(struct sqlclntstate *client,
                                                 char *table, int idx);

/**
 * Retrieve the actual number of compressed records in this sampled (compressed)
 * index.
 * This is required for sqlite_stat1
 */
int analyze_get_nrecs(int iTable);

/**
 * Retrieve the number of sampled (previously misnamed compressed) records in
 *this sampled index.  This
 * optimizes sqlite3BtreeCount for sampled indexes.
 *
 */
int64_t analyze_get_sampled_nrecs(const char *dbname, int ixnum);

/**
 * Scale and analyze this table.  Write the results to sqlite_stat1.
 */
int analyze_table(char *table, SBUF2 *sb, int scale, int override_llmeta);

/**
 * Scale and analyze all tables in the database.  Write the results to
 * sqlite_stat1.
 */
int analyze_database(SBUF2 *sb, int scale, int override_llmeta);

/**
 * Backout to the previous analysis for table(s), or to no-analysis if there
 * is none.
 */
void handle_backout(SBUF2 *sb, char *table);

/* rename idx old to new for table tbl */
void add_idx_stats(const char *tbl, const char *oldname, const char *newname);

/**
 * Enable the sampled (compressed) btree logic.
 */
void analyze_enable_sampled_indicies(void);

/**
 * Set the maximum number of concurrent tables analyzed.
 */
int analyze_set_max_table_threads(void *context, void *maxtd);

/**
 * Set the maximum number of concurrent sampling (compression) threads.
 */
int analyze_set_max_sampling_threads(void *context, void *maxtd);

/**
 * Disable the sampled (compressed) btree logic.
 */
void analyze_disable_sampled_indicies(void);

/**
 * Print analyze stats
 */
int analyze_dump_stats(void);

/**
 * Return 1 if analyze is running, 0 otherwise.
 */
int analyze_is_running(void);

/**
 * Cleanup stale stats, delete all entries that don't have a valid table/index
 */
void cleanup_stats(SBUF2 *sb);

int do_analyze(char *tbl, int percent);

/* Get analyze_abort_requested variable state */
int get_analyze_abort_requested();

#endif
