/*
   Copyright 2017, Bloomberg Finance L.P.

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

#ifndef CONFIG_H
#define CONFIG_H

void print_usage_and_exit();
int handle_cmdline_options(int argc, char **argv, char **lrlname);
int read_lrl_files(struct dbenv *dbenv, const char *lrlname);
void getmyaddr();

struct read_lrl_option_type;
typedef int(lrl_reader)(struct dbenv *, char *, struct read_lrl_option_type *, int);
int deferred_do_commands(struct dbenv *, char *, struct read_lrl_option_type *, int);
void process_deferred_options(struct dbenv *, enum deferred_option_level, void *, lrl_reader *);
void clear_deferred_options(struct dbenv *, enum deferred_option_level);
void add_cmd_line_tunables_to_file(FILE *);

#endif /* CONFIG_H */
