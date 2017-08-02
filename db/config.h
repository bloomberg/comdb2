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
int process_deferred_options(struct dbenv *dbenv,
                             enum deferred_option_level lvl, void *usrdata,
                             int (*callback)(struct dbenv *env, char *option,
                                             void *p, int len));
int deferred_do_commands(struct dbenv *env, char *option, void *p, int len);
void getmyaddr();

#endif /* CONFIG_H */
