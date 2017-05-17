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

/*
 * This module maintains a list of genids that are associated with inprogress
 * transactions, and allows you to block until a given genid is no longer
 * "in prgress" (i.e. replicating).
 */

#ifndef INCLUDED_REPL_WAIT_H
#define INCLUDED_REPL_WAIT_H

struct repl_object;

void repl_list_init(void);
void repl_wait_stats(void);

/* Add the given genid to a list of genids associated with a transaction.
 * Pass in the head of a singly linked list of existing replication objects
 * associated ith the transaction.  The return value is the new list head. */
struct repl_object *add_genid_to_repl_list(unsigned long long genid,
                                           struct repl_object *head);

/* Block until the given genid is no longer active in our replication list. */
void wait_for_genid_repl(unsigned long long genid);

/* Given the list head, remove all the genids associated with a transaction
 * from memory and release any threads blocked on them. */
void clear_trans_from_repl_list(struct repl_object *head);

#endif /* INCLUDED_REPL_WAIT_H */
