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

#ifndef INCLUDE_SC_QUEUES_H
#define INCLUDE_SC_QUEUES_H

int do_alter_queues_int(struct schema_change_type *);
int consumer_change(const char *queuename, int consumern, const char *method);
int add_queue_to_environment(char *table, int avgitemsz, int pagesize);
int perform_trigger_update(struct schema_change_type *, struct ireq *);
int perform_trigger_update_tran(struct schema_change_type *, struct ireq *, tran_type *ltran, int lock_schema_and_sp_lk);
int perform_trigger_update_replicant(tran_type *, const char *queue_name,
                                     scdone_t);
int finalize_trigger(struct schema_change_type *);

int reopen_qdb(const char *queue_name, uint32_t flags, tran_type *tran);

int do_add_qdb_file(struct ireq *iq, struct schema_change_type *s,
                    tran_type *tran);

int finalize_add_qdb_file(struct ireq *iq, struct schema_change_type *s,
                          tran_type *tran);

int do_del_qdb_file(struct ireq *iq, struct schema_change_type *s,
                    tran_type *tran);

int finalize_del_qdb_file(struct ireq *iq, struct schema_change_type *s,
                          tran_type *tran);

#endif
