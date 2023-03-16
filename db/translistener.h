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

#ifndef INCLUDED_JAVASP_H
#define INCLUDED_JAVASP_H

#include "comdb2.h"
#include "block_internal.h"

struct javasp_trans_state;
struct javasp_rec;
struct timer_element;

enum {
    /* These flags mark listeners set to take effect before or after the
     * basic write operations.  BEFORE means just before we attempt to
     * perform the operation, giving the listener an opportunity to alter
     * the operation.  AFTER means just after the operation has been added
     * to the transaction (remember - the transaction may yet fail to commit).
     *
     * For now we just support AFTER since that's all we need for now.
     * Probably never support BEFORE - no use case for it.
     *
     * The SAVE_BLOB_ flags specify whether or not to remember the blob values
     * as they were before the transaction.  If not specified then they are
     * forgotten and the before records will have NULLs for all blob fields.
     */
    JAVASP_TRANS_LISTEN_BEFORE_ADD = 0x0001,
    JAVASP_TRANS_LISTEN_AFTER_ADD = 0x0002,

    JAVASP_TRANS_LISTEN_BEFORE_UPD = 0x0010,
    JAVASP_TRANS_LISTEN_AFTER_UPD = 0x0020,
    JAVASP_TRANS_LISTEN_SAVE_BLOBS_UPD = 0x0040,

    JAVASP_TRANS_LISTEN_BEFORE_DEL = 0x0100,
    JAVASP_TRANS_LISTEN_AFTER_DEL = 0x0200,
    JAVASP_TRANS_LISTEN_SAVE_BLOBS_DEL = 0x0400,

    /* Just before and just after the main commit. */
    JAVASP_TRANS_LISTEN_BEFORE_COMMIT = 0x1000,
    JAVASP_TRANS_LISTEN_AFTER_COMMIT = 0x2000,
};

enum {
    SP_FIELD_UINT16 = 1,
    SP_FIELD_UINT32 = 2,
    SP_FIELD_UINT64 = 3,
    SP_FIELD_INT16 = 4,
    SP_FIELD_INT32 = 5,
    SP_FIELD_INT64 = 6,
    SP_FIELD_REAL32 = 7,
    SP_FIELD_REAL64 = 8,
    SP_FIELD_STRING = 9,
    SP_FIELD_BYTEARRAY = 10,
    SP_FIELD_BLOB = 11,
    SP_FIELD_DATETIME = 12,
    SP_FIELD_INTERVALYM = 13,
    SP_FIELD_INTERVALDS = 14,
    SP_FIELD_DATETIMEUS = 15,
    SP_FIELD_INTERVALDSUS = 16
};

enum { TYPE_TAGGED_ADD = 0x10, TYPE_TAGGED_UPD = 0x20, TYPE_TAGGED_DEL = 0x40 };

enum { FIELD_FLAG_ABSENT = 0, FIELD_FLAG_NULL = 1, FIELD_FLAG_VALUE = 2 };

enum { JAVASP_OP_LOAD = 1, JAVASP_OP_RELOAD = 2, JAVASP_OP_UNLOAD = 3 };

/* This gets called from glue.c when a request is broadcast to us. */
int javasp_do_procedure_op(int op, const char *name, const char *param,
                           const char *paramvalue);

/* Called once from main() to init the Java subsystem, must come before any
 * of these other calls. */
void javasp_once_init(void);

int javasp_add_procedure(const char *name, const char *jar, const char *param);

/* Load the stored procedures contained in the given jar file.  param must be
 * a NULL terminated string which will be trimmed and split into whitespace
 * delimted arguments.
 */
int javasp_load_procedure(const char *name, const char *jarfile,
                          const char *param);

/* Reload the given stored procedure in one operation. */
int javasp_reload_procedure(const char *name, const char *jarfile,
                            const char *param);

int javasp_unload_procedure(const char *name);

/* Initialise all as yet uninitialised procedures. */
int javasp_init_procedures(void);

/* Signal restore/unrestore/status */
int restore_prejava_signals(void);
int restore_java_signals(void);

/* Print statistics about the Java engine. */
void javasp_stat(const char *args);

/* Process a Java custom operation request. */
int tocustom(struct ireq *iq);

/* Call this to begin a new transaction.  It initializes any state needed by
 * the Java SP engine to listen to transaction events. */
struct javasp_trans_state *javasp_trans_start(int debug);
struct javasp_trans_state *javasp_consumer_trans_start(int debug);

/* Set the ireq and transaction for the Java handle.  Must be called before
 * any Java triggers are invoked. */
void javasp_trans_set_trans(struct javasp_trans_state *javasp_trans_handle,
                            struct ireq *ireq, void *parent_trans, void *trans);

/* Call this at the end of a transaction (committed or aborted).  Cleans up. */
void javasp_trans_end(struct javasp_trans_state *javasp_trans_handle);

/* Call this prior to distributed commit to release splock */
void javasp_trans_release(struct javasp_trans_state *javasp_trans_handle);

/* This is used to determine if we want to be notified about a particular
 * event - if we don't care, then the block processor needn't waste time
 * creating temporary things.  It is not an error to report events that
 * the stored procedures don't care about (at this level),
 * it's just a waste of time. */
int javasp_trans_care_about(struct javasp_trans_state *javasp_trans_handle,
                            int event);

/* This is called for events in a tagged transaction (add/upd/del). */
int javasp_trans_tagged_trigger(struct javasp_trans_state *javasp_trans_handle,
                                int event, struct javasp_rec *oldrec,
                                struct javasp_rec *newrec, const char *tblname);

/* This is called for events in an untagged transaction (add/upd/del). */
int javasp_trans_untagged_trigger(
    struct javasp_trans_state *javasp_trans_handle, int event, void *oldrec,
    void *newrec, size_t reclen, const char *tblname);

/* A custom write operation.  Fails if no such operation is registered.
 * Calling this will also notify any registered listeners. */
int javasp_custom_write(struct javasp_trans_state *javasp_trans_handle,
                        const char *opname, const void *data, size_t datalen);

/* Execute a javasp queue consumer. */
int javasp_queue_consumer(struct javasp_trans_state *javasp_trans_handle,
                          const char *opname, const char *queuename,
                          const void *data, size_t datalen);

/* A custom read operation.  Fails is no such operation is registered.
 * A copy is made of the input data before the operation commences, so the
 * outdata pointer can overlap the input data (i.e. it can safely write the
 * reply data directly to the swapin buffer). */
int javasp_custom_read(struct ireq *iq, const char *opname, const void *indata,
                       size_t indatalen, void *outdata, size_t outdatamax,
                       size_t *outdatalen);

/* Allocate a javasp_tagged_rec structure for use in communicating record
 * contents to the Java code. */
struct javasp_rec *javasp_alloc_rec(const void *od_dta, size_t od_len,
                                    const char *tblname);

/* For an allocated record, set the Java transaction handle, rrn and genid.
 * This allows the record to retrieve unknown blobs. */
void javasp_rec_set_trans(struct javasp_rec *rec,
                          struct javasp_trans_state *javasp_trans_handle,
                          int rrn, long long genid);
void javasp_dealloc_rec(struct javasp_rec *rec);

/* Set the blob data for a particular blob. */
void javasp_rec_have_blob(struct javasp_rec *rec, int blobn,
                          const void *blobdta, size_t bloboff, size_t bloblen);

/* Set the blob data for a full record.  Caller still owns the blob memory. */
void javasp_rec_set_blobs(struct javasp_rec *rec, blob_status_t *blobs);
int javasp_load_procedure_int(const char *name, const char *param,
                              const char *paramvalue);
int javasp_unload_procedure_int(const char *name);

/* Check if stored procedure exists. */
int javasp_exists(const char *name);

/* Get info for qdb, suitable for comdb2_triggers */
#include <list.h>
typedef struct trigger_col_info trigger_col_info;
struct trigger_col_info {
    LINKC_T(trigger_col_info) lnk;
    char *name;
    int type;
};
typedef struct trigger_tbl_info trigger_tbl_info;
struct trigger_tbl_info {
    LINKC_T(trigger_tbl_info) lnk;
    char *name;
    LISTC_T(trigger_col_info) cols;
};
typedef struct {
    LISTC_T(trigger_tbl_info) tbls;
} trigger_info;
void get_trigger_info(const char *, trigger_info *);
void get_trigger_info_lk(const char *, trigger_info *);

void javasp_splock_wrlock(void);
void javasp_splock_rdlock(void);
void javasp_splock_unlock(void);

#endif
