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

#ifndef __bdb_fetch_h__
#define __bdb_fetch_h__

#include "bdb_api.h"
#include "bdb_cursor.h"
#include <stdlib.h>
#include <inttypes.h>

/*
  bdb_fetch(): "fetch" a record.
  INPUT:       bdb_handle : valid handle returned from bdb_open() or
                            bdb_open_more()
               ix         : pointer to index data
               ixnum      : which index are we using for the find
               ixlen      : size in bytes of index specified data
               dta        : pointer to memory in which to return
                            the data portion of the found record
               dtalen     : size in bytes of the memory pointed to
                            by dta
  OUTPUT:      RETURN     : success :  0 : found exact match.  next key
                                           is not exact match.
                                       1 : found exact match.  next key
                                           is also exact match.
                                       2 : not found,
                                           here is next
                                       3 : not found, end of db hit,
                                           here is last
                                       99: not found, db empty,
                                           here is nothing
                            failure : -1 : additional info in bdberr
               dta        : pointer to the data of the found record
               reqdtalen  : the size in bytes of the returned record.
                            if (reqdtalen > dtalen) then only the first
                            dtalen bytes of the record have been returned
                            into the memory pointed to by dta.
               ixfound    : pointer to the full key that was found.  ixfound
                            must point to memory >= to the size of the ix
                            as was initially speicfied when bdb_open() was
                            called.
               rrn        : rrn of the record that is being returned.
                            only defined for RETURN = 0, 1, 2, 3.
               bdberr     : will be set to provide additional
                            info on failure.  possibilities are:
                             catastrophic:
                              BDBERR_FETCH_IX  : trouble with ix file
                              BDBERR_FETCH_DTA : trouble with dta file
                              BDBERR_MISC      : some problem
                             non catastrophic:
                              BDBERR_BADARGS   : invalid args
*/

typedef struct {
    uint8_t ver;
    uint8_t ignore_incoherent;
    uint8_t page_order;
    uint8_t for_write;
    void *(*fn_malloc)(int); /* user-specified malloc function */
    void (*fn_free)(void *); /* user-specified free function */
} bdb_fetch_args_t;

int bdb_fetch(bdb_state_type *bdb_handle, void *ix, int ixnum, int ixlen,
              void *dta, int dtalen, int *reqdtalen, void *ixfound, int *rrn,
              bdb_fetch_args_t *arg, int *bdberr);

int bdb_fetch_by_recnum(bdb_state_type *bdb_handle, int ixnum, int recnum,
                        void *dta, int dtalen, int *reqdtalen, void *ixfound,
                        int *rrn, bdb_fetch_args_t *arg, int *bdberr);

int bdb_fetch_nodta(bdb_state_type *bdb_state, void *ix, int ixnum, int ixlen,
                    void *ixfound, int *rrn, bdb_fetch_args_t *arg,
                    int *bdberr);

int bdb_fetch_nodta_by_recnum(bdb_state_type *bdb_state, int ixnum, int recnum,
                              void *ixfound, int *rrn, bdb_fetch_args_t *arg,
                              int *bdberr);

int bdb_fetch_nodta_by_recnum_genid(bdb_state_type *bdb_state, int ixnum,
                                    int recnum, void *ixfound, int *rrn,
                                    unsigned long long *genid,
                                    bdb_fetch_args_t *arg, int *bdberr);

int bdb_fetch_next(bdb_state_type *bdb_handle, void *ix, int ixnum, int ixlen,
                   void *lastix, int lastrrn, unsigned long long lastgenid,
                   void *dta, int dtalen, int *reqdtalen, void *ixfound,
                   int *rrn, bdb_fetch_args_t *arg, int *bdberr);

int bdb_fetch_next_nodta(bdb_state_type *bdb_handle, void *ix, int ixnum,
                         int ixlen, void *lastix, int lastrrn,
                         unsigned long long lastgenid, void *ixfound, int *rrn,
                         bdb_fetch_args_t *arg, int *bdberr);

int bdb_fetch_prev(bdb_state_type *bdb_handle, void *ix, int ixnum, int ixlen,
                   void *lastix, int lastrrn, unsigned long long lastgenid,
                   void *dta, int dtalen, int *reqdtalen, void *ixfound,
                   int *rrn, bdb_fetch_args_t *arg, int *bdberr);

int bdb_fetch_prev_nodta(bdb_state_type *bdb_handle, void *ix, int ixnum,
                         int ixlen, void *lastix, int lastrrn,
                         unsigned long long lastgenid, void *ixfound, int *rrn,
                         bdb_fetch_args_t *arg, int *bdberr);

int bdb_fetch_recnum(bdb_state_type *bdb_handle, void *ix, int ixnum, int ixlen,
                     void *dta, int dtalen, int *reqdtalen, void *ixfound,
                     int *rrn, int *recnum, bdb_fetch_args_t *arg, int *bdberr);

int bdb_fetch_recnum_by_recnum_genid(bdb_state_type *bdb_state, int ixnum,
                                     int recnum_in, void *dta, int dtalen,
                                     int *reqdtalen, void *ixfound, int *rrn,
                                     int *recnum_out, unsigned long long *genid,
                                     bdb_fetch_args_t *arg, int *bdberr);

int bdb_fetch_recnum_by_recnum(bdb_state_type *bdb_handle, int ixnum,
                               int recnum_in, void *dta, int dtalen,
                               int *reqdtalen, void *ixfound, int *rrn,
                               int *recnum_out, bdb_fetch_args_t *arg,
                               int *bdberr);

int bdb_fetch_lastdupe_recnum(bdb_state_type *bdb_handle, void *ix, int ixnum,
                              int ixlen, void *dta, int dtalen, int *reqdtalen,
                              void *ixfound, int *rrn, int *recnum,
                              bdb_fetch_args_t *arg, int *bdberr);

int bdb_fetch_nodta_lastdupe_recnum(bdb_state_type *bdb_handle, void *ix,
                                    int ixnum, int ixlen, void *ixfound,
                                    int *rrn, int *recnum,
                                    bdb_fetch_args_t *arg, int *bdberr);

int bdb_fetch_nodta_recnum(bdb_state_type *bdb_handle, void *ix, int ixnum,
                           int ixlen, void *ixfound, int *rrn, int *recnum,
                           bdb_fetch_args_t *arg, int *bdberr);

int bdb_fetch_nodta_recnum_by_recnum(bdb_state_type *bdb_handle, int ixnum,
                                     int recnum_in, void *ixfound, int *rrn,
                                     int *recnum_out, bdb_fetch_args_t *arg,
                                     int *bdberr);

int bdb_fetch_genid_trans(bdb_state_type *bdb_state, void *ix, int ixnum,
                          int ixlen, void *dta, int dtalen, int *reqdtalen,
                          void *ixfound, int *rrn, unsigned long long *genid,
                          int ignore_ixdta, void *tran, bdb_fetch_args_t *arg,
                          int *bdberr);

int bdb_fetch_next_recnum(bdb_state_type *bdb_handle, void *ix, int ixnum,
                          int ixlen, void *lastix, int lastrrn,
                          unsigned long long lastgenid, void *dta, int dtalen,
                          int *reqdtalen, void *ixfound, int *rrn, int *recnum,
                          bdb_fetch_args_t *arg, int *bdberr);

int bdb_fetch_next_nodta_recnum(bdb_state_type *bdb_handle, void *ix, int ixnum,
                                int ixlen, void *lastix, int lastrrn,
                                unsigned long long lastgenid, void *ixfound,
                                int *rrn, int *recnum, bdb_fetch_args_t *arg,
                                int *bdberr);

int bdb_fetch_prev_recnum(bdb_state_type *bdb_handle, void *ix, int ixnum,
                          int ixlen, void *lastix, int lastrrn,
                          unsigned long long lastgenid, void *dta, int dtalen,
                          int *reqdtalen, void *ixfound, int *rrn, int *recnum,
                          bdb_fetch_args_t *arg, int *bdberr);

int bdb_fetch_prev_nodta_recnum(bdb_state_type *bdb_handle, void *ix, int ixnum,
                                int ixlen, void *lastix, int lastrrn,
                                unsigned long long lastgenid, void *ixfound,
                                int *rrn, int *recnum, bdb_fetch_args_t *arg,
                                int *bdberr);

int bdb_fetch_genid(bdb_state_type *bdb_handle, void *ix, int ixnum, int ixlen,
                    void *dta, int dtalen, int *reqdtalen, void *ixfound,
                    int *rrn, unsigned long long *genid, int ignore_ixdta,
                    bdb_fetch_args_t *arg, int *bdberr);

int bdb_fetch_genid_nl_ser(bdb_state_type *bdb_state, void *ix, int ixnum,
                           int ixlen, void *dta, int dtalen, int *reqdtalen,
                           void *ixfound, int *rrn, unsigned long long *genid,
                           int ignore_ixdta, bdb_cursor_ser_t *cur_ser,
                           bdb_fetch_args_t *arg, int *bdberr);

int bdb_fetch_genid_trans(bdb_state_type *bdb_handle, void *ix, int ixnum,
                          int ixlen, void *dta, int dtalen, int *reqdtalen,
                          void *ixfound, int *rrn, unsigned long long *genid,
                          int ignore_ixdta, void *tran, bdb_fetch_args_t *arg,
                          int *bdberr);

int bdb_fetch_genid_dirty(bdb_state_type *bdb_state, void *ix, int ixnum,
                          int ixlen, void *dta, int dtalen, int *reqdtalen,
                          void *ixfound, int *rrn, unsigned long long *genid,
                          int ignore_ixdta, bdb_fetch_args_t *arg, int *bdberr);

int bdb_fetch_genid_prefault(bdb_state_type *bdb_state, void *ix, int ixnum,
                             int ixlen, void *dta, int dtalen, int *reqdtalen,
                             void *ixfound, int *rrn, unsigned long long *genid,
                             int ignore_ixdta, bdb_fetch_args_t *arg,
                             int *bdberr);

int bdb_fetch_blobs_genid(bdb_state_type *bdb_handle, void *ix, int ixnum,
                          int ixlen, void *dta, int dtalen, int *reqdtalen,
                          void *ixfound, int *rrn, unsigned long long *genid,
                          int numblobs, int *dtafilenums, size_t *blobsizes,
                          size_t *bloboffs, void **blobptrs, int ignore_ixdta,
                          bdb_fetch_args_t *arg, int *bdberr);

int bdb_fetch_blobs_genid_dirty(bdb_state_type *bdb_state, void *ix, int ixnum,
                                int ixlen, void *dta, int dtalen,
                                int *reqdtalen, void *ixfound, int *rrn,
                                unsigned long long *genid, int numblobs,
                                int *dtafilenums, size_t *blobsizes,
                                size_t *bloboffs, void **blobptrs, int ixdta,
                                bdb_fetch_args_t *arg, int *bdberr);

int bdb_fetch_genid_by_recnum(bdb_state_type *bdb_handle, int ixnum, int recnum,
                              void *dta, int dtalen, int *reqdtalen,
                              void *ixfound, int *rrn,
                              unsigned long long *genid, bdb_fetch_args_t *arg,
                              int *bdberr);

int bdb_fetch_nodta_genid(bdb_state_type *bdb_state, void *ix, int ixnum,
                          int ixlen, void *ixfound, int *rrn,
                          unsigned long long *genid, bdb_fetch_args_t *arg,
                          int *bdberr);

int bdb_fetch_nodta_genid_nl_ser(bdb_state_type *bdb_state, void *ix, int ixnum,
                                 int ixlen, void *ixfound, int *rrn,
                                 unsigned long long *genid,
                                 bdb_cursor_ser_t *cur_ser,
                                 bdb_fetch_args_t *arg, int *bdberr);

int bdb_fetch_nodta_genid_dirty(bdb_state_type *bdb_state, void *ix, int ixnum,
                                int ixlen, void *ixfound, int *rrn,
                                unsigned long long *genid,
                                bdb_fetch_args_t *arg, int *bdberr);

int bdb_fetch_nodta_genid_prefault(bdb_state_type *bdb_state, void *ix,
                                   int ixnum, int ixlen, void *ixfound,
                                   int *rrn, unsigned long long *genid,
                                   bdb_fetch_args_t *arg, int *bdberr);

int bdb_fetch_nodta_genid_by_recnum(bdb_state_type *bdb_state, int ixnum,
                                    int recnum, void *ixfound, int *rrn,
                                    unsigned long long *genid,
                                    bdb_fetch_args_t *arg, int *bdberr);

int bdb_fetch_next_genid_nl_ser(bdb_state_type *bdb_state, void *ix, int ixnum,
                                int ixlen, void *lastix, int lastrrn,
                                unsigned long long lastgenid, void *dta,
                                int dtalen, int *reqdtalen, void *ixfound,
                                int *rrn, unsigned long long *genid,
                                bdb_cursor_ser_t *cur_ser,
                                bdb_fetch_args_t *arg, int *bdberr);

int bdb_fetch_next_genid_tran(bdb_state_type *bdb_handle, void *ix, int ixnum,
                              int ixlen, void *lastix, int lastrrn,
                              unsigned long long lastgenid, void *dta,
                              int dtalen, int *reqdtalen, void *ixfound,
                              int *rrn, unsigned long long *genid, void *tran,
                              bdb_fetch_args_t *arg, int *bdberr);

int bdb_fetch_next_nodta_genid_tran(bdb_state_type *bdb_handle, void *ix,
                                    int ixnum, int ixlen, void *lastix,
                                    int lastrrn, unsigned long long lastgenid,
                                    void *ixfound, int *rrn,
                                    unsigned long long *genid, void *tran,
                                    bdb_fetch_args_t *arg, int *bdberr);

int bdb_fetch_next_nodta_genid_tran(bdb_state_type *bdb_state, void *ix,
                                    int ixnum, int ixlen, void *lastix,
                                    int lastrrn, unsigned long long lastgenid,
                                    void *ixfound, int *rrn,
                                    unsigned long long *genid, void *tran,
                                    bdb_fetch_args_t *args, int *bdberr);

int bdb_fetch_next_nodta_genid_nl_ser(bdb_state_type *bdb_state, void *ix,
                                      int ixnum, int ixlen, void *lastix,
                                      int lastrrn, unsigned long long lastgenid,
                                      void *ixfound, int *rrn,
                                      unsigned long long *genid,
                                      bdb_cursor_ser_t *cur_ser,
                                      bdb_fetch_args_t *arg, int *bdberr);

int bdb_fetch_prev_genid_tran(bdb_state_type *bdb_handle, void *ix, int ixnum,
                              int ixlen, void *lastix, int lastrrn,
                              unsigned long long lastgenid, void *dta,
                              int dtalen, int *reqdtalen, void *ixfound,
                              int *rrn, unsigned long long *genid, void *tran,
                              bdb_fetch_args_t *arg, int *bdberr);

int bdb_fetch_prev_genid_nl_ser(bdb_state_type *bdb_state, void *ix, int ixnum,
                                int ixlen, void *lastix, int lastrrn,
                                unsigned long long lastgenid, void *dta,
                                int dtalen, int *reqdtalen, void *ixfound,
                                int *rrn, unsigned long long *genid,
                                bdb_cursor_ser_t *cur_ser,
                                bdb_fetch_args_t *arg, int *bdberr);

int bdb_fetch_prev_nodta_genid_tran(bdb_state_type *bdb_handle, void *ix,
                                    int ixnum, int ixlen, void *lastix,
                                    int lastrrn, unsigned long long lastgenid,
                                    void *ixfound, int *rrn,
                                    unsigned long long *genid, void *tran,
                                    bdb_fetch_args_t *arg, int *bdberr);

int bdb_fetch_prev_nodta_genid_nl_ser(bdb_state_type *bdb_state, void *ix,
                                      int ixnum, int ixlen, void *lastix,
                                      int lastrrn, unsigned long long lastgenid,
                                      void *ixfound, int *rrn,
                                      unsigned long long *genid,
                                      bdb_cursor_ser_t *cur_ser,
                                      bdb_fetch_args_t *arg, int *bdberr);

int bdb_fetch_recnum_genid(bdb_state_type *bdb_handle, void *ix, int ixnum,
                           int ixlen, void *dta, int dtalen, int *reqdtalen,
                           void *ixfound, int *rrn, int *recnum,
                           unsigned long long *genid, bdb_fetch_args_t *arg,
                           int *bdberr);

int bdb_fetch_recnum_genid_by_recnum(bdb_state_type *bdb_handle, int ixnum,
                                     int recnum_in, void *dta, int dtalen,
                                     int *reqdtalen, void *ixfound, int *rrn,
                                     int *recnum_out, unsigned long long *genid,
                                     bdb_fetch_args_t *arg, int *bdberr);

int bdb_fetch_lastdupe_recnum_genid(bdb_state_type *bdb_handle, void *ix,
                                    int ixnum, int ixlen, void *dta, int dtalen,
                                    int *reqdtalen, void *ixfound, int *rrn,
                                    int *recnum, unsigned long long *genid,
                                    bdb_fetch_args_t *arg, int *bdberr);

int bdb_fetch_nodta_lastdupe_recnum_genid(bdb_state_type *bdb_handle, void *ix,
                                          int ixnum, int ixlen, void *ixfound,
                                          int *rrn, int *recnum,
                                          unsigned long long *genid,
                                          bdb_fetch_args_t *arg, int *bdberr);

int bdb_fetch_nodta_recnum_genid(bdb_state_type *bdb_handle, void *ix,
                                 int ixnum, int ixlen, void *ixfound, int *rrn,
                                 int *recnum, unsigned long long *genid,
                                 bdb_fetch_args_t *arg, int *bdberr);

int bdb_fetch_nodta_recnum_genid_by_recnum(bdb_state_type *bdb_handle,
                                           int ixnum, int recnum_in,
                                           void *ixfound, int *rrn,
                                           int *recnum_out,
                                           unsigned long long *genid,
                                           bdb_fetch_args_t *arg, int *bdberr);

int bdb_fetch_next_recnum_genid(bdb_state_type *bdb_handle, void *ix, int ixnum,
                                int ixlen, void *lastix, int lastrrn,
                                unsigned long long lastgenid, void *dta,
                                int dtalen, int *reqdtalen, void *ixfound,
                                int *rrn, int *recnum,
                                unsigned long long *genid,
                                bdb_fetch_args_t *arg, int *bdberr);

int bdb_fetch_next_nodta_recnum_genid(bdb_state_type *bdb_handle, void *ix,
                                      int ixnum, int ixlen, void *lastix,
                                      int lastrrn, unsigned long long lastgenid,
                                      void *ixfound, int *rrn, int *recnum,
                                      unsigned long long *genid,
                                      bdb_fetch_args_t *arg, int *bdberr);

int bdb_fetch_prev_recnum_genid(bdb_state_type *bdb_handle, void *ix, int ixnum,
                                int ixlen, void *lastix, int lastrrn,
                                unsigned long long lastgenid, void *dta,
                                int dtalen, int *reqdtalen, void *ixfound,
                                int *rrn, int *recnum,
                                unsigned long long *genid,
                                bdb_fetch_args_t *arg, int *bdberr);

int bdb_fetch_prev_nodta_recnum_genid(bdb_state_type *bdb_handle, void *ix,
                                      int ixnum, int ixlen, void *lastix,
                                      int lastrrn, unsigned long long lastgenid,
                                      void *ixfound, int *rrn, int *recnum,
                                      unsigned long long *genid,
                                      bdb_fetch_args_t *arg, int *bdberr);

int bdb_fetch_next_blobs_genid(
    bdb_state_type *bdb_state, void *ix, int ixnum, int ixlen, void *lastix,
    int lastrrn, unsigned long long lastgenid, void *dta, int dtalen,
    int *reqdtalen, void *ixfound, int *rrn, unsigned long long *genid,
    int numblobs, int *dtafilenums, size_t *blobsizes, size_t *bloboffs,
    void **blobptrs, bdb_fetch_args_t *arg, int *bdberr);

int bdb_fetch_prev_blobs_genid(
    bdb_state_type *bdb_state, void *ix, int ixnum, int ixlen, void *lastix,
    int lastrrn, unsigned long long lastgenid, void *dta, int dtalen,
    int *reqdtalen, void *ixfound, int *rrn, unsigned long long *genid,
    int numblobs, int *dtafilenums, size_t *blobsizes, size_t *bloboffs,
    void **blobptrs, bdb_fetch_args_t *arg, int *bdberr);

/* "nl" : No Lookahead.  No "next also matches" semantics for apis (like sql)
   or future comdb2 streaming apis that dont care about this and dont need
   to be slower because of it */
int bdb_fetch_next_genid_nl(bdb_state_type *bdb_state, void *ix, int ixnum,
                            int ixlen, void *lastix, int lastrrn,
                            unsigned long long lastgenid, void *dta, int dtalen,
                            int *reqdtalen, void *ixfound, int *rrn,
                            unsigned long long *genid, bdb_fetch_args_t *arg,
                            int *bdberr);

int bdb_fetch_prev_genid_nl(bdb_state_type *bdb_state, void *ix, int ixnum,
                            int ixlen, void *lastix, int lastrrn,
                            unsigned long long lastgenid, void *dta, int dtalen,
                            int *reqdtalen, void *ixfound, int *rrn,
                            unsigned long long *genid, bdb_fetch_args_t *arg,
                            int *bdberr);

/*
  fetch by rrn/genid:
    we need this in the comdb2 layer to support delete by rrn/genid and update
    rrn/genid to newdta in order to be able to form the keys for the old
    rrn/genid that we need to delete.
  it is not intended that this api be exposed directly to the human user.
*/
int bdb_fetch_by_rrn_and_genid(bdb_state_type *bdb_state, int rrn,
                               unsigned long long genid, void *dta, int dtalen,
                               int *reqdtalen, bdb_fetch_args_t *arg,
                               int *bdberr);

int bdb_fetch_by_rrn_and_genid_dirty(bdb_state_type *bdb_state, int rrn,
                                     unsigned long long genid, void *dta,
                                     int dtalen, int *reqdtalen,
                                     bdb_fetch_args_t *arg, int *bdberr);

int bdb_fetch_by_rrn_and_genid_prefault(bdb_state_type *bdb_state, int rrn,
                                        unsigned long long genid, void *dta,
                                        int dtalen, int *reqdtalen,
                                        bdb_fetch_args_t *arg, int *bdberr);

int bdb_fetch_by_rrn_and_genid_get_curgenid(bdb_state_type *bdb_state, int rrn,
                                            unsigned long long genid,
                                            unsigned long long *outgenid,
                                            void *dta, int dtalen,
                                            int *reqdtalen,
                                            bdb_fetch_args_t *arg, int *bdberr);

int bdb_fetch_by_rrn_and_genid_tran(bdb_state_type *bdb_state, tran_type *tran,
                                    int rrn, unsigned long long genid,
                                    void *dta, int dtalen, int *reqdtalen,
                                    bdb_fetch_args_t *arg, int *bdberr);

/*
  fetch blobs by rrn/genid
  this is used to get the data record(s) for a given rrn and genid.  at this
  low level if it not considered an error if there are no data records in some
  of the data files.
*/
int bdb_fetch_blobs_by_rrn_and_genid(bdb_state_type *bdb_state, int rrn,
                                     unsigned long long genid, int numblobs,
                                     int *dtafilenums, size_t *blobsizes,
                                     size_t *bloboffs, void **blobptrs,
                                     bdb_fetch_args_t *arg, int *bdberr);
int bdb_fetch_blobs_by_rrn_and_genid_cursor(
    bdb_state_type *bdb_state, int rrn, unsigned long long genid, int numblobs,
    int *dtafilenums, size_t *blobsizes, size_t *bloboffs, void **blobptrs,
    bdb_cursor_ifn_t *cursor_parent, bdb_fetch_args_t *arg, int *bdberr);
int bdb_fetch_blobs_by_rrn_and_genid_tran(
    bdb_state_type *bdb_state, tran_type *tran, int rrn,
    unsigned long long genid, int numblobs, int *dtafilenums, size_t *blobsizes,
    size_t *bloboffs, void **blobptrs, bdb_fetch_args_t *arg, int *bdberr);

/*
  fetch by primkey
  this is needed for compatibility with non-tagged comdb2 api
*/
int bdb_fetch_by_primkey_tran(bdb_state_type *bdb_state, tran_type *tran,
                              void *ix, void *dta, int dtalen, int *reqdtalen,
                              int *rrn, unsigned long long *genid,
                              bdb_fetch_args_t *arg, int *bdberr);

/*
  fetch by key
  this is used in constraints code to do reverse lookups on keys
*/
int bdb_fetch_by_key_tran(bdb_state_type *bdb_state, tran_type *tran, void *ix,
                          int ixlen, int ixnum, void *ixfound, void *dta,
                          int dtalen, int *reqdtalen, int *rrn,
                          unsigned long long *genid, bdb_fetch_args_t *arg,
                          int *bdberr);

/* fetch the next record in a dtastripe data file.  the vector passed in
 * records the genids we've already processed for the various stripes.
 * *dta will point to the record data, the caller should free it
 * if stay_in_stripe is set, it will only look for the next genid in the current
 * stripe
 */
int bdb_fetch_next_dtastripe_record(bdb_state_type *bdb_state,
                                    const unsigned long long *p_genid_vector,
                                    unsigned long long *p_genid, int *p_stripe,
                                    int stay_in_stripe, void *p_dta, int dtalen,
                                    int *p_reqdtalen, void *trans,
                                    bdb_fetch_args_t *arg, int *p_bdberr);

#endif
