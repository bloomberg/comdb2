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

#ifndef INCLUDED_RNGEXTRX_H
#define INCLUDED_RNGEXTRX_H

#define BLK_DATA_OFFSET 30
#define BLK_ASKARR_OFFSET 10
#define MAXKEYS 1000000

enum { SEGMAX = 256, MAXBUF = (1024 * 16 + BLK_DATA_OFFSET * sizeof(int)) };
enum {
    FLAG_CONTINUE = 0x80,
    FLAG_VERBOSE = 0x01,
    FLAG_LIGHTTRACE = 0x02,
    FLAG_NOCOUNT = 0x04,
    FLAG_USEFIRSTKEY = 0x08
};

typedef struct rngextrquery {
    int db;               /* DB number */
    int keylength;        /* key length */
    int index;            /* index number */
    int askarray[SEGMAX]; /* ask array */
    int maxkeys;          /* maximum keys to extract */
    int remote;           /* remote node to go to */
    int blk[MAXBUF];      /* block array */
    int blksz;            /* size of the block to use */
    char *retbuf;         /* pointer to return record data buffer */
    char startkey[64];    /* starting key */
    char endkey[64];      /* end key */
    int reserved[256];    /* reserved integers */
} rngextx_t;

#ifdef __cplusplus
extern "C" {
#endif

void rangextx_(int *db, char *skey, char *ekey, int *keyl, int *idx,
               char *retbuf, int *blk, int *blksz, int *ask, int *remote,
               int *maxkeys, int *rcode);

int rangextx(rngextx_t *);

int rangextx_full(int db, char *skey, char *ekey, int keyl, int idx,
                  char *retbuf, int *blk, int blksz, int *ask, int maxkeys,
                  int remote);

int rangextxf_bsz(int db, char *skey, char *ekey, int keyl, int idx,
                  char *retbuf, int *blk, int blksz, int *ask, int maxkeys,
                  int remote, int bufsize);

/* This is a fortran wrapper for the 'full' version */
void rangextxf_(int *db, short *skey, short *ekey, int *keyl, int *idx,
                char *retbuf, int *blk, int *blksz, int *ask, int *maxkeys,
                int *remote, int *rcode);

void rangkeyx_compat_(int *db, short *skey, short *ekey, int *keyl, int *idx,
                      short *ubuf, short blk[128], short ask[42], int *rcode,
                      int *remote);

extern int rangextxp(int db, char *skey, char *ekey, int keyl, int idx,
                     char *retbuf, int *blk, int MAXBUF, int *ask, int maxkeys,
                     int remote);

extern void rangextx_compat_(int *db, short *skey, short *ekey, int *keyl,
                             int *idx, short *ubuf, short blk[128],
                             short ask[42], int *rcode, int *remote);

extern void rangextx_compat2_(int *db, short *skey, short *ekey, int *keylp,
                              int *idx, short *ubuf, short blk[128],
                              short ask[42], int *rcode, int *remote,
                              int *glblchk);

void rangext_log_(const int *p_dbnum); /* private, do not call directly */

#ifdef __cplusplus
}
#endif

#endif
