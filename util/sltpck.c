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

#include <sys/types.h>
#include <inttypes.h>
#include <sltpck.h>
#include <stdio.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <string.h>
#include <ctype.h>
#include <arpa/inet.h>

/**
 * @brief  Header block packed before data.
 */
struct slt_hdr_t {
    uint16_t sz;  /**< size of following data */
    uint16_t off; /**< offset to following data */
};

/**
 * @brief  Cursor used to pack data.
 */
struct slt_cur_t {
    size_t sz;      /**< total size allowed for cursor operation */
    size_t payload; /**< current size of data stored by cursor */
    int flg;        /**< flags stored by cursor */
    slt_dir_t dir;  /**< Direction packing is done */
    char *buf;      /**< Pointer to start of buffer */
    char *off;      /**< current offset in given buffer */
};

/* *** Prototypes *** */
static void slt_pck_buf(struct slt_cur_t *cur, void *dta, size_t dtasz);
static void slt_pck_hdr(struct slt_cur_t *cur, struct slt_hdr_t *hdr,
                        size_t dtasz);
static void *slt_unpck_bck(unsigned int FLAG, void *buf, int bufsz,
                           size_t *sltsz);
static void *slt_unpck_fwd(unsigned int FLAG, void *buf, int bufsz,
                           size_t *sltsz);
static int chkflg(unsigned int FLAG);

static void hdr_to_cpu(struct slt_hdr_t *);
static void hdr_from_cpu(struct slt_hdr_t *);

struct slt_cur_t *slt_init(void *buf, size_t bufsz, slt_dir_t dir)
{
    struct slt_cur_t *self;
    uintptr_t misalign;

    /* check there is a buffer */
    if (!buf) {
        return NULL;
    }

    /* check alignment */
    misalign = ((uintptr_t)buf) & (sizeof(int) - 1);
    if (misalign) {
        misalign = sizeof(int) - misalign;
        buf = (void *)((uintptr_t)buf + misalign);
        bufsz -= misalign;
    }

    /* check size */
    if (bufsz < sizeof(self->flg)) {
        return NULL;
    }

    /* get new cursor */
    self = (struct slt_cur_t *)malloc(sizeof(struct slt_cur_t));

    if (self) {
        /* init */
        self->sz = bufsz - sizeof(self->flg);
        self->payload = 0;
        if (misalign) {
            self->payload += misalign;
        }
        self->flg = 0;
        self->buf = buf;
        self->dir = dir;
        if (SLTPCK_BACKWARD == dir) {
            self->off = (char *)buf;
        } else {
            self->off = (char *)buf + sizeof(self->flg);
        }
    }

    return self;
}

int slt_pck(struct slt_cur_t *cur, unsigned int FLAG, void *dta, size_t dtasz)
{
    struct slt_hdr_t hdr;
    uintptr_t align;
    int sz;
    int payload;
    int new_payload;

    /* check param */
    if (!cur || !dta || dtasz <= 0) {
        return SLT_ERR_PARAM;
    }

    /* check flag and duplicate */
    if (!chkflg(FLAG) || (cur->flg & FLAG)) {
        return SLT_ERR_FLG;
    }

    /* check size */
    sz = cur->sz;
    payload = cur->payload;
    new_payload = dtasz + sizeof(hdr) + (dtasz % sizeof(int));
    if ((sz - payload) < new_payload) {
        return SLT_ERR_SZ;
    }

    if (SLTPCK_BACKWARD == cur->dir) {
        slt_pck_buf(cur, dta, dtasz);

        hdr.off = dtasz;
        align = dtasz % sizeof(int);
        if (align) {
            hdr.off += sizeof(int) - align;
        }
        slt_pck_hdr(cur, &hdr, dtasz);
    } else {
        hdr.off = sizeof(struct slt_hdr_t);
        slt_pck_hdr(cur, &hdr, dtasz);

        slt_pck_buf(cur, dta, dtasz);
    }

    /* update slt_cur_t */
    cur->flg |= htonl(FLAG);

    return SLT_ERR_OK;
}

size_t slt_stamp(struct slt_cur_t *cur)
{
    size_t payload;

    if (!cur) {
        return 0;
    }

    /* pack flags */
    if (SLTPCK_BACKWARD == cur->dir) {
        memcpy(cur->off, &cur->flg, sizeof(cur->flg));
    } else {
        memcpy(cur->buf, &cur->flg, sizeof(cur->flg));
    }
    payload = cur->payload + sizeof(cur->flg);

    /* free cursor */
    free(cur);

    /* tell caller sizeof packed data */
    return payload;
}

void *slt_unpck(unsigned int FLAG, void *buf, int bufsz, size_t *sltsz,
                slt_dir_t dir)
{
    if (SLTPCK_BACKWARD == dir) {
        return slt_unpck_bck(FLAG, buf, bufsz, sltsz);
    } else {
        return slt_unpck_fwd(FLAG, buf, bufsz, sltsz);
    }
}

void slt_filt(unsigned int FLAG, void *buf, int bufsz, slt_dir_t dir)
{
    char *slt = NULL;
    size_t sltsz = 0;
    int *flg = NULL;
    struct slt_hdr_t *slthdr = NULL;
    struct slt_hdr_t *nxthdr = NULL;

    /* get in position */
    slt = (char *)slt_unpck(FLAG, buf, bufsz, &sltsz, dir);
    if (!slt) {
        return;
    }

    if (SLTPCK_BACKWARD == dir) {
        /* check ranges */
        if ((slt - sizeof(struct slt_hdr_t)) < (char *)buf) {
            return;
        }

        /* get flag */
        flg = (int *)((char *)buf + bufsz - sizeof(*flg));

        /* check flag is set */
        if (!(*flg & FLAG)) {
            return;
        }

        if (*flg & (*flg - 1)) {
            /* get headers */
            slthdr = (struct slt_hdr_t *)(slt + sltsz);
            nxthdr = (struct slt_hdr_t *)(slt - sizeof(struct slt_hdr_t));

            /* adjust */
            slthdr->sz = nxthdr->sz;
            slthdr->off += sizeof(struct slt_hdr_t) + nxthdr->off;
        }
    } else {
        /* check ranges */
        if ((slt + sizeof(struct slt_hdr_t)) > ((char *)buf + bufsz)) {
            return;
        }

        /* get flag */
        flg = (int *)(buf);

        /* check flag is set */
        if (!(*flg & FLAG)) {
            return;
        }

        if (*flg & (*flg - 1)) {
            /* get headers */
            slthdr = (struct slt_hdr_t *)(slt - sizeof(struct slt_hdr_t));
            nxthdr = (struct slt_hdr_t *)(slt + sltsz);

            /* adjust */
            slthdr->sz = nxthdr->sz;
            slthdr->off += sizeof(struct slt_hdr_t) + sltsz;
        }
    }

    /* remove flag */
    *flg &= ~FLAG;
}

/* *** Private functions *** */
static void slt_pck_buf(struct slt_cur_t *cur, void *dta, size_t dtasz)
{
    int align;

    memcpy(cur->off, dta, dtasz);
    cur->payload += dtasz;
    cur->off += dtasz;
    align = dtasz % sizeof(int);
    if (align) {
        cur->payload += sizeof(int) - align;
        cur->off += sizeof(int) - align;
    }
}

static void slt_pck_hdr(struct slt_cur_t *cur, struct slt_hdr_t *hdr,
                        size_t dtasz)
{
    hdr->sz = dtasz;
    hdr_from_cpu(hdr);
    memcpy(cur->off, hdr, sizeof(struct slt_hdr_t));
    cur->off += sizeof(struct slt_hdr_t);
    cur->payload += sizeof(struct slt_hdr_t);
}

static void *slt_unpck_bck(unsigned int FLAG, void *buf, int bufsz,
                           size_t *sltsz)
{
    int *flg;
    int alignflg;
    int jmp = 0;
    struct slt_hdr_t *hdr;
    struct slt_hdr_t alignhdr;
    int misaligned = 0 /* FALSE */;
    int tmpflg = 0x40000000;
    int payload = 0;
    int totpayload = 0; /* keep track of unpacked data size to validate buf */

    /* check param */
    if (!buf || (bufsz < sizeof(FLAG)) || !sltsz) {
        return NULL;
    }

    /* check flag */
    if (!chkflg(FLAG)) {
        return NULL;
    }

    /* get buffer flag */
    totpayload = bufsz - sizeof(int);
    if ((intptr_t)((char *)buf + totpayload) % sizeof(int)) {
        /* buffer is misaligned */
        memcpy(&alignflg, ((char *)buf + totpayload), sizeof(alignflg));
        flg = &alignflg;
        misaligned = 1 /* TRUE */;
    } else {
        flg = (int *)((char *)buf + totpayload);
    }
    *sltsz = 0;

    /* check flag is set */
    *flg = ntohl(*flg);
    if (!(*flg & FLAG)) {
        return NULL;
    }

    /* check presence of header */
    payload = sizeof(*hdr);
    if ((totpayload - payload) < 0) {
        return NULL;
    }

    /* get first header */
    if (misaligned) {
        memcpy(&alignhdr, ((char *)buf + totpayload - payload),
               sizeof(alignhdr));
        hdr = &alignhdr;
    } else {
        hdr = (struct slt_hdr_t *)((char *)flg - payload);
    }

    hdr_to_cpu(hdr);

    totpayload -= payload;

    /* get slot position */
    while (tmpflg > FLAG) {
        if (*flg & tmpflg) { /* jump to next header */
            /* check buffer size */
            payload = (size_t)(hdr->off + sizeof(*hdr));
            if ((totpayload - payload) < 0) {
                return NULL;
            }

            /* grab header */
            if (misaligned) {
                memcpy(&alignhdr, ((char *)buf + totpayload - payload),
                       sizeof(alignhdr));
                hdr = &alignhdr;
            } else {
                hdr = (struct slt_hdr_t *)((char *)hdr - payload);
            }

            hdr_to_cpu(hdr);

            /* keep track */
            totpayload -= payload;
        }
        tmpflg >>= 1;
    }

    /* retrieve size */
    *sltsz = (size_t)hdr->sz;

    /* check slot is correct */
    payload = (size_t)hdr->off;
    if ((totpayload - payload) < 0) {
        return NULL;
    }

    /* return buf@offset */
    if (misaligned) {
        return ((char *)buf + totpayload - payload);
    }

    return (char *)hdr - payload;
}

static void *slt_unpck_fwd(unsigned int FLAG, void *buf, int bufsz,
                           size_t *sltsz)
{
    int *flg;
    int jmp = 0;
    struct slt_hdr_t *hdr;
    int tmpflg = 0x00000001;
    int payload = 0;
    int totpayload = 0; /* keep track of unpacked data size to validate buf */

    /* check param */
    if (!buf || (bufsz < sizeof(FLAG)) || !sltsz) {
        return NULL;
    }

    /* check flag */
    if (!chkflg(FLAG)) {
        return NULL;
    }

    /* get buffer flag */
    /* NOTE: this can only work if buf is already offset correctly */
    totpayload = bufsz - sizeof(int);
    flg = (int *)(buf);
    *sltsz = 0;

    *flg = ntohl(*flg);
    /* check flag is set */
    if (!(*flg & FLAG)) {
        return NULL;
    }

    /* check presence of header */
    payload = sizeof(*hdr);
    if ((totpayload - payload) < 0) {
        return NULL;
    }

    /* get first header */
    hdr = (struct slt_hdr_t *)((char *)flg + sizeof(int));
    totpayload -= payload;

    /* get slot position */
    while (tmpflg < FLAG) {
        if (*flg & tmpflg) { /* jump to next header */
            /* check buffer size */
            hdr_to_cpu(hdr);
            payload = hdr->off + hdr->sz;
            if ((totpayload - payload) < 0) {
                return NULL;
            }

            /* grab header */
            hdr = (struct slt_hdr_t *)((char *)hdr + payload);

            /* keep track */
            totpayload -= payload;
        }
        tmpflg <<= 1;
    }

    hdr_to_cpu(hdr);

    /* retrieve size */
    *sltsz = hdr->sz;

    /* check slot is correct */
    payload = hdr->off + hdr->sz;
    if ((totpayload - payload) < 0) {
        return NULL;
    }

    /* return buf@offset */
    return (char *)hdr + hdr->off;
}

static int chkflg(unsigned int FLAG)
{
    /* check there is a flag */
    if (FLAG < 1 || (FLAG & (FLAG - 1))) {
        return 0; /* NOT OK */
    }

    return 1; /* OK */
}

static void hdr_to_cpu(struct slt_hdr_t *hdr)
{
    hdr->sz = ntohs(hdr->sz);
    hdr->off = ntohs(hdr->off);
}

static void hdr_from_cpu(struct slt_hdr_t *hdr)
{
    hdr->sz = htons(hdr->sz);
    hdr->off = htons(hdr->off);
}

#ifdef SLTPCK_TESTSUITE
/*
# MAKEFILE FOR sltpck.tsk generated by pcomp
include /bb/bin/machdep.newlink

USER_CFLAGS=-I. -DSLTPCK_TESTSUITE

TASK=sltpck.tsk
OBJDIR=
OBJS= sltpck.o
LIBS=-ldbutil -lpeutil -lbbipc -lsysutil
IS_CMAIN=true
#IS_64BIT=yes

include /bb/bin/linktask.newlink
*/

enum TAIL_SLOTS {
    EMPTY = 0,
    SLOT1 = 1,
    SLOT2 = 2,
    SLOT3 = 4,
    SLOT4 = 8,
    SLOT5 = 16
};

struct slot5 {
    char s[16];
    double d;
    int i;
};

extern void fsnapf(FILE *fil, char *buf, int len);

static size_t pckdrv(char *buffer, size_t bufsz, slt_dir_t dir);
static void unpckdrv(char *buffer, size_t bufsz, slt_dir_t dir);
static void errpck(int flag, int rc);

int main(int ac, char **av)
{
    enum { BUFSZ = 1024, OFFSZ = 60 };
    char buffer[BUFSZ] = {0};
    char *off = buffer + OFFSZ;
    size_t payload = OFFSZ;

    /* pack and check SLTPCK_FORWARD */
    payload += pckdrv(off, BUFSZ - OFFSZ, SLTPCK_FORWARD);
    fsnapf(stdout, buffer, payload);
    unpckdrv(buffer + OFFSZ, payload, SLTPCK_FORWARD);

    /* reset */
    memset(buffer, 0, sizeof(buffer));
    payload = OFFSZ;

    /* pack and check SLTPCK_BACKWARD */
    payload += pckdrv(off, BUFSZ - OFFSZ, SLTPCK_BACKWARD);
    fsnapf(stdout, buffer, payload);
    unpckdrv(buffer, payload, SLTPCK_BACKWARD);

    exit(0);
}

static size_t pckdrv(char *buffer, size_t bufsz, slt_dir_t dir)
{
    char slt1[8] = "deadbeef";
    double slt2 = 234.75;
    int slt3 = 42;
    struct slot5 slt5 = {"deadbabe", 57.432, 24};
    struct slt_cur_t *cur;
    int rc = 0;

    /* get a packing cursor */
    cur = slt_init(buffer, bufsz, dir);
    if (!cur) {
        return 0;
    }

    /* pack elements */
    rc = slt_pck(cur, SLOT1, slt1, sizeof(slt1));
    if (rc) {
        errpck(SLOT1, rc);
    }
    rc = slt_pck(cur, SLOT2, &slt2, sizeof(slt2));
    if (rc) {
        errpck(SLOT2, rc);
    }
    rc = slt_pck(cur, SLOT3, &slt3, sizeof(slt3));
    if (rc) {
        errpck(SLOT3, rc);
    }
    printf("expecting error ... ");
    rc = slt_pck(cur, SLOT4, NULL, 0);
    if (rc) {
        errpck(SLOT4, rc);
    } else {
        printf("????\n");
    }
    rc = slt_pck(cur, SLOT5, &slt5, sizeof(slt5));
    if (rc) {
        errpck(SLOT5, rc);
    }

    /* finalize data and get payload */
    return slt_stamp(cur);
}

static void unpckdrv(char *buffer, size_t bufsz, slt_dir_t dir)
{
    char *slt;
    size_t sltsz;

    printf("filtering SLOT2\n");
    slt_filt(SLOT2, buffer, bufsz, dir);

    slt = slt_unpck(SLOT1, buffer, bufsz, &sltsz, dir);
    if (slt && (sltsz > 0)) {

        printf("SLOT1 -> %.*s\n", sltsz, (char *)slt);
    } else {
        printf("SLOT1 -> NOT FOUND\n");
    }

    slt = slt_unpck(SLOT2, buffer, bufsz, &sltsz, dir);
    if (slt && (sltsz > 0)) {
        printf("SLOT2 -> %lf\n", *((double *)slt));
    } else {
        printf("SLOT2 -> NOT FOUND\n");
    }

    slt = slt_unpck(SLOT3, buffer, bufsz, &sltsz, dir);
    if (slt && (sltsz > 0)) {
        printf("SLOT3 -> %d\n", *((int *)slt));
    } else {
        printf("SLOT3 -> NOT FOUND\n");
    }

    slt = slt_unpck(SLOT4, buffer, bufsz, &sltsz, dir);
    if (slt && (sltsz > 0)) {
        printf("SLOT4 -> %.*s\n", sltsz, (char *)slt);
    } else {
        printf("SLOT4 -> NOT FOUND\n");
    }

    slt = slt_unpck(SLOT5, buffer, bufsz, &sltsz, dir);
    if (slt && (sltsz > 0)) {
        printf("SLOT5 -> ('%s', %lf, %d)\n", ((struct slot5 *)slt)->s,
               ((struct slot5 *)slt)->d, ((struct slot5 *)slt)->i);
    } else {
        printf("SLOT5 -> NOT FOUND\n");
    }
}

static void errpck(int flag, int rc)
{
    printf("error packing flag #%d -> %d\n", flag, rc);
}
#endif
