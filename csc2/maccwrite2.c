#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <strings.h>
#include <ctype.h>
#include <fcntl.h>
#include <strings.h>
#include "maccparse.h"
#include "macc.h"
#include "logmsg.h"

static int maxrecsz = COMDB2_MAX_RECORD_SIZE;
/* OUTPUT WITH VARIABLE EXPAND AND TABBING */

void write_rngext();
void write_index_function();
void write_internal_common();
void write_block_err(char *oper, FILE *fil);
void write_init_control();
void write_comdb2_init_control(int i);
void dump_symbol(int, int);
int case_first(int tidx, int symb);
extern int get_union_size(int un);
extern int get_idx_case_size(int idx);
extern int get_prev_sym(int idx);

int offsetpad(int off, int idx, int tbl);
int largest_idx(int tidx, int unionnum);
int iscstr(int tidx, int idx);
int ispstr(int tidx, int idx);
int isblob(int tidx, int idx);
/*____________OUTPUT WITH VARIABLES____________*/

int strncmp_ci(char *a, int alen, char *b)
{
    int oo;
    for (oo = 0; oo < alen && (*b) && a[oo] == (*b); oo++, b++)
        ;
    if (oo < alen)
        return a[oo] - (*b);
    else
        return -(*b);
}

static FILE *OUTFIL = 0;
static int outtab;
static int outlang;

void OUTINIT(FILE *fil, char lang)
{
    OUTFIL = fil;
    outtab = 0;
    outlang = toupper(lang);
}

/*ACCEPTS TOLOWER FLAG*/
void strcpy_f(char *dst, char *src, int flag)
{
    while (*src) {
        switch (flag) {
        case 1:
            *dst = tolower(*src);
            break;
        case 2:
            *dst = toupper(*src);
            break;
        default:
            *dst = *src;
            break;
        }
        src++;
        dst++;
    }
    *dst = 0;
}

int compute_all_data(int tidx)
{
    int i = 0, off = 0, padk = 0, pad = 0, afterpad = 0;
    int j = 0, maxsym = 0, maxaln = 0, padbytes = 0;

    for (i = 0; i < tables[tidx].nsym; i++) {
        if (i == un_start[tables[tidx].sym[i].un_idx]) {

        } else if (un_reset[i]) {
            off = tables[tidx].sym[un_start[tables[tidx].sym[i].un_idx]].off;
        }

        /* NOW CHECK IF PADDING IS NECESSARY */
        padk = offsetpad(off, i, tidx); /* gets bytes for padding        */
        tables[tidx].sym[i].padb = padk;
        if (padk && strcmp(tables[tidx].table_tag, ONDISKTAG))
            off += padk;

        tables[tidx].sym[i].off = off; /* set offset for symbol         */
        off += (tables[tidx].sym[i].szof);

        /* if symbol is member of the untion, reset the size
           of the union to current symbol, if it is past current end.
           the size is stored in last symbol in the union. */
        if (tables[tidx].sym[i].un_idx != -1) {
            /*fprintf(stderr, "(%d:%d) new off %d %d\n", i,
             * un_end[sym[i].un_idx],sym[un_end[sym[i].un_idx]].un_max_size,off);*/
            for (j = 0; j < tables[tidx].sym[i].dpth; j++) {
                char *curdpth = (char *)&tables[tidx].sym[i].dpth_tree[j];
                if (curdpth[0] == 'u') {
                    int union_num = (int)(curdpth[1]);
                    if (off > tables[tidx].sym[un_end[union_num]].un_max_size)
                        tables[tidx].sym[un_end[union_num]].un_max_size = off;
                }
            }
        }

        /* if we're last symbol in the union,
           compute any padding that would need to be done AFTER untion, case,
           etc.
           Reset 'off' to be the proper offset after the union
           */
        pad = afterpad = 0;
        if (tables[tidx].sym[i].un_idx != -1 &&
            i == un_end[tables[tidx].sym[i].un_idx]) {
            int largest = largest_idx(tidx, tables[tidx].sym[i].un_idx);
            if (character(tidx, largest)) {
                afterpad = 0;
                pad = tables[tidx].sym[i].un_max_size;
            } else {
                pad = tables[tidx].sym[i].un_max_size;
                if ((tables[tidx].sym[i].un_max_size %
                     tables[tidx].sym[largest].size) != 0) {
                    afterpad = tables[tidx].sym[largest].size -
                               tables[tidx].sym[i].un_max_size %
                                   tables[tidx].sym[largest].size;
                    tables[tidx].sym[i].padaf =
                        tables[tidx]
                            .sym[un_end[tables[tidx].sym[i].un_idx]]
                            .un_max_size -
                        tables[tidx]
                            .sym[un_start[tables[tidx].sym[i].un_idx]]
                            .off +
                        afterpad;
                }

                /*fprintf(stderr, "AFTERPAD %d %d\n", afterpad, pad);*/
            }
            if (!strcmp(tables[tidx].table_tag, ONDISKTAG)) {
                afterpad = 0;
            }
        }

        if (pad != 0) {
            off = pad + afterpad;
            afterpad = 0;
            pad = 0;
        }
    }
    /* find maximum non-char symbol size in the record. also find maximum
     * alignment required */
    maxsym = 0;
    maxaln = 0;
    for (j = 0; j < tables[tidx].nsym; j++) {
        if ((!character(tidx, j) &&
             (tables[tidx].sym[j].size >= tables[tidx].sym[maxsym].size)) ||
            (character(tidx, maxsym) && !character(tidx, j)))
            maxsym = j;
        if (tables[tidx].sym[j].align > tables[tidx].sym[maxaln].align)
            maxaln = j;
    }
    if (opt_reclen == 0 && !opt_macc2pack &&
        strcmp(tables[tidx].table_tag, ONDISKTAG)) {
        if (!character(tidx, maxsym)) {
            /* if structure does not have 4 bytes members, it
               still needs to be aligned on word boundary */
            if (tables[tidx].sym[maxsym].size < 4) {
                padbytes = offpad(off, 4);
            } else /* otherwise, just pad by max symbol */
            {
                padbytes = offpad(off, tables[tidx].sym[maxsym].size);
            }
            if (padbytes != 0) {
                /*fprintf(stderr, "WARNING:CMACC STRUCTURE MAY NOT BE COMPATIBLE
                 * WITH MACC2 RECORD.\n");*/
            }
            /* we must also make sure we are padded by the maximum alignment
             * (which can be 8 if we have long longs) */
            padbytes += offpad(off + padbytes, tables[tidx].sym[maxaln].align);
        }
        off += padbytes;
        tables[tidx].table_padbytes = padbytes;
    }
    if (opt_reclen != 0) {
        if (off > opt_reclen) {
            logmsg(LOGMSG_ERROR, " **** ERROR: RECORD LEN IS %d BYTES; "
                            "YOU SPECIFIED A RECLEN OF %d BYTES.\n",
                    off, opt_reclen);
            return -1;
        }

        if (!character(tidx, maxsym) && !opt_macc2pack) {
            padbytes = offpad(opt_reclen, tables[tidx].sym[maxsym].size);
            if (padbytes != 0) {
                /*fprintf(stderr, "WARNING: CMACC STRUCTURE MAY NOT BE
                 * COMPATIBLE WITH MACC2 RECORD\n");*/
            }
        }

        padbytes += opt_reclen - off;
        off += padbytes;
        tables[tidx].table_padbytes += padbytes;
    }
    /*printf("FINAL OFFSET %d %d Final PAD %d SYM %s %d\n",
     * off,opt_reclen,padbytes, sym[maxsym].nm, sym[maxsym].size);*/

    if (off > maxrecsz) {
        logmsg(LOGMSG_ERROR, " **** ERROR: RECORD LEN IS %d BYTES; "
                        "MAXIMUM FOR COMDB2 IS %d BYTES (%d WORDS).\n",
                off, maxrecsz, maxrecsz / 4);
        extern int gbl_broken_max_rec_sz;
        if (off > (maxrecsz + gbl_broken_max_rec_sz)) {
            return -1;
        }
    }

    /* we're done with computations of unions and offsets, and padbytes */
    un_init = 1;

    /* set tables size */
    tables[tidx].table_size = off;

    bufszw = (off + 3) / 4;
    bufszb = bufszw * 4;
    bufszhw = bufszw * 2;

    return 0;
}

int compute_key_data(void)
{
    int lastix, i, j;
    /* compute key sizes */
    for (lastix = -1, i = 0; i < numkeys(); i++) {
        if (lastix != keyixnum[i]) {
            lastix = keyixnum[i];
            ixsize[lastix] = -1;
        }
        j = wholekeysize(keys[i]); /* figure out largest index size */
        if (j > ixsize[lastix]) {
            ixsize[lastix] = j;
        }
    }
    return 0;
}

int case_first(int tidx, int symb)
{
    int i;
    for (i = 0; i < tables[tidx].nsym; i++) {
        if ((tables[tidx].sym[symb].un_idx == tables[tidx].sym[i].un_idx) &&
            (tables[tidx].sym[symb].caseno == tables[tidx].sym[i].caseno)) {
            return i;
        }
    }
    return 0;
}

int largest_idx(int tidx, int unionnum)
{
    int largest = -1, i = 0, endidx = 0, startidx = 0;
    endidx = un_end[unionnum];
    startidx = un_start[unionnum];
    for (i = 0; i < current_union; i++) {
        if (un_start[i] == startidx && un_end[i] > endidx) {
            endidx = un_end[i];
        }
    }

    for (i = startidx; i <= endidx; i++) {
        if (largest == -1 && !character(tidx, i)) {
            largest = i;
            continue;
        } else if (character(tidx, i))
            continue;

        if (tables[tidx].sym[i].size > tables[tidx].sym[largest].size) {
            largest = i;
        }
    }
    if (largest == -1)
        largest = un_start[unionnum];
    return largest;
}

int offsetpad(int off, int idx, int tbl)
{
    if (tables[tbl].sym[idx].un_idx ==
        -1) /*  if we're simple struct symbol, do regular pad, and return */
    {
        int mostpad = offpad(off, tables[tbl].sym[idx].align);
        tables[tbl].sym[idx].padex = mostpad;
        return mostpad;
    }
    if (tables[tbl].sym[idx].un_idx != -1 &&
        tables[tbl].sym[idx].caseno == -1) /* UNION symbol, but NOT CASE */
    {
        int mostpad = 0, i, lrgsz = idx;
        mostpad = offpad(
            off,
            tables[tbl].sym[idx].align); /* compute padding for this symbol */

        for (i = 0; i < nsym; i++) {
            if (tables[tbl].sym[i].un_idx == tables[tbl].sym[idx].un_idx) {
                if (offpad(off, tables[tbl].sym[i].align) > mostpad)
                /* find symbol in the table which need most */
                { /* padding.  Use it to adjust pad bytes for */
                    mostpad =
                        offpad(off, tables[tbl].sym[i].align); /* every symbol
                                                                  in a union. */
                    lrgsz = i; /* Also find largest non-string element     */
                }
            }
        }

        tables[tbl].sym[lrgsz].padex = mostpad;
        for (i = 0; i < tables[tbl].nsym; i++) {
            if (tables[tbl].sym[i].un_idx == tables[tbl].sym[lrgsz].un_idx) {
                tables[tbl].sym[i].padex = tables[tbl].sym[lrgsz].padex;
            }
        }
        /*printf("offset %d, mostpad %d maxsz %d align %d\n", off,
         * mostpad,sym[i].un_max_size, sym[idx].align);*/
        return (mostpad);
    }
    if (tables[tbl].sym[idx].caseno != -1) {
        int i, mostpad = 0, largest = 0;
        largest = -1;
        largest = largest_idx(tbl, tables[tbl].sym[idx].un_idx);
        if (tables[tbl].sym[idx].padcs == -1) {
            mostpad = offpad(off, tables[tbl].sym[largest].align);
            for (i = 0; i < tables[tbl].nsym; i++) {
                if (tables[tbl].sym[idx].un_idx == tables[tbl].sym[i].un_idx) {
                    tables[tbl].sym[i].padcs = mostpad;
                }
            }
            tables[tbl].sym[idx].padex = mostpad;
            /*  printf("case1: offset %d, mostpad %d maxsz %d align %d largest
             * %d\n", off, mostpad, sym[i].un_max_size, sym[idx].align,
             * largest);*/
            return mostpad;
        }

        if (case_first(tbl, idx) == idx) /* first member of every case */
        {
            mostpad = 0; /*sym[idx].padcs;*/
        } else           /* non-first member of case */
        {
            mostpad = offpad(off, tables[tbl].sym[idx].align);
        }
        tables[tbl].sym[idx].padex = mostpad;
        /*printf("case: offset %d, mostpad %d maxsz %d align %d\n", off,
         * mostpad, sym[idx].un_max_size, sym[idx].align);*/
        return mostpad;
    }
    return 0;
}

int character(int tidx, int idx)
{
    if ((tables[tidx].sym[idx].type == T_PSTR) ||
        (tables[tidx].sym[idx].type == T_UCHAR) ||
        (tables[tidx].sym[idx].type == T_CSTR))
        return 1;
    return 0;
}

int iscstr(int tidx, int idx)
{
    if (tables[tidx].sym[idx].type == T_CSTR)
        return 1;
    return 0;
}

int ispstr(int tidx, int idx)
{
    if (tables[tidx].sym[idx].type == T_PSTR)
        return 1;
    return 0;
}

int isblob(int tidx, int idx)
{
    if (tables[tidx].sym[idx].type == T_BLOB)
        return 1;
    return 0;
}

int isvutf8(int tidx, int idx)
{
    if (tables[tidx].sym[idx].type == T_VUTF8 ||
        tables[tidx].sym[idx].type == T_BLOB2)
        return 1;
    return 0;
}

int isblob2(int tidx, int idx)
{
    if (tables[tidx].sym[idx].type == T_BLOB2)
        return 1;
    return 0;
}
