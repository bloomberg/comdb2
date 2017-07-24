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

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stddef.h>
#include <stdint.h>

#include "comdb2.h"
#include "sbuf2.h"
#include "strbuf.h"

static uint8_t hexval(uint8_t b)
{
    if (b >= '0' && b <= '9')
        return b - '0';
    else if (b >= 'a' && b <= 'f')
        return 10 + (b - 'a');
    else if (b >= 'A' && b <= 'F')
        return 10 + (b - 'A');
    return 0xff;
}

static int readhex(char *hex, uint8_t **bufp, int *sz)
{
    int hexlen = strlen(hex);
    if (hexlen % 2)
        return -1;
    hexlen /= 2;
    if (hexlen > *sz) {
        *sz = hexlen;
        *bufp = realloc(*bufp, hexlen);
        if (*bufp == NULL) {
            fprintf(stderr, "realloc %d failed\n", hexlen);
            return -1;
        }
    }
    for (int i = 0; i < hexlen; i++) {
        uint8_t h, l;
        h = hexval(hex[i * 2]);
        l = hexval(hex[i * 2 + 1]);
        /* invalid hex char? */
        if (h == 0xff || l == 0xff) {
            printf("h %c %02x  l %c %02x\n", hex[i * 2], h, hex[i * 2 + 1], l);
            return -1;
        }
        (*bufp)[i] = h << 4 | l;
    }
    return hexlen;
}

static void printhex(SBUF2 *sb, uint8_t *hex, int sz)
{
    const char hexbytes[] = "0123456789abcdef";
    for (int i = 0; i < sz; i++)
        sbuf2printf(sb, "%c%c", hexbytes[(hex[i] & 0xf0) >> 4],
                    hexbytes[hex[i] & 0xf]);
}

/* Read a stream of operations from an sbuf, and do them. This is used for
 * cases where the database is somehow logically corrupt (ie: when we have a
 *bug)
 * to get it into a working state.
 *
 * TODO
 * [ ] addall HEXBYTES   - HEXBYTES is a record. try to form all the keys and
 *insert them.
 *
 * */
int handle_llops(SBUF2 *sb, struct dbenv *dbenv)
{
    /* need a nice general readline somewhere one day */
    char line[1024];
    int rc;
    char *table = NULL;
    char *tok, *endp;
    int stripe = -1;
    int dtafile = 0;
    int ix = -1;
    tran_type *trans = NULL;
    struct ireq iq;
    int key_alloc = 0, data_alloc = 0, datacopy_alloc = 0;
    uint8_t *key = NULL, *data = NULL, *datacopy = NULL;
    int keylen, datalen, datacopylen;
    int have_key = 0, have_data = 0, have_datacopy = 0;
    int raw = 1;
    char *errstr = NULL;
    int lnum = 0;
    int interactive = 0;

    init_fake_ireq(dbenv, &iq);

    rc = sbuf2gets(line, sizeof(line), sb);
    while (rc > 0) {
        if (line[rc - 1] == '\n')
            line[rc - 1] = 0;

        lnum++;

        rc = 0;

        if (strcmp(line, ".") == 0)
            break;
        if (line[0] == '#') {
            rc = sbuf2gets(line, sizeof(line), sb);
            if (interactive)
                goto next;
            else
                continue;
        }

        tok = strtok_r(line, " ", &endp);
        if (tok == NULL) {
            if (interactive)
                goto next;
            else
                continue;
        }
        if (strcmp(tok, "table") == 0) {
            tok = strtok_r(NULL, " ", &endp);
            if (tok == NULL) {
                sbuf2printf(sb, "!expected table anem\n");
                rc = -1;
                if (interactive)
                    goto next;
                else
                    break;
            }
            table = tok;
            iq.usedb = get_dbtable_by_name(table);
            if (iq.usedb == NULL) {
                rc = -1;
                sbuf2printf(sb, "!unknown table %s\n", table);
                if (interactive)
                    goto next;
                else
                    break;
            }
        } else if (strcmp(tok, "begin") == 0) {
            if (trans) {
                rc = -1;
                sbuf2printf(sb, "!begin while in active transaction!\n");
                if (interactive)
                    goto next;
                else
                    break;
            }
            rc = trans_start(&iq, NULL, &trans);
            if (rc) {
                sbuf2printf(sb, "!can't start transaction rc %d\n", rc);
                if (interactive)
                    goto next;
                else
                    break;
            }
        } else if (strcmp(tok, "stripe") == 0) {
            tok = strtok_r(NULL, " ", &endp);
            if (tok == NULL) {
                rc = -1;
                sbuf2printf(sb, "!expected stripe number\n");
                if (interactive)
                    goto next;
                else
                    break;
            }
            stripe = atoi(tok);
        } else if (strcmp(tok, "dtafile") == 0) {
            tok = strtok_r(NULL, " ", &endp);
            if (tok == NULL) {
                rc = -1;
                sbuf2printf(sb, "!expected dtafile number\n");
                if (interactive)
                    goto next;
                else
                    break;
            }
            dtafile = atoi(tok);
        } else if (strcmp(tok, "commit") == 0) {
            if (trans == NULL) {
                rc = -1;
                sbuf2printf(sb, "!no active transaction\n");
                if (interactive)
                    goto next;
                else
                    break;
            }
            rc = trans_commit(&iq, trans, gbl_mynode);
            if (rc) {
                sbuf2printf(sb, "!commit rc %d\n", rc);
                if (interactive)
                    goto next;
                else
                    break;
            }
            trans = NULL;
        } else if (strcmp(tok, "ix") == 0) {
            tok = strtok_r(NULL, " ", &endp);
            if (tok == NULL) {
                rc = -1;
                sbuf2printf(sb, "!expected ix number\n");
                if (interactive)
                    goto next;
                else
                    break;
            }
            ix = atoi(tok);
        } else if (strcmp(tok, "key") == 0) {
            tok = strtok_r(NULL, " ", &endp);
            if (tok == NULL) {
                rc = -1;
                sbuf2printf(sb, "!expected hex bytes for key\n");
                if (interactive)
                    goto next;
                else
                    break;
            }
            keylen = readhex(tok, &key, &key_alloc);
            if (keylen == -1) {
                rc = -1;
                sbuf2printf(sb, "!can't read key (%s <- proper hex?)\n", tok);
                if (interactive)
                    goto next;
                else
                    break;
            }
            have_key = 1;
        } else if (strcmp(tok, "datacopy") == 0) {
            tok = strtok_r(NULL, " ", &endp);
            if (tok == NULL) {
                rc = -1;
                sbuf2printf(sb, "!expected hex bytes for key\n");
                if (interactive)
                    goto next;
                else
                    break;
            }
            datacopylen = readhex(tok, &datacopy, &datacopy_alloc);
            if (datacopylen == -1) {
                rc = -1;
                sbuf2printf(sb, "!can't read datacopy\n");
                if (interactive)
                    goto next;
                else
                    break;
            }
            have_datacopy = 1;
        }

        else if (strcmp(tok, "data") == 0) {
            tok = strtok_r(NULL, " ", &endp);
            if (tok == NULL) {
                rc = -1;
                sbuf2printf(sb, "!expected hex bytes for data\n");
                if (interactive)
                    goto next;
                else
                    break;
            }
            datalen = readhex(tok, &data, &data_alloc);
            if (keylen == -1) {
                rc = -1;
                sbuf2printf(sb, "!can't read data\n");
                if (interactive)
                    goto next;
                else
                    break;
            }
            have_data = 1;
        } else if (strcmp(tok, "del") == 0 || strcmp(tok, "add") == 0) {
            if (iq.usedb == NULL) {
                rc = -1;
                sbuf2printf(sb, "!don't have table specified\n");
                if (interactive)
                    goto next;
                else
                    break;
            }
            if (!have_key) {
                rc = -1;
                sbuf2printf(sb, "!don't have key specified\n");
                if (interactive)
                    goto next;
                else
                    break;
            }
            if (ix == -1) {
                if (stripe == -1) {
                    rc = -1;
                    sbuf2printf(sb, "!don't have stripe specified\n");
                    if (interactive)
                        goto next;
                    else
                        break;
                }
            }

            if (strcmp(tok, "del") == 0) {
                rc = bdb_llop_del(iq.usedb->handle, trans, stripe, dtafile, ix,
                                  key, keylen, &errstr);
                if (rc) {
                    sbuf2printf(sb, "!line %d del rc %d err %s\n", lnum, rc,
                                errstr ? errstr : "???");
                    free(errstr);
                    errstr = NULL;
                }
            } else if (strcmp(tok, "add") == 0) {
                if (!have_data) {
                    sbuf2printf(sb, "!don't have data specified\n");
                }
                errstr = NULL;
                rc = bdb_llop_add(iq.usedb->handle, trans, raw, stripe, dtafile,
                                  ix, key, keylen, data, datalen,
                                  have_datacopy ? datacopy : NULL, datacopylen,
                                  &errstr);
                if (rc) {
                    sbuf2printf(sb, "!line %d add rc %d err %s\n", lnum, rc,
                                errstr ? errstr : "???");
                    if (errstr) {
                        free(errstr);
                        errstr = NULL;
                    }
                }
            }
        } else if (strcmp(tok, "raw") == 0) {
            int onoff;
            tok = strtok_r(NULL, " ", &endp);
            if (tok == NULL) {
                rc = -1;
                sbuf2printf(sb, "!expected on or off for raw\n");
                if (interactive)
                    goto next;
                else
                    break;
            }
            if (strcasecmp(tok, "on") == 0 || strcmp(tok, "1") == 0)
                onoff = 1;
            else if (strcasecmp(tok, "off") == 0 || strcmp(tok, "0") == 0)
                onoff = 0;
            else {
                rc = -1;
                sbuf2printf(sb, "!expected on or off for raw (got %s)\n", tok);
                if (interactive)
                    goto next;
                else
                    break;
            }
            raw = onoff;
        } else if (strcmp(tok, "info") == 0) {
            if (table)
                sbuf2printf(sb, ">table %s\n", table);
            sbuf2printf(sb, ">ix %d dtafile %d stripe %d\n", ix, dtafile,
                        stripe);
            if (have_key) {
                sbuf2printf(sb, ">key: ");
                printhex(sb, key, keylen);
                sbuf2printf(sb, "\n");
            }
            if (have_data) {
                sbuf2printf(sb, ">data: ");
                printhex(sb, data, datalen);
                sbuf2printf(sb, "\n");
            }
            if (have_datacopy) {
                sbuf2printf(sb, ">datacopy: ");
                printhex(sb, datacopy, datacopylen);
                sbuf2printf(sb, "\n");
            }
            sbuf2printf(sb, ">%s mode\n", raw ? "raw" : "cooked");

            sbuf2flush(sb);
        } else if (strcmp(tok, "help") == 0) {
            sbuf2printf(sb,
                        ">Commands: \n"
                        ">begin             - start transaction \n"
                        ">commit            - commit transaction \n"
                        ">stripe N          - set stripe number to N \n"
                        ">dtafile N         - set data file to N (0 is data, "
                        "1-15 are blobs) \n"
                        ">ix N              - set index to N \n"
                        ">table NAME        - set current table to NAME \n"
                        ">key HEXBYTES      - set key to HEXBYTES \n"
                        ">data HEXBYTES     - set payload to HEXBYTES \n"
                        ">del               - delete current record \n"
                        ">add               - add current record \n"
                        ">delall HEXBYTES   - HEXBYTES is a genid.  try to "
                        "find and delete all the index/blob entries for it \n"
                        ">addall HEXBYTES   - HEXBYTES is a record. try to "
                        "form all the keys and insert them.  \n"
                        ">pass multiple HEXBYTES for blobs \n"
                        ">raw N             - set the raw flags (raw means no "
                        "odh/compression on items) \n"
                        ">datacopy HEXBYTES - set datacopy \n"
                        ">info              - dump whatever has been set \n"
                        ">find              - find key, dump record, if "
                        "dtafile 0, form and dump keys\n"
                        ">help              - list commands \n");
            sbuf2flush(sb);
        } else if (strcmp(tok, "find") == 0) {
            if (!have_key) {
                sbuf2printf(sb, "!don't have key speciied\n");
                if (interactive)
                    goto next;
                else
                    break;
            }
            if (!table) {
                sbuf2printf(sb, "!don't have table specified\n");
                if (interactive)
                    goto next;
                else
                    break;
            }
            void *p;
            int fndlen;
            uint8_t ver;
            p = bdb_llop_find(iq.usedb->handle, trans, raw, stripe, dtafile, ix,
                              key, keylen, &fndlen, &ver, &errstr);
            if (p) {
                sbuf2printf(sb, ">found len %d: ", fndlen);
                printhex(sb, p, fndlen);
                sbuf2printf(sb, "\n");
                sbuf2flush(sb);
                if (dtafile == 0 && !raw) {
                    struct convert_failure fail;
                    /* try to form and find all the keys */
                    for (int i = 0; i < iq.usedb->nix; i++) {
                        char formkey[17 * 1024];
                        char keytag[32];
                        char mangled_key[MAXKEYLEN];

                        char *od_dta_tail = NULL;
                        int od_len_tail;

                        snprintf(keytag, sizeof(keytag), ".ONDISK_IX_%d", i);

                        rc = create_key_from_ondisk(
                            iq.usedb, i, &od_dta_tail, &od_len_tail,
                            mangled_key, ".ONDISK", p, fndlen, keytag, formkey,
                            NULL, NULL);

                        if (rc) {
                            sbuf2printf(
                                sb, "!failed to form key for index %d\n", i);
                        } else {
                            int sz;
                            sz = get_size_of_schema(iq.usedb->ixschema[i]);
                            /* kludge - this is normally done in bdb land */
                            if (iq.usedb->ix_dupes[i]) {
                                memcpy(formkey + sz, key, keylen);
                                sz += keylen;
                            }
                            sbuf2printf(sb, ">ix %d len %d: ", i, sz);
                            printhex(sb, formkey, sz);
                            sbuf2printf(sb, " ");
                            printhex(sb, od_dta_tail, od_len_tail);
                            sbuf2printf(sb, "\n");

                            void *k;
                            int klen;
                            uint8_t ver;
                            k = bdb_llop_find(iq.usedb->handle, trans, 1, 0, 0,
                                              i, formkey, sz, &klen, &ver,
                                              &errstr);
                            if (k == NULL) {
                                sbuf2printf(sb, "!not found\n");
                            } else {
                                sbuf2printf(sb, "!found, ");
                                printhex(sb, k, klen);
                                sbuf2printf(sb, "\n");
                                free(k);
                            }
                        }
                    }
                    sbuf2flush(sb);
                }
                free(p);
            } else {
                sbuf2printf(sb, "!line %d find err %s\n", lnum,
                            errstr ? errstr : "???");
                sbuf2flush(sb);
                free(errstr);
            }
        } else if (strcmp(tok, "delall") == 0) {
            void *p;
            int fndlen;
            uint8_t ver;
            int stripe;

            if (!have_key) {
                sbuf2printf(sb, "!don't have key speciied\n");
                if (interactive)
                    goto next;
                else
                    break;
            }
            if (!table) {
                sbuf2printf(sb, "!don't have table specified\n");
                if (interactive)
                    goto next;
                else
                    break;
            }

            /* ignore stripe setting, get from genid - it's the last nybble */
            stripe = key[keylen - 1] & 0x0f;

            p = bdb_llop_find(iq.usedb->handle, trans, 0, stripe, 0, -1, key,
                              keylen, &fndlen, &ver, &errstr);
            if (p) {
                struct convert_failure fail;
                /* try to form and find all the keys */
                for (int i = 0; i < iq.usedb->nix; i++) {
                    char formkey[17 * 1024];
                    char keytag[32];
                    char mangled_key[MAXKEYLEN];

                    char *od_dta_tail = NULL;
                    int od_len_tail;

                    snprintf(keytag, sizeof(keytag), ".ONDISK_IX_%d", i);

                    rc = create_key_from_ondisk(
                        iq.usedb, i, &od_dta_tail, &od_len_tail, mangled_key,
                        ".ONDISK", p, fndlen, keytag, formkey, NULL, NULL);

                    if (rc) {
                        sbuf2printf(sb, "!failed to form key for index %d\n",
                                    i);
                    } else {
                        int sz;
                        sz = get_size_of_schema(iq.usedb->ixschema[i]);
                        /* kludge - this is normally done in bdb land */
                        if (iq.usedb->ix_dupes[i]) {
                            memcpy(formkey + sz, key, keylen);
                            sz += keylen;
                        }
                        sbuf2printf(sb, ">looking for: ");
                        printhex(sb, formkey, sz);
                        sbuf2printf(sb, "\n");
                        rc = bdb_llop_del(iq.usedb->handle, trans, stripe,
                                          dtafile, i, formkey, sz, &errstr);
                        if (rc) {
                            sbuf2printf(sb, "!line %d del ix %d ", lnum, i);
                            printhex(sb, formkey, sz);
                            sbuf2printf(sb, " failed rc %d %s\n", rc,
                                        errstr ? errstr : "???");
                            free(errstr);
                        } else {
                            sbuf2printf(sb, ">deleted key in ix %d\n", i);
                        }
                    }
                }
                /* forget blobs for now - orphaned blobs are ok. */
                rc = bdb_llop_del(iq.usedb->handle, trans, stripe, 0, -1, key,
                                  keylen, &errstr);
                if (rc) {
                    sbuf2printf(sb, "!line %d del dta failed rc %d %s\n", lnum,
                                rc, errstr ? errstr : "???");
                    free(errstr);
                } else {
                    sbuf2printf(sb, ">deleted data\n");
                }
                sbuf2flush(sb);
                free(p);
            } else {
                sbuf2printf(sb, "!line %d can't find key ", lnum);
                printhex(sb, key, keylen);
                sbuf2printf(sb, "\n");
                sbuf2flush(sb);
            }
        } else if (strcmp(tok, "interactive") == 0) {
            interactive = 1;
            /* we start being interactive after the next command */
            sbuf2settimeout(sb, 0, 0);
            rc = sbuf2gets(line, sizeof(line), sb);
            continue;
        } else {
            rc = -1;
            sbuf2printf(sb, "!unknown command %s\n", tok);
            if (interactive)
                goto next;
            else
                break;
        }

    next:
        if (interactive) {
            sbuf2printf(sb, ".\n");
            sbuf2flush(sb);
        }
        rc = sbuf2gets(line, sizeof(line), sb);
    }

    if (trans) {
        rc = -1;
        sbuf2printf(sb, "!have active transaction at end of processing\n");
        trans_abort(&iq, trans);
    }

    if (rc)
        sbuf2printf(sb, "FAILED\n");
    else
        sbuf2printf(sb, "SUCCESS\n");
    sbuf2flush(sb);
    return 0;
}
