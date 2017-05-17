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

/**
 * Test the type system.
 * Can be run in interactive mode from the command line or from a script.
 * Tests conversion of supplied type/value into all other types.
 *
 * To add a new type:
 *  add it to the appropriate *_TYPES array below
 *  add a read_*() function if needed
 *  add it to the switch in read_type()
 *  add it to the switch in dumpval()
 */

/* if we do this abomanation in sqlglue.c, we should probably do it here too
#include "types.c"
TOO BADs WON'T MAKE A RIGHT
 */

#include <sys/types.h>
#include <netinet/in.h>

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <compile_time_assert.h>

#include <fsnap.h>

#include "types.h"
#include "flibc.h"
#include "dfpal.h"

/* types */

enum {
    TYPE_SIZES_MAX_PER = 3 /* max number of valid sizes per type, if a type has
                        * less then this many valid sizes, the remainder are 0
                        * filled */
};
struct type_info {
    const char const *name;
    int sizes[TYPE_SIZES_MAX_PER];
};
typedef struct type_info type_info_t;

/* constants */

enum {
    CLIENT_TYPES_HDR_NUM = (CLIENT_MAXTYPE - CLIENT_MINTYPE + 1),
    SERVER_TYPES_HDR_NUM = (SERVER_MAXTYPE - SERVER_MINTYPE + 1)
};

/* CLIENT side types */
const type_info_t CLIENT_TYPES[] = {
    {"CLIENT_MINTYPE", {0, 0, 0}},
    {"CLIENT_UINT", {2, 4, 8}},
    {"CLIENT_INT", {2, 4, 8}},
    {"CLIENT_REAL", {4, 8, 0}},
    {"CLIENT_CSTR", {-1, 0, 0}},
    {"CLIENT_PSTR", {-1, 0, 0}},
    {"CLIENT_BYTEARRAY", {-1, 0, 0}},
    {"CLIENT_PSTR2", {-1, 0, 0}},
    {"CLIENT_BLOB", {-1, 0, 0}},
    {"CLIENT_DATETIME", {CLIENT_DATETIME_LEN, 0, 0}},
    {"CLIENT_INTVYM", {CLIENT_INTV_YM_LEN, 0, 0}},
    {"CLIENT_INTVDS", {CLIENT_INTV_DS_LEN, 0, 0}},
    {"CLIENT_VUTF8", {-1, 0, 0}},
    {"CLIENT_MAXTYPE", {0, 0, 0}}};
/* ONDISK types, subtract SERVER_MINTYPE to get the right index */
const type_info_t SERVER_TYPES[] = {{"SERVER_MINTYPE", {0, 0, 0}},
                                    {"SERVER_UINT", {3, 5, 9}},
                                    {"SERVER_BINT", {3, 5, 9}},
                                    {"SERVER_BREAL", {5, 9, 0}},
                                    {"SERVER_BCSTR", {-1, 0, 0}},
                                    {"SERVER_BYTEARRAY", {-1, 0, 0}},
                                    {"SERVER_BLOB", {-1, 0, 0}},
                                    {"SERVER_DATETIME", {11, 0, 0}},
                                    {"SERVER_INTVYM", {5, 0, 0}},
                                    {"SERVER_INTVDS", {11, 0, 0}},
                                    {"SERVER_VUTF8", {-1, 0, 0}},
                                    {"SERVER_DECIMAL", {5, 9, 17}},
                                    {"SERVER_MAXTYPE", {0, 0, 0}}};
enum {
    CLIENT_TYPES_NUM = sizeof(CLIENT_TYPES) / sizeof(CLIENT_TYPES[0]),
    SERVER_TYPES_NUM = sizeof(SERVER_TYPES) / sizeof(SERVER_TYPES[0]),
};
BB_COMPILE_TIME_ASSERT(client_names_num,
                       CLIENT_TYPES_NUM == CLIENT_TYPES_HDR_NUM);
BB_COMPILE_TIME_ASSERT(server_names_num,
                       SERVER_TYPES_NUM == SERVER_TYPES_HDR_NUM);

/* globals */

int g_read_in_hex = 0;
int g_dump_buffers = 0;
int g_outstrlen = 100;
int gbl_allowbrokendatetime = 0;

/* dummies */

int db_time2struct(const char *const name, const db_time_t *const timeval,
                   struct tm *outtm)
{
    fprintf(stderr, "dummy %s called\n", __func__);
    abort();
}

db_time_t db_struct2time(name, tmp) register const char *const name;
struct tm *const tmp;
{
    fprintf(stderr, "dummy %s called\n", __func__);
    abort();
}

/* functions */

/* useful for debugging */
static void hexdump(void *p_buf, int len) { fsnapf(stdout, p_buf, len); }

static int choice(char *p_msg, int min, int max)
{
    char s[20];
    int i;

    for (;;) {
        printf("%s", p_msg);
        if (fgets(s, 20, stdin) == NULL)
            exit(EXIT_FAILURE); /* end of file */
        i = atoi(s);
        if (i >= min && i <= max)
            return i;
    }
}

static char *skipws(char *b)
{
    while (*b && isspace(*b))
        b++;
    return b;
}

static void read_sized_blob(void *p_buf, int len)
{
    char rbuf[1024];
    int offset;
    int rc;
    int val;
    char *b;
    int bufoffset = 0;
    unsigned char *obuf = (unsigned char *)p_buf;

    memset(p_buf, 0, len);
    b = rbuf;
    while (len) {
        if (fgets(rbuf, sizeof(rbuf), stdin) == NULL) {
            fprintf(stderr, "Unexpected end of file\n");
            exit(EXIT_FAILURE);
        }
        b = rbuf;
        while (*b) {
            b = skipws(b);
            if (!*b)
                break;
            rc = sscanf(b, "%2x%n", &val, &offset);
            if (rc == 1) {
                len--;
                obuf[bufoffset++] = val;
            }

            b += offset;
        }
    }
}

static void read_int(void *p_buf, int len)
{
    char rbuf[80];
    int ival;
    short sval;
    long long lval;
    int rc = -1;
    void *src;

    if (g_read_in_hex) {
        read_sized_blob(p_buf, len);
        return;
    }

    if (fgets(rbuf, sizeof(rbuf), stdin) == NULL) {
        fprintf(stderr, "Early end of file.\n");
        exit(EXIT_FAILURE);
    }
    if (len == 2) {
        src = &sval;
        rc = sscanf(rbuf, "%hd", &sval);
        sval = htons(sval);
    } else if (len == 4) {
        src = &ival;
        rc = sscanf(rbuf, "%d", &ival);
        ival = htonl(ival);
    } else if (len == 8) {
        src = &lval;
        rc = sscanf(rbuf, "%lld", &lval);
        lval = flibc_htonll(lval);
    }
    if (rc != 1) {
        fprintf(stderr, "Couldn't read an integer of size %d\n", len);
        exit(EXIT_FAILURE);
    }
    memcpy(p_buf, src, len);
}

static void read_uint(void *p_buf, int len)
{
    char rbuf[80];
    unsigned int ival;
    unsigned short sval;
    unsigned long long lval;
    int rc = -1;
    void *src;

    if (g_read_in_hex) {
        read_sized_blob(p_buf, len);
        return;
    }

    if (fgets(rbuf, sizeof(rbuf), stdin) == NULL) {
        fprintf(stderr, "Early end of file.\n");
        exit(EXIT_FAILURE);
    }
    if (len == 2) {
        src = &sval;
        rc = sscanf(rbuf, "%hu", &sval);
        sval = htons(sval);
    } else if (len == 4) {
        src = &ival;
        rc = sscanf(rbuf, "%u", &ival);
        ival = htonl(ival);
    } else if (len == 8) {
        src = &lval;
        rc = sscanf(rbuf, "%llu", &lval);
        lval = flibc_htonll(lval);
    }
    if (rc != 1) {
        fprintf(stderr, "Couldn't read an unsigned integer of size %d\n", len);
        exit(EXIT_FAILURE);
    }
    memcpy(p_buf, src, len);
}

static void read_real(void *p_buf, int len)
{
    char rbuf[80];
    float fval;
    double dval;
    int rc = -1;
    void *src;

    if (g_read_in_hex) {
        read_sized_blob(p_buf, len);
        return;
    }

    if (fgets(rbuf, sizeof(rbuf), stdin) == NULL) {
        fprintf(stderr, "Early end of file.\n");
        exit(EXIT_FAILURE);
    }
    if (len == 4) {
        src = &fval;
        rc = sscanf(rbuf, "%f", &fval);
        fval = flibc_htonf(fval);
    } else if (len == 8) {
        src = &dval;
        rc = sscanf(rbuf, "%lf", &dval);
        dval = flibc_htond(dval);
    }
    if (rc != 1) {
        fprintf(stderr, "Couldn't read a real of size %d\n", len);
        return;
    }
    memcpy(p_buf, src, len);
}

static void read_cstr(void *p_buf, int len)
{
    char rbuf[1024];
    int rlen = 0;

    if (g_read_in_hex) {
        read_sized_blob(p_buf, len);
        return;
    }

    memset(p_buf, 0, len);
    while (rlen < len) {
        if (fgets(rbuf, sizeof(rbuf), stdin) == NULL) {
            fprintf(stderr, "Unexpected end of file");
            exit(EXIT_FAILURE);
        }
        if (rbuf[0] == '.' && (rbuf[1] == '\n' || rbuf[1] == '\0'))
            break;
        if (rlen + strlen(p_buf) > len - 1) {
            fprintf(stderr, "Too long.  Not added.\n");
            continue;
        }
        strcat((char *)p_buf, rbuf);
        rlen += strlen(p_buf);
    }
    if (((char *)p_buf)[strlen((char *)p_buf) - 1] == '\n')
        ((char *)p_buf)[strlen((char *)p_buf) - 1] =
            '\0'; /* get rid of last newline so I can test conversions */
}

static void read_pstr(void *p_buf, int len)
{
    char *s;

    if (g_read_in_hex) {
        read_sized_blob(p_buf, len);
        return;
    }

    read_cstr(p_buf, len);
    s = (char *)p_buf;
    while (len > 0) {
        if (*s == 0) {
            *s = ' ';
            while (len > 0) {
                *s = ' ';
                s++;
                len--;
            }
            return;
        }

        len--;
        s++;
    }
}

static void read_pstr2(void *p_buf, int len, int *dtasz)
{
    char *s;

    if (g_read_in_hex) {
        read_sized_blob(p_buf, len);
        return;
    }

    read_cstr(p_buf, len);
    s = (char *)p_buf;
    *dtasz = 0;
    while (len > 0) {
        if (*s == 0) {
            *s = ' ';
            while (len > 0) {
                *s = ' ';
                s++;
                len--;
            }
            return;
        }
        *dtasz = *dtasz + 1;
        len--;
        s++;
    }
}

/**
 * Check if client type.
 * @param type to check
 * @return true if is client type; false otherwise
 */
static int is_client_type(int type)
{
    return (type > CLIENT_MINTYPE) && (type < CLIENT_MAXTYPE);
}

/**
 * Check if server type.
 * @param type to check
 * @return true if is server type; false otherwise
 */
static int is_server_type(int type)
{
    return (type > SERVER_MINTYPE) && (type < SERVER_MAXTYPE);
}

/**
 * Check if client or server type.
 * @param type to check
 * @return true if is valid type; false otherwise
 */
static int is_valid_type(int type)
{
    return (is_client_type(type) || is_server_type(type));
}

/**
 * Gets the name of the type.  Returned pointer is to global static meory, do
 * not free().
 * @param type  type to get name for
 * @return pointer to string containing type name if successful; NULL otherwise
 */
static const char *type_str(int type)
{
    if (is_client_type(type))
        return CLIENT_TYPES[type].name;

    if (is_server_type(type))
        return SERVER_TYPES[type - SERVER_MINTYPE].name;

    return NULL;
}

/**
 * Build the type choice string.  Creates a list of all the different types.
 * @param p_choice_str  pointer to buffer to store choice string in
 * @param choice_str_len    length of p_choice_str
 * @return 0 if successful; !0 otherwise
 */
static int build_type_choice_str(char *p_choice_str, size_t choice_str_len)
{
    unsigned offset;
    int len;
    int largest_num_len;
    unsigned type;

    offset = 0;

    /* get the number of characters it takes to print the larges type number */
    if ((largest_num_len = snprintf(NULL, 0, "%u", SERVER_MAXTYPE)) < 0)
        return -1;

    /* add header */
    len = snprintf(p_choice_str + offset, choice_str_len - offset,
                   "Input type:\n");
    offset += len;

    if (len < 0 || offset >= choice_str_len)
        return -1;

    /* add types */
    for (type = CLIENT_MINTYPE + 1; type < SERVER_MAXTYPE; ++type) {
        /* if done with client types, jump ahead to server types */
        if (type == CLIENT_MAXTYPE)
            type = SERVER_MINTYPE + 1;

        len = snprintf(p_choice_str + offset, choice_str_len - offset,
                       "%*u, %s\n", largest_num_len, type, type_str(type));
        offset += len;

        if (len < 0 || offset >= choice_str_len)
            return -1;
    }

    /* add footer */
    len = snprintf(p_choice_str + offset, choice_str_len - offset, ">");
    offset += len;

    if (len < 0 || offset >= choice_str_len)
        return -1;

    return 0;
}

/**
 * Check to see if size is valid for given type.
 * @param type  type to check size for
 * @param size   size to check
 * @return !0 if size is valid; 0 otherwise
 */
int is_type_size_valid(int type, size_t size)
{
    const int *p_type_sizes;
    unsigned size_idx;

    p_type_sizes = (is_client_type(type))
                       ? CLIENT_TYPES[type].sizes
                       : SERVER_TYPES[type - SERVER_MINTYPE].sizes;

    /* if any length is allowed */
    if (p_type_sizes[0] < 0)
        return 1;

    for (size_idx = 0;
         size_idx < TYPE_SIZES_MAX_PER && p_type_sizes[size_idx] > 0;
         ++size_idx) {
        if (p_type_sizes[size_idx] == size)
            return 1;
    }

    return 0;
}

static void read_type_int(int type, void *p_buf, int len, int *p_dsz,
                          int stdin_is_tty)
{
    switch (type) {
    case CLIENT_UINT:
        if (stdin_is_tty)
            printf("Enter unsigned integer: ");
        read_uint(p_buf, len);
        break;

    case CLIENT_INT:
        if (stdin_is_tty)
            printf("Enter integer: ");
        read_int(p_buf, len);
        break;

    case CLIENT_REAL:
        if (stdin_is_tty)
            printf("Enter real: ");
        read_real(p_buf, len);
        break;

    case CLIENT_CSTR:
        if (stdin_is_tty)
            printf("Enter string: ");
        read_cstr(p_buf, len);
        break;

    case CLIENT_PSTR:
        if (stdin_is_tty)
            printf("Enter string: ");
        read_pstr(p_buf, len);
        break;

    case CLIENT_PSTR2:
        if (stdin_is_tty)
            printf("Enter string: ");
        read_pstr2(p_buf, len, p_dsz);
        break;

    case CLIENT_BYTEARRAY:
    case SERVER_UINT:
    case SERVER_BINT:
    case SERVER_BREAL:
    case SERVER_BCSTR:
    case SERVER_BYTEARRAY:
    case SERVER_DECIMAL:
        if (stdin_is_tty)
            printf("Enter hex value: ");
        read_sized_blob(p_buf, len);
        break;

    default:
        fprintf(stderr, "%s: unknown type %d, update this program to "
                        "include support for it\n",
                __func__, type);
        exit(EXIT_FAILURE);
    }
}

static void *read_type(int *p_type, int *p_len, int *p_dsz)
{
    void *p_buf;
    int stdin_is_tty;
    char choice_str[512];

    /* we don't print prompts if we're not getting input from a tty */
    stdin_is_tty = isatty(fileno(stdin));

    if (stdin_is_tty) {
        if (build_type_choice_str(choice_str, sizeof(choice_str))) {
            fprintf(stderr, "%s: build_type_choice_str failed\n", __func__);
            exit(EXIT_FAILURE);
        }
    } else
        choice_str[0] = '\0';

    /* get type */
    do {
        *p_type = choice(choice_str, CLIENT_MINTYPE + 1, SERVER_MAXTYPE - 1);
    } while (!is_valid_type(*p_type));

    /* get len */
    *p_len = choice((stdin_is_tty) ? "Length: " : "", 1, INT_MAX);
    if (!is_type_size_valid(*p_type, *p_len)) {
        fprintf(stderr, "%s: invalid length: %d for %s\n", __func__, *p_len,
                type_str(*p_type));
        exit(EXIT_FAILURE);
    }

    if (!(p_buf = malloc(*p_len))) {
        fprintf(stderr, "%s: malloc failed length: %d\n", __func__, *p_len);
        exit(EXIT_FAILURE);
    }

    /* read in the value */
    read_type_int(*p_type, p_buf, *p_len, p_dsz, stdin_is_tty);

    return p_buf;
}

static void *convert(const void *in, int inlen, int intype,
                     const struct field_conv_opts *inopts,
                     blob_buffer_t *inblob, int outlen, int *routlen,
                     int outtype, int *outdtsz,
                     const struct field_conv_opts *outopts,
                     blob_buffer_t *outblob)
{
    void *p_out;
    int isnull = 0; /* forget nulls, test data */
    int flags = 0;
    int rc;

    /* handle variable length types: */
    if (outlen == -1) {
        if (intype == CLIENT_BYTEARRAY || intype == SERVER_BCSTR)
            outlen = inlen + 1;
        else if (outtype == SERVER_BYTEARRAY && intype == SERVER_BYTEARRAY)
            outlen = inlen;
        else if (outtype == SERVER_BYTEARRAY && intype == CLIENT_BYTEARRAY)
            outlen = inlen + 1;
        else if (intype == CLIENT_CSTR)
            outlen = inlen;
        else if (intype == CLIENT_PSTR)
            outlen = inlen;
        else if (intype == CLIENT_PSTR2)
            outlen = inlen;
        else if (outtype == CLIENT_CSTR || outtype == CLIENT_PSTR ||
                 outtype == SERVER_BCSTR || outtype == CLIENT_PSTR2)
            outlen = g_outstrlen; /* enough to write float, int, etc */
        if (outtype == CLIENT_CSTR || outtype == SERVER_BCSTR)
            outlen += 1;
    }
    if (routlen)
        *routlen = outlen;

    p_out = malloc(outlen);

    if (is_client_type(intype)) {
        if (is_client_type(outtype))
            rc = CLIENT_to_CLIENT(in, inlen, intype, inopts, inblob, p_out,
                                  outlen, outtype, outdtsz, outopts, outblob);
        else
            rc = CLIENT_to_SERVER(in, inlen, intype, isnull, inopts, inblob,
                                  p_out, outlen, outtype, flags, outdtsz,
                                  outopts, outblob);
    } else {
        if (is_client_type(outtype))
            rc = SERVER_to_CLIENT(in, inlen, intype, inopts, inblob, flags,
                                  p_out, outlen, outtype, &isnull, outdtsz,
                                  outopts, outblob);
        else
            rc = SERVER_to_SERVER(in, inlen, intype, inopts, inblob, flags,
                                  p_out, outlen, outtype, flags, outdtsz,
                                  outopts, outblob);
    }
    if (rc) {
        free(p_out);
        p_out = NULL;
        return NULL;
    }
    return p_out;
}

static void dumpval_client(char *p_buf, int type, int len)
{
    int slen;
    /* variables for all possible types */
    long long lval;
    int ival;
    short sval;
    unsigned long long ulval;
    unsigned int uival;
    unsigned short usval;
    float fval;
    double dval;
    int8b blval;
    int4b bival;
    int2b bsval;
    ieee4b bfval;
    ieee8b bdval;

    switch (type) {
    case CLIENT_UINT:
        switch (len) {
        case 2:
            memcpy(&usval, p_buf, len);
            printf("%hu\n", usval);
            break;

        case 4:
            memcpy(&uival, p_buf, len);
            printf("%u\n", uival);
            break;

        case 8:
            memcpy(&ulval, p_buf, len);
            printf("%llu\n", ulval);
            break;

        default:
            printf("ERROR: invalid CLIENT_UINT length: %d\n", len);
            exit(EXIT_FAILURE);
        }
        break;

    case CLIENT_INT:
        switch (len) {
        case 2:
            memcpy(&sval, p_buf, len);
            printf("%hd\n", sval);
            break;

        case 4:
            memcpy(&ival, p_buf, len);
            printf("%d\n", ival);
            break;

        case 8:
            memcpy(&lval, p_buf, len);
            printf("%lld\n", lval);
            break;

        default:
            printf("ERROR: invalid CLIENT_INT length: %d\n", len);
            exit(EXIT_FAILURE);
        }
        break;

    case CLIENT_REAL:
        switch (len) {
        case 4:
            memcpy(&fval, p_buf, len);
            printf("%f\n", (double)fval);
            break;

        case 8:
            memcpy(&dval, p_buf, len);
            printf("%f\n", dval);
            break;

        default:
            printf("ERROR: invalid CLIENT_REAL length: %d\n", len);
            exit(EXIT_FAILURE);
        }
        break;

    case CLIENT_CSTR:
        printf("'%s'\n", p_buf);
        break;

    case CLIENT_PSTR:
        slen = pstrlenlim(p_buf, len);
        printf("'%.*s'\n", slen, p_buf);
        break;

    case CLIENT_PSTR2:
        slen = pstr2lenlim(p_buf, len);
        printf("'%.*s'\n", slen, p_buf);
        break;

    case CLIENT_BYTEARRAY:
    /* TODO handle these properly */
    case CLIENT_BLOB:
    case CLIENT_DATETIME:
    case CLIENT_INTVYM:
    case CLIENT_INTVDS:
    case CLIENT_VUTF8:
        /* TODO add more support here */
        fsnapf(stdout, p_buf, len);
        break;

    default:
        fprintf(stderr, "%s: unknown type %d, update this program to "
                        "include support for it\n",
                __func__, type);
        exit(EXIT_FAILURE);
        break;
    }
}

static void dumpval_server(char *p_buf, int type, int len)
{
    int slen;
    /* variables for all possible types */
    long long lval;
    int ival;
    short sval;
    unsigned long long ulval;
    unsigned int uival;
    unsigned short usval;
    float fval;
    double dval;
    int8b blval;
    int4b bival;
    int2b bsval;
    ieee4b bfval;
    ieee8b bdval;

    printf("flag: %#x ", (unsigned)*p_buf);
    ++p_buf;
    --len;

    switch (type) {
    case SERVER_UINT:
        switch (len) {
        case 2:
            memcpy(&usval, p_buf, len);
            printf("%hu\n", usval);
            break;

        case 4:
            memcpy(&uival, p_buf, len);
            printf("%u\n", uival);
            break;

        case 8:
            memcpy(&ulval, p_buf, len);
            printf("%llu\n", ulval);
            break;

        default:
            printf("Invalid biased unsigned int length: %d\n", len);
            exit(EXIT_FAILURE);
            break;
        }
        break;

    case SERVER_BINT:
        switch (len) {
        case 2:
            memcpy(&bsval, p_buf, len);
            int2b_to_int2(bsval, &sval);
            printf("b%hd\n", sval);
            break;

        case 4:
            memcpy(&bival, p_buf, len);
            int4b_to_int4(bival, &ival);
            printf("b%d\n", ival);
            break;

        case 8:
            memcpy(&blval, p_buf, len);
            int8b_to_int8(blval, &lval);
            printf("b%lld\n", lval);
            break;

        default:
            printf("Invalid biased int length: %d\n", len);
            exit(EXIT_FAILURE);
            break;
        }
        break;

    case SERVER_BREAL:
        switch (len) {
        case 4:
            memcpy(&bfval, p_buf, len);
            ieee4b_to_ieee4(bfval, &fval);
            printf("b%f\n", (double)fval);
            break;

        case 8:
            memcpy(&bdval, p_buf, len);
            ieee8b_to_ieee8(bdval, &dval);
            printf("b%f\n", dval);
            break;

        default:
            printf("Invalid biased real length: %d\n", len);
            exit(EXIT_FAILURE);
            break;
        }
        break;

    case SERVER_BCSTR:
        printf("'%.*s'\n", len, p_buf);
        break;

    case SERVER_BYTEARRAY:
    /* TODO handle these properly */
    case SERVER_BLOB:
    case SERVER_DATETIME:
    case SERVER_INTVYM:
    case SERVER_INTVDS:
    case SERVER_VUTF8:
        fsnapf(stdout, p_buf, len);
        break;

    default:
        fprintf(stderr, "%s: unknown type %d, update this program to "
                        "include support for it\n",
                __func__, type);
        exit(EXIT_FAILURE);
        break;
    }
}

static void dumpval(char *p_buf, int type, int len)
{
    printf("    ");
    hexdump(p_buf, len);
    return;

    /*if(is_client_type(type))*/
    /*{*/
    /*dumpval_client(p_buf, type, len);*/
    /*return;*/
    /*}*/

    /*if(is_server_type(type))*/
    /*{*/
    /*dumpval_server(p_buf, type, len);*/
    /*return;*/
    /*}*/
    /*
        fprintf(stderr, "%s: unknown type %d, update this program to include "
                "support for it\n", __func__, type);
        exit(EXIT_FAILURE);
        */
}

/**
 * Test the type system.
 * @return EXIT_SUCCESS if successful; EXIT_FAILURE otherwise
 */
int main(int argc, char *argv[])
{
    void *p_buf;
    int type;
    int otype;
    int len;
    int idsz;
    struct field_conv_opts opts = {0};
    blob_buffer_t inblob = {0};
    blob_buffer_t outblob = {0};
    blob_buffer_t routblob = {0};
    unsigned size_idx;

    /* pop prog name */
    --argc;
    ++argv;

    while (argc > 0) {
        if (strcmp(argv[0], "-x") == 0)
            g_read_in_hex = 1;
        else if (strcmp(argv[0], "-d") == 0)
            g_dump_buffers = 1;
        else if (strcmp(argv[0], "-c") == 0) {
            if (argc < 2) {
                fprintf(stderr, "Expected length for -c\n");
                return EXIT_FAILURE;
            }
            --argc;
            ++argv;
            g_outstrlen = atoi(argv[0]);
        } else {
            fprintf(stderr, "Unknown option %s\n", argv[0]);
            return EXIT_FAILURE;
        }
        --argc;
        ++argv;
    }

    setbuf(stdout, NULL);

    /* TODO delme? */
    /*if(test_fixup())*/
    /*return EXIT_FAILURE;*/

    if (dfpalInit((void *)malloc(dfpalMemSize())) != DFPAL_ERR_NO_ERROR) {
        int32_t init_err, init_os_err;
        char *err_str = NULL;

        dfpalGetError(&init_err, &init_os_err, &err_str);
        fprintf(stderr, "DFPAL Init error number:%d, error: %s\n", init_err,
                err_str);
        dfpalEnd(free);
        return (1);
    }

    printf("Version of DFPAL: %s\n", dfpalVersion());

    /* Is it running in softwar or hardware? */
    if (dfpalGetExeMode() != PPCHW)
        printf("DFPAL is operating in software\n");
    else
        printf("DFPAL is operating in hardware\n");

    idsz = 0;
    p_buf = read_type(&type, &len, &idsz);
    if (idsz)
        len = idsz;

    printf("Input:\n");
    fsnapf(stdout, p_buf, len);

    for (otype = CLIENT_MINTYPE + 1; otype < SERVER_MAXTYPE; ++otype) {
        const int *p_type_sizes;

        /* if done with client types, jump ahead to server types */
        if (otype == CLIENT_MAXTYPE)
            otype = SERVER_MINTYPE + 1;

        p_type_sizes = (is_client_type(otype))
                           ? CLIENT_TYPES[otype].sizes
                           : SERVER_TYPES[otype - SERVER_MINTYPE].sizes;

        for (size_idx = 0;
             size_idx < TYPE_SIZES_MAX_PER && p_type_sizes[size_idx];
             ++size_idx) {
            void *p_cbuf;
            int olen;
            int odsz;

            p_cbuf = convert(p_buf, len, type, &opts, &inblob,
                             p_type_sizes[size_idx], &olen, otype, &odsz, &opts,
                             &outblob);

            printf("%6s: %20s(%3d) -> %20s(%3d:%3d)\n",
                   (p_cbuf) ? "OK" : "FAILED", type_str(type), len,
                   type_str(otype), olen, odsz);

            if (p_cbuf) {
                void *p_rcbuf;
                int rolen;
                int rodsz;
                int isWrong;
                const char *p_msg;

                if (g_dump_buffers)
                    fsnapf(stdout, p_cbuf, olen);
                dumpval(p_cbuf, otype, olen);

                /* test conversion back */
                p_rcbuf = convert(p_cbuf, olen, otype, &opts, &outblob, len,
                                  &rolen, type, &rodsz, &opts, &routblob);

                /* if the reverse doesn't match the orig input */
                isWrong =
                    !p_rcbuf || (len != rolen) || memcmp(p_buf, p_rcbuf, len);

                /* figure out the right msg */
                if (p_rcbuf)
                    p_msg = (isWrong) ? "WRONG" : "MATCH";

                else
                    p_msg = "FAILED";

                printf("%6s: %20s(%3d:%3d) <- %20s(%3d)\n", p_msg,
                       type_str(type), rolen, rodsz, type_str(otype), olen);

                if (isWrong && p_rcbuf) {
                    if (g_dump_buffers)
                        fsnapf(stdout, p_rcbuf, rolen);
                    dumpval(p_rcbuf, type, rolen);
                }

                free(p_rcbuf);
                p_rcbuf = NULL;
            }

            free(p_cbuf);
            p_cbuf = NULL;
        }
    }

    free(p_buf);
    p_buf = NULL;
    return EXIT_SUCCESS;
}
