/*
   Copyright 2015, 2017, Bloomberg Finance L.P.

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

#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stddef.h>
#include <ctype.h>

#include <list.h>
#include <segstr.h>

#include "comdb2.h"

/* prox2-specific requests */

struct proxy_config_line {
    char *line;
    LINKC_T(struct proxy_config_line) lnk;
};

typedef LISTC_T(struct proxy_config_line) proxy_config;

static proxy_config proxy_config_lines;
static pthread_rwlock_t proxy_config_lk = PTHREAD_RWLOCK_INITIALIZER;

void proxy_init(void)
{
    listc_init(&proxy_config_lines, offsetof(struct proxy_config_line, lnk));
}

void handle_proxy_lrl_line(char *line)
{
    struct proxy_config_line *ln;

    ln = malloc(sizeof(struct proxy_config_line));
    while (*line && isspace(*line))
        line++;
    ln->line = strdup(line);

    Pthread_rwlock_wrlock(&proxy_config_lk);
    listc_abl(&proxy_config_lines, ln);
    Pthread_rwlock_unlock(&proxy_config_lk);
}

/* TODO: this shouldn't exist.  there should be some general way
   to reload an lrl file and change whatever should be changed
   on the fly, including the proxy stuff */
void reload_proxy_lrl_lines(char *lrlfile)
{
    FILE *f;
    char line[4096];
    char *tok;
    int st = 0;
    int tlen;
    char *proxy_line;
    proxy_config new_config;
    struct proxy_config_line *l;

    listc_init(&new_config, offsetof(struct proxy_config_line, lnk));

    f = fopen(lrlfile, "r");
    if (f == NULL) {
        printf("Can't open lrl file %s: %d %s\n", lrlfile, errno,
               strerror(errno));
        return;
    }

    while (fgets(line, sizeof(line), f)) {
        st = 0;
        tok = segtok(line, sizeof(line), &st, &tlen);
        if (tokcmp(tok, tlen, "proxy") == 0) {
            tok = seglinel(line, sizeof(line), &st, &tlen);
            if (tlen > 0) {
                proxy_line = tokdup(tok, tlen);
                l = malloc(sizeof(struct proxy_config_line));
                l->line = proxy_line;
                listc_abl(&new_config, l);
            }
        }
    }

    Pthread_rwlock_wrlock(&proxy_config_lk);
    l = listc_rtl(&proxy_config_lines);
    while (l) {
        free(l->line);
        free(l);
        l = listc_rtl(&proxy_config_lines);
    }

    proxy_config_lines = new_config;
    Pthread_rwlock_unlock(&proxy_config_lk);

    fclose(f);
    printf("Reloaded proxy config\n");
}

/**
 * Gets a copy of the current prox2 config and puts it directly into the buffer.
 * @param p_buf pointer to the buffer to write to
 * @param p_buf_end pointer to just past the end of the buffer
 * @return pointer to just after the written data if success; NULL otherwise
 */
uint8_t *get_prox2_config_info_put(uint8_t *p_buf, const uint8_t *p_buf_end)
{
    /* we used to make a copy of this linked list and then pack the copy into
     * the out buffer after releasing the lock; however, I beleive that making
     * a copy (using a malloc for each lnk, then strdup()ing each line) takes
     * longer then it does just to pack the data straight to the buffer */
    Pthread_rwlock_rdlock(&proxy_config_lk);
    {
        struct db_proxy_config_rsp rsp;
        struct proxy_config_line *p_l;

        /* set and pack the prox config hdr */
        rsp.nlines = listc_size(&proxy_config_lines);
        p_buf = db_proxy_config_rsp_no_hdr_no_lines_put(&rsp, p_buf, p_buf_end);
        if (p_buf) {
            /* if the hdr was packed successfully, pack each line's length
             * (including NUL byte) followed by the line itself */
            LISTC_FOR_EACH(&proxy_config_lines, p_l, lnk)
            {
                size_t len = strlen(p_l->line) + 1 /*NUL byte*/;
                if (!(p_buf = buf_put(&len, sizeof(len), p_buf, p_buf_end)))
                    break;

                if (!(p_buf = buf_no_net_put(p_l->line, len, p_buf, p_buf_end)))
                    break;
            }
        }
    }
    Pthread_rwlock_unlock(&proxy_config_lk);

    return p_buf;
}

void dump_proxy_config(void)
{
    struct proxy_config_line *l;

    Pthread_rwlock_rdlock(&proxy_config_lk);
    if (listc_size(&proxy_config_lines) > 0)
        printf("Proxy configuration parameters:\n");
    else
        printf("No proxy configuration options loaded from lrl file\n");
    LISTC_FOR_EACH(&proxy_config_lines, l, lnk) { printf("   %s\n", l->line); }
    Pthread_rwlock_unlock(&proxy_config_lk);
}
