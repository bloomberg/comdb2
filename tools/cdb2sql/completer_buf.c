#include <stdlib.h>
#include <stdio.h> 
#include <stddef.h>
#include <inttypes.h>
#include <string.h>
#include <assert.h>

#include "completer_buf.h"

static const int minbuf = 10;
static const int initbuf = 1024;

struct completer_buf {
    char *buf;
    int buflen;                        /* max read into buffer */
    int allocated;                     /* max allocated size   */
    int offset;                        /* offset of first unused byte */
    void *usrptr;                      /* passed to feed when it needs to be called */
};

struct completer_buf* completerbuf_new(void) {
    struct completer_buf *b = malloc(sizeof(struct completer_buf));
    if (b == NULL)
        return NULL;
    b->buf = malloc(initbuf);
    if (b->buf == NULL)
        return NULL;
    b->allocated = initbuf;
    b->buflen = 0;
    b->offset = 0;
    return b;
}

void completerbuf_append(struct completer_buf *b, char *line) {
    int bytes = strlen(line);
    // +2 for newline and nul
    if (bytes > 0 && bytes+2 < (b->allocated - b->buflen)) {
        memcpy(b->buf + b->buflen, line, bytes+1);
        b->buflen += bytes;
        b->buf[b->buflen++] = '\n';
    }
    else {
        int needmore = bytes - (b->allocated - b->buflen);
        b->buf = realloc(b->buf, b->allocated + needmore + 2);
        memcpy(b->buf + b->buflen, line, bytes);
        b->allocated = b->allocated + needmore;
        b->buflen = b->allocated;
        b->buf[b->buflen++] = '\n';
    }
}

char* completerbuf_get(struct completer_buf *b) {
    int available = b->buflen - b->offset;

    // if we have something, return it
    if (available) {
        // reset if there's just a few bytes left
        if ((b->buflen - b->offset) < minbuf) {
            memmove(b->buf, b->buf + b->offset, b->buflen - b->offset);
            b->buflen = b->buflen - b->offset;
            b->offset = 0;
        }
        return b->buf + b->offset;
    }
    else
        return 0;
}

void completerbuf_advance(struct completer_buf *b, int bytes) {
    assert(b->offset + bytes <= b->buflen);
    b->offset += bytes;
    // reset if we consumed everything
    if (b->offset >= b->buflen) {
        b->buflen = 0;
        b->offset = 0;
    }
}

void completerbuf_clear(struct completer_buf *b) {
    b->offset = 0;
    b->buflen = 0;
}


void completerbuf_free(struct completer_buf *b) {
    free(b->buf);
    free(b);
}

int completerbuf_available(struct completer_buf *b) {
    return b->buflen - b->offset;
}
