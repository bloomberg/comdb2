#ifndef BUF_H
#define BUF_H

#ifdef __cplusplus
extern "C" {
#endif

typedef int (*completer_buf_feedfunc)(void *usrptr, char **buf);
typedef void(*completer_buf_freefunc)(void *);

struct completer_buf;
struct completer_buf* completerbuf_new(void);
char* completerbuf_get(struct completer_buf *b);
void completerbuf_append(struct completer_buf *b, char *s);
void completerbuf_advance(struct completer_buf *b, int bytes);
void completerbuf_clear(struct completer_buf *b);
int completerbuf_available(struct completer_buf *b);

#ifdef __cplusplus
}
#endif

#endif

