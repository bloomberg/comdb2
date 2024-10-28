#ifndef NET_APPSOCK_H
#define NET_APPSOCK_H

#include <pthread.h>
#include <event2/event.h>

#include <berkdb/dbinc/queue.h>

#define KB(x) ((x) * 1024)
#define MB(x) ((x) * 1024 * 1024)

struct sqlclntstate;

struct appsock_handler_arg {
    int fd;
    int is_readonly;
    int secure; /* whether connection is routed from a secure pmux port */
    int admin;
    struct sockaddr_in addr;
    struct evbuffer *rd_buf;
    struct event_base *base;

    /* gethostinfo */
    struct timeval start;
    char *origin;
    TAILQ_ENTRY(appsock_handler_arg) entry;
};

int add_appsock_handler(const char *, event_callback_fn);
int maxquerytime_cb(struct sqlclntstate *);
void make_server_socket(int fd);
struct event_base *get_dispatch_event_base(void);
struct event_base *get_main_event_base(void);
void do_revconn_evbuffer(int fd, short what, void *data);

typedef void(*run_on_base_fn)(void *);
void run_on_base(struct event_base *, run_on_base_fn, void *);

extern int32_t active_appsock_conns;
extern int64_t gbl_denied_appsock_connection_count;

#undef SKIP_CHECK_THD
#ifdef SKIP_CHECK_THD
#  define check_thd(...)
#else
#  define check_thd(thd)                                                                    \
    if (!pthread_equal(thd, pthread_self())) {                                              \
        fprintf(stderr, "FATAL ERROR: %s EVENT NOT DISPATCHED on " #thd "\n", __func__);    \
        abort();                                                                            \
    }
#endif

#define evtimer_once(a, b, c)                                                               \
    ({                                                                                      \
        int erc;                                                                            \
        if ((erc = event_base_once(a, -1, EV_TIMEOUT, b, c, NULL)) != 0) {                  \
            logmsg(LOGMSG_ERROR, "%s:%d event_base_once failed\n", __func__, __LINE__);     \
        }                                                                                   \
        erc;                                                                                \
    })

#ifndef container_of
/* I'm requiring that pointer variable and struct member have the same name */
#define container_of(ptr, type) (type *)((uint8_t *)ptr - offsetof(type, ptr))
#endif

#endif
