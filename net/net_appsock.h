#ifndef NET_APPSOCK_H
#define NET_APPSOCK_H

#include <pthread.h>

struct event_base;
struct evbuffer;
struct sockaddr_in;

typedef void (*appsock_cb)(struct evbuffer *, int, struct sockaddr_in *);
int add_appsock_handler(const char *key, appsock_cb cb);

extern pthread_t appsock_timer_thd;
extern struct event_base *appsock_timer_base;

extern pthread_t appsock_rd_thd;
extern struct event_base *appsock_rd_base;

extern int active_appsock_conns;
extern int64_t gbl_denied_appsock_connection_count;

#undef SKIP_CHECK_THD
#ifdef SKIP_CHECK_THD
#  define check_thd(...)
#else
#  define check_thd(thd)                                                       \
    if (!pthread_equal(thd, pthread_self())) {                                 \
        fprintf(stderr, "FATAL ERROR: %s EVENT NOT DISPATCHED on " #thd "\n",  \
                __func__);                                                     \
        abort();                                                               \
    }
#endif

#define check_appsock_rd_thd() check_thd(appsock_rd_thd)
#define check_appsock_timer_thd() check_thd(appsock_timer_thd)

#endif
