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

#endif
