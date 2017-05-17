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

#include <list.h>
#include <tcputil.h>

#ifndef INC__SQLPROXY_H
#define INC__SQLPROXY_H

#include <plhash.h>
#include <pool_c.h>

#define BOOL_SETTING(var, dfl, descr) SETTING_MACRO(var, dfl, descr, BOOL)
#define VALUE_SETTING(var, dfl, descr) SETTING_MACRO(var, dfl, descr, VALUE)
#define SECS_SETTING(var, dfl, descr) SETTING_MACRO(var, dfl, descr, SECS)
#define MSECS_SETTING(var, dfl, descr) SETTING_MACRO(var, dfl, descr, MSECS)
#define BYTES_SETTING(var, dfl, descr) SETTING_MACRO(var, dfl, descr, BYTES)

/* Declare all global settings */
struct setting;
#define SETTING_MACRO(var, dfl, descr, type) extern unsigned var;
#include "settings.h"
#undef SETTING_MACRO

enum { MAX_BUFLINKS = 10 };

/* Used to describe a segment of one of our send or receive buffers. */
struct buflink {
    LINKC_T(struct buflink) linkv;
    unsigned offset;
    unsigned length;
    int ignore;
};

extern pthread_mutex_t sockpool_lk;

struct sql_fwd_state {

    int client_fd;
    struct sockaddr_in client_addr;

    int server_fd;

    /* The send and receive buffers.  These are allocated as part of the
     * same block as the state structure so are freed when the state is
     * freed. */
    unsigned send_buf_sz;
    char *send_buf;
    unsigned recv_buf_sz;
    char *recv_buf;

    /* The send buffer is used to receieve messages from the client, process
     * them (filtering out anything intended for sqlproxy) and forward the
     * remaining stuff onwards to server_fd.  We have a read list (buffer space
     * available for use for reading from client_fd), a process list (data
     * read from client which is awaiting processing) and a write list (data
     * ready to be written back to the server). */
    LISTC_T(struct buflink) send_buf_rd_list;
    LISTC_T(struct buflink) send_buf_pr_list;
    LISTC_T(struct buflink) send_buf_wr_list;

    /* The receive buffer is used to receieve replies from the server which
     * we forward down to the client.  We have a read list (buffer space
     * available for use for reading on server_fd) and a write list (data
     * ready to be written back to client_fd). */
    LISTC_T(struct buflink) recv_buf_rd_list;
    LISTC_T(struct buflink) recv_buf_wr_list;

    struct buflink buflinks[MAX_BUFLINKS];
    LISTC_T(struct buflink) spare_links;
};

void print_setting(const struct setting *setting);
void print_all_settings(void);
int set_setting(const char *name, unsigned new_value);
void setting_changed(unsigned *setting);

int pthread_create_attrs(pthread_t *tid, int detachstate, size_t stacksize,
                         void *(*start_routine)(void *), void *arg);

void *local_accept_thd(void *voidarg);

#endif /* INC__SQLPROXY_H */
