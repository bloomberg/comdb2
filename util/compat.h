/*
   Copyright 2018 Bloomberg Finance L.P.

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

#ifndef INCLUDED_COMPAT_H
#define INCLUDED_COMPAT_H

#include <stdio.h>
#include <stdint.h>
#include <alloca.h>
#include <arpa/inet.h>
#include <netinet/in.h>

typedef struct ack_info_t {
    uint32_t hdrsz;
    int32_t type;
    uint32_t len; /* payload len */
    int32_t from;
    int32_t to;
    int32_t fromlen;
    /* To prevent double copying of payload, allocate enough
    ** memory beyond every ack_info to hold the payload.
    */
} ack_info;

/*
** new_ack_info() allocates storage on the stack as its intended to be sent
** inline. no need to free this storage.
*/
#define ack_info_from_host(info)                                               \
    (char *)(((info)->fromlen <= 0 || (info)->hdrsz <= sizeof(ack_info) ||     \
              (info)->fromlen > ((info)->hdrsz - sizeof(ack_info)))            \
                 ? NULL                                                        \
                 : (uint8_t *)(info) + ((info)->hdrsz - (info)->fromlen))

#define new_ack_info(ptr, payloadsz, fromhost)                                 \
    do {                                                                       \
        int __len = strlen(fromhost) + 1;                                      \
        (ptr) = alloca(sizeof(ack_info) + payloadsz + __len);                  \
        (ptr)->hdrsz = sizeof(ack_info) + __len;                               \
        (ptr)->len = payloadsz;                                                \
        (ptr)->fromlen = __len;                                                \
        (ptr)->from = (ptr)->to = 0;                                           \
        strcpy((char *)((uint8_t *)(ptr) + sizeof(ack_info)), fromhost);       \
    } while (0)

#define ack_info_data(info) (void *)((uint8_t *)(info) + (info)->hdrsz)

#define ack_info_size(info)                                                    \
    (((info)->hdrsz + (info)->len) < (info)->hdrsz                             \
         ? 0                                                                   \
         : (info)->hdrsz + (info)->len)

/* Convert ack_info header into big endian */
#define ack_info_from_cpu(info)                                                \
    do {                                                                       \
        (info)->hdrsz = htonl((info)->hdrsz);                                  \
        (info)->type = htonl((info)->type);                                    \
        (info)->len = htonl((info)->len);                                      \
        (info)->from = htonl((info)->from);                                    \
        (info)->to = htonl((info)->to);                                        \
        (info)->fromlen = htonl((info)->fromlen);                              \
    } while (0)

#define ack_info_to_cpu(info)                                                  \
    do {                                                                       \
        (info)->hdrsz = ntohl((info)->hdrsz);                                  \
        (info)->type = ntohl((info)->type);                                    \
        (info)->len = ntohl((info)->len);                                      \
        (info)->from = ntohl((info)->from);                                    \
        (info)->to = ntohl((info)->to);                                        \
        (info)->fromlen = ntohl((info)->fromlen);                              \
    } while (0)

struct bdb_state_tag;
struct trigger_reg;
struct netinfo_struct;
struct host_node_tag;

int bdb_udp_send(struct bdb_state_tag *, const char *, size_t, void *);
int write_decom_msg(struct netinfo_struct *, struct host_node_tag *, int,
                    void *, int, void *, int);

typedef void(netinfo_dumper)(FILE *, struct bdb_state_tag *);
void set_netinfo_dumper(netinfo_dumper *);

typedef int(udp_sender)(struct bdb_state_tag *, ack_info *, const char *);
void set_udp_sender(udp_sender *);

typedef int(udp_receiver)(ack_info *, ssize_t *);
void set_udp_receiver(udp_receiver *);

typedef struct trigger_reg *(trigger_sender)(uint8_t *, struct trigger_reg *, size_t *);
void set_trigger_sender(trigger_sender *);

typedef struct trigger_reg *(trigger_receiver)(struct trigger_reg *, uint8_t *);
void set_trigger_receiver(trigger_receiver *);

typedef int(del_writer)(struct netinfo_struct *, const char *, char *);
void set_del_writer(del_writer *);

typedef int(add_writer)(struct netinfo_struct *, const char *, char *);
void set_add_writer(add_writer *);

typedef int(decom_writer)(struct netinfo_struct *, struct host_node_tag *, const char *, int, const char *);
void set_decom_writer(decom_writer *);

typedef int(nodenum_mapper)(char *);
void set_nodenum_mapper(nodenum_mapper *);
nodenum_mapper nodenum;

typedef char *(hostname_mapper)(int);
void set_hostname_mapper(hostname_mapper *);
hostname_mapper hostname;

typedef char *(nodenum_str_mapper)(char *);
void set_nodenum_str_mapper(nodenum_str_mapper *);
nodenum_str_mapper nodenum_str;

typedef char *(hostname_str_mapper)(char *);
void set_hostname_str_mapper(hostname_str_mapper *);
hostname_str_mapper hostname_str;

#endif
