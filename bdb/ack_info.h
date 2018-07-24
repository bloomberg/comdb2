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

#ifndef ACK_INFO_H
#define ACK_INFO_H

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
    (char *)((uint8_t *)(info) + ((info)->hdrsz - (info)->fromlen))

#define new_ack_info(ptr, payloadsz, fromhost)                                 \
    do {                                                                       \
        int __len = strlen(fromhost) + 1;                                      \
        (ptr) = alloca(sizeof(ack_info) + payloadsz + __len);                  \
        (ptr)->hdrsz = sizeof(ack_info) + __len;                               \
        (ptr)->len = payloadsz;                                                \
        (ptr)->fromlen = __len;                                                \
        (ptr)->from = (ptr)->to = 0;                                           \
        strcpy(ack_info_from_host(ptr), fromhost);                             \
    } while (0)

#define ack_info_data(info) (void *)((uint8_t *)(info) + (info)->hdrsz)

#define ack_info_size(info) ((info)->hdrsz + (info)->len)

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
typedef int(udp_sender)(struct bdb_state_tag *, ack_info *, const char *);

void set_udp_sender(udp_sender *);
int bdb_udp_send(struct bdb_state_tag *, const char *, size_t, void *);

typedef int(udp_receiver)(ack_info *, ssize_t *);
void set_udp_receiver(udp_receiver *);

#endif
