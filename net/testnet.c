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

/*
 * Test the network library in isolation.
 *
 * Note - streamid==cpu - we have one stream for each cpu
 */

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#include <pthread.h>

#include <epochlib.h>
#include <db.h>
#include <bsnodeusr.h>
#include <lockmacro.h>

#include <machine.h>

#include "net.h"

enum { MSGTYPE_MSG = 0, MSGTYPE_RETRAN_REQUEST = 1, MSGTYPE_NEEDACK = 2 };

struct netnode {
    int node;
    char hostname[32];
    host_node_type *netptr;
    pthread_t generator_tid;

    unsigned n_need_acks_sent;
    unsigned n_need_acks_failed;

    unsigned send_seqn;

    pthread_mutex_t rcv_mutex;
    unsigned next_seqn;  /* next seq number expected to be received */
    unsigned n_in_order; /* number of retransmits requested */
    unsigned n_too_low;
    unsigned n_too_high;
    unsigned n_retrans_sent; /* number in order received */
};

struct message {
    unsigned msg_length;  /* Length of complete message, header+data */
    unsigned data_length; /* Length of data[] */
    unsigned streamid;
    unsigned seqn;
    unsigned crc32; /* Checksum of data[] */

    /* Then comes data */
    char data[1];
};

struct retrans_request_message {
    unsigned streamid;
    unsigned seqn;
};

static netinfo_type *net;
static struct netnode *netnodes, *mynetnode;
static int numnodes;

static unsigned gbl_seed = 0;
static unsigned gbl_maxlength = 1024;
static unsigned gbl_usleep = 10;

void myfree(void *ptr)
{
    if (ptr)
        free(ptr);
}

static void fsnapfp(const char *pfx, FILE *fil, const char *buf, int len)
{
    static char *hexchars = "0123456789ABCDEF";
    char hdsp[60], cdsp[60], ch;
    int ii, jj, boff, hoff, coff;
    for (ii = 0; ii < len; ii += 16) {
        hoff = coff = 0;
        for (jj = 0; jj < 16; jj++) {
            boff = ii + jj;
            if (boff >= len) {
                hdsp[hoff++] = ' ';
                hdsp[hoff++] = ' ';
                cdsp[coff++] = ' ';
            } else {
                ch = buf[boff];
                hdsp[hoff++] = hexchars[(ch >> 4) & 0x0f];
                hdsp[hoff++] = hexchars[ch & 0x0f];
                if (ch >= ' ' && ch < 0x7f)
                    cdsp[coff++] = ch;
                else
                    cdsp[coff++] = '.';
            }
            if ((jj & 3) == 3)
                hdsp[hoff++] = ' ';
        }
        hdsp[hoff] = 0;
        cdsp[coff] = 0;
        fprintf(fil, "%s%5x:%s |%s|\n", pfx, ii, hdsp, cdsp);
    }
}

static void *gen_message(unsigned streamid, unsigned seqn)
{
    unsigned int seed = gbl_seed ^ streamid ^ seqn;
    struct message *msg;
    unsigned length;
    unsigned pos;

    /* Decide message length */
    rand_r(&seed);
    length = seed % gbl_maxlength;
    msg = malloc(length + offsetof(struct message, data));
    if (!msg) {
        fprintf(stderr, "%s: cannot malloc length %u\n", __func__, length);
        exit(1);
    }

    /* Message data */
    for (pos = 0; pos < length; pos += 4) {
        unsigned bytesleft = length - pos;
        rand_r(&seed);
        if (bytesleft < 4) {
            memcpy(msg->data + pos, &seed, bytesleft);
        } else {
            *((unsigned *)(msg->data + pos)) = seed;
        }
    }

    /* Compute checksum and fill in header */
    msg->data_length = length;
    msg->msg_length = length + offsetof(struct message, data);
    msg->streamid = streamid;
    msg->seqn = seqn;
    msg->crc32 = crc32c(&msg->data, length);

    return msg;
}

/* Generate a sequence of messages to send to a given node */
static void *generator_thd(void *voidarg)
{
    struct netnode *dest = voidarg;
    unsigned streamid = dest->node;
    int rc;
    char pfx[48];

    snprintf(pfx, sizeof(pfx), "%s:%d:%s", __func__, dest->node,
             dest->hostname);

    printf("%s: starting\n", pfx);

    while (1) {
        struct message *msg;

        msg = gen_message(streamid, dest->send_seqn);

        rc = net_send(net, dest->node, MSGTYPE_MSG, msg, msg->msg_length, 0);

        free(msg);

        if (rc != 0) {
            fprintf(stderr, "%s: net_send rcode %d seqn %u\n", pfx, rc,
                    dest->send_seqn);
            sleep(1);
        } else {
            dest->send_seqn++;
            usleep(gbl_usleep);
        }
    }

    return NULL;
}

/* This message type needs an ack */
void process_need_ack(void *ack_handle, void *usr_ptr, int fromnode,
                      int usertype, void *dta, int dtalen)
{
    int rc;
    rc = net_ack_message(ack_handle, 0);
    ack_state = NULL;

    if (rc != 0) {
        fprintf(stderr, "%s: net_ack_message for node %d failed %d\n", __func__,
                fromnode, rc);
    } else {
        printf("%s: from %d\n", __func__, fromnode);
    }
}

/* Process a message. */
void process_message(void *ack_handle, void *usr_ptr, int fromnode,
                     int usertype, void *dta, int dtalen)
{
    struct message *msg = dta;
    struct message *verify_msg;
    int ii, rc;
    char pfx[48];

    snprintf(pfx, sizeof(pfx), "%s:%d", __func__, fromnode);

    if (!msg) {
        fprintf(stderr, "%s: from %d: NULL msg!\n", pfx, fromnode);
        return;
    }

    if (dtalen < offsetof(struct message, data)) {
        fprintf(stderr, "%s: from %d: header too small %d\n", pfx, fromnode,
                dtalen);
        fsnapfp(pfx, stderr, dta, dtalen);
        return;
    }

    verify_msg = gen_message(msg->streamid, msg->seqn);
    if (dtalen != verify_msg->msg_length || memcmp(msg, verify_msg, dtalen)) {
        fprintf(stderr, "%s: from %d: incorrect message for stream %u seq %u\n",
                pfx, fromnode, dtalen, msg->streamid, msg->seqn);
        fsnapfp(pfx, stderr, dta, dtalen);
        return;
    }
    free(verify_msg);

    /* We have a good message.  Find the struct for the correct stream */
    for (ii = 0; ii < numnodes; ii++) {
        if (netnodes[ii].node == msg->streamid) {
            LOCK(&netnodes[ii].rcv_mutex)
            {

                if (msg->seqn == netnodes[ii].next_seqn) {
                    /* Bingo! */
                    netnodes[ii].next_seqn++;

                } else if (msg->seqn > netnodes[ii].next_seqn) {
                    /* Wrong!  Ask for a retransmit if too high */
                    struct retrans_request_message rmsg;
                    rmsg.streamid = msg->streamid;
                    rmsg.seqn = netnodes[ii].next_seqn;
                    netnodes[ii].n_too_low++;

                    rc = net_send(net, fromnode, MSGTYPE_RETRAN_REQUEST, &rmsg,
                                  sizeof(rmsg), 0);
                    if (rc == 0) {
                        netnodes[ii].n_retrans_sent++;
                    } else {
                        fprintf(stderr,
                                "%s: error %d sending retrans for seqn %u\n",
                                pfx, rc, netnodes[ii].next_seqn);
                    }

                } else {
                    /* Too low - ignore */
                    netnodes[ii].n_too_high++;
                }
            }
            UNLOCK(&netnodes[ii].rcv_mutex);
            return;
        }
    }

    /* Nobody wants this */
    fprintf(stderr, "%s: discard msg\n", pfx);
}

/* Process a retransmit request.  This doesn't need acking, we just send
 * the requested message. */
void process_retrans_request(void *ack_handle, void *usr_ptr, int fromnode,
                             int usertype, void *dta, int dtalen)
{
    struct retrans_request_message *inmsg = dta;
    struct message *msg;
    int rc;
    char pfx[48];

    snprintf(pfx, sizeof(pfx), "%s:%d", __func__, fromnode);

    if (dtalen != sizeof(*inmsg)) {
        fprintf(stderr, "%s: from node %d bad size %d\n", __func__, fromnode,
                dtalen);
    }

    msg = gen_message(inmsg->streamid, inmsg->seqn);

    rc = net_send(net, fromnode, MSGTYPE_MSG, msg, msg->msg_length, 0);

    free(msg);

    if (rc != 0) {
        fprintf(stderr, "%s: net_send rcode %d seqn %u\n", pfx, rc,
                inmsg->seqn);
    }
}

static const char *usage_text[] = {"Usage: testnet.tsk node1 node2 node3...",
                                   NULL};

static void usage(FILE *fh)
{
    int ii;
    for (ii = 0; usage_text[ii]; ii++) {
        fprintf(fh, "%s\n", usage_text[ii]);
    }
}

int main(int argc, char *argv[])
{
    extern char *optarg;
    extern int optind, optopt;

    int c, ii, rc;
    const char *appname = "testnet";
    const char *svcname = "test";
    const char *instname = "test";

    while ((c = getopt(argc, argv, "hs:")) != EOF) {
        switch (c) {
        case 'h':
            usage(stdout);
            exit(0);
            break;

        case 's':
            gbl_usleep = atoi(optarg) * 1000;
            break;

        case '?':
            fprintf(stderr, "Unrecognised option: -%c\n", optopt);
            usage(stderr);
            exit(2);
        }
    }

    argc -= optind;
    argv += optind;

    /* Read sibling nodes from command line */
    numnodes = argc;
    netnodes = calloc(numnodes, sizeof(struct netnode));
    if (!netnodes) {
        perror("calloc");
        exit(1);
    }

    mynetnode = NULL;
    for (ii = 0; ii < argc; ii++) {
        netnodes[ii].node = atoi(argv[ii]);
        pthread_mutex_init(&netnodes[ii].rcv_mutex, NULL);
        rc = bsnode_hostname(netnodes[ii].node, sizeof(netnodes[ii].hostname),
                             netnodes[ii].hostname);
        if (rc != 0) {
            fprintf(stderr, "bsnode_hostname failed for %d\n",
                    netnodes[ii].node);
            exit(1);
        }
        if (netnodes[ii].node == machine()) {
            if (mynetnode) {
                fprintf(stderr, "I appear twice in node list!\n");
                exit(1);
            }
            mynetnode = &netnodes[ii];
        }
        printf("Node %4d host %16s %s\n", netnodes[ii].node,
               netnodes[ii].hostname,
               mynetnode == &netnodes[ii] ? "LOCALHOST" : "");
    }
    if (!mynetnode) {
        fprintf(stderr, "Localhost must appear in node list\n");
        exit(1);
    }

    /* Setup network library */
    net = create_netinfo(mynetnode->hostname, 0, -1, mynetnode->node, appname,
                         svcname, instname);
    if (!net) {
        fprintf(stderr, "create_netinfo failed\n");
        exit(1);
    }

    net_register_handler(net, MSGTYPE_RETRAN_REQUEST, process_retrans_request);
    net_register_handler(net, MSGTYPE_MSG, process_message);
    net_register_handler(net, MSGTYPE_NEEDACK, process_need_ack);

    for (ii = 0; ii < numnodes; ii++) {
        if (mynetnode != &netnodes[ii]) {
            netnodes[ii].netptr = add_to_netinfo(net, netnodes[ii].hostname, 0,
                                                 netnodes[ii].node);
            if (!netnodes[ii].netptr) {
                fprintf(stderr, "Error adding sibling node %d\n",
                        netnodes[ii].node);
                exit(1);
            }
        }
    }

    /* For each node start a generator thread which will send out a stream
     * of data for that node to receive. */
    for (ii = 0; ii < numnodes; ii++) {
        if (mynetnode != &netnodes[ii]) {
            rc = pthread_create(&netnodes[ii].generator_tid, NULL,
                                generator_thd, &netnodes[ii]);
            if (rc != 0) {
                fprintf(stderr, "pthread_create: %d %s\n", rc, strerror(rc));
                exit(1);
            }
        }
    }

    rc = net_init(net);
    if (rc != 0) {
        fprintf(stderr, "net_init faild %d\n", rc);
        exit(1);
    }

    /* Let it run. */
    while (1) {
        for (ii = 0; ii < numnodes; ii++) {
            /* Report status */
            struct netnode *n = &netnodes[ii];

            if (n != mynetnode) {
                /* Request an ack */
                rc = net_send_message(net, n->node, MSGTYPE_NEEDACK, NULL, 0, 1,
                                      1000);
                if (rc != 0) {
                    n->n_need_acks_failed++;
                } else {
                    n->n_need_acks_sent++;
                }
            }

            printf("node %-4d snd_next %10u rcv_next %10u (%5u < %5u > %5u) rt "
                   "%5u needack good %3u bad %3u\n",
                   n->node, n->send_seqn, n->next_seqn, n->n_too_low,
                   n->n_in_order, n->n_too_high, n->n_retrans_sent,
                   n->n_need_acks_sent, n->n_need_acks_failed);
        }
        sleep(5);
    }

    return 0;
}
