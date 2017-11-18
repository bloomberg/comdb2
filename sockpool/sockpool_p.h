/*
 * Private header file defining the protocol used to communicate with the
 * machine wide socket pool.
 *
 * Currently I have only one protocol version (zero) defined and only two
 * message types (donate and request).  This is how it works:
 *
 * To donate a fd to sockpool:
 *  - client sends a donate message with the fd
 *  - server does not respond
 *
 * To request a fd from sockpool:
 *  - client sends a request message
 *  - server responds with a donate message and the same typestr
 *      - server may or may not include an fd with its donate message
 */

#ifndef INCLUDED_SOCKPOOL_P_H
#define INCLUDED_SOCKPOOL_P_H

#define SOCKPOOL_SOCKET_NAME "/tmp/sockpool.socket"

enum { SOCKPOOL_DONATE = 0, SOCKPOOL_REQUEST = 1, SOCKPOOL_FORGET_PORT = 2 };

/* Clients should send one of these to the sql proxy after making a new
 * unix domain socket connection.  If the sqlproxy doesn't like what it gets
 * it will rudely close the socket. */
struct sockpool_hello {
    char magic[4];        /* SQLP */
    int protocol_version; /* 0 for now */
    int pid;              /* client pid */
    int slot;             /* client slot */
};

struct sockpool_msg_vers0 {
    unsigned char request;
    char padding[3];  /* zeroes for now */
    int dbnum;        /* db number that this request relates to; if this
                         is non zero (and valid)*/
    int timeout;      /* timeout in seconds */
    char typestr[48]; /* string identifying the socket */
};

struct sockpool_msg_vers1 {
    unsigned char request;
    char padding[3]; /* zeroes for now */
    int timeout;     /* timeout in seconds */
    int typestrlen;  /* length of type string */
};

#endif
