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

#ifndef INCLUDED_COMDB2_QUEUE_H
#define INCLUDED_COMDB2_QUEUE_H

/*
 * Defines the protocol by which comdb2 fstsnds messages to listening
 * applications.
 */

#include <bb_stdint.h>

struct dbq_key {
    bbuint32_t keywords[2];
};

struct dbq_msgbuf {
    /* prccom header */
    bbint16_t prccom[4];

    /* Eight characters - CDB2_MSG */
    char comdb2[8];

    /* Total size of this buffer in bytes including prccom header */
    bbuint32_t buflen;

    /* Sending database name */
    char dbname[8];

    /* Name of queue that this message came from. */
    char queue_name[32];

    /* Should be sent as all zeroes for now. */
    bbuint32_t reserved[8];

    /* How many messages are encoded in this buffer. */
    bbuint32_t num_messages;

    /* The messages. */
    char msgs[1];
};

struct dbq_msg {
    bbuint32_t length;

    struct dbq_key key;

    char data[1];
};

#endif
