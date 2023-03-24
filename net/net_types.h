/*
   Copyright 2020 Bloomberg Finance L.P.

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

#ifndef NET_TYPES_H
#define NET_TYPES_H

#if 0
+---------------+
| WIRE PROTOCOL |
+---------------+

wire_header_type followed by payload.

wire_header_type.type == WIRE_HEADER_HEARTBEAT, has no payload.

If wire_header_type.type == WIRE_HEADER_USER_MSG, then payload is
net_send_message_header with usertype = USER_TYPE_* or NET_*. This is
optionally followed by datalen bytes.

If wire_header_type.type == WIRE_HEADER_ACK, then payload is
net_ack_message_type.

If wire_header_type.type == WIRE_HEADER_ACK_PAYLOAD, then payload is
net_send_message_payload_ack.

WIRE_HEADER_HELLO, WIRE_HEADER_HELLO_REPLY, WIRE_HEADER_DECOM,
WIRE_HEADER_DECOM_NAME do not have a struct defining the payload.
Would be nice to have this.
#endif

enum {
    WIRE_HEADER_MIN,
    WIRE_HEADER_HEARTBEAT = 1,
    WIRE_HEADER_HELLO = 2,
    WIRE_HEADER_DECOM = 3, /* deprecated */
    WIRE_HEADER_USER_MSG = 5,
    WIRE_HEADER_ACK = 6,
    WIRE_HEADER_HELLO_REPLY = 7,
    WIRE_HEADER_DECOM_NAME = 8,
    WIRE_HEADER_ACK_PAYLOAD = 9,
    WIRE_HEADER_MAX
};

enum {
    USER_TYPE_MIN,
    USER_TYPE_BERKDB_REP = 1,
    USER_TYPE_BERKDB_NEWSEQ = 2,
    USER_TYPE_BERKDB_FILENUM = 3,
    USER_TYPE_TEST = 4,
    USER_TYPE_ADD = 5,
    USER_TYPE_DEL = 6,
    USER_TYPE_DECOM_DEPRECATED = 7,
    USER_TYPE_ADD_DUMMY = 8,
    USER_TYPE_REPTRC = 9,
    USER_TYPE_RECONNECT = 10,
    USER_TYPE_LSNCMP = 11,
    USER_TYPE_RESYNC = 12,
    USER_TYPE_DOWNGRADEANDLOSE = 13,
    USER_TYPE_INPROCMSG = 14,
    USER_TYPE_COMMITDELAYMORE = 15,
    USER_TYPE_COMMITDELAYNONE = 16,
    USER_TYPE_MASTERCMPCONTEXTLIST = 18,
    USER_TYPE_GETCONTEXT = 19,
    USER_TYPE_HEREISCONTEXT = 20,
    USER_TYPE_TRANSFERMASTER = 21,
    USER_TYPE_GBLCONTEXT = 22,
    USER_TYPE_YOUARENOTCOHERENT = 23,
    USER_TYPE_YOUARECOHERENT = 24,
    USER_TYPE_UDP_ACK,
    USER_TYPE_UDP_PING,
    USER_TYPE_UDP_TIMESTAMP,
    USER_TYPE_UDP_TIMESTAMP_ACK,
    USER_TYPE_UDP_PREFAULT,
    USER_TYPE_TCP_TIMESTAMP,
    USER_TYPE_TCP_TIMESTAMP_ACK,
    USER_TYPE_PING_TIMESTAMP,
    USER_TYPE_PING_TIMESTAMP_ACK,
    USER_TYPE_ANALYZED_TBL,
    USER_TYPE_COHERENCY_LEASE,
    USER_TYPE_PAGE_COMPACT,

    /* by hostname messages */
    USER_TYPE_DECOM_NAME_DEPRECATED,
    USER_TYPE_ADD_NAME,
    USER_TYPE_DEL_NAME,
    USER_TYPE_TRANSFERMASTER_NAME,
    USER_TYPE_REQ_START_LSN,
    USER_TYPE_TRUNCATE_LOG,
    USER_TYPE_COMMITDELAYTIMED = 43,

    NET_QUIESCE_THREADS = 100,
    NET_RESUME_THREADS = 101,
    NET_RELOAD_SCHEMAS = 102,
    NET_CLOSE_DB = 103,
    NET_NEW_DB = 104,
    NET_NEW_QUEUE = 105,
    NET_ADD_CONSUMER = 106,
    NET_JAVASP_OP = 107,
    NET_PREFAULT_OPS = 108,
    NET_CLOSE_ALL_DBS = 109,
    NET_STATISTICS_CHANGED = 111,
    NET_PREFAULT2_OPS = 112,
    NET_FLUSH_ALL = 113,
    NET_CHECK_SC_OK = 114,
    NET_START_SC = 115,
    NET_STOP_SC = 116,
    NET_ODH_OPTIONS = 117,
    NET_OSQL_BLOCK_REQ = 118,        /* obsolete */
    NET_OSQL_BLOCK_RPL = 119,        /* obsolete */
    NET_SETLOCKS = 120,
    NET_SET_ALL_LOCKS = 121,
    NET_RELEASE_LOCKS = 122,
    NET_OSQL_SOCK_REQ = 123,         /* this goes only on offload net */
    NET_OSQL_SOCK_RPL = 124,         /* this goes only on offload net */
    NET_OSQL_RECOM_REQ = 125,        /* this goes only on offload net */
    NET_OSQL_RECOM_RPL = 126,        /* this goes only on offload net */
    NET_HBEAT_SQL = 127,             /* this goes only on offload net */
    NET_FORGETMENOT = 128,           /* to remind master of an incoherent node */
    NET_USE_LLMETA = 129,            /* depricated, in this version of comdb2, * all dbs
                                        must be llmeta */
    NET_OSQL_POKE = 130,             /* obsolete */
    NET_OSQL_SIGNAL = 131,           /* this goes only on offload net */
    NET_OSQL_SERIAL_REQ = 132,       /* this goes only on offload net */
    NET_OSQL_SERIAL_RPL = 133,       /* this goes only on offload net */
    NET_OSQL_BLOCK_REQ_PARAMS = 134, /* obsolete */
    NET_OSQL_ECHO_PING = 135,        /* latency debug instrumentation */
    NET_OSQL_ECHO_PONG = 136,        /*  - "" - */
    NET_OSQL_BLOCK_REQ_COST = 137,   /* obsolete */
    NET_OSQL_SOCK_REQ_COST = 138,    /* like SOCK_REQ, but passes dbglog ids */
    NET_OSQL_SNAPISOL_REQ = 139,     /* this goes only on offload net */
    NET_OSQL_SNAPISOL_RPL = 140,     /* this goes only on offload net */
    NET_RELOAD_LUA = 141,            /* Delete the cached lua machines at load time */
    NET_OSQL_MASTER_CHECK = 142,     /* this goes only on offload net */
    NET_OSQL_MASTER_CHECKED = 143,   /* this goes only on offload net */
    NET_BLOCK_REQ = 144,             /* this is to process block request on master. */
    NET_BLOCK_REPLY = 145,           /* process block request reply from master. */
    NET_OSQL_SNAP_UID_REQ = 146,     /* the request to check snapshot UID */
    NET_OSQL_SNAP_UID_RPL = 147,     /* the reply from master for uid check */
    NET_TRIGGER_REGISTER = 148,
    NET_TRIGGER_UNREGISTER = 149,
    NET_TRIGGER_START = 150,

    /* Define duplicates of all offload requests. The new types identify
     * requests by uuid instead of rqid. They are sent when gbl_noenv_messages
     * is enabled */
    NET_OSQL_UUID_REQUEST_MIN = 151,
    NET_OSQL_BLOCK_REQ_UUID = 152,        /* obsolete */
    NET_OSQL_BLOCK_REQ_PARAMS_UUID = 153, /* obsolete */
    NET_OSQL_BLOCK_REQ_COST_UUID = 154,   /* obsolete */
    NET_OSQL_BLOCK_RPL_UUID = 155,        /* obsolete */
    NET_OSQL_SOCK_REQ_UUID = 156,
    NET_OSQL_SOCK_RPL_UUID = 157,
    NET_OSQL_SIGNAL_UUID = 158,
    NET_OSQL_RECOM_REQ_UUID = 159,
    NET_OSQL_RECOM_RPL_UUID = 160,
    NET_OSQL_SNAPISOL_REQ_UUID = 161,
    NET_OSQL_SNAPISOL_RPL_UUID = 162,
    NET_OSQL_SERIAL_REQ_UUID = 163,
    NET_OSQL_SERIAL_RPL_UUID = 164,
    NET_HBEAT_SQL_UUID = 165,
    NET_OSQL_POKE_UUID = 166, /* obsolete */
    NET_OSQL_MASTER_CHECK_UUID = 167,
    NET_OSQL_MASTER_CHECKED_UUID = 168,
    NET_OSQL_SOCK_REQ_COST_UUID = 169,
    NET_AUTHENTICATION_CHECK = 170,
    NET_OSQL_UUID_REQUEST_MAX,
    USER_TYPE_MAX = NET_OSQL_UUID_REQUEST_MAX
};

#endif /* NET_TYPES_H */
