#ifndef NET_TYPES_H
#define NET_TYPES_H

/* nethandler types */
enum {
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
    NET_OSQL_BLOCK_REQ = 118, /* this goes only on offload net */
    NET_OSQL_BLOCK_RPL = 119, /* this goes only on offload net */
    NET_SETLOCKS = 120,
    NET_SET_ALL_LOCKS = 121,
    NET_RELEASE_LOCKS = 122,
    NET_OSQL_SOCK_REQ = 123,  /* this goes only on offload net */
    NET_OSQL_SOCK_RPL = 124,  /* this goes only on offload net */
    NET_OSQL_RECOM_REQ = 125, /* this goes only on offload net */
    NET_OSQL_RECOM_RPL = 126, /* this goes only on offload net */
    NET_HBEAT_SQL = 127,      /* this goes only on offload net */
    NET_FORGETMENOT = 128,    /* to remind master of an incoherent node */
    NET_USE_LLMETA = 129,  /* depricated, in this version of comdb2, * all dbs
                              must be llmeta */
    NET_OSQL_POKE = 130,   /* this goes only on offload net */
    NET_OSQL_SIGNAL = 131, /* this goes only on offload net */
    NET_OSQL_SERIAL_REQ = 132, /* this goes only on offload net */
    NET_OSQL_SERIAL_RPL = 133, /* this goes only on offload net */
    NET_OSQL_BLOCK_REQ_PARAMS = 134,
    NET_OSQL_ECHO_PING = 135,      /* latency debug instrumentation */
    NET_OSQL_ECHO_PONG = 136,      /*  - "" - */
    NET_OSQL_BLOCK_REQ_COST = 137, /* like BLOCK_REQ but asks remote node to
                                      record/pass back query stats */
    NET_OSQL_SOCK_REQ_COST = 138,  /* like SOCK_REQ, but passes dbglog ids */
    NET_OSQL_SNAPISOL_REQ = 139,   /* this goes only on offload net */
    NET_OSQL_SNAPISOL_RPL = 140,   /* this goes only on offload net */
    NET_RELOAD_LUA = 141, /* Delete the cached lua machines at load time */
    NET_OSQL_MASTER_CHECK = 142,   /* this goes only on offload net */
    NET_OSQL_MASTER_CHECKED = 143, /* this goes only on offload net */
    NET_BLOCK_REQ = 144,   /* this is to process block request on master. */
    NET_BLOCK_REPLY = 145, /*  process block request reply from master. */
    NET_OSQL_SNAP_UID_REQ = 146, /* the request to check snapshot UID */
    NET_OSQL_SNAP_UID_RPL = 147, /* the reply from master for uid check */
    NET_TRIGGER_REGISTER = 148,
    NET_TRIGGER_UNREGISTER = 149,
    NET_TRIGGER_START = 150,

    /* Define duplicates of all offload requests.  The new types identify
     * requests by uuid instead of
     * rqid.  They are sent when gbl_noenv_messages is enabled */
    NET_OSQL_UUID_REQUEST_MIN = 151,
    NET_OSQL_BLOCK_REQ_UUID = 152,
    NET_OSQL_BLOCK_REQ_PARAMS_UUID = 153,
    NET_OSQL_BLOCK_REQ_COST_UUID = 154,
    NET_OSQL_BLOCK_RPL_UUID = 155,
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
    NET_OSQL_POKE_UUID = 166,
    NET_OSQL_MASTER_CHECK_UUID = 167,
    NET_OSQL_MASTER_CHECKED_UUID = 168,
    NET_OSQL_SOCK_REQ_COST_UUID = 169,
    NET_AUTHENTICATION_CHECK = 170,
    NET_OSQL_UUID_REQUEST_MAX,

    MAX_USER_TYPE
};

#endif /* NET_TYPES_H */
