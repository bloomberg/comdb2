#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stddef.h>

#include "comdb2.h"
#include "comdb2_atomic.h"
#include "sql.h"
#include "logmsg.h"
#include "ctrace.h"

static struct ireq *first_iq;
static sqlclntstate *first_clnt;
static pthread_once_t once = PTHREAD_ONCE_INIT;
int gbl_legacy_requests_verbose = 0;

static void log_req(struct ireq *iq, sqlclntstate *clnt, int do_ctrace) {
    if (clnt) {
        if (do_ctrace)
            ctrace("Got a legacy sql request from \"%s\" pid %d\n", clnt->origin, clnt->conninfo.pid);
        else
            logmsg(LOGMSG_INFO, "Got a legacy sql request from \"%s\" pid %d\n", clnt->origin, clnt->conninfo.pid);
    }
    else {
        if (do_ctrace)
            ctrace("Got a legacy request from \"%s\" opcode %d (%s)\n", iq->corigin[0] ? iq->corigin : iq->frommach, iq->opcode, iq->where);
        else
            logmsg(LOGMSG_INFO, "Got a legacy request from \"%s\" opcode %d (%s)\n", iq->corigin[0] ? iq->corigin : iq->frommach, iq->opcode, iq->where);
    }
}

static void legacy_init(void) {
    log_req(first_iq, first_clnt, 0);
}

void log_legacy_request(struct ireq *iq, sqlclntstate *clnt) {
    ATOMIC_ADD64(gbl_legacy_requests, 1);
    if (gbl_legacy_requests_verbose)
        log_req(iq, clnt, 1);
    first_iq = iq;
    first_clnt = clnt;
    pthread_once(&once, legacy_init);
}

void (*gbl_tagged_request_callback)(struct ireq *iq) = NULL;
