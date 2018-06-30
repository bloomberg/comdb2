#include <stdlib.h>
#include <string.h>
#include <stddef.h>

#include "comdb2.h"
#include "comdb2systblInt.h"
#include "sql.h"
#include "ezsystables.h"
#include <net.h>
#include "bdb_int.h"

typedef struct systable_net_userfunc {
    char                    *service;
    char                    *userfunc;
    int64_t                 count;
    int64_t                 totus;
} systable_net_userfunc_t;

typedef struct net_get_userfunc {
    int count;
    int alloc;
    systable_net_userfunc_t *records;
} net_get_userfunc_t;

static void userfunc_to_systable(struct netinfo_struct *netinfo_ptr, void *arg,
        char *service, char *userfunc, int64_t count, int64_t totus)
{
    net_get_userfunc_t *uf = (net_get_userfunc_t *)arg;
    uf->count++;

    if (uf->count >= uf->alloc) {
        if (uf->alloc == 0) uf->alloc = 512;
        else uf->alloc = uf->alloc * 2;
        uf->records = realloc(uf->records, uf->alloc *
                sizeof(systable_net_userfunc_t));
    }

    systable_net_userfunc_t *u = &uf->records[uf->count - 1];
    memset(u, 0, sizeof(*u));
    u->service = service;
    u->userfunc = userfunc;
    u->count = count;
    u->totus = totus;
}

static int get_net_userfuncs(void **data, int *records)
{
    net_get_userfunc_t uf = {0};
    bdb_state_type *bdb_state = thedb->bdb_env;
    net_userfunc_iterate(bdb_state->repinfo->netinfo, userfunc_to_systable,
            &uf);
    net_userfunc_iterate(thedb->handle_sibling_offload, userfunc_to_systable,
            &uf);
    *data = uf.records;
    *records = uf.count;
    return 0;
}

static void free_net_userfuncs(void *p, int n)
{
    free(p);
}

int systblNetUserfuncsInit(sqlite3 *db) {
    return create_system_table(db, "comdb2_net_userfuncs", get_net_userfuncs,
            free_net_userfuncs, sizeof(systable_net_userfunc_t),
            CDB2_CSTRING, "service", offsetof(systable_net_userfunc_t, service),
            CDB2_CSTRING, "userfunc", offsetof(systable_net_userfunc_t, userfunc),
            CDB2_INTEGER, "count", offsetof(systable_net_userfunc_t, count),
            CDB2_INTEGER, "totalus", offsetof(systable_net_userfunc_t, totus),
            SYSTABLE_END_OF_FIELDS);
}
