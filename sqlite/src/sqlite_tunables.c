#include "limit_fortify.h"
#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stddef.h>

#include "sqliteInt.h"

struct sqlite3_tunables_type sqlite3_gbl_tunables;

static inline comdb2_tunable_type sqlite_to_tunable_type(int type)
{
    switch (type) {
    case SQLITE_ATTR_QUANTITY: return TUNABLE_INTEGER;
    case SQLITE_ATTR_BOOLEAN: return TUNABLE_BOOLEAN;
    default: assert(0);
    }
    return TUNABLE_INVALID;
}

void sqlite3_tunables_init(void)
{
#define DEF_ATTR(NAME, name, type, dflt)                                       \
    sqlite3_gbl_tunables.name = dflt;                                          \
    REGISTER_TUNABLE(#NAME, NULL, sqlite_to_tunable_type(SQLITE_ATTR_##type),  \
                     &sqlite3_gbl_tunables.name, 0, NULL, NULL, NULL, NULL);

#include "sqlite_tunables.h"
}
#undef DEF_ATTR
#include "logmsg.h"

void sqlite3_dump_tunables(void)
{
#define DEF_ATTR(NAME, name, type, dflt)                                       \
    logmsg(LOGMSG_USER, "%-35s", #NAME);                                       \
    switch (SQLITE_ATTR_##type) {                                              \
    default:                                                                   \
    case SQLITE_ATTR_QUANTITY:                                                 \
        logmsg(LOGMSG_USER, "%d\n", sqlite3_gbl_tunables.name);                \
        break;                                                                 \
    case SQLITE_ATTR_BOOLEAN:                                                  \
        logmsg(LOGMSG_USER, "%s\n", sqlite3_gbl_tunables.name ? "ON" : "OFF"); \
    };
#include "sqlite_tunables.h"
}
#undef DEF_ATTR

void sqlite3_set_tunable_by_name(char *tname, char *val)
{
    int nval;
    char *endp;
    nval = strtol(val, &endp, 10);
    if (0)
        ;
#define DEF_ATTR(NAME, name, type, dflt)                                       \
    else if (strcasecmp(tname, #NAME) == 0)                                    \
    {                                                                          \
        switch (SQLITE_ATTR_##type) {                                          \
        default:                                                               \
        case SQLITE_ATTR_QUANTITY: sqlite3_gbl_tunables.name = nval; break;    \
        case SQLITE_ATTR_BOOLEAN:                                              \
            if (strcasecmp(val, "on") == 0)                                    \
                sqlite3_gbl_tunables.name = 1;                                 \
            else if (strcasecmp(val, "off") == 0)                              \
                sqlite3_gbl_tunables.name = 0;                                 \
            else {                                                             \
                logmsg(LOGMSG_ERROR, "Expected ON or OFF\n");                  \
            }                                                                  \
            break;                                                             \
        }                                                                      \
    }
#include "sqlite_tunables.h"
    else {
        logmsg(LOGMSG_ERROR, "Unknown tunable %s\n", tname);
    }
}
#undef DEF_ATTR
