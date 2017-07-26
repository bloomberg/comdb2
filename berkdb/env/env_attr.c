#include "limit_fortify.h"
#include "db_config.h"

#include "db_int.h"
#include "dbinc/crypto.h"
#include "dbinc/hmac.h"
#include "dbinc/db_shash.h"
#include "dbinc/db_page.h"
#include "dbinc/db_am.h"
#include "dbinc/lock.h"
#include "dbinc/log.h"
#include "dbinc/mp.h"
#include "dbinc/txn.h"

#include <string.h>

#include "dbinc/db_attr_types.h"

#include "logmsg.h"

#define BERK_DEF_ATTR

int __dbenv_setattr(DB_ENV *dbenv, char *attr, char *val, int ival)
{
#undef BERK_DEF_ATTR
#define BERK_DEF_ATTR(option, description, type, default_value)                \
    else if (strcmp(#option, attr) == 0)                                       \
    {                                                                          \
        switch (type) {                                                        \
        case BERK_ATTR_TYPE_INTEGER:                                           \
        case BERK_ATTR_TYPE_BOOLEAN: dbenv->attr.option = ival; break;         \
        case BERK_ATTR_TYPE_PERCENT:                                           \
            if (ival < 0 || ival > 100) {                                      \
                logmsg(LOGMSG_ERROR, "Percent value not between 0 and 100\n"); \
                return 1;                                                      \
            } else                                                             \
                dbenv->attr.option = ival;                                     \
            break;                                                             \
        }                                                                      \
    }

    if (strcmp(attr, "iomap") == 0) {
        dbenv->attr.iomap_enabled = ival;
        if (ival == 0 && dbenv->iomap) {
            dbenv->iomap->memptrickle_active = 0;
        }
    }
#include "dbinc/db_attr.h"
    else
        return 1;
    return 0;
}

int __dbenv_getattr __P((DB_ENV * dbenv, char *attr, char **val, int *ival))
{
#undef BERK_DEF_ATTR
#define BERK_DEF_ATTR(option, description, type, default_value)                \
    else if (strcmp(#option, attr) == 0)                                       \
    {                                                                          \
        *ival = dbenv->attr.option;                                            \
    }

    if (0)
        ;
#include "dbinc/db_attr.h"
    else
        return 1;

    return 0;
}

int __dbenv_dumpattrs __P((DB_ENV * dbenv, FILE *out))
{
    logmsgf(LOGMSG_USER, out, "%-60s: %-10s\n", "iomapfile",
            dbenv->attr.iomapfile ? dbenv->attr.iomapfile : "not set");
#undef BERK_DEF_ATTR
#define BERK_DEF_ATTR(option, description, type, default_value)                \
    if (type == BERK_ATTR_TYPE_INTEGER || type == BERK_ATTR_TYPE_PERCENT)      \
        logmsgf(LOGMSG_USER, out, "%-60s: %-10d (%s)\n", description,          \
                dbenv->attr.option, #option);                                  \
    else if (type == BERK_ATTR_TYPE_BOOLEAN)                                   \
        logmsgf(LOGMSG_USER, out, "%-60s: %-10s (%s)\n", description,          \
                dbenv->attr.option ? "enabled" : "disabled", #option);
#include "dbinc/db_attr.h"

    return 0;
}

static inline comdb2_tunable_type berkdb_to_tunable_type(int type)
{
    switch (type) {
    case BERK_ATTR_TYPE_INTEGER:
    case BERK_ATTR_TYPE_PERCENT: return TUNABLE_INTEGER;
    case BERK_ATTR_TYPE_BOOLEAN: return TUNABLE_BOOLEAN;
    default: assert(0);
    }
}

void __dbenv_attr_init __P((DB_ENV * dbenv))
{
    memset(&dbenv->attr, 0, sizeof(DBENV_ATTR));

#undef BERK_DEF_ATTR
#define BERK_DEF_ATTR(option, description, type, default_value)                \
    dbenv->attr.option = default_value;                                        \
    REGISTER_TUNABLE(#option, description, berkdb_to_tunable_type(type),       \
                     &dbenv->attr.option, 0, NULL, NULL, NULL, NULL);

#include "dbinc/db_attr.h"
}
