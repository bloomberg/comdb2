/*
   Copyright 2026 Bloomberg Finance L.P.

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

#include <stdlib.h>
#include <string.h>
#include <stddef.h>

#include "comdb2.h"
#include "comdb2systblInt.h"
#include "sql.h"
#include "ezsystables.h"
#include "cdb2api.h"
#include "comdb2_ruleset.h"
#include "tohex.h"

struct rule {
    int64_t ruleid;
    char *action;
    char *pool;

    char *fingerprint;
    char *host;
    char *user;
    char *task;
    char *sql;

    char *flags;
    char *mode;
    int64_t hits;
};

sqlite3_module systblRulesetsModule = {
    .access_flag = CDB2_ALLOW_ALL,
};

extern struct ruleset *gbl_ruleset;

static char *strdup_maybe(const char *s) {
    return s ? strdup(s) : NULL;
}

static int gather_rules(void **data, int *npoints) {
    if (gbl_ruleset == NULL) {
        *npoints = 0;
        return 0;
    }
    struct rule *rules = malloc(sizeof(struct rule) * gbl_ruleset->nRule);
    for (int i = 0; i < gbl_ruleset->nRule; i++) {
        struct ruleset_item *ri = &gbl_ruleset->aRule[i];
        struct rule *r = &rules[i];
        r->ruleid = ri->ruleNo;
        char s[100] = {0};
        if (ri->action == RULESET_A_INVALID  || ri->action == RULESET_A_NONE) {
            strcat(s, "|none");
        }
        if (ri->action & RULESET_A_REJECT) {
            strcat(s, "|reject");
        }
        if (ri->action & RULESET_A_REJECT_ALL) {
            strcat(s, "|reject_all");
        }
        if (ri->action & RULESET_A_UNREJECT) {
            strcat(s, "|unreject");
        }
        if (ri->action & RULESET_A_SET_POOL) {
            strcat(s, "|set_pool");
        }
        r->action = strdup(s[0] == '|' ? &s[1] : s);
        r->pool = strdup_maybe(ri->zPool);
        r->fingerprint = NULL;
        if (ri->criteria.pFingerprint) {
            char fp[FPSZ*2+1];
            util_tohex(fp, (char*) ri->criteria.pFingerprint, FPSZ);
            r->fingerprint = strdup(fp);
        }
        r->host = strdup_maybe(ri->criteria.zOriginHost);
        r->user = strdup_maybe(ri->criteria.zUser);
        r->task = strdup_maybe(ri->criteria.zOriginTask);
        r->sql = strdup_maybe(ri->criteria.zSql);
        s[0] = 0;
        if (ri->flags == RULESET_F_INVALID || ri->flags == RULESET_F_NONE)
            strcat(s, "|none");
        if (ri->flags & RULESET_F_DISABLE) {
            strcat(s, "|disabled");
        }
        if (ri->flags & RULESET_F_PRINT) {
            strcat(s, "|print");
        }
        if (ri->flags & RULESET_F_STOP) {
            strcat(s, "|stop");
        }
        if (ri->flags & RULESET_F_DYN_POOL) {
            strcat(s, "|dynamic_pool");
        }
        r->flags = strdup(s[0] == '|' ? &s[1] : s);
        s[0] = 0;
        if (ri->mode == RULESET_MM_INVALID || ri->mode == RULESET_MM_NONE) {
            strcat(s, "|none");
        }
        if (ri->mode & RULESET_MM_EXACT) {
            strcat(s, "|exact");
        }
        if (ri->mode & RULESET_MM_GLOB) {
            strcat(s, "|glob");
        }
        if (ri->mode & RULESET_MM_REGEXP) {
            strcat(s, "|regex");
        }
        if (ri->mode & RULESET_MM_NOCASE) {
            strcat(s, "|nocase");
        }
        r->mode = strdup(s[0] == '|' ? &s[1] : s);
        r->hits = ri->matchCount;
    }
    *data = rules;
    *npoints = gbl_ruleset->nRule;

    return 0;
}

void release_rules(void *data, int npoints) {
    struct rule *rules = (struct rule*) data;
    struct rule *r;
    for (int i = 0; i < npoints; i++) {
        r = &rules[i];
        free(r->action);
        free(r->pool);
        free(r->fingerprint);
        free(r->host);
        free(r->user);
        free(r->task);
        free(r->sql);
        free(r->flags);
        free(r->mode);
    }
    free(rules);
}


int systblRulesetsInit(sqlite3 *db) {
    return create_system_table(db, "comdb2_ruleset",
            &systblRulesetsModule, gather_rules, release_rules,
            sizeof(struct rule),
            CDB2_INTEGER, "ruleid", -1, offsetof(struct rule, ruleid),
            CDB2_CSTRING, "action", -1, offsetof(struct rule, action),
            CDB2_CSTRING, "flags", -1, offsetof(struct rule, flags),
            CDB2_CSTRING, "mode", -1, offsetof(struct rule, mode),
            CDB2_INTEGER, "hits", -1, offsetof(struct rule, hits),
            CDB2_CSTRING, "pool", -1, offsetof(struct rule, pool),
            CDB2_CSTRING, "fingerprint", -1, offsetof(struct rule, fingerprint),
            CDB2_CSTRING, "host", -1, offsetof(struct rule, host),
            CDB2_CSTRING, "user", -1, offsetof(struct rule, user),
            CDB2_CSTRING, "task", -1, offsetof(struct rule, task),
            CDB2_CSTRING, "sql", -1, offsetof(struct rule, sql),
            SYSTABLE_END_OF_FIELDS);
}
