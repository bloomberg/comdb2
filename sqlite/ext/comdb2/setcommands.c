#include <stddef.h>
#include <pthread.h>

#include "list.h"
#include "sql.h"

#include "comdb2systblInt.h"
#include "ezsystables.h"

#include "settings.h"

struct setcmd_table_ent {
    char *query;
    char *command;
    char *value;
};

int populate_set_commands(void **data, int *npoints)
{

    struct sql_thread *thd = pthread_getspecific(query_info_key);
    struct sqlclntstate *clnt = thd->clnt;
    struct setcmd_table_ent *cmds = NULL;
    size_t count = listc_size(&settings);
    int rc = 0;

    //    rc = clnt->plugin.get_set_commands(clnt, (void ***)&commands, &count);
    //    if (rc || (count == 0))
    //        return rc;

    cmds = (struct setcmd_table_ent *)malloc(count * sizeof(struct setcmd_table_ent));

    db_clnt_setting_t *lst;
    int i = 0;
    char def[8] = "default";
    LISTC_FOR_EACH(&settings, lst, lnk)
    {
        if (lst->desc) {
            // TODO: get the actual query used to set this command
            cmds[i].query = strndup(def, 8);
            cmds[i].command = lst->desc;
            void *value = get_value(lst, clnt);

            char out[100] = {0};
            if (lst->type == SETTING_STRING) {
                snprintf(out, 100, "%s", (char*)value);
            }
            else if (lst->type == SETTING_INTEGER) {
                snprintf(out, 100, "%d", *(int*)value);
            } else if (lst->type == SETTING_DOUBLE) {
                snprintf(out, 100, "%f", *(float*)value);
            }
            cmds[i].value = strndup(out, 100);
        }
        ++i;
    }

    *data = cmds;
    *npoints = i;

    return rc;
}

void free_set_commands(void *p, int n)
{
    for (int i = 0; i < n; i++) {
        free(((struct setcmd_table_ent *)p)[i].value);
        free(((struct setcmd_table_ent *)p)[i].query);
    }
    // free(p);
}

sqlite3_module systblSetCommandsModule = {
    .access_flag = CDB2_ALLOW_ALL,
};

int systblSetCommandsModuleInit(sqlite3 *db)
{
    return create_system_table(db, "comdb2_set_commands", &systblSetCommandsModule, populate_set_commands,
                               free_set_commands, sizeof(struct setcmd_table_ent), CDB2_CSTRING, "query", -1,
                               offsetof(struct setcmd_table_ent, query), CDB2_CSTRING, "command", -1,
                               offsetof(struct setcmd_table_ent, command), CDB2_CSTRING, "args", -1,
                               offsetof(struct setcmd_table_ent, value), SYSTABLE_END_OF_FIELDS);
}
