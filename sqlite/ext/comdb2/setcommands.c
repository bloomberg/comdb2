#include <stddef.h>
#include <pthread.h>

#include "list.h"
#include "sql.h"

#include "comdb2systblInt.h"
#include "ezsystables.h"

#include "settings.h"

struct setcmd_table_ent {
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

    cmds = (struct setcmd_table_ent *)malloc(count * sizeof(struct setcmd_table_ent));

    db_clnt_setting_t *lst;
    int i = 0;
    LISTC_FOR_EACH(&settings, lst, lnk)
    {
        if ((lst->desc) && !(lst->flag & SETFLAG_INTERNAL) && !(lst->flag & SETFLAG_WRITEONLY)) {
            // TODO: get the actual query used to set this command
            cmds[i].command = lst->desc;
            char value[100] = {0};
            get_value(clnt, lst, value, 100);
            cmds[i].value = strndup(value, 100);
            ++i;
        }
    }

    *data = cmds;
    *npoints = i;

    return rc;
}

void free_set_commands(void *p, int n)
{
    for (int i = 0; i < n; i++) {
        free(((struct setcmd_table_ent *)p)[i].value);
    }
    // free(p);
}

sqlite3_module systblSetCommandsModule = {
    .access_flag = CDB2_ALLOW_ALL,
};

int systblSetCommandsModuleInit(sqlite3 *db)
{
    return create_system_table(db, "comdb2_set_commands", &systblSetCommandsModule, populate_set_commands,
                               free_set_commands, sizeof(struct setcmd_table_ent), CDB2_CSTRING, "command", -1,
                               offsetof(struct setcmd_table_ent, command), CDB2_CSTRING, "args", -1,
                               offsetof(struct setcmd_table_ent, value), SYSTABLE_END_OF_FIELDS);
}
