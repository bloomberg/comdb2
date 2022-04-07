#include <stddef.h>
#include <pthread.h>

#include "sql.h"

#include "comdb2systblInt.h"
#include "ezsystables.h"

struct setcommands {
    char *query;
    char* command;
    char * args;
};

int populate_set_commands(void **data, int *npoints)
{

    struct sql_thread *thd = pthread_getspecific(query_info_key);
    struct sqlclntstate *clnt = thd->clnt;
    struct setcommands *cmds = NULL;
    char **commands = NULL;
    size_t count = 0;
    int rc = 0;

    rc = clnt->plugin.get_set_commands(clnt, (void ***)&commands, &count);
    if (rc || (count == 0))
        return rc;

    cmds = (struct setcommands *)malloc(count * sizeof(struct setcommands));

    for (int i = 0; i < count; i++) {
        // argv[3] = {'SET', COMM, ARGS};
        char **ap, *argv[3];
        int cmd_len = 0;

        cmds[i].query = strdup(commands[i]);
        char *temp = strdup(commands[i]);

        for (ap = argv; (*ap = strsep(&temp, " \t")) != NULL;)
            if (**ap != '\0') {
                ++cmd_len;
                if (++ap >= &argv[3])
                    break;
            }

        cmds[i].command = (cmd_len >= 2) ? argv[1] : "";
        cmds[i].args = ((cmd_len >= 3) && (strncasecmp(argv[1], "password", 9) == 0)) ? "***" : argv[2];
        // free the "SET" part of the command right here
        free(temp);
    }

    *data = cmds;
    *npoints = count;

    return rc;
}

void free_set_commands(void *p, int n)
{
    for (int i = 0; i < n; i++)
        free(((struct setcommands *)p)[i].query);
    free(p);
}

sqlite3_module systblSetCommandsModule = {
    .access_flag = CDB2_ALLOW_ALL,
};

int systblSetCommandsModuleInit(sqlite3 *db)
{
    return create_system_table(db, "comdb2_set_commands", &systblSetCommandsModule, populate_set_commands,
                               free_set_commands, sizeof(struct setcommands), CDB2_CSTRING, "query", -1,
                               offsetof(struct setcommands, query), CDB2_CSTRING, "command", -1,
                               offsetof(struct setcommands, command), CDB2_CSTRING, "args", -1,
                               offsetof(struct setcommands, args), SYSTABLE_END_OF_FIELDS);
}
