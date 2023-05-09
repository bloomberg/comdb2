#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <cdb2api.h>


void usage(FILE *f, const char *argv0)
{
    fprintf(f, "Usage: %s <options>\n", argv0);
    fprintf(f, " -d <dbname>                - dbname to config\n");
    fprintf(f, " -c <dbname_config>         - config to reach dbname\n");
    fprintf(f, " -s <foreign_metadb_config> - configuration info for server cdb2api\n");

    exit(1);
}

int main (int argc, char *argv[])
{
    char *dbname = NULL;
    char *config = NULL;
    char *metadb_config = NULL;
    const char *argv0 = argv[0];
    int opt, rc;


    while ((opt = getopt(argc, argv, "d:c:s:")) != EOF)
    {
        switch (opt)
        {
            case 'd':
                dbname = optarg;
                break;
            case 'c':
                config = optarg;
                break;
            case 's':
                metadb_config = optarg;
                break;
            default:
                usage(stderr, argv0);
        }
    }
    if (!dbname || !config || !metadb_config)
        usage(stderr, argv0);
    

    cdb2_hndl_tp *hndl = NULL;

    cdb2_set_comdb2db_config(config);

    rc = cdb2_open(&hndl, dbname, "default", 0);
    if (rc) 
    {
        fprintf(stderr, "Failed to open db '%s' rc %d\n", dbname, rc);
        usage(stderr, argv0);
    }

    char sql[10000];
    snprintf(sql, sizeof(sql), "PUT TuNABLE foreign_metadb_config \'%s\'",
             metadb_config);

    rc = cdb2_run_statement(hndl, sql);
    if (rc)
    {
        fprintf(stderr, "Failed to put tunable foreign_metadb_config to '%s'\n",
                metadb_config);
        usage(stderr, argv0);
    }


    cdb2_close(hndl);
}
