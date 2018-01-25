#include <limits.h>
#include <stdint.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>


#include "sqlresponse.pb-c.h"
#include "sqlquery.pb-c.h"

#define WITH_SSL 1

#include "cdb2api_priv.h"


int state; // global 

typedef struct cdb2_hndl cdb2_hndl_tp;
#define PATH_MAX 1024
static char *CDB2DBCONFIG_BUF = NULL;
static char CDB2DBCONFIG_NOBBENV[512] = "/opt/bb/etc/cdb2/config/comdb2db.cfg";
static char CDB2DBCONFIG_TEMP_BB_BIN[512] = "/bb/bin/comdb2db.cfg";
static char cdb2_default_cluster[64] = "";
static char cdb2_dnssuffix[255] = "";

static int get_comdb2db_hosts(cdb2_hndl_tp *hndl, char comdb2db_hosts[][64],
                               int *comdb2db_ports, int *master,
                               char *comdb2db_name, int *num_hosts,
                               int *comdb2db_num, char *dbname, char *dbtype,
                               char db_hosts[][64], int *num_db_hosts,
                               int *dbnum, int just_defaults);

static int read_available_comdb2db_configs(
        cdb2_hndl_tp *hndl, char comdb2db_hosts[][64],
        char *comdb2db_name, int *num_hosts,
        int *comdb2db_num, char *dbname,
        char db_hosts[][64], int *num_db_hosts,
        int *dbnum, int *comdb2db_found, int *dbname_found)
{
    if (state == 1) return -1;

    if (state == 2) {
        printf("read_available_comdb2db_configs state %d, returning 0\n", state);
    }

    if (state == 3) {
        printf("read_available_comdb2db_configs state %d, returning 0\n", state);
        *comdb2db_found = 1;
    }

    if (state == 4) {
        printf("read_available_comdb2db_configs state %d, returning 0\n", state);
        *dbname_found = 1;
    }

    if (state == 5) {
        printf("read_available_comdb2db_configs state %d, returning 0\n", state);
    }

    assert(state <= 7);
    return 0;
}


static int cdb2_dbinfo_query(cdb2_hndl_tp *hndl, char *type, char *dbname,                                                                        
                             int dbnum, char *host, char valid_hosts[][64],
                             int *valid_ports, int *master_node,
                             int *num_valid_hosts,
                             int *num_valid_sameroom_hosts)
{
    assert(state != 1);
    assert(state != 2);
    assert(state != 3);
    assert(state != 4);
    if (state == 5) {
        printf("cdb2_dbinfo_query correctly called for state %d, returning 0\n", state);
        strcpy(valid_hosts[0], "comdb2db_node1");
        strcpy(valid_hosts[1], "comdb2db_node2");
        strcpy(valid_hosts[2], "comdb2db_node3");
        *num_valid_hosts = 3;
        return 0;
    }
    if (state == 6 || state == 7) {
        printf("cdb2_dbinfo_query correctly called for state %d, returning -1\n", state);
        return -1;
    }

}

static int get_host_by_name(char *comdb2db_name, char comdb2db_hosts[][64], int *num_hosts)
{
    assert(state != 1);
    assert(state != 2);
    assert(state != 3);
    assert(state != 4);
    assert(state != 5);
    if (state == 6)
        return -1;
    if (state == 7) {
        strcpy(comdb2db_hosts[0], "comdb2db_node1");
        strcpy(comdb2db_hosts[1], "comdb2db_node2");
        strcpy(comdb2db_hosts[2], "comdb2db_node3");
        *num_hosts = 3;
    return 0;
    }
}


int main()
{
    int rc;

    int num_hosts = -1;
    int num_db_hosts = -1;
    int master = -1;

	printf("starting with state 1\n");
    
    state = 1; // read_available_comdb2db_configs returns -1, nothing gets set

    rc = get_comdb2db_hosts(NULL,NULL, NULL, &master, 
            NULL, &num_hosts, NULL,NULL, NULL,
            NULL, &num_db_hosts, NULL, 0);

    assert(rc == -1);
    assert(num_hosts == -1);
    assert(num_db_hosts == -1);
    assert(master == -1);



    state = 2; // read_available_comdb2db_configs returns 0, just get defaults

    rc = get_comdb2db_hosts(NULL,NULL, NULL, &master, 
            NULL, &num_hosts, NULL,NULL, NULL,
            NULL, &num_db_hosts, NULL, 1); //just get defaults

    assert(rc == 0);
    assert(num_hosts == 0);
    assert(num_db_hosts == 0);
    assert(master == -1);

    state = 3; // read_available_comdb2db_configs returns 0 and sets comdb2db_found
               // should populate and check comdb2db_hosts
    rc = get_comdb2db_hosts(NULL,NULL, NULL, &master, 
            NULL, &num_hosts, NULL, NULL, NULL,
            NULL, &num_db_hosts, NULL, 0);

    assert(rc == 0);
    assert(num_hosts == 0);
    assert(num_db_hosts == 0);
    assert(master == -1);

    state = 4; // read_available_comdb2db_configs returns 0 and sets dbname_found
               // should populate and check db_hosts

    rc = get_comdb2db_hosts(NULL,NULL, NULL, &master, 
            NULL, &num_hosts, NULL, NULL, NULL,
            NULL, &num_db_hosts, NULL, 0);

    assert(rc == 0);
    assert(num_hosts == 0);
    assert(num_db_hosts == 0);
    assert(master == -1);

    state = 5; // read_available_comdb2db_configs returns 0, will call cdb2_dbinfo_query

    {
    char comdb2db_hosts[MAX_NODES][64] = {0};
    char db_hosts[MAX_NODES][64];
    int comdb2db_num = 0;
    rc = get_comdb2db_hosts(NULL, comdb2db_hosts, NULL, &master, 
            NULL, &num_hosts, &comdb2db_num, NULL, NULL,
            db_hosts, &num_db_hosts, NULL, 0);

    assert(rc == 0);
    assert(num_hosts == 3);
    assert(strcmp(comdb2db_hosts[0], "comdb2db_node1") == 0);
    assert(strcmp(comdb2db_hosts[1], "comdb2db_node2") == 0);
    assert(strcmp(comdb2db_hosts[2], "comdb2db_node3") == 0);
    assert(comdb2db_hosts[3][0] == '\0');
    assert(num_db_hosts == 0);
    assert(master == -1);
    }


    state = 6; // cdb2_dbinfo_query will return -1, will call get_host_by_name

    {
    char comdb2db_hosts[MAX_NODES][64] = {0};
    char db_hosts[MAX_NODES][64];
    int comdb2db_num = 0;
    rc = get_comdb2db_hosts(NULL, comdb2db_hosts, NULL, &master, 
            NULL, &num_hosts, &comdb2db_num, NULL, NULL,
            db_hosts, &num_db_hosts, NULL, 0);

    assert(rc == -1);
    assert(num_hosts == 0);
    assert(num_db_hosts == 0);
    assert(master == -1);
    }

    state = 7; // cdb2_dbinfo_query will return -1, will call get_host_by_name

    {
    char comdb2db_hosts[MAX_NODES][64] = {0};
    char db_hosts[MAX_NODES][64];
    int comdb2db_num = 0;
    rc = get_comdb2db_hosts(NULL, comdb2db_hosts, NULL, &master, 
            NULL, &num_hosts, &comdb2db_num, NULL, NULL,
            db_hosts, &num_db_hosts, NULL, 0);

    assert(rc == 0);
    assert(num_hosts == 3);
    assert(strcmp(comdb2db_hosts[0], "comdb2db_node1") == 0);
    assert(strcmp(comdb2db_hosts[1], "comdb2db_node2") == 0);
    assert(strcmp(comdb2db_hosts[2], "comdb2db_node3") == 0);
    assert(comdb2db_hosts[3][0] == '\0');
    assert(num_db_hosts == 0);
    assert(master == -1);
    }


    /*
    get_comdb2db_hosts(cdb2_hndl_tp *hndl, char comdb2db_hosts[][64], int *comdb2db_ports, int *master,
                      char *comdb2db_name, int *num_hosts, int *comdb2db_num, char *dbname, char *dbtype,
                      char db_hosts[][64], int *num_db_hosts, int *dbnum, int just_defaults);
                      */



    return 0;
}
