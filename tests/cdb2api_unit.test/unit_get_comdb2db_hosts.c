#include <limits.h>
#include <stdint.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <stdio.h>
#include <string.h>


#include "sqlresponse.pb-c.h"
#include "sqlquery.pb-c.h"

int global_state; // global 

#define debugprint(fmt, args...)                                               \
    do {                                                                       \
        if (hndl->debug_trace)                                                 \
            fprintf(stderr, "td 0x%u %s:%d " fmt, (uint32_t)pthread_self(),    \
                    __func__, __LINE__, ##args);                               \
    } while (0);

typedef struct cdb2_hndl cdb2_hndl_tp;
#define CDB2HOSTNAME_LEN 128
#define PATH_MAX 1024
static char *CDB2DBCONFIG_BUF = NULL;
static char CDB2DBCONFIG_NOBBENV[512] = "/opt/bb/etc/cdb2/config/comdb2db.cfg";
static char CDB2DBCONFIG_TEMP_BB_BIN[512] = "/bb/bin/comdb2db.cfg";
static char cdb2_default_cluster[64] = "";
static char cdb2_dnssuffix[255] = "";

// forward declare the function we are testing
static int get_comdb2db_hosts(cdb2_hndl_tp *hndl, char comdb2db_hosts[][COMDB2HOSTNAME_LEN],
                              int *comdb2db_ports, int *master,
                              const char *comdb2db_name, int *num_hosts,
                              int *comdb2db_num, const char *dbname,
                              char db_hosts[][COMDB2HOSTNAME_LEN],
                              int *num_db_hosts, int *dbnum, int read_cfg,
                              int dbinfo_or_dns);


// we need here all the functions that get_comdb2db_hosts() calls
static int read_available_comdb2db_configs(
    cdb2_hndl_tp *hndl, char comdb2db_hosts[][COMDB2HOSTNAME_LEN], const char *comdb2db_name,
    int *num_hosts, int *comdb2db_num, const char *dbname, char db_hosts[][COMDB2HOSTNAME_LEN],
    int *num_db_hosts, int *dbnum, int noLock, int defaultOnly)
{
    if (global_state == 1) return -1;

    if (num_hosts) *num_hosts = 0;
    if (num_db_hosts) *num_db_hosts = 0;

    if (global_state == 2) {
        printf("read_available_comdb2db_configs global_state %d\n", global_state);
    }

    if (global_state == 3) {
        printf("read_available_comdb2db_configs global_state %d\n", global_state);
        strcpy(comdb2db_hosts[0], "comdb2db_node1");
        strcpy(comdb2db_hosts[1], "comdb2db_node2");
        strcpy(comdb2db_hosts[2], "comdb2db_node3");
        *num_hosts = 3;
    }

    if (global_state == 4) {
        printf("read_available_comdb2db_configs global_state %d\n", global_state);
        strcpy(db_hosts[0], "node1");
        strcpy(db_hosts[1], "node2");
        strcpy(db_hosts[2], "node3");
        *num_db_hosts = 3;
    }

    if (global_state == 5) {
        printf("read_available_comdb2db_configs global_state %d\n", global_state);
    }

    assert(global_state <= 7);
    return 0;
}


static int cdb2_dbinfo_query(cdb2_hndl_tp *hndl, const char *type, const char *dbname,
                             int dbnum, const char *host, char valid_hosts[][COMDB2HOSTNAME_LEN],
                             int *valid_ports, int *master_node,
                             int *num_valid_hosts,
                             int *num_valid_sameroom_hosts)
{
    assert(global_state != 1);
    assert(global_state != 2);
    assert(global_state != 3);
    assert(global_state != 4);
    if (global_state == 5) {
        printf("cdb2_dbinfo_query correctly called for global_state %d, returning 0\n", global_state);
        strcpy(valid_hosts[0], "comdb2db_node1");
        strcpy(valid_hosts[1], "comdb2db_node2");
        strcpy(valid_hosts[2], "comdb2db_node3");
        *num_valid_hosts = 3;
        return 0;
    }
    if (global_state == 6 || global_state == 7) {
        printf("cdb2_dbinfo_query correctly called for global_state %d, returning -1\n", global_state);
        return -1;
    }

}

static int get_host_by_name(const char *comdb2db_name, 
                            char comdb2db_hosts[][COMDB2HOSTNAME_LEN], int *num_hosts)
{
    assert(global_state != 1);
    assert(global_state != 2);
    assert(global_state != 3);
    assert(global_state != 4);
    assert(global_state != 5);
    if (global_state == 6)
        return -1;
    if (global_state == 7) {
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

    printf("starting test_get_comdb2db_hosts\n");
    
    global_state = 1; // read_available_comdb2db_configs returns -1, nothing gets set

    rc = get_comdb2db_hosts(NULL,NULL, NULL, &master, 
            NULL, &num_hosts, NULL,NULL,
            NULL, &num_db_hosts, NULL, 1, 1);

    assert(rc == -1);
    assert(num_hosts == -1);
    assert(num_db_hosts == -1);
    assert(master == -1);



    global_state = 2; // read_available_comdb2db_configs returns 0, just get defaults

    rc = get_comdb2db_hosts(NULL,NULL, NULL, &master, 
            NULL, &num_hosts, NULL,NULL,
            NULL, &num_db_hosts, NULL, 1, 0); //just get defaults

    assert(rc == 0);
    assert(num_hosts == 0);
    assert(num_db_hosts == 0);
    assert(master == -1);

    global_state = 3; // read_available_comdb2db_configs returns 0 and sets comdb2db_found
               // should populate and check comdb2db_hosts

    {
    char comdb2db_hosts[MAX_NODES][COMDB2HOSTNAME_LEN] = {0};
    char db_hosts[MAX_NODES][COMDB2HOSTNAME_LEN] = {0};
    rc = get_comdb2db_hosts(NULL, comdb2db_hosts, NULL, &master, 
            NULL, &num_hosts, NULL, NULL,
            db_hosts, &num_db_hosts, NULL, 1, 1);

    assert(rc == 0);
    assert(num_db_hosts == 0);
    assert(master == -1);
    assert(num_hosts == 3);
    assert(strcmp(comdb2db_hosts[0], "comdb2db_node1") == 0);
    assert(strcmp(comdb2db_hosts[1], "comdb2db_node2") == 0);
    assert(strcmp(comdb2db_hosts[2], "comdb2db_node3") == 0);
    assert(comdb2db_hosts[3][0] == '\0');
    assert(db_hosts[0][0] == '\0');
    }

    global_state = 4; // read_available_comdb2db_configs returns 0 and sets dbname_found
               // should populate and check db_hosts

    {
    char comdb2db_hosts[MAX_NODES][COMDB2HOSTNAME_LEN] = {0};
    char db_hosts[MAX_NODES][COMDB2HOSTNAME_LEN] = {0};
    rc = get_comdb2db_hosts(NULL, comdb2db_hosts, NULL, &master, 
            NULL, &num_hosts, NULL, NULL,
            db_hosts, &num_db_hosts, NULL, 1, 1);

    assert(rc == 0);
    assert(num_hosts == 0);
    assert(num_db_hosts == 3);
    assert(master == -1);
    assert(strcmp(db_hosts[0], "node1") == 0);
    assert(strcmp(db_hosts[1], "node2") == 0);
    assert(strcmp(db_hosts[2], "node3") == 0);
    assert(db_hosts[3][0] == '\0');
    assert(comdb2db_hosts[0][0] == '\0');
    }

    global_state = 5; // read_available_comdb2db_configs returns 0, will call cdb2_dbinfo_query

    {
    char comdb2db_hosts[MAX_NODES][COMDB2HOSTNAME_LEN] = {0};
    char db_hosts[MAX_NODES][COMDB2HOSTNAME_LEN] = {0};
    int comdb2db_num = 0;
    rc = get_comdb2db_hosts(NULL, comdb2db_hosts, NULL, &master, 
            NULL, &num_hosts, &comdb2db_num, NULL,
            db_hosts, &num_db_hosts, NULL, 1, 1);

    assert(rc == 0);
    assert(num_hosts == 3);
    assert(strcmp(comdb2db_hosts[0], "comdb2db_node1") == 0);
    assert(strcmp(comdb2db_hosts[1], "comdb2db_node2") == 0);
    assert(strcmp(comdb2db_hosts[2], "comdb2db_node3") == 0);
    assert(comdb2db_hosts[3][0] == '\0');
    assert(db_hosts[0][0] == '\0');
    assert(num_db_hosts == 0);
    assert(master == -1);
    }


    global_state = 6; // cdb2_dbinfo_query will return -1, will call get_host_by_name

    {
    char comdb2db_hosts[MAX_NODES][COMDB2HOSTNAME_LEN] = {0};
    char db_hosts[MAX_NODES][COMDB2HOSTNAME_LEN] = {0};
    int comdb2db_num = 0;
    rc = get_comdb2db_hosts(NULL, comdb2db_hosts, NULL, &master, 
            NULL, &num_hosts, &comdb2db_num, NULL,
            db_hosts, &num_db_hosts, NULL, 1, 1);

    assert(rc == -1);
    assert(num_hosts == 0);
    assert(num_db_hosts == 0);
    assert(comdb2db_hosts[0][0] == '\0');
    assert(db_hosts[0][0] == '\0');
    assert(master == -1);
    }

    global_state = 7; // cdb2_dbinfo_query will return -1, will call get_host_by_name

    {
    char comdb2db_hosts[MAX_NODES][COMDB2HOSTNAME_LEN] = {0};
    char db_hosts[MAX_NODES][COMDB2HOSTNAME_LEN] = {0};
    int comdb2db_num = 0;
    rc = get_comdb2db_hosts(NULL, comdb2db_hosts, NULL, &master, 
            NULL, &num_hosts, &comdb2db_num, NULL,
            db_hosts, &num_db_hosts, NULL, 1, 1);

    assert(rc == 0);
    assert(num_hosts == 3);
    assert(strcmp(comdb2db_hosts[0], "comdb2db_node1") == 0);
    assert(strcmp(comdb2db_hosts[1], "comdb2db_node2") == 0);
    assert(strcmp(comdb2db_hosts[2], "comdb2db_node3") == 0);
    assert(comdb2db_hosts[3][0] == '\0');
    assert(db_hosts[0][0] == '\0');
    assert(num_db_hosts == 0);
    assert(master == -1);
    }

    printf("Completed\n");
    return 0;
}
