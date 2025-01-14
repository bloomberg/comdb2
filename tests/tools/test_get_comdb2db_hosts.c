#undef NDEBUG
#include <arpa/inet.h>
#include <assert.h>
#include <limits.h>
#include <netdb.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>

#include <cdb2api.c>

int main()
{
    int rc;

    int num_hosts = -1;
    int num_db_hosts = -1;
    int master = -1;

    printf("starting test_get_comdb2db_hosts\n");

    rc = get_comdb2db_hosts(NULL,NULL, NULL, &master,
            NULL, &num_hosts, NULL,NULL,
            NULL, &num_db_hosts, NULL, 1);

    assert(rc == -1);
    assert(num_hosts == -1);
    assert(num_db_hosts == -1);
    assert(master == -1);


    rc = get_comdb2db_hosts(NULL,NULL, NULL, &master,
            NULL, &num_hosts, NULL,NULL,
            NULL, &num_db_hosts, NULL, 1); //just get defaults

    assert(rc == 0);
    assert(num_hosts == 0);
    assert(num_db_hosts == 0);
    assert(master == -1);


    {
    char comdb2db_hosts[MAX_NODES][CDB2HOSTNAME_LEN] = {0};
    char db_hosts[MAX_NODES][CDB2HOSTNAME_LEN] = {0};
    rc = get_comdb2db_hosts(NULL, comdb2db_hosts, NULL, &master,
            NULL, &num_hosts, NULL, NULL,
            db_hosts, &num_db_hosts, NULL, 1);

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


    {
    char comdb2db_hosts[MAX_NODES][CDB2HOSTNAME_LEN] = {0};
    char db_hosts[MAX_NODES][CDB2HOSTNAME_LEN] = {0};
    rc = get_comdb2db_hosts(NULL, comdb2db_hosts, NULL, &master,
            NULL, &num_hosts, NULL, NULL,
            db_hosts, &num_db_hosts, NULL, 1);

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


    {
    char comdb2db_hosts[MAX_NODES][CDB2HOSTNAME_LEN] = {0};
    char db_hosts[MAX_NODES][CDB2HOSTNAME_LEN] = {0};
    int comdb2db_num = 0;
    rc = get_comdb2db_hosts(NULL, comdb2db_hosts, NULL, &master,
            NULL, &num_hosts, &comdb2db_num, NULL,
            db_hosts, &num_db_hosts, NULL, 1);

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


    {
    char comdb2db_hosts[MAX_NODES][CDB2HOSTNAME_LEN] = {0};
    char db_hosts[MAX_NODES][CDB2HOSTNAME_LEN] = {0};
    int comdb2db_num = 0;
    rc = get_comdb2db_hosts(NULL, comdb2db_hosts, NULL, &master,
            NULL, &num_hosts, &comdb2db_num, NULL,
            db_hosts, &num_db_hosts, NULL, 1);

    assert(rc == -1);
    assert(num_hosts == 0);
    assert(num_db_hosts == 0);
    assert(comdb2db_hosts[0][0] == '\0');
    assert(db_hosts[0][0] == '\0');
    assert(master == -1);
    }


    {
    char comdb2db_hosts[MAX_NODES][CDB2HOSTNAME_LEN] = {0};
    char db_hosts[MAX_NODES][CDB2HOSTNAME_LEN] = {0};
    int comdb2db_num = 0;
    rc = get_comdb2db_hosts(NULL, comdb2db_hosts, NULL, &master,
            NULL, &num_hosts, &comdb2db_num, NULL,
            db_hosts, &num_db_hosts, NULL, 1);

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
