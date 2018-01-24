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

static int get_config_file(const char *dbname, char *f, size_t s)
{
    if (state == 1) return -1;

    if (state == 2 || state == 3) {
        printf("get_config_file correctly called for state %d, returning 0\n", state);
        sprintf(f, "nonexistent");
        return 0;
    }
}

static void read_comdb2db_cfg(cdb2_hndl_tp *hndl, FILE *fp, char *comdb2db_name,                                                                  
                              const char *buf, char comdb2db_hosts[][64],
                              int *num_hosts, int *comdb2db_num, char *dbname,
                              char db_hosts[][64], int *num_db_hosts,
                              int *dbnum, int *dbname_found,
                              int *comdb2db_found)
{
    assert(state != 1);
    assert(state != 2);
    assert(state != 3);
}

#ifdef __APPLE__
        hp = gethostbyname(tok);
#elif _LINUX_SOURCE
int gethostbyname_r(const char *name,
               struct hostent *ret, char *buf, size_t buflen,
               struct hostent **result, int *h_errnop)
{
    assert(state != 1);
    assert(state != 2);
    assert(state != 3);
}

#elif _SUN_SOURCE
        hp = gethostbyname_r(tok, &hostbuf, tmp, tmplen, &herr);
#else
        hp = gethostbyname(tok);
#endif


static int cdb2_dbinfo_query(cdb2_hndl_tp *hndl, char *type, char *dbname,                                                                        
                             int dbnum, char *host, char valid_hosts[][64],
                             int *valid_ports, int *master_node,
                             int *num_valid_hosts,
                             int *num_valid_sameroom_hosts)
{
    assert(state != 1);
    assert(state != 2);
    if (state == 3) {
        printf("cdb2_dbinfo_query correctly called for state %d, returning 0\n", state);
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
    
    state = 1; // only get_config_file() gets called which returns -1, nothing gets set

    rc = get_comdb2db_hosts(NULL,NULL, NULL, &master, 
            NULL, &num_hosts, NULL,NULL, NULL,
            NULL, &num_db_hosts, NULL, 0);

    assert(rc == -1);
    assert(num_hosts == -1);
    assert(num_db_hosts == -1);
    assert(master == -1);



    state = 2; // get_config_file() returns a filename that does not exist

    rc = get_comdb2db_hosts(NULL,NULL, NULL, &master, 
            NULL, &num_hosts, NULL,NULL, NULL,
            NULL, &num_db_hosts, NULL, 1); //just get defaults

    assert(rc == 0);
    assert(num_hosts == 0);
    assert(num_db_hosts == 0);
    assert(master == -1);

    state = 3; // same as state 2, but get more than defaults

    rc = get_comdb2db_hosts(NULL,NULL, NULL, &master, 
            NULL, &num_hosts, NULL,NULL, NULL,
            NULL, &num_db_hosts, NULL, 0);

    assert(rc == 0);
    assert(num_hosts == 0);
    assert(num_db_hosts == 0);
    assert(master == -1);



    /*
    get_comdb2db_hosts(cdb2_hndl_tp *hndl, char comdb2db_hosts[][64], int *comdb2db_ports, int *master,
                      char *comdb2db_name, int *num_hosts, int *comdb2db_num, char *dbname, char *dbtype,
                      char db_hosts[][64], int *num_db_hosts, int *dbnum, int just_defaults);
                      */



    return 0;
}
