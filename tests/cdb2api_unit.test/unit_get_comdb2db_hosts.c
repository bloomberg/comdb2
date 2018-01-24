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


}

static void read_comdb2db_cfg(cdb2_hndl_tp *hndl, FILE *fp, char *comdb2db_name,                                                                  
                              const char *buf, char comdb2db_hosts[][64],
                              int *num_hosts, int *comdb2db_num, char *dbname,
                              char db_hosts[][64], int *num_db_hosts,
                              int *dbnum, int *dbname_found,
                              int *comdb2db_found)
{

}

#ifdef __APPLE__
        hp = gethostbyname(tok);
#elif _LINUX_SOURCE
int gethostbyname_r(const char *name,
               struct hostent *ret, char *buf, size_t buflen,
               struct hostent **result, int *h_errnop)
{

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


}


int main()
{
	printf("hello");
    return 0;
}
