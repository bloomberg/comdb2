/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996-2003
 *	Sleepycat Software.  All rights reserved.
 */

#include "db_config.h"

#ifndef lint
static const char revid[] = "$Id: os_region.c,v 11.17 2003/07/13 17:45:23 bostic Exp $";
#endif /* not lint */

#ifndef NO_SYSTEM_INCLUDES
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/types.h>
#endif

#include "db_int.h"
#include <cdb2_constants.h>
#include "logmsg.h"

extern char gbl_dbname[MAX_DBNAME_LENGTH];
extern int gbl_largepages;


struct region {
	int maxsize;
	int used;
};

char *
__dbenv_regiontype(int type)
{
	switch (type) {
	case REGION_TYPE_ENV:
		return "env";
	case REGION_TYPE_LOCK:
		return "lock";
	case REGION_TYPE_LOG:
		return "log";
	case REGION_TYPE_MPOOL:
		return "mpool";
	case REGION_TYPE_MUTEX:
		return "mutex";
	case REGION_TYPE_TXN:
		return "txn";
	default:
		return "unknown";
	}
}


/*
  large page support:

  linux only for now.  lrl option "largepages" enables.
  
  to enable large pages and set it up to work with comdb2, do the
  following as root:

       mkdir -p /mnt/hugetlbfs
       mount -t hugetlbfs none  /mnt/hugetlbfs
       chmod 777 /mnt/hugetlbfs               
       echo 21480 >>  /proc/sys/vm/nr_hugepages
       cat /proc/meminfo |grep Huge
       edit /etc/security/limits.conf to contain a line for "op" like
       op              -       memlock         unlimited

  all of these things can be done live and do not require reboot.

  large pages are 2 megs on our systems.  this code assumes that to be true.
  you can verify the large page size by running 
  cat /proc/meminfo |grep Hugepagesize
 
  you tell the system how many of them you want with
  the echo command.  after the echo, run cat /proc/sys/vm/nr_hugepages
  to see if it worked.  it may take multiple tries based on fragmentaton.
  once the memory gets put into the large page pool, it will dissapear from
  rpm.  it will be visible at /proc/meminfo/HugePages_Free

  we name the files in /mnt/hugetlbfs as <dbname>.regionnumber
  if a file is left around, the memory from that db WILL NOT be returned
  to the system.  rm /mnt/hugetlbfs/<dbname>.* (if the db isnt running) 
  will return the memory.
*/

/*
 * __os_r_attach --
 *	Attach to a shared memory region.
 *
 * PUBLIC: int __os_r_attach __P((DB_ENV *, REGINFO *, REGION *));
 */
int
__os_r_attach(dbenv, infop, rp)
	DB_ENV *dbenv;
	REGINFO *infop;
	REGION *rp;
{
	int ret;
	size_t MB_2 = 2 * 1024 * 1024UL;

	/* Round off the requested size for the underlying VM. */
	OS_VMROUNDOFF(rp->size);

	infop->fd = -1;

	/* memory regions are separately tracked. To avoid
	   double-counting on "berkdb" allocator, system malloc is
	   used to create the region. */
	dbenv->set_use_sys_malloc(dbenv, 1);

	if (!gbl_largepages || rp->size < MB_2) {
        if (rp->size != 0)
            ret = __os_calloc(dbenv, 1, rp->size, &infop->addr);
        else {
            infop->addr = NULL;
            ret = 0;
        }
	} else {
		char name[MAXPATHLEN];
		snprintf(name, sizeof(name) - 1, "/mnt/hugetlbfs/%s.%u",
			 gbl_dbname, infop->id);
		infop->fd = open(name, O_CREAT | O_RDWR | O_TRUNC, 0755);
		if (infop->fd < 0) {
			logmsgperror("open hugepage");
			exit(1);
		}
		size_t less = rp->size % MB_2;
		if (less) {
			logmsg(LOGMSG_INFO, "os_r_attach: increasing size from %zu ",
				rp->size);
			rp->size += (MB_2 - less);
			logmsg(LOGMSG_INFO, "to %zu\n", rp->size);
		}
		infop->addr =
			mmap(NULL, rp->size, PROT_READ | PROT_WRITE, MAP_SHARED,
			     infop->fd, 0);
		if (infop->addr == MAP_FAILED) {
			logmsgperror("mmap");
			close(infop->fd);
			unlink(name);
			exit(1);
		}
		logmsg(LOGMSG_INFO, "os_r_attach: mmaped %s (size: %zu) at %p\n",
			name, rp->size, infop->addr);
		ret = 0;
	}

	/* finish creating memory region. set allocator back to "berkdb" */
	dbenv->set_use_sys_malloc(dbenv, 0);
	return (ret);

}

/*
 * __os_r_detach --
 *	Detach from a shared memory region.
 *
 * PUBLIC: int __os_r_detach __P((DB_ENV *, REGINFO *, int));
 */
int
__os_r_detach(dbenv, infop, destroy)
	DB_ENV *dbenv;
	REGINFO *infop;
	int destroy;
{
	REGION *rp;

	rp = infop->rp;

	dbenv->set_use_sys_malloc(dbenv, 1);

	if (infop->fd < 0 && infop->addr) {
		__os_free(dbenv, infop->addr);
	} else {
		char name[MAXPATHLEN];

		snprintf(name, sizeof(name) - 1, "/mnt/hugetlbfs/%s.%u",
		    gbl_dbname, infop->id);
        if (rp->size)
            munmap(infop->addr, rp->size);
		close(infop->fd);
		unlink(name);
	}

	dbenv->set_use_sys_malloc(dbenv, 0);

	return (0);

}
