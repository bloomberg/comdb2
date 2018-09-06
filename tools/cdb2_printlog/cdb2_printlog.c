/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996-2003
 *	Sleepycat Software.  All rights reserved.
 */

#include "build/db_config.h"

#ifndef lint
static const char copyright[] =
    "Copyright (c) 1996-2003\nSleepycat Software Inc.  All rights reserved.\n";
static const char revid[] =
    "$Id: db_printlog.c,v 11.59 2003/08/18 18:00:31 ubell Exp $";
#endif

#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <ctype.h>
#include <sys/types.h>
#include <mem.h>
#include <sys/stat.h>

/*#include "list.h"*/
#include "build/db.h"
#include "build/db_int.h"
#include "dbinc/db_page.h"

#if (! _SUN_SOURCE )
#include "dbinc/btree.h"
#else
int

__bam_init_print(DB_ENV *dbenv, int (***dtabp) (DB_ENV *, DBT *, DB_LSN *,
	db_recops, void *), size_t * dtabsizep);
#endif

#include "dbinc/fop.h"
#include "dbinc/hash.h"
#include "dbinc/log.h"
#include "dbinc/qam.h"
#include "dbinc/txn.h"

#include "llog_auto.h"
#include "llog_ext.h"

#include "bdb_int.h"
#include <crc32c.h>
#include "util.h"

int main __P((int, char *[]));
int cdb2_printlog_usage __P((void));
int cdb2_print_version_check __P((const char *));
int open_rep_db __P((DB_ENV *, DB **, DBC **));
extern int bdb_apprec(DB_ENV *dbenv, DBT *log_rec, DB_LSN *lsn, db_recops op);
extern char printlog_endline;

extern pthread_key_t comdb2_open_key;

bdb_state_type *gbl_bdb_state;

extern int comdb2ma_init(size_t init_sz, size_t max_cap);

static char *orig_dir;
#define PRINTLOG_RANGE_DIR "printlog_range"
void remove_tempdir()
{
	if (orig_dir && chdir(orig_dir) == 0) {
		free(orig_dir);
		int dum = system("rm -rf " PRINTLOG_RANGE_DIR);
	}
}

int tool_cdb2_printlog_main(argc, argv)
	int argc;
	char *argv[];
{
	crc32c_init(0);
	extern char *optarg;
	extern int optind;
	const char *progname = "cdb2_printlog";
	int first = 1;
	unsigned int start_file = 0;
	unsigned int start_offset = 0;
	char *p;
	DB *dbp;
	DBC *dbc;
	DB_ENV *dbenv;
	DB_LOGC *logc;
	int (**dtab) __P((DB_ENV *, DBT *, DB_LSN *, db_recops, void *));
	size_t dtabsize;
	DBT data, keydbt;
	DB_LSN key;
	int ch, exitval, nflag, rflag, ret, repflag;
	char *home, passwd[1024], cmd[128];
	bdb_state_type st;
	FILE *crypto;

	comdb2ma_init(0, 0);

	pthread_key_create(&comdb2_open_key, NULL);
	pthread_key_create(&DBG_FREE_CURSOR, NULL);

	if ((ret = cdb2_print_version_check(progname)) != 0)
		return (ret);

	dbenv = NULL;
	dbp = NULL;
	dbc = NULL;
	logc = NULL;

	exitval = nflag = rflag = repflag = 0;
	home = NULL;
	memset(passwd, 0, sizeof(passwd));

	dtabsize = 0;
	dtab = NULL;
	while ((ch = getopt(argc, argv, "h:s:gNP:rVl:")) != EOF)
		switch (ch) {
		case 'h':
			home = optarg;
			break;
		case 'N':
			nflag = 1;
			break;
		case 'g':
			printlog_endline = ';';
			break;
		case 'P':
			crypto = fopen(optarg, "r");
			if (crypto == NULL) {
				fprintf(stderr, "%s fopen(s) %s errno:%d (%s)\n", __func__,
						optarg, errno, strerror(errno));
				exit(1);
			}
			if ((fgets(passwd, sizeof(passwd), crypto)) == NULL) {
				fprintf(stderr, "%s fgets returned NULL -- ferror:%d feof:%d errno:%d (%s)\n",
						__func__, ferror(crypto), feof(crypto), errno, strerror(errno));
				exit(1);
			}
			fclose(crypto);
			break;
		case 's':
			if ((p = strchr(optarg, ':')) == NULL) {
				fprintf(stderr, "%s: invalid lsn.\n", progname);
				return (EXIT_FAILURE);
			}
			*p = '\0';
			start_file = atoi(optarg);
			start_offset = atoi(&p[1]);
			break;
		case 'r':
			rflag = 1;
			break;
		case 'R':
			repflag = 1;
			break;
		case 'V':
			printf("%s\n", db_version(NULL, NULL, NULL));

			return (EXIT_SUCCESS);
		case 'l': {
			/* Printing only a few of the logs. */
			int start, end;
			char *p;
			if ((p = strchr(optarg, '-')) != NULL) {
				*p = '\0';
				start = atoi(optarg);
				end = atoi(&p[1]);
			} else {
				start = end = atoi(optarg);
			}
			if (mkdir(PRINTLOG_RANGE_DIR, S_IRWXU) != 0) {
				fprintf(stderr, "mkdir " PRINTLOG_RANGE_DIR " failed -- %s\n", strerror(errno));
				exit(1);
			}
			orig_dir = getcwd(NULL, 0);
			atexit(remove_tempdir);
			if (chdir(PRINTLOG_RANGE_DIR) != 0) {
				fprintf(stderr, "chdir failed errno:%d %s\n", errno, strerror(errno));
				exit(1);
			}

			/* Copy logfiles into this directory. */
			for (int i = start; i <= end; i++) {
				char fl[PATH_MAX];
				snprintf(fl, sizeof(fl), "../" LFNAME, i);
				if (symlink(fl, fl + 3) != 0) {
					fprintf(stderr, "symlink %s %s failed errno:%d -- %s\n",
					    fl, fl + 3, errno, strerror(errno));
					exit(1);
				}
			}
			break;
		}
		case '?':
		default:
			return (cdb2_printlog_usage());
		}
	argc -= optind;
	argv += optind;

	if (argc > 0)
		return (cdb2_printlog_usage());

	/* Handle possible interruptions. */
	/* __db_util_siginit(); */

	/*
	 * Create an environment object and initialize it for error
	 * reporting.
	 */
	if ((ret = db_env_create(&dbenv, 0)) != 0) {
		fprintf(stderr,
		    "%s: db_env_create: %s\n", progname, db_strerror(ret));
		goto shutdown;
	}

	dbenv->set_errfile(dbenv, stderr);
	dbenv->set_errpfx(dbenv, progname);

	if (nflag) {
		if ((ret = dbenv->set_flags(dbenv, DB_NOLOCKING, 1)) != 0) {
			dbenv->err(dbenv, ret, "set_flags: DB_NOLOCKING");
			goto shutdown;
		}
		if ((ret = dbenv->set_flags(dbenv, DB_NOPANIC, 1)) != 0) {
			dbenv->err(dbenv, ret, "set_flags: DB_NOPANIC");
			goto shutdown;
		}
	}

	if (strlen(passwd) > 0 && (ret = dbenv->set_encrypt(dbenv,
	    passwd, DB_ENCRYPT_AES)) != 0) {
		dbenv->err(dbenv, ret, "set_passwd");
		goto shutdown;
	}

	/*
	 * Set up an app-specific dispatch function so that we can gracefully
	 * handle app-specific log records.
	 */
	if ((ret = dbenv->set_app_dispatch(dbenv, bdb_apprec)) != 0) {
		dbenv->err(dbenv, ret, "app_dispatch");
		goto shutdown;
	}

	st.dir = "./";
	st.attr = calloc(1, sizeof(bdb_attr_type));
	st.attr->snapisol = 1;
	st.dbenv = dbenv;
	st.passed_dbenv_open = 1;

	/* Default to 1. */
	st.ondisk_header = 1;
	pthread_mutex_init(&st.gblcontext_lock, NULL);

	gbl_bdb_state = &st;
	dbenv->app_private = &st;

#if 0
	/*
	 * An environment is required, but as all we're doing is reading log
	 * files, we create one if it doesn't already exist.  If we create
	 * it, create it private so it automatically goes away when we're done.
	 * If we are reading the replication database, do not open the env
	 * with logging, because we don't want to log the opens.
	 */
	if (repflag) {
		if ((ret = dbenv->open(dbenv, home,
		    DB_INIT_MPOOL | DB_USE_ENVIRON, 0)) != 0 &&
		    (ret = dbenv->open(dbenv, home,
		    DB_CREATE | DB_INIT_MPOOL | DB_PRIVATE | DB_USE_ENVIRON, 0))
		    != 0) {
			dbenv->err(dbenv, ret, "open");
			goto shutdown;
		}
	} else if ((ret = dbenv->open(dbenv, home,
	    DB_JOINENV | DB_USE_ENVIRON, 0)) != 0 &&
	    (ret = dbenv->open(dbenv, home,
	    DB_CREATE | DB_INIT_LOG | DB_PRIVATE | DB_USE_ENVIRON, 0)) != 0) {
		dbenv->err(dbenv, ret, "open");
		goto shutdown;
	}
#endif
	int flags = DB_CREATE | DB_INIT_LOG | DB_PRIVATE | DB_USE_ENVIRON;
	if ((ret = dbenv->open(dbenv, home, flags, 0)) != 0) {
		dbenv->err(dbenv, ret, "open");
		goto shutdown;
	}

	/* Initialize print callbacks. */
	if ((ret = __bam_init_print(dbenv, &dtab, &dtabsize)) != 0 ||
	    (ret = __dbreg_init_print(dbenv, &dtab, &dtabsize)) != 0 ||
	    (ret = __crdel_init_print(dbenv, &dtab, &dtabsize)) != 0 ||
	    (ret = __db_init_print(dbenv, &dtab, &dtabsize)) != 0 ||
	    (ret = __fop_init_print(dbenv, &dtab, &dtabsize)) != 0 ||
	    (ret = __qam_init_print(dbenv, &dtab, &dtabsize)) != 0 ||
	    (ret = __ham_init_print(dbenv, &dtab, &dtabsize)) != 0 ||
	    (ret = __txn_init_print(dbenv, &dtab, &dtabsize)) != 0) {
		dbenv->err(dbenv, ret, "callback: initialization");
		goto shutdown;
	}

	/* Allocate a log cursor. */
	if (repflag) {
		if ((ret = open_rep_db(dbenv, &dbp, &dbc)) != 0)
			goto shutdown;
	} else if ((ret = dbenv->log_cursor(dbenv, &logc, 0)) != 0) {
		dbenv->err(dbenv, ret, "DB_ENV->log_cursor");
		goto shutdown;
	}

	memset(&data, 0, sizeof(data));
	memset(&keydbt, 0, sizeof(keydbt));
	for (;;) {
		if (repflag) {
			ret = dbc->c_get(dbc,
			    &keydbt, &data, rflag ? DB_PREV : DB_NEXT);
			if (ret == 0)
				key = ((REP_CONTROL *)keydbt.data)->lsn;
		} else if (first && start_file) {
			key.file = start_file;
			key.offset = start_offset;
			ret = logc->get(logc, &key, &data, DB_SET);
		} else {
			ret = logc->get(logc,
			    &key, &data, rflag ? DB_PREV : DB_NEXT);
		}
		if (ret != 0) {
			if (ret == DB_NOTFOUND)
				break;
			dbenv->err(dbenv,
			    ret, repflag ? "DB_LOGC->get" : "DBC->get");
			goto shutdown;
		}

		ret = __db_dispatch(dbenv,
		    dtab, dtabsize, &data, &key, DB_TXN_PRINT, NULL);

		/*
		 * XXX
		 * Just in case the underlying routines don't flush.
		 */
		(void)fflush(stdout);

		if (ret != 0) {
			dbenv->err(dbenv, ret, "tx: dispatch");
			goto shutdown;
		}
		first = 0;
	}

	if (0) {
shutdown:	exitval = 1;
	}
	if (logc != NULL && (ret = logc->close(logc, 0)) != 0)
		exitval = 1;

	if (dbc != NULL && (ret = dbc->c_close(dbc)) != 0)
		exitval = 1;

	if (dbp != NULL && (ret = dbp->close(dbp, 0)) != 0)
		exitval = 1;

	/*
	 * The dtab is allocated by __db_add_recovery (called by *_init_print)
	 * using the library malloc function (__os_malloc).  It thus needs to be
	 * freed using the corresponding free (__os_free).
	 */
	if (dtab != NULL)
		__os_free(dbenv, dtab);
	if (dbenv != NULL && (ret = dbenv->close(dbenv, 0)) != 0) {
		exitval = 1;
		fprintf(stderr,
		    "%s: dbenv->close: %s\n", progname, db_strerror(ret));
	}

	/* Resend any caught signal. */
	/* __db_util_sigresend(); */

	return (exitval == 0 ? EXIT_SUCCESS : EXIT_FAILURE);
}

int
cdb2_printlog_usage()
{
	fprintf(stderr, "%s\n",
	    "usage: cdb2_printlog [-NrgcV] [-l range ] [-h home] [-P /path/to/passwd]");
	fprintf(stderr, "   -h home             - Set db home.\n");
	fprintf(stderr,
	    "   -N                  - Set no-locking and no-panic.\n");
	fprintf(stderr,
	    "   -g                  - Grep'able output: 1 log per line.\n");
	fprintf(stderr, "   -P /path/to/passwd  - Set password path.\n");
	fprintf(stderr, "   -r                  - Set reverse-flag.\n");
	fprintf(stderr, "   -V                  - Print version & exit.\n");
	fprintf(stderr,
	    "   -l range            - Set range (i.e. 2, or 3-5).\n");
	fprintf(stderr, "   -s file:offset      - Set start lsn.\n");

	return (EXIT_FAILURE);
}

int
cdb2_print_version_check(progname)
	const char *progname;
{
	int v_major, v_minor, v_patch;

	/* Make sure we're loaded with the right version of the DB library. */
	(void)db_version(&v_major, &v_minor, &v_patch);
	if (v_major != DB_VERSION_MAJOR || v_minor != DB_VERSION_MINOR) {
		fprintf(stderr,
	"%s: version %d.%d doesn't match library version %d.%d\n",
		    progname, DB_VERSION_MAJOR, DB_VERSION_MINOR,
		    v_major, v_minor);
		return (EXIT_FAILURE);
	}
	return (0);
}


/* Print an unknown, application-specific log record as best we can. */
int
print_app_record(dbenv, dbt, lsnp, op)
	DB_ENV *dbenv;
	DBT *dbt;
	DB_LSN *lsnp;
	db_recops op;
{
	int ch;
	u_int32_t i, rectype;

	DB_ASSERT(op == DB_TXN_PRINT);

	COMPQUIET(dbenv, NULL);
	COMPQUIET(op, DB_TXN_PRINT);

	/*
	 * Fetch the rectype, which always must be at the beginning of the
	 * record (if dispatching is to work at all).
	 */
	memcpy(&rectype, dbt->data, sizeof(rectype));

	/*
	 * Applications may wish to customize the output here based on the
	 * rectype.  We just print the entire log record in the generic
	 * mixed-hex-and-printable format we use for binary data.
	 */
	printf("[%lu][%lu]application specific record: rec: %lu\n",
	    (u_long)lsnp->file, (u_long)lsnp->offset, (u_long)rectype);
	printf("\tdata: ");
	for (i = 0; i < dbt->size; i++) {
		ch = ((u_int8_t *)dbt->data)[i];
		printf(isprint(ch) || ch == 0x0a ? "%c" : "%#x ", ch);
	}
	printf("\n\n");

	return (0);
}

int
open_rep_db(dbenv, dbpp, dbcp)
	DB_ENV *dbenv;
	DB **dbpp;
	DBC **dbcp;
{
	int ret;

	DB *dbp;
	*dbpp = NULL;
	*dbcp = NULL;

	if ((ret = db_create(dbpp, dbenv, 0)) != 0) {
		dbenv->err(dbenv, ret, "db_create");
		return (ret);
	}

	dbp = *dbpp;
	if ((ret =
	    dbp->open(dbp, NULL, "__db.rep.db", NULL, DB_BTREE, 0, 0)) != 0) {
		dbenv->err(dbenv, ret, "DB->open");
		goto err;
	}

	if ((ret = dbp->cursor(dbp, NULL, dbcp, 0)) != 0) {
		dbenv->err(dbenv, ret, "DB->cursor");
		goto err;
	}

	return (0);

err:	if (*dbpp != NULL)
		(void)(*dbpp)->close(*dbpp, 0);
	return (ret);
}
