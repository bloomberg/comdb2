/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996-2003
 *	Sleepycat Software.  All rights reserved.
 */

#include "db_config.h"

#ifndef lint
static const char copyright[] =
    "Copyright (c) 1996-2003\nSleepycat Software Inc.  All rights reserved.\n";
static const char revid[] =
    "$Id: db_dump.c,v 11.88 2003/08/13 19:57:06 ubell Exp $";
#endif

#ifndef NO_SYSTEM_INCLUDES
#include <sys/types.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#endif

#include "db_int.h"
#include "dbinc/db_page.h"
#include "dbinc/db_am.h"

#include <crc32c.h>
#include <syslog.h>
#include <logmsg.h>

int db_init __P((DB_ENV *, char *, int, u_int32_t, int *));
int dump __P((DB *, int, int));
int dump_sub __P((DB_ENV *, DB *, char *, int, int));
int is_sub __P((DB *, int *));
int main __P((int, char *[]));
int show_subs __P((DB *));
static int cdb2_dump_usage __P((void));
int version_check __P((const char *));
extern int comdb2ma_init(size_t init_sz, size_t max_cap);
extern int io_override_init(void);
extern int io_override_set_std(FILE *f);
pthread_key_t comdb2_open_key;

int
tool_cdb2_dump_main(argc, argv)
	int argc;
	char *argv[];
{
	crc32c_init(0);
	comdb2ma_init(0, 0);
	extern char *optarg;
	extern int optind;
	const char *progname = "cdb2_dump";
	DB_ENV *dbenv;
	DB *dbp;
	u_int32_t cache;
	int ch;
	int exitval, keyflag, lflag, nflag, pflag, private;
	int ret, Rflag, rflag, resize, subs;
	char *dopt, *home, passwd[1024], *subname;
	FILE *crypto;

	pthread_key_create(&comdb2_open_key, NULL);

	if ((ret = version_check(progname)) != 0)
		return (ret);

	dbenv = NULL;
	dbp = NULL;
	exitval = lflag = nflag = pflag = rflag = Rflag = 0;
	keyflag = 0;
	cache = MEGABYTE;
	private = 0;
	dopt = home = subname = NULL;
	memset(passwd, 0, sizeof(passwd));
	while ((ch = getopt(argc, argv, "d:f:h:klNpP:rRs:V")) != EOF)
		switch (ch) {
		case 'd':
			dopt = optarg;
			break;
		case 'f':
			if (freopen(optarg, "w", stdout) == NULL) {
				fprintf(stderr, "%s: %s: reopen: %s\n",
				    progname, optarg, strerror(errno));
				return (EXIT_FAILURE);
			}
			break;
		case 'h':
			home = optarg;
			break;
		case 'k':
			keyflag = 1;
			break;
		case 'l':
			lflag = 1;
			break;
		case 'N':
			nflag = 1;
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
		case 'p':
			pflag = 1;
			break;
		case 's':
			subname = optarg;
			break;
		case 'R':
			Rflag = 1;
			/* DB_AGGRESSIVE requires DB_SALVAGE */
			/* FALLTHROUGH */
		case 'r':
			rflag = 1;
			break;
		case 'V':
			printf("%s\n", db_version(NULL, NULL, NULL));
			return (EXIT_SUCCESS);
		case '?':
		default:
			return (cdb2_dump_usage());
		}
	argc -= optind;
	argv += optind;

	if (argc != 1)
		return (cdb2_dump_usage());

	if (dopt != NULL && pflag) {
		fprintf(stderr,
		    "%s: the -d and -p options may not both be specified\n",
		    progname);
		return (EXIT_FAILURE);
	}
	if (lflag && subname != NULL) {
		fprintf(stderr,
		    "%s: the -l and -s options may not both be specified\n",
		    progname);
		return (EXIT_FAILURE);
	}

	if (keyflag && rflag) {
		fprintf(stderr, "%s: %s",
		    "the -k and -r or -R options may not both be specified\n",
		    progname);
		return (EXIT_FAILURE);
	}

	if (subname != NULL && rflag) {
		fprintf(stderr, "%s: %s",
		    "the -s and -r or R options may not both be specified\n",
		    progname);
		return (EXIT_FAILURE);
	}

	/* Handle possible interruptions. */
	__db_util_siginit();

	/*
	 * Create an environment object and initialize it for error
	 * reporting.
	 */
retry:	if ((ret = db_env_create(&dbenv, 0)) != 0) {
		fprintf(stderr,
		    "%s: db_env_create: %s\n", progname, db_strerror(ret));
		goto err;
	}

	dbenv->set_errfile(dbenv, stderr);
	dbenv->set_errpfx(dbenv, progname);
	if (nflag) {
		if ((ret = dbenv->set_flags(dbenv, DB_NOLOCKING, 1)) != 0) {
			dbenv->err(dbenv, ret, "set_flags: DB_NOLOCKING");
			goto err;
		}
		if ((ret = dbenv->set_flags(dbenv, DB_NOPANIC, 1)) != 0) {
			dbenv->err(dbenv, ret, "set_flags: DB_NOPANIC");
			goto err;
		}
	}

	if (strlen(passwd) > 0 && (ret = dbenv->set_encrypt(dbenv,
	    passwd, DB_ENCRYPT_AES)) != 0) {
		dbenv->err(dbenv, ret, "set_passwd");
		goto err;
	}

	/* Initialize the environment. */
	if (db_init(dbenv, home, rflag, cache, &private) != 0)
		goto err;

	/* Create the DB object and open the file. */
	if ((ret = db_create(&dbp, dbenv, 0)) != 0) {
		dbenv->err(dbenv, ret, "db_create");
		goto err;
	}

	/*
	 * If we're salvaging, don't do an open;  it might not be safe.
	 * Dispatch now into the salvager.
	 */
	if (rflag) {
		/* The verify method is a destructor. */
		ret = dbp->verify(dbp, argv[0], NULL, stdout,
		    DB_SALVAGE |
		    (Rflag ? DB_AGGRESSIVE : 0) |
		    (pflag ? DB_PRINTABLE : 0));
		dbp = NULL;
		if (ret != 0)
			goto err;
		goto done;
	}

	if ((ret = dbp->open(dbp, NULL,
	    argv[0], subname, DB_UNKNOWN, DB_RDONLY, 0)) != 0) {
		dbp->err(dbp, ret, "open: %s", argv[0]);
		goto err;
	}
	if (private != 0) {
		if ((ret = __db_util_cache(dbenv, dbp, &cache, &resize)) != 0)
			goto err;
		if (resize) {
			(void)dbp->close(dbp, NULL, 0);
			dbp = NULL;

			(void)dbenv->close(dbenv, 0);
			dbenv = NULL;
			goto retry;
		}
	}

	if (io_override_init())
		goto err;
	io_override_set_std(stdout);

	if (dopt != NULL) {
		if (__db_dump(dbp, dopt, NULL)) {
			dbp->err(dbp, ret, "__db_dump: %s", argv[0]);
			goto err;
		}
	} else if (lflag) {
		if (is_sub(dbp, &subs))
			goto err;
		if (subs == 0) {
			dbp->errx(dbp,
			    "%s: does not contain multiple databases", argv[0]);
			goto err;
		}
		if (show_subs(dbp))
			goto err;
	} else {
		subs = 0;
		if (subname == NULL && is_sub(dbp, &subs))
			goto err;
		if (subs) {
			if (dump_sub(dbenv, dbp, argv[0], pflag, keyflag))
				goto err;
		} else
			if (__db_prheader(dbp, NULL, pflag, keyflag, stdout,
			    __db_pr_callback, NULL, 0) ||
			    dump(dbp, pflag, keyflag))
				goto err;
	}

	if (0) {
err:		exitval = 1;
	}
done:	if (dbp != NULL && (ret = dbp->close(dbp, NULL, 0)) != 0) {
		exitval = 1;
		dbenv->err(dbenv, ret, "close");
	}
	if (dbenv != NULL && (ret = dbenv->close(dbenv, 0)) != 0) {
		exitval = 1;
		fprintf(stderr,
		    "%s: dbenv->close: %s\n", progname, db_strerror(ret));
	}

	/* Resend any caught signal. */
	__db_util_sigresend();

	return (exitval == 0 ? EXIT_SUCCESS : EXIT_FAILURE);
}

/*
 * db_init --
 *	Initialize the environment.
 */
int
db_init(dbenv, home, is_salvage, cache, is_privatep)
	DB_ENV *dbenv;
	char *home;
	int is_salvage;
	u_int32_t cache;
	int *is_privatep;
{
	int ret;

	/*
	 * Try and use the underlying environment when opening a database.
	 * We wish to use the buffer pool so our information is as up-to-date
	 * as possible, even if the mpool cache hasn't been flushed.
	 *
	 * If we are not doing a salvage, we wish to use the DB_JOINENV flag;
	 * if a locking system is present, this will let us use it and be
	 * safe to run concurrently with other threads of control.  (We never
	 * need to use transactions explicitly, as we're read-only.)  Note
	 * that in CDB, too, this will configure our environment
	 * appropriately, and our cursors will (correctly) do locking as CDB
	 * read cursors.
	 *
	 * If we are doing a salvage, the verification code will protest
	 * if we initialize transactions, logging, or locking;  do an
	 * explicit DB_INIT_MPOOL to try to join any existing environment
	 * before we create our own.
	 */
	*is_privatep = 0;
	if (dbenv->open(dbenv, home,
	    DB_USE_ENVIRON | (is_salvage ? DB_INIT_MPOOL : DB_JOINENV), 0) == 0)
		return (0);

	/*
	 * An environment is required because we may be trying to look at
	 * databases in directories other than the current one.  We could
	 * avoid using an environment iff the -h option wasn't specified,
	 * but that seems like more work than it's worth.
	 *
	 * No environment exists (or, at least no environment that includes
	 * an mpool region exists).  Create one, but make it private so that
	 * no files are actually created.
	 */
	*is_privatep = 1;
	if ((ret = dbenv->set_cachesize(dbenv, 0, cache, 1)) == 0 &&
	    (ret = dbenv->open(dbenv, home,
	    DB_CREATE | DB_INIT_MPOOL | DB_PRIVATE | DB_USE_ENVIRON, 0)) == 0)
		return (0);

	/* An environment is required. */
	dbenv->err(dbenv, ret, "open");
	return (1);
}

/*
 * is_sub --
 *	Return if the database contains subdatabases.
 */
int
is_sub(dbp, yesno)
	DB *dbp;
	int *yesno;
{
	DB_BTREE_STAT *btsp;
	DB_HASH_STAT *hsp;
	int ret;

	switch (dbp->type) {
	case DB_BTREE:
	case DB_RECNO:
		if ((ret = dbp->stat(dbp, &btsp, DB_FAST_STAT)) != 0) {
			dbp->err(dbp, ret, "DB->stat");
			return (ret);
		}
		*yesno = btsp->bt_metaflags & BTM_SUBDB ? 1 : 0;
		free(btsp);
		break;
	case DB_HASH:
		if ((ret = dbp->stat(dbp, &hsp, DB_FAST_STAT)) != 0) {
			dbp->err(dbp, ret, "DB->stat");
			return (ret);
		}
		*yesno = hsp->hash_metaflags & DB_HASH_SUBDB ? 1 : 0;
		free(hsp);
		break;
	case DB_QUEUE:
		break;
	case DB_UNKNOWN:
	default:
		dbp->errx(dbp, "unknown database type");
		return (1);
	}
	return (0);
}

/*
 * dump_sub --
 *	Dump out the records for a DB containing subdatabases.
 */
int
dump_sub(dbenv, parent_dbp, parent_name, pflag, keyflag)
	DB_ENV *dbenv;
	DB *parent_dbp;
	char *parent_name;
	int pflag, keyflag;
{
	DB *dbp;
	DBC *dbcp;
	DBT key, data;
	int ret;
	char *subdb;

	/*
	 * Get a cursor and step through the database, dumping out each
	 * subdatabase.
	 */
	if ((ret = parent_dbp->cursor(parent_dbp, NULL, &dbcp, 0)) != 0) {
		dbenv->err(dbenv, ret, "DB->cursor");
		return (1);
	}

	memset(&key, 0, sizeof(key));
	memset(&data, 0, sizeof(data));
	while ((ret = dbcp->c_get(dbcp, &key, &data, DB_NEXT)) == 0) {
		/* Nul terminate the subdatabase name. */
		if ((subdb = malloc(key.size + 1)) == NULL) {
			dbenv->err(dbenv, ENOMEM, NULL);
			return (1);
		}
		memcpy(subdb, key.data, key.size);
		subdb[key.size] = '\0';

		/* Create the DB object and open the file. */
		if ((ret = db_create(&dbp, dbenv, 0)) != 0) {
			dbenv->err(dbenv, ret, "db_create");
			free(subdb);
			return (1);
		}
		if ((ret = dbp->open(dbp, NULL,
		    parent_name, subdb, DB_UNKNOWN, DB_RDONLY, 0)) != 0)
			dbp->err(dbp, ret,
			    "DB->open: %s:%s", parent_name, subdb);
		if (ret == 0 &&
		    (__db_prheader(dbp, subdb, pflag, keyflag, stdout,
		    __db_pr_callback, NULL, 0) ||
		    dump(dbp, pflag, keyflag)))
			ret = 1;
		(void)dbp->close(dbp, NULL, 0);
		free(subdb);
		if (ret != 0)
			return (1);
	}
	if (ret != DB_NOTFOUND) {
		parent_dbp->err(parent_dbp, ret, "DBcursor->get");
		return (1);
	}

	if ((ret = dbcp->c_close(dbcp)) != 0) {
		parent_dbp->err(parent_dbp, ret, "DBcursor->close");
		return (1);
	}

	return (0);
}

/*
 * show_subs --
 *	Display the subdatabases for a database.
 */
int
show_subs(dbp)
	DB *dbp;
{
	DBC *dbcp;
	DBT key, data;
	int ret;

	/*
	 * Get a cursor and step through the database, printing out the key
	 * of each key/data pair.
	 */
	if ((ret = dbp->cursor(dbp, NULL, &dbcp, 0)) != 0) {
		dbp->err(dbp, ret, "DB->cursor");
		return (1);
	}

	memset(&key, 0, sizeof(key));
	memset(&data, 0, sizeof(data));
	while ((ret = dbcp->c_get(dbcp, &key, &data, DB_NEXT)) == 0) {
		if ((ret = __db_prdbt(&key, 1, NULL, stdout,
		    __db_pr_callback, 0, NULL)) != 0) {
			dbp->errx(dbp, NULL);
			return (1);
		}
	}
	if (ret != DB_NOTFOUND) {
		dbp->err(dbp, ret, "DBcursor->get");
		return (1);
	}

	if ((ret = dbcp->c_close(dbcp)) != 0) {
		dbp->err(dbp, ret, "DBcursor->close");
		return (1);
	}
	return (0);
}

/*
 * dump --
 *	Dump out the records for a DB.
 */
int
dump(dbp, pflag, keyflag)
	DB *dbp;
	int pflag, keyflag;
{
	DBC *dbcp;
	DBT key, data;
	DBT keyret, dataret;
	db_recno_t recno;
	int is_recno, failed, ret;
	void *pointer;

	/*
	 * Get a cursor and step through the database, printing out each
	 * key/data pair.
	 */
	if ((ret = dbp->cursor(dbp, NULL, &dbcp, 0)) != 0) {
		dbp->err(dbp, ret, "DB->cursor");
		return (1);
	}

	failed = 0;
	memset(&key, 0, sizeof(key));
	memset(&data, 0, sizeof(data));
	data.data = malloc(1024 * 1024);
	if (data.data == NULL) {
		dbp->err(dbp, ENOMEM, "bulk get buffer");
		failed = 1;
		goto err;
	}
	data.ulen = 1024 * 1024;
	data.flags = DB_DBT_USERMEM;
	is_recno = (dbp->type == DB_RECNO || dbp->type == DB_QUEUE);
	keyflag = is_recno ? keyflag : 1;
	if (is_recno) {
		keyret.data = &recno;
		keyret.size = sizeof(recno);
	}

retry:
	while ((ret =
	    dbcp->c_get(dbcp, &key, &data, DB_NEXT | DB_MULTIPLE_KEY)) == 0) {
		DB_MULTIPLE_INIT(pointer, &data);
		for (;;) {
			if (is_recno)
				DB_MULTIPLE_RECNO_NEXT(pointer, &data,
				    recno, dataret.data, dataret.size);
			else
				DB_MULTIPLE_KEY_NEXT(pointer,
				    &data, keyret.data,
				    keyret.size, dataret.data, dataret.size);

			if (dataret.data == NULL)
				break;

			if ((keyflag && (ret = __db_prdbt(&keyret,
			    pflag, " ", stdout, __db_pr_callback,
			    is_recno, NULL)) != 0) || (ret =
			    __db_prdbt(&dataret, pflag, " ", stdout,
				__db_pr_callback, 0, NULL)) != 0) {
				dbp->errx(dbp, NULL);
				failed = 1;
				goto err;
			}
		}
	}
	if (ret == ENOMEM) {
		data.size = ALIGN(data.size, 1024);
		data.data = realloc(data.data, data.size);
		if (data.data == NULL) {
			dbp->err(dbp, ENOMEM, "bulk get buffer");
			failed = 1;
			goto err;
		}
		data.ulen = data.size;
		goto retry;
	}

	if (ret != DB_NOTFOUND) {
		dbp->err(dbp, ret, "DBcursor->get");
		failed = 1;
	}

err:	if (data.data != NULL)
		free(data.data);

	if ((ret = dbcp->c_close(dbcp)) != 0) {
		dbp->err(dbp, ret, "DBcursor->close");
		failed = 1;
	}

	(void)__db_prfooter(stdout, __db_pr_callback);
	return (failed);
}

/*
 * usage --
 *	Display the usage message.
 */
static int
cdb2_dump_usage()
{
	(void)fprintf(stderr, "%s\n\t%s\n",
	    "usage: cdb2_dump [-klNprRV]",
    "[-d ahr] [-f output] [-h home] [-P /path/to/password] [-s database] db_file");
	return (EXIT_FAILURE);
}

int
version_check(progname)
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
