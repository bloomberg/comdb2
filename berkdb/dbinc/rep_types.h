/*- * See the file LICENSE for redistribution information.

 *
 * Copyright (c) 2001-2003
 *	Sleepycat Software.  All rights reserved.
 */


#ifndef _REP_TYPES_H_
#define	_REP_TYPES_H_

#define	REP_ALIVE	1	/* I am alive message. */
#define	REP_ALIVE_REQ	2	/* Request for alive messages. */
#define	REP_ALL_REQ	3	/* Request all log records greater than LSN. */
#define	REP_DUPMASTER	4	/* Duplicate master detected; propagate. */
#define	REP_FILE	5	/* Page of a database file. */
#define	REP_FILE_REQ	6	/* Request for a database file. */
#define	REP_LOG		7	/* Log record. */
#define	REP_LOG_MORE	8	/* There are more log records to request. */
#define	REP_LOG_REQ	9	/* Request for a log record. */
#define	REP_MASTER_REQ	10	/* Who is the master */
#define	REP_NEWCLIENT	11	/* Announces the presence of a new client. */
#define	REP_NEWFILE	12	/* Announce a log file change. */
#define	REP_NEWMASTER	13	/* Announces who the master is. */
#define	REP_NEWSITE	14	/* Announces that a site has heard from a new
				 * site; like NEWCLIENT, but indirect.  A
				 * NEWCLIENT message comes directly from the new
				 * client while a NEWSITE comes indirectly from
				 * someone who heard about a NEWSITE.
				 */
#define	REP_PAGE	15	/* Database page. */
#define	REP_PAGE_REQ	16	/* Request for a database page. */
#define	REP_PLIST	17	/* Database page list. */
#define	REP_PLIST_REQ	18	/* Request for a page list. */
#define	REP_VERIFY	19	/* A log record for verification. */
#define	REP_VERIFY_FAIL	20	/* The client is outdated. */
#define	REP_VERIFY_REQ	21	/* Request for a log record to verify. */
#define	REP_VOTE1	22	/* Send out your information for an election. */
#define	REP_VOTE2	23	/* Send a "you are master" vote. */

/* COMDB2 MODIFICATION */
/* We want to be able to throttle log propagation to avoid filling 
   the net queue; this will allow signal messages and catching up
   log transfer to be transferred even though the database is under heavy
   load
   Problem is in berkdb_send_rtn both regular log messages and catching
   up log replies are coming as REP_LOG
   In __log_push we replace REP_LOG with REP_LOG_LOGPUT so we know
   that this must be throttled; we revertto REP_LOG in the same routine
 */
#define	REP_LOG_LOGPUT		24	/* Master internal: same as REP_LOG */

/* COMDB2 MODIFICATION */
/* Dump a page for a given fileid, for debugging */
#define REP_PGDUMP_REQ      25

/* COMDB2 MODIFICATION: bias election to the largest generation number written to the log */
#define REP_GEN_VOTE1   26  /* Send our your information for an election */
#define REP_GEN_VOTE2   27 /* Send a "you are master" vote. */

#define REP_LOG_FILL    28
#define REP_MAX_TYPE    29

#endif
