/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996-2003
 *	Sleepycat Software.  All rights reserved.
 *
 * $Id: shqueue.h,v 11.13 2003/04/24 15:41:02 bostic Exp $
 */

#ifndef	_SYS_SHQUEUE_H_
#define	_SYS_SHQUEUE_H_

/*
 * This file defines two types of data structures: lists and tail queues
 * similarly to the include file <sys/queue.h>.
 *
 * The difference is that this set of macros can be used for structures that
 * reside in shared memory that may be mapped at different addresses in each
 * process.  In most cases, the macros for shared structures exactly mirror
 * the normal macros, although the macro calls require an additional type
 * parameter, only used by the HEAD and ENTRY macros of the standard macros.
 *
 * Since we use relative offsets of type ssize_t rather than pointers, 0
 * (aka NULL) is a valid offset and cannot be used to indicate the end
 * of a list.  Therefore, we use -1 to indicate end of list.
 *
 * The macros ending in "P" return pointers without checking for end or
 * beginning of lists, the others check for end of list and evaluate to
 * either a pointer or NULL.
 *
 * For details on the use of these macros, see the queue(3) manual page.
 */

#if defined(__cplusplus)
extern "C" {
#endif

#include <dbinc/queue.h>

/*
 * Shared memory list definitions.
 */
#define SH_LIST_HEAD						LIST_HEAD
#define SH_LIST_HEAD_INITIALIZER				LIST_HEAD_INITIALIZER
#define SH_LIST_ENTRY						LIST_ENTRY

/*
 * Shared memory list functions.
 */
#define	SH_LIST_EMPTY						LIST_EMPTY
#define	SH_LIST_FIRST(head, type)				LIST_FIRST(head)
#define	SH_LIST_NEXT(elm, field, type)				LIST_NEXT(elm, field)
#define	SH_LIST_FOREACH(var, head, field, type)			LIST_FOREACH(var, head, field)
#define	SH_LIST_INIT						LIST_INIT
#define	SH_LIST_INSERT_BEFORE(head, listelm, elm, field, type)	LIST_INSERT_BEFORE(listelm, elm, field)
#define	SH_LIST_INSERT_AFTER(listelm, elm, field, type)		LIST_INSERT_AFTER(listelm, elm, field)
#define	SH_LIST_INSERT_HEAD(head, elm, field, type)		LIST_INSERT_HEAD(head, elm, field)
#define	SH_LIST_REMOVE(elm, field, type)			LIST_REMOVE(elm, field)

/*
 * Shared memory tail queue definitions.
 */
#define	SH_TAILQ_HEAD						TAILQ_HEAD
#define	SH_TAILQ_HEAD_INITIALIZER				TAILQ_HEAD_INITIALIZER
#define	SH_TAILQ_ENTRY						TAILQ_ENTRY

/*
 * Shared memory tail queue functions.
 */
#define	SH_TAILQ_EMPTY						TAILQ_EMPTY
#define	SH_TAILQ_FIRST(head, type)				TAILQ_FIRST(head)
#define	SH_TAILQ_NEXT(elm, field, type)				TAILQ_NEXT(elm, field)
#define	SH_TAILQ_PREV						TAILQ_PREV
#define	SH_TAILQ_LAST						TAILQ_LAST
#define	SH_TAILQ_FOREACH(var, head, field, type)		TAILQ_FOREACH(var, head, field)
#define	SH_TAILQ_FOREACH_SAFE(var, head, field, type, tvar)	TAILQ_FOREACH_SAFE(var, head, field, tvar)
#define	SH_TAILQ_FOREACH_REVERSE				TAILQ_FOREACH_REVERSE
#define	SH_TAILQ_INIT						TAILQ_INIT
#define	SH_TAILQ_INSERT_HEAD(head, elm, field, type)		TAILQ_INSERT_HEAD(head, elm, field)
#define	SH_TAILQ_INSERT_TAIL					TAILQ_INSERT_TAIL
#define	SH_TAILQ_INSERT_BEFORE(head, listelm, elm, field, type)	TAILQ_INSERT_BEFORE(listelm, elm, field)
#define	SH_TAILQ_INSERT_AFTER(head, listelm, elm, field, type)	TAILQ_INSERT_AFTER(head, listelm, elm, field)
#define	SH_TAILQ_REMOVE(head, elm, field, type)			TAILQ_REMOVE(head, elm, field)

#if defined(__cplusplus)
}
#endif

#endif	/* !_SYS_SHQUEUE_H_ */
