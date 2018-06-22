#ifndef _DB_DBT_H_
#define	_DB_DBT_H_

#include <sys/types.h>
#include <inttypes.h>

#ifndef        __BIT_TYPES_DEFINED__
#if defined _SUN_SOURCE || defined _HP_SOURCE
typedef unsigned char u_int8_t;
typedef unsigned short u_int16_t;
typedef unsigned int u_int32_t;
typedef unsigned long long u_int64_t;
#endif
#define        __BIT_TYPES_DEFINED__
#endif

struct __db_dbt;	typedef struct __db_dbt DBT;

#define __DB_DBT_INTERNAL                                       \
	void	 *data;			/* Key/data */                      \
	u_int32_t size;			/* key/data length */               \
	u_int32_t ulen;			/* RO: length of user buffer. */    \
	u_int32_t dlen;			/* RO: get/put record length. */    \
	u_int32_t doff;			/* RO: get/put record offset. */

struct __db_dbt_internal {
    __DB_DBT_INTERNAL
};

/* Key/data structure -- a Data-Base Thang. */
struct __db_dbt {
	/*
	 * data/size must be fields 1 and 2 for DB 1.85 compatibility.
	 */
    __DB_DBT_INTERNAL

    void *app_data;

#define	DB_DBT_APPMALLOC	0x001	/* Callback allocated memory. */
#define	DB_DBT_ISSET		0x002	/* Lower level calls set value. */
#define	DB_DBT_MALLOC		0x004	/* Return in malloc'd memory. */
#define	DB_DBT_PARTIAL		0x008	/* Partial put/get. */
#define	DB_DBT_REALLOC		0x010	/* Return in realloc'd memory. */
#define	DB_DBT_USERMEM		0x020	/* Return in user's memory. */
#define	DB_DBT_DUPOK		0x040	/* Insert if duplicate. */
	u_int32_t flags;
};

#endif
