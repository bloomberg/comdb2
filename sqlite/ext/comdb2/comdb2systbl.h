#include "sqlite3.h"

#ifndef _SQL_H_
#include <mem_sqlite.h>
#include <mem_override.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif  /* __cplusplus */

int comdb2SystblInit(sqlite3 *db);

#ifdef __cplusplus
}  /* extern "C" */
#endif  /* __cplusplus */
