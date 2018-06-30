/*
   Copyright 2015 Bloomberg Finance L.P.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */

#include <alloca.h>
#include <errno.h>
#include <float.h>
#include <limits.h>
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <stddef.h>
#include <string.h>
#include <strings.h>
#include <stdarg.h>
#include <math.h>
#include <assert.h>

#include <plbitlib.h>
#include <segstr.h>

#include <sys/types.h>
#include <netinet/in.h>

#include "types.h"
#include "endian_core.h"
#include "flibc.h"

#include "decContext.h"
#include "dfpal.h"
#include "decNumber.h"
#include "decSingle.h"
#include "decDouble.h"
#include "decQuad.h"

#include <mem.h>
#include <mem_override.h>

#include "debug_switches.h"
#include "logmsg.h"
#include "util.h"

struct thr_handle;
struct reqlogger *thrman_get_reqlogger(struct thr_handle *thr);
struct thr_handle *thrman_self(void);

#if 0
#define TEST_OSQL
#endif

#ifdef DEBUG_TYPES
#define TYPES_INLINE
extern int gbl_decimal_rounding;
#define DEC_ROUND_NONE (-1)
#else
#define TYPES_INLINE inline
#endif

#define REAL_OUTOFRANGE(dbl, rng)                                              \
    ((dbl) != 1.0 / 0.0 && (dbl) != -1.0 / 0.0 &&                              \
     ((dbl) < -(rng) || (dbl) > (rng)))

int gbl_fix_validate_cstr = 1;
int gbl_warn_validate_cstr = 1;

int gbl_forbid_datetime_truncation = 0;
int gbl_forbid_datetime_promotion = 0;
int gbl_forbid_datetime_ms_us_s2s = 0;

enum { BLOB_ON_DISK_LEN = 5, VUTF8_ON_DISK_LEN = BLOB_ON_DISK_LEN };

BB_COMPILE_TIME_ASSERT(server_datetime_size,
                       sizeof(struct server_datetime) == SERVER_DATETIME_LEN);

static uint8_t *server_datetime_put(const server_datetime_t *p_server_datetime,
                                    uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || SERVER_DATETIME_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_server_datetime->flag), sizeof(p_server_datetime->flag),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_server_datetime->sec), sizeof(p_server_datetime->sec),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_server_datetime->msec), sizeof(p_server_datetime->msec),
                    p_buf, p_buf_end);

    return p_buf;
}

BB_COMPILE_TIME_ASSERT(server_datetimeus_size,
                       sizeof(struct server_datetimeus) ==
                           SERVER_DATETIMEUS_LEN);

static uint8_t *
server_datetimeus_put(const server_datetimeus_t *p_server_datetimeus,
                      uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || SERVER_DATETIMEUS_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_server_datetimeus->flag),
                    sizeof(p_server_datetimeus->flag), p_buf, p_buf_end);
    p_buf = buf_put(&(p_server_datetimeus->sec),
                    sizeof(p_server_datetimeus->sec), p_buf, p_buf_end);
    p_buf = buf_put(&(p_server_datetimeus->usec),
                    sizeof(p_server_datetimeus->usec), p_buf, p_buf_end);

    return p_buf;
}

static uint8_t *server_intv_ym_put(const server_intv_ym_t *p_server_intv_ym,
                                   uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || SERVER_INTV_YM_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_server_intv_ym->flag), sizeof(p_server_intv_ym->flag),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_server_intv_ym->months),
                    sizeof(p_server_intv_ym->months), p_buf, p_buf_end);

    return p_buf;
}

static uint8_t *server_intv_ds_put(const server_intv_ds_t *p_server_intv_ds,
                                   uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || SERVER_INTV_DS_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_server_intv_ds->flag), sizeof(p_server_intv_ds->flag),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_server_intv_ds->sec), sizeof(p_server_intv_ds->sec),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_server_intv_ds->msec), sizeof(p_server_intv_ds->msec),
                    p_buf, p_buf_end);

    return p_buf;
}

static uint8_t *
server_intv_dsus_put(const server_intv_dsus_t *p_server_intv_dsus,
                     uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || SERVER_INTV_DSUS_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_server_intv_dsus->flag),
                    sizeof(p_server_intv_dsus->flag), p_buf, p_buf_end);
    p_buf = buf_put(&(p_server_intv_dsus->sec), sizeof(p_server_intv_dsus->sec),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_server_intv_dsus->usec),
                    sizeof(p_server_intv_dsus->usec), p_buf, p_buf_end);

    return p_buf;
}

BB_COMPILE_TIME_ASSERT(tm_size,
                       offsetof(struct tm, tm_isdst) + sizeof(int) == TM_LEN);

uint8_t *tm_put(const cdb2_tm_t *p_tm, uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || TM_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_tm->tm_sec), sizeof(p_tm->tm_sec), p_buf, p_buf_end);
    p_buf = buf_put(&(p_tm->tm_min), sizeof(p_tm->tm_min), p_buf, p_buf_end);
    p_buf = buf_put(&(p_tm->tm_hour), sizeof(p_tm->tm_hour), p_buf, p_buf_end);
    p_buf = buf_put(&(p_tm->tm_mday), sizeof(p_tm->tm_mday), p_buf, p_buf_end);
    p_buf = buf_put(&(p_tm->tm_mon), sizeof(p_tm->tm_mon), p_buf, p_buf_end);
    p_buf = buf_put(&(p_tm->tm_year), sizeof(p_tm->tm_year), p_buf, p_buf_end);
    p_buf = buf_put(&(p_tm->tm_wday), sizeof(p_tm->tm_wday), p_buf, p_buf_end);
    p_buf = buf_put(&(p_tm->tm_yday), sizeof(p_tm->tm_yday), p_buf, p_buf_end);
    p_buf =
        buf_put(&(p_tm->tm_isdst), sizeof(p_tm->tm_isdst), p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *tm_get(cdb2_tm_t *p_tm, const uint8_t *p_buf,
                      const uint8_t *p_buf_end)
{
    int lft;
    if (p_buf_end < p_buf || TM_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_tm->tm_sec), sizeof(p_tm->tm_sec), p_buf, p_buf_end);
    p_buf = buf_get(&(p_tm->tm_min), sizeof(p_tm->tm_min), p_buf, p_buf_end);
    p_buf = buf_get(&(p_tm->tm_hour), sizeof(p_tm->tm_hour), p_buf, p_buf_end);
    p_buf = buf_get(&(p_tm->tm_mday), sizeof(p_tm->tm_mday), p_buf, p_buf_end);
    p_buf = buf_get(&(p_tm->tm_mon), sizeof(p_tm->tm_mon), p_buf, p_buf_end);
    p_buf = buf_get(&(p_tm->tm_year), sizeof(p_tm->tm_year), p_buf, p_buf_end);
    p_buf = buf_get(&(p_tm->tm_wday), sizeof(p_tm->tm_wday), p_buf, p_buf_end);
    p_buf = buf_get(&(p_tm->tm_yday), sizeof(p_tm->tm_yday), p_buf, p_buf_end);
    p_buf =
        buf_get(&(p_tm->tm_isdst), sizeof(p_tm->tm_isdst), p_buf, p_buf_end);

    return p_buf;
}

uint8_t *tm_little_put(const cdb2_tm_t *p_tm, uint8_t *p_buf,
                       const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || TM_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf =
        buf_little_put(&(p_tm->tm_sec), sizeof(p_tm->tm_sec), p_buf, p_buf_end);
    p_buf =
        buf_little_put(&(p_tm->tm_min), sizeof(p_tm->tm_min), p_buf, p_buf_end);
    p_buf = buf_little_put(&(p_tm->tm_hour), sizeof(p_tm->tm_hour), p_buf,
                           p_buf_end);
    p_buf = buf_little_put(&(p_tm->tm_mday), sizeof(p_tm->tm_mday), p_buf,
                           p_buf_end);
    p_buf =
        buf_little_put(&(p_tm->tm_mon), sizeof(p_tm->tm_mon), p_buf, p_buf_end);
    p_buf = buf_little_put(&(p_tm->tm_year), sizeof(p_tm->tm_year), p_buf,
                           p_buf_end);
    p_buf = buf_little_put(&(p_tm->tm_wday), sizeof(p_tm->tm_wday), p_buf,
                           p_buf_end);
    p_buf = buf_little_put(&(p_tm->tm_yday), sizeof(p_tm->tm_yday), p_buf,
                           p_buf_end);
    p_buf = buf_little_put(&(p_tm->tm_isdst), sizeof(p_tm->tm_isdst), p_buf,
                           p_buf_end);

    return p_buf;
}

const uint8_t *tm_little_get(cdb2_tm_t *p_tm, const uint8_t *p_buf,
                             const uint8_t *p_buf_end)
{
    int lft;
    if (p_buf_end < p_buf || TM_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf =
        buf_little_get(&(p_tm->tm_sec), sizeof(p_tm->tm_sec), p_buf, p_buf_end);
    p_buf =
        buf_little_get(&(p_tm->tm_min), sizeof(p_tm->tm_min), p_buf, p_buf_end);
    p_buf = buf_little_get(&(p_tm->tm_hour), sizeof(p_tm->tm_hour), p_buf,
                           p_buf_end);
    p_buf = buf_little_get(&(p_tm->tm_mday), sizeof(p_tm->tm_mday), p_buf,
                           p_buf_end);
    p_buf =
        buf_little_get(&(p_tm->tm_mon), sizeof(p_tm->tm_mon), p_buf, p_buf_end);
    p_buf = buf_little_get(&(p_tm->tm_year), sizeof(p_tm->tm_year), p_buf,
                           p_buf_end);
    p_buf = buf_little_get(&(p_tm->tm_wday), sizeof(p_tm->tm_wday), p_buf,
                           p_buf_end);
    p_buf = buf_little_get(&(p_tm->tm_yday), sizeof(p_tm->tm_yday), p_buf,
                           p_buf_end);
    p_buf = buf_little_get(&(p_tm->tm_isdst), sizeof(p_tm->tm_isdst), p_buf,
                           p_buf_end);

    return p_buf;
}

uint8_t *tm_extended_put(const struct tm *p_tm, uint8_t *p_buf,
                         const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || TM_EXT_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_tm->tm_sec), sizeof(p_tm->tm_sec), p_buf, p_buf_end);
    p_buf = buf_put(&(p_tm->tm_min), sizeof(p_tm->tm_min), p_buf, p_buf_end);
    p_buf = buf_put(&(p_tm->tm_hour), sizeof(p_tm->tm_hour), p_buf, p_buf_end);
    p_buf = buf_put(&(p_tm->tm_mday), sizeof(p_tm->tm_mday), p_buf, p_buf_end);
    p_buf = buf_put(&(p_tm->tm_mon), sizeof(p_tm->tm_mon), p_buf, p_buf_end);
    p_buf = buf_put(&(p_tm->tm_year), sizeof(p_tm->tm_year), p_buf, p_buf_end);
    p_buf = buf_put(&(p_tm->tm_wday), sizeof(p_tm->tm_wday), p_buf, p_buf_end);
    p_buf = buf_put(&(p_tm->tm_yday), sizeof(p_tm->tm_yday), p_buf, p_buf_end);
    p_buf =
        buf_put(&(p_tm->tm_isdst), sizeof(p_tm->tm_isdst), p_buf, p_buf_end);

    /* zero-out the two fields for tm_gmtoff and tm_zone */
    p_buf = buf_zero_put(sizeof(int), p_buf, p_buf_end);
    p_buf = buf_zero_put(sizeof(int), p_buf, p_buf_end);

    return p_buf;
}

uint8_t *client_intv_ym_put(const cdb2_client_intv_ym_t *p_client_intv_ym,
                            uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || CLIENT_INTV_YM_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_client_intv_ym->sign), sizeof(p_client_intv_ym->sign),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_client_intv_ym->years), sizeof(p_client_intv_ym->years),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_client_intv_ym->months),
                    sizeof(p_client_intv_ym->months), p_buf, p_buf_end);

    return p_buf;
}

uint8_t *
client_intv_ym_little_put(const cdb2_client_intv_ym_t *p_client_intv_ym,
                          uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || CLIENT_INTV_YM_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_little_put(&(p_client_intv_ym->sign),
                           sizeof(p_client_intv_ym->sign), p_buf, p_buf_end);
    p_buf = buf_little_put(&(p_client_intv_ym->years),
                           sizeof(p_client_intv_ym->years), p_buf, p_buf_end);
    p_buf = buf_little_put(&(p_client_intv_ym->months),
                           sizeof(p_client_intv_ym->months), p_buf, p_buf_end);

    return p_buf;
}

uint8_t *client_intv_ds_put(const cdb2_client_intv_ds_t *p_client_intv_ds,
                            uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || CLIENT_INTV_DS_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_client_intv_ds->sign), sizeof(p_client_intv_ds->sign),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_client_intv_ds->days), sizeof(p_client_intv_ds->days),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_client_intv_ds->hours), sizeof(p_client_intv_ds->hours),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_client_intv_ds->mins), sizeof(p_client_intv_ds->mins),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_client_intv_ds->sec), sizeof(p_client_intv_ds->sec),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_client_intv_ds->msec), sizeof(p_client_intv_ds->msec),
                    p_buf, p_buf_end);

    return p_buf;
}

uint8_t *
client_intv_ds_little_put(const cdb2_client_intv_ds_t *p_client_intv_ds,
                          uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || CLIENT_INTV_DS_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_little_put(&(p_client_intv_ds->sign),
                           sizeof(p_client_intv_ds->sign), p_buf, p_buf_end);
    p_buf = buf_little_put(&(p_client_intv_ds->days),
                           sizeof(p_client_intv_ds->days), p_buf, p_buf_end);
    p_buf = buf_little_put(&(p_client_intv_ds->hours),
                           sizeof(p_client_intv_ds->hours), p_buf, p_buf_end);
    p_buf = buf_little_put(&(p_client_intv_ds->mins),
                           sizeof(p_client_intv_ds->mins), p_buf, p_buf_end);
    p_buf = buf_little_put(&(p_client_intv_ds->sec),
                           sizeof(p_client_intv_ds->sec), p_buf, p_buf_end);
    p_buf = buf_little_put(&(p_client_intv_ds->msec),
                           sizeof(p_client_intv_ds->msec), p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *client_intv_ds_get(cdb2_client_intv_ds_t *p_client_intv_ds,
                                  const uint8_t *p_buf,
                                  const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || CLIENT_INTV_DS_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_client_intv_ds->sign), sizeof(p_client_intv_ds->sign),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_client_intv_ds->days), sizeof(p_client_intv_ds->days),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_client_intv_ds->hours), sizeof(p_client_intv_ds->hours),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_client_intv_ds->mins), sizeof(p_client_intv_ds->mins),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_client_intv_ds->sec), sizeof(p_client_intv_ds->sec),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_client_intv_ds->msec), sizeof(p_client_intv_ds->msec),
                    p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *
client_intv_ds_little_get(cdb2_client_intv_ds_t *p_client_intv_ds,
                          const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || CLIENT_INTV_DS_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_little_get(&(p_client_intv_ds->sign),
                           sizeof(p_client_intv_ds->sign), p_buf, p_buf_end);
    p_buf = buf_little_get(&(p_client_intv_ds->days),
                           sizeof(p_client_intv_ds->days), p_buf, p_buf_end);
    p_buf = buf_little_get(&(p_client_intv_ds->hours),
                           sizeof(p_client_intv_ds->hours), p_buf, p_buf_end);
    p_buf = buf_little_get(&(p_client_intv_ds->mins),
                           sizeof(p_client_intv_ds->mins), p_buf, p_buf_end);
    p_buf = buf_little_get(&(p_client_intv_ds->sec),
                           sizeof(p_client_intv_ds->sec), p_buf, p_buf_end);
    p_buf = buf_little_get(&(p_client_intv_ds->msec),
                           sizeof(p_client_intv_ds->msec), p_buf, p_buf_end);

    return p_buf;
}

uint8_t *client_intv_dsus_put(const cdb2_client_intv_dsus_t *p_client_intv_dsus,
                              uint8_t *p_buf, const uint8_t *p_buf_end)
{
    return client_intv_ds_put((const cdb2_client_intv_ds_t *)p_client_intv_dsus,
                              p_buf, p_buf_end);
}

uint8_t *
client_intv_dsus_little_put(const cdb2_client_intv_dsus_t *p_client_intv_dsus,
                            uint8_t *p_buf, const uint8_t *p_buf_end)
{
    return client_intv_ds_little_put(
        (const cdb2_client_intv_ds_t *)p_client_intv_dsus, p_buf, p_buf_end);
}

const uint8_t *client_intv_dsus_get(cdb2_client_intv_dsus_t *p_client_intv_dsus,
                                    const uint8_t *p_buf,
                                    const uint8_t *p_buf_end)
{
    return client_intv_ds_get((cdb2_client_intv_ds_t *)p_client_intv_dsus,
                              p_buf, p_buf_end);
}

const uint8_t *
client_intv_dsus_little_get(cdb2_client_intv_dsus_t *p_client_intv_dsus,
                            const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    return client_intv_ds_little_get(
        (cdb2_client_intv_ds_t *)p_client_intv_dsus, p_buf, p_buf_end);
}

const uint8_t *client_intv_ym_get(cdb2_client_intv_ym_t *p_client_intv_ym,
                                  const uint8_t *p_buf,
                                  const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || CLIENT_INTV_YM_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_client_intv_ym->sign), sizeof(p_client_intv_ym->sign),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_client_intv_ym->years), sizeof(p_client_intv_ym->years),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_client_intv_ym->months),
                    sizeof(p_client_intv_ym->months), p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *
client_intv_ym_little_get(cdb2_client_intv_ym_t *p_client_intv_ym,
                          const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || CLIENT_INTV_YM_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_little_get(&(p_client_intv_ym->sign),
                           sizeof(p_client_intv_ym->sign), p_buf, p_buf_end);
    p_buf = buf_little_get(&(p_client_intv_ym->years),
                           sizeof(p_client_intv_ym->years), p_buf, p_buf_end);
    p_buf = buf_little_get(&(p_client_intv_ym->months),
                           sizeof(p_client_intv_ym->months), p_buf, p_buf_end);

    return p_buf;
}

BB_COMPILE_TIME_ASSERT(client_datetime_size,
                       sizeof(struct cdb2_client_datetime) ==
                           CLIENT_DATETIME_LEN);

uint8_t *client_datetime_put(const cdb2_client_datetime_t *p_client_datetime,
                             uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || CLIENT_DATETIME_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = tm_put(&(p_client_datetime->tm), p_buf, p_buf_end);
    p_buf = buf_put(&(p_client_datetime->msec), sizeof(p_client_datetime->msec),
                    p_buf, p_buf_end);
    p_buf = buf_no_net_put(&(p_client_datetime->tzname),
                           sizeof(p_client_datetime->tzname), p_buf, p_buf_end);

    return p_buf;
}

uint8_t *
client_datetime_little_put(const cdb2_client_datetime_t *p_client_datetime,
                           uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || CLIENT_DATETIME_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = tm_little_put(&(p_client_datetime->tm), p_buf, p_buf_end);
    p_buf = buf_little_put(&(p_client_datetime->msec),
                           sizeof(p_client_datetime->msec), p_buf, p_buf_end);
    p_buf = buf_no_net_put(&(p_client_datetime->tzname),
                           sizeof(p_client_datetime->tzname), p_buf, p_buf_end);

    return p_buf;
}

/* For sqlinterfaces.c */
uint8_t *
client_extended_datetime_put(const cdb2_client_datetime_t *p_client_datetime,
                             uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || CLIENT_DATETIME_EXT_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = tm_extended_put((struct tm *)&(p_client_datetime->tm), p_buf,
                            p_buf_end);
    p_buf = buf_put(&(p_client_datetime->msec), sizeof(p_client_datetime->msec),
                    p_buf, p_buf_end);
    p_buf = buf_no_net_put(&(p_client_datetime->tzname),
                           sizeof(p_client_datetime->tzname), p_buf, p_buf_end);

    return p_buf;
}

uint8_t *
client_datetime_put_switch(const cdb2_client_datetime_t *p_client_datetime,
                           uint8_t *p_buf, const uint8_t *p_buf_end,
                           int little_endian)
{
    if (little_endian) {
        return client_datetime_little_put(p_client_datetime, p_buf, p_buf_end);
    } else {
        return client_datetime_put(p_client_datetime, p_buf, p_buf_end);
    }
}

const uint8_t *client_datetime_get(cdb2_client_datetime_t *p_client_datetime,
                                   const uint8_t *p_buf,
                                   const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || CLIENT_DATETIME_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = tm_get(&(p_client_datetime->tm), p_buf, p_buf_end);
    p_buf = buf_get(&(p_client_datetime->msec), sizeof(p_client_datetime->msec),
                    p_buf, p_buf_end);
    p_buf = buf_no_net_get(&(p_client_datetime->tzname),
                           sizeof(p_client_datetime->tzname), p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *
client_datetime_little_get(cdb2_client_datetime_t *p_client_datetime,
                           const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || CLIENT_DATETIME_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = tm_little_get(&(p_client_datetime->tm), p_buf, p_buf_end);
    p_buf = buf_little_get(&(p_client_datetime->msec),
                           sizeof(p_client_datetime->msec), p_buf, p_buf_end);
    p_buf = buf_no_net_get(&(p_client_datetime->tzname),
                           sizeof(p_client_datetime->tzname), p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *
client_datetime_get_switch(cdb2_client_datetime_t *p_client_datetime,
                           const uint8_t *p_buf, const uint8_t *p_buf_end,
                           int little_endian)
{
    if (little_endian) {
        return client_datetime_little_get(p_client_datetime, p_buf, p_buf_end);
    } else {
        return client_datetime_get(p_client_datetime, p_buf, p_buf_end);
    }
}

BB_COMPILE_TIME_ASSERT(client_datetimeus_size,
                       sizeof(struct cdb2_client_datetimeus) ==
                           CLIENT_DATETIME_LEN);

uint8_t *
client_datetimeus_put(const cdb2_client_datetimeus_t *p_client_datetime,
                      uint8_t *p_buf, const uint8_t *p_buf_end)
{
    return client_datetime_put(
        (const cdb2_client_datetime_t *)p_client_datetime, p_buf, p_buf_end);
}

uint8_t *
client_datetimeus_little_put(const cdb2_client_datetimeus_t *p_client_datetime,
                             uint8_t *p_buf, const uint8_t *p_buf_end)
{
    return client_datetime_little_put(
        (const cdb2_client_datetime_t *)p_client_datetime, p_buf, p_buf_end);
}

/* For sqlinterfaces.c */
uint8_t *client_extended_datetimeus_put(
    const cdb2_client_datetimeus_t *p_client_datetime, uint8_t *p_buf,
    const uint8_t *p_buf_end)
{
    return client_extended_datetime_put(
        (const cdb2_client_datetime_t *)p_client_datetime, p_buf, p_buf_end);
}

uint8_t *
client_datetimeus_put_switch(const cdb2_client_datetimeus_t *p_client_datetime,
                             uint8_t *p_buf, const uint8_t *p_buf_end,
                             int little_endian)
{
    return client_datetime_put_switch(
        (const cdb2_client_datetime_t *)p_client_datetime, p_buf, p_buf_end,
        little_endian);
}

const uint8_t *
client_datetimeus_get(cdb2_client_datetimeus_t *p_client_datetime,
                      const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    return client_datetime_get((cdb2_client_datetime_t *)p_client_datetime,
                               p_buf, p_buf_end);
}

const uint8_t *
client_datetimeus_little_get(cdb2_client_datetimeus_t *p_client_datetime,
                             const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    return client_datetime_little_get(
        (cdb2_client_datetime_t *)p_client_datetime, p_buf, p_buf_end);
}

const uint8_t *
client_datetimeus_get_switch(cdb2_client_datetimeus_t *p_client_datetime,
                             const uint8_t *p_buf, const uint8_t *p_buf_end,
                             int little_endian)
{
    return client_datetime_get_switch(
        (cdb2_client_datetime_t *)p_client_datetime, p_buf, p_buf_end,
        little_endian);
}

/*
   in my great wisdom i messed up the datetime sorting,
   deeming the time quantums as unsigned orders negative,
   datetimes before epoch after post epoch times.
   THEREFORE, there're shall be database aborts and protests
   for any broken datetime record, until the day come when
   the datetimes will be schema-changed and gone into oblivion.
   11142007dh
   */

int null_bit = null_bit_low;

int gbl_empty_strings_dont_convert_to_numbers = 1;

void comdb2_types_set_null_bit(int n) { null_bit = n; }

TYPES_INLINE int ieee8b_to_ieee8(ieee8b x, ieee8 *y)
{
    unsigned long long f;
    unsigned long long mask;
    unsigned long long out;

    memcpy(&f, &x, 8);

    mask = ((f >> 63) - 1) | 0x8000000000000000ULL;
    out = f ^ mask;

    memcpy(y, &out, 8);

    return 0;
}

/*
   flip a float for sorting
   finds SIGN of fp number.
   if it's 1 (negative float), it flips all bits
   if it's 0 (positive float), it flips the sign only
   */
TYPES_INLINE int ieee8_to_ieee8b(ieee8 x, ieee8b *y)
{
    unsigned long long f;
    unsigned long long mask;

    memcpy(&f, &x, 8);

    mask = -(comdb2_int8)(f >> 63) | 0x8000000000000000ULL;
    *y = f ^ mask;

    return 0;
}

/*
   flip a float back
   signed was flipped from above, so:
   if sign is 1 (negative), it flips the sign bit back
   if sign is 0 (positive), it flips all bits back
   */
TYPES_INLINE int ieee4b_to_ieee4(ieee4b x, ieee4 *y)
{
    unsigned int f;
    unsigned int mask;
    unsigned int out;

    memcpy(&f, &x, 4);

    mask = ((f >> 31) - 1) | 0x80000000;
    out = f ^ mask;

    memcpy(y, &out, 4);

    return 0;
}

/*
   flip a float for sorting
   finds SIGN of fp number.
   if it's 1 (negative float), it flips all bits
   if it's 0 (positive float), it flips the sign only
   */
TYPES_INLINE int ieee4_to_ieee4b(ieee4 x, ieee4b *y)
{
    unsigned int f;
    unsigned int mask;

    memcpy(&f, &x, 4);

    mask = -(comdb2_int4)(f >> 31) | 0x80000000;
    *y = f ^ mask;

    return 0;
}

/* all of the following bit twizzleing functions take in and return ints in host
 * byte order */

#define EIGHT_BYTE_MSB (0x8000000000000000ULL)
#define FOUR_BYTE_MSB (0x80000000UL)
#define TWO_BYTE_MSB (0x8000U)

TYPES_INLINE int int8b_to_int8(int8b x, comdb2_int8 *y)
{
    x ^= EIGHT_BYTE_MSB;
    memcpy(y, &x, 8);

    return 0;
}

TYPES_INLINE int int8_to_int8b(comdb2_int8 x, int8b *y)
{
    memcpy(y, &x, 8);
    *y ^= EIGHT_BYTE_MSB;

    return 0;
}

TYPES_INLINE int int4b_to_int4(int4b x, comdb2_int4 *y)
{
    x ^= FOUR_BYTE_MSB;
    memcpy(y, &x, 4);

    return 0;
}

TYPES_INLINE int int4_to_int4b(comdb2_int4 x, int4b *y)
{
    memcpy(y, &x, 4);
    *y ^= FOUR_BYTE_MSB;

    return 0;
}

TYPES_INLINE int int2b_to_int2(int2b x, comdb2_int2 *y)
{
    x ^= TWO_BYTE_MSB;
    memcpy(y, &x, 2);

    return 0;
}

TYPES_INLINE int int2_to_int2b(comdb2_int2 x, int2b *y)
{
    memcpy(y, &x, 2);
    *y ^= TWO_BYTE_MSB;

    return 0;
}

TYPES_INLINE int cstrlenlim(const char *s, int lim)
{
    int len = 0;
    while (len < lim && *s) {
        len++;
        s++;
    }
    return len;
}

TYPES_INLINE int cstrlenlimflipped(const unsigned char *s, int lim)
{
    int len = 0;
    while (len < lim && *s != 0xff) {
        len++;
        s++;
    }
    return len;
}

TYPES_INLINE int validate_cstr(const char *s, int lim)
{
    int len = 0;
    const char *begin = s;

    while (len < lim) {
        /* stop walking when we hit a null */
        if (*s == '\0')
            return 0;

        s++;
        len++;
    }

#ifndef DEBUG_TYPES
    /* No \0 - that's bad.  For a while this code was broken and we were
     * returning 0 here and therefore allowing the string to go through.
     * We'll fix it, but on a tunable in case anyone is relying on the
     * broken behaviour now.
     * It turned out that prqssvc relied on this broken behaviour.  Quelle
     * surprise.  Enforce on alpha/beta for now and give me a way of catching
     * the offendors easily. */
    if (gbl_warn_validate_cstr) {
        struct reqlogger *logger;
        logger = thrman_get_reqlogger(thrman_self());
        if (logger) {
            reqlog_logf(logger, 1 /*REQL_INFO*/, "bad cstr <%*.*s>", len, len,
                        begin);
            reqlog_setflag(logger, 1 /*REQL_BAD_CSTR_FLAG*/);
        }
    }
    return gbl_fix_validate_cstr;
#else
    return -1;
#endif
}

TYPES_INLINE int validate_pstr(const char *s, int lim)
{
    int len = 0;

    /* walk the whole string - it must not have nulls */
    while (len < lim) {
        if (!iscomdb2pstrchar(*((unsigned char *)s)))
            return -1;

        s++;
        len++;
    }

    return 0;
}

/* pstr2 strings are not supposed to be "sniffed" to learn about their length.
   there is an out of band length that goes along with them. */
TYPES_INLINE int pstr2lenlim(const char *s, int lim)
{

#if 0
    int len=0;
    /* now check the remaining string to make sure it has no invalid chars */
    while (*s && len < lim) {
        s++;
        len++;
    }
    return len;
#endif

    return lim;
}

TYPES_INLINE int pstrlenlim(const char *s, int lim)
{
    const char *sp;
    int len = 0;

    sp = s + lim - 1;
    while (sp >= s) {
        if (*sp != ' ')
            break;
        lim--;
        sp--;
        len++;
    }
    len = lim;

    /* now check the remaining string to make sure it has no invalid chars */
    while (*s && lim > 0) {
        s++;
        lim--;
    }
    return len;
}

/* Copy one byte array to the other taking into account padding where needed.
 * If the source is larger than the destination then it is trimmed provided that
 * padding is enabled on the source and that the lost bytes match the padding
 * byte; otherwise conversion error.
 * If the destination is larger than the source then it is padded if we have a
 * padding value; else conversion error.
 */
static TYPES_INLINE int
bytearray_copy(const void *in, int inlen, const struct field_conv_opts *inopts,
               blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
               const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    int dbpad;

    enum { DBPAD_NONE = -1, DBPAD_CONFLICT = -2 };

    /* since only server fields can have a dbpad attribute we shouldn't have
     * a conflict here. */
    dbpad = DBPAD_NONE;
    if (inopts && (inopts->flags & FLD_CONV_DBPAD))
        dbpad = inopts->dbpad;
    if (outopts && (outopts->flags & FLD_CONV_DBPAD)) {
        /* This might happen in a schema change if they change dbpad and the
         * array size at the same time - this is such a borderline case I'm
         * just going to fail it. */
        if (dbpad >= 0 && dbpad != outopts->dbpad)
            dbpad = DBPAD_CONFLICT;
        else
            dbpad = outopts->dbpad;
    }

    if (inlen > outlen) {
        if (dbpad >= 0) {
            /* Make sure the lost bytes match the padding value */
            int ii;
            const char *cptr = in;
            for (ii = outlen; ii < inlen; ii++)
                if ((int)cptr[ii] != dbpad)
                    return -1;
        } else
            return -1;
        /* no buffer overflows please */
        inlen = outlen;
    } else if (inlen < outlen) {
        if (dbpad >= 0)
            memset(((char *)out) + inlen, dbpad, outlen - inlen);
        else
            return -1;
    }
    memcpy(out, in, inlen);
    *outdtsz = outlen;
    return 0;
}

#ifdef _LINUX_SOURCE
#define CHECK_FLIP(opts, flip)                                                 \
    if (!(opts && opts->flags & FLD_CONV_LENDIAN)) flip = 1;
#else
#define CHECK_FLIP(opts, flip)                                                 \
    if ((opts && opts->flags & FLD_CONV_LENDIAN)) flip = 1;
#endif

TYPES_INLINE int CLIENT_UINT_to_CLIENT_UINT(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    unsigned long long from_8;
    unsigned int from_4;
    unsigned short from_2;
    int from_flip = 0;

    unsigned long long to_8;
    unsigned int to_4;
    unsigned short to_2;
    int to_flip = 0;

    /* Determine if the from-data is little endian. */
    CHECK_FLIP(inopts, from_flip);

    /* Determine if the to-data is little endian. */
    CHECK_FLIP(outopts, to_flip);

    *outdtsz = 0;
    switch (inlen) {
    case 8:
        memcpy(&from_8, in, 8);
        if (from_flip) {
            from_8 = flibc_llflip(from_8);
        }

        switch (outlen) {
        case 8:
            to_8 = from_8;

            if (to_flip) {
                to_8 = flibc_llflip(to_8);
            }
            memcpy(out, &to_8, 8);
            *outdtsz = 8;
            break;
        case 4:
            if (from_8 > UINT_MAX)
                return -1;
            to_4 = from_8;

            if (to_flip) {
                to_4 = flibc_intflip(to_4);
            }

            memcpy(out, &to_4, 4);
            *outdtsz = 4;
            break;
        case 2:
            if (from_8 > USHRT_MAX)
                return -1;
            to_2 = from_8;

            if (to_flip) {
                to_2 = flibc_shortflip(to_2);
            }

            memcpy(out, &to_2, 2);
            *outdtsz = 2;
            break;
        default:
            return -1;
            break;
        }
        break;

    case 4:
        memcpy(&from_4, in, 4);
        if (from_flip) {
            from_4 = flibc_intflip(from_4);
        }

        switch (outlen) {
        case 8:
            to_8 = from_4;

            if (to_flip) {
                to_8 = flibc_llflip(to_8);
            }
            memcpy(out, &to_8, 8);
            *outdtsz = 8;
            break;
        case 4:
            to_4 = from_4;

            if (to_flip) {
                to_4 = flibc_intflip(to_4);
            }

            memcpy(out, &to_4, 4);
            *outdtsz = 4;
            break;
        case 2:
            if (from_4 > USHRT_MAX)
                return -1;
            to_2 = from_4;

            if (to_flip) {
                to_2 = flibc_shortflip(to_2);
            }
            memcpy(out, &to_2, 2);
            *outdtsz = 2;
            break;
        default:
            return -1;
            break;
        }
        break;

    case 2:
        memcpy(&from_2, in, 2);
        if (from_flip) {
            from_2 = flibc_shortflip(from_2);
        }

        switch (outlen) {
        case 8:
            to_8 = from_2;

            if (to_flip) {
                to_8 = flibc_llflip(to_8);
            }
            memcpy(out, &to_8, 8);
            *outdtsz = 8;
            break;
        case 4:
            to_4 = from_2;

            if (to_flip) {
                to_4 = flibc_intflip(to_4);
            }
            memcpy(out, &to_4, 4);
            *outdtsz = 4;
            break;
        case 2:
            to_2 = from_2;

            if (to_flip) {
                to_2 = flibc_shortflip(to_2);
            }
            memcpy(out, &to_2, 2);
            *outdtsz = 2;
            break;
        default:
            return -1;
            break;
        }
        break;

    default:
        return -1;
        break;
    }

    return 0;
}

TYPES_INLINE int CLIENT_UINT_to_CLIENT_INT(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    unsigned long long from_8;
    unsigned int from_4;
    unsigned short from_2;
    int from_flip = 0;

    signed long long to_8;
    signed int to_4;
    signed short to_2;
    int to_flip = 0;

    /* Determine if the from-data is little endian. */
    CHECK_FLIP(inopts, from_flip);

    /* Determine if the to-data is little endian. */
    CHECK_FLIP(outopts, to_flip);

    *outdtsz = 0;
    switch (inlen) {
    case 8:
        memcpy(&from_8, in, 8);

        if (from_flip) {
            from_8 = flibc_llflip(from_8);
        }

        switch (outlen) {
        case 8:
            if (from_8 > LLONG_MAX)
                return -1;
            to_8 = from_8;

            if (to_flip) {
                to_8 = flibc_llflip(to_8);
            }
            memcpy(out, &to_8, 8);
            *outdtsz = 8;
            break;
        case 4:
            if (from_8 > INT_MAX)
                return -1;
            to_4 = from_8;

            if (to_flip) {
                to_4 = flibc_intflip(to_4);
            }
            memcpy(out, &to_4, 4);
            *outdtsz = 4;
            break;
        case 2:
            if (from_8 > SHRT_MAX)
                return -1;
            to_2 = from_8;

            if (to_flip) {
                to_2 = flibc_shortflip(to_2);
            }
            memcpy(out, &to_2, 2);
            *outdtsz = 2;
            break;
        default:
            return -1;
            break;
        }
        break;

    case 4:
        memcpy(&from_4, in, 4);
        if (from_flip) {
            from_4 = flibc_intflip(from_4);
        }

        switch (outlen) {
        case 8:
            to_8 = from_4;

            if (to_flip) {
                to_8 = flibc_llflip(to_8);
            }
            memcpy(out, &to_8, 8);
            *outdtsz = 8;
            break;
        case 4:
            if (from_4 > INT_MAX)
                return -1;
            to_4 = from_4;

            if (to_flip) {
                to_4 = flibc_intflip(to_4);
            }
            memcpy(out, &to_4, 4);
            *outdtsz = 4;
            break;
        case 2:
            if (from_4 > SHRT_MAX)
                return -1;
            to_2 = from_4;

            if (to_flip) {
                to_2 = flibc_shortflip(to_2);
            }
            memcpy(out, &to_2, 2);
            *outdtsz = 2;
            break;
        default:
            return -1;
            break;
        }
        break;

    case 2:
        memcpy(&from_2, in, 2);
        if (from_flip) {
            from_2 = flibc_shortflip(from_2);
        }

        switch (outlen) {
        case 8:
            to_8 = from_2;
            if (to_flip) {
                to_8 = flibc_llflip(to_8);
            }
            memcpy(out, &to_8, 8);
            *outdtsz = 8;
            break;
        case 4:
            to_4 = from_2;

            if (to_flip) {
                to_4 = flibc_intflip(to_4);
            }
            memcpy(out, &to_4, 4);
            *outdtsz = 4;
            break;
        case 2:
            if (from_2 > SHRT_MAX)
                return -1;
            to_2 = from_2;

            if (to_flip) {
                to_2 = flibc_shortflip(to_2);
            }
            memcpy(out, &to_2, 2);
            *outdtsz = 2;
            break;
        default:
            return -1;
            break;
        }
        break;

    default:
        return -1;
        break;
    }

    return 0;
}

TYPES_INLINE int CLIENT_UINT_to_CLIENT_REAL(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    unsigned long long from_8;
    unsigned int from_4;
    unsigned short from_2;

    double to_8;
    float to_4;
    int from_flip = 0;
    int to_flip = 0;

    /* Determine if the from-data is little endian. */
    CHECK_FLIP(inopts, from_flip);

    /* Determine if the to-data is little endian. */
    CHECK_FLIP(outopts, to_flip);

    *outdtsz = 0;
    switch (inlen) {
    case 8:
        memcpy(&from_8, in, 8);
        if (from_flip) {
            from_8 = flibc_llflip(from_8);
        }

        switch (outlen) {
        case 8:
            if (REAL_OUTOFRANGE(from_8, DBL_MAX))
                return -1;
            to_8 = from_8;

            if (to_flip) {
                to_8 = flibc_dblflip(to_8);
            }
            memcpy(out, &to_8, 8);
            *outdtsz = 8;
            break;
        case 4:
            if (REAL_OUTOFRANGE(from_8, FLT_MAX))
                return -1;
            to_4 = from_8;

            if (to_flip) {
                to_4 = flibc_floatflip(to_4);
            }
            memcpy(out, &to_4, 4);
            *outdtsz = 4;
            break;
        default:
            return -1;
            break;
        }
        break;

    case 4:
        memcpy(&from_4, in, 4);
        if (from_flip) {
            from_4 = flibc_intflip(from_4);
        }

        switch (outlen) {
        case 8:
            if (REAL_OUTOFRANGE(from_4, DBL_MAX))
                return -1;
            to_8 = from_4;

            if (to_flip) {
                to_8 = flibc_dblflip(to_8);
            }

            *outdtsz = 8;
            memcpy(out, &to_8, 8);
            break;
        case 4:
            if (REAL_OUTOFRANGE(from_4, FLT_MAX))
                return -1;
            to_4 = from_4;

            if (to_flip) {
                to_4 = flibc_floatflip(to_4);
            }
            memcpy(out, &to_4, 4);
            *outdtsz = 4;
            break;
        default:
            return -1;
            break;
        }
        break;

    case 2:
        memcpy(&from_2, in, 2);
        if (from_flip) {
            from_2 = flibc_shortflip(from_2);
        }

        switch (outlen) {
        case 8:
            if (REAL_OUTOFRANGE(from_2, DBL_MAX))
                return -1;
            to_8 = from_2;

            if (to_flip) {
                to_8 = flibc_dblflip(to_8);
            }
            memcpy(out, &to_8, 8);
            *outdtsz = 8;
            break;
        case 4:
            to_4 = from_2;

            if (to_flip) {
                to_4 = flibc_floatflip(to_4);
            }
            memcpy(out, &to_4, 4);
            if (REAL_OUTOFRANGE(from_2, FLT_MAX))
                return -1;
            *outdtsz = 4;
            break;
        default:
            return -1;
            break;
        }
        break;

    default:
        return -1;
        break;
    }

    return 0;
}

TYPES_INLINE int CLIENT_UINT_to_CLIENT_CSTR(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    unsigned long long from_8;
    unsigned int from_4;
    unsigned short from_2;
    int rc;

    char *to;

    int i;

    int from_flip = 0;

    /* Determine if the from-data is little endian. */
    CHECK_FLIP(inopts, from_flip);

    /* To-little doesn't matter for strings. */

    to = (char *)out;

    *outdtsz = 0;

    switch (inlen) {
    case 8:
        memcpy(&from_8, in, 8);
        if (from_flip) {
            from_8 = flibc_llflip(from_8);
        }

        rc = snprintf(out, outlen, "%llu", from_8);
        break;

    case 4:
        memcpy(&from_4, in, 4);
        if (from_flip) {
            from_4 = flibc_intflip(from_4);
        }

        rc = snprintf(out, outlen, "%u", from_4);
        break;

    case 2:
        memcpy(&from_2, in, 2);
        if (from_flip) {
            from_2 = flibc_shortflip(from_2);
        }

        rc = snprintf(out, outlen, "%hu", from_2);
        break;

    default:
        return -1;
        break;
    }

    if (rc >= outlen) /* couldn't fit into string */
        return -1;

    /* zap end to be nulls past the null byte */
    for (i = 0; to[i] != '\0'; i++)
        ;
    *outdtsz = i;
    for (; i < outlen; i++)
        to[i] = '\0';

    return 0;
}

TYPES_INLINE int CLIENT_UINT_to_CLIENT_PSTR(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    unsigned long long from_8;
    unsigned int from_4;
    unsigned short from_2;
    int rc;

    char *to;

    char *outbuf;

    int i;

    int from_flip = 0;

    /* Determine if the from-data is little endian. */
    CHECK_FLIP(inopts, from_flip);

    *outdtsz = 0;
    outbuf = alloca(outlen + 1);

    to = (char *)out;

    switch (inlen) {
    case 8:
        memcpy(&from_8, in, 8);
        if (from_flip) {
            from_8 = flibc_llflip(from_8);
        }
        rc = snprintf(outbuf, outlen + 1, "%llu", from_8);
        break;

    case 4:
        memcpy(&from_4, in, 4);
        if (from_flip) {
            from_4 = flibc_intflip(from_4);
        }
        rc = snprintf(outbuf, outlen + 1, "%u", from_4);
        break;

    case 2:
        memcpy(&from_2, in, 2);
        if (from_flip) {
            from_2 = flibc_shortflip(from_2);
        }
        rc = snprintf(outbuf, outlen + 1, "%hu", from_2);
        break;

    default:
        return -1;
        break;
    }

    if (rc > outlen) { /* couldn't fit into string */
        return -1;
    }

    memcpy(out, outbuf, outlen);

    /* zap end to be spaces past the null byte */
    for (i = 0; to[i] != '\0'; i++)
        ;
    *outdtsz = outlen;
    for (; i < outlen; i++)
        to[i] = ' ';

    return 0;
}

TYPES_INLINE int CLIENT_UINT_to_CLIENT_PSTR2(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    unsigned long long from_8;
    unsigned int from_4;
    unsigned short from_2;
    int rc;

    char *to;

    char *outbuf;

    int i;

    int from_flip = 0;

    /* Determine if the from-data is little endian. */
    CHECK_FLIP(inopts, from_flip);

    outbuf = alloca(outlen + 1);

    to = (char *)out;
    *outdtsz = 0;
    switch (inlen) {
    case 8:
        memcpy(&from_8, in, 8);
        if (from_flip) {
            from_8 = flibc_llflip(from_8);
        }

        rc = snprintf(outbuf, outlen + 1, "%llu", from_8);
        break;

    case 4:
        memcpy(&from_4, in, 4);

        if (from_flip) {
            from_4 = flibc_intflip(from_4);
        }

        rc = snprintf(outbuf, outlen + 1, "%u", from_4);
        break;

    case 2:
        memcpy(&from_2, in, 2);

        if (from_flip) {
            from_2 = flibc_shortflip(from_2);
        }

        rc = snprintf(outbuf, outlen + 1, "%hu", from_2);
        break;

    default:
        return -1;
        break;
    }

    if (rc > outlen) { /* couldn't fit into string */
        return -1;
    }

    memcpy(out, outbuf, outlen);

    /* zap end to be spaces past the null byte */
    for (i = 0; to[i] != '\0'; i++)
        ;
    *outdtsz = i;
    for (; i < outlen; i++)
        to[i] = ' ';

    return 0;
}

TYPES_INLINE int CLIENT_UINT_to_CLIENT_BYTEARRAY(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outle, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    return -1;
}

TYPES_INLINE int CLIENT_INT_to_CLIENT_UINT(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    signed long long from_8;
    signed int from_4;
    signed short from_2;
    int from_flip = 0;

    unsigned long long to_8;
    unsigned int to_4;
    unsigned short to_2;
    int to_flip = 0;

    /* Determine if the from-data is little endian. */
    CHECK_FLIP(inopts, from_flip);

    /* Determine if the to-data is little endian. */
    CHECK_FLIP(outopts, to_flip);

    *outdtsz = 0;

    switch (inlen) {
    case 8:
        memcpy(&from_8, in, 8);

        if (from_flip) {
            from_8 = flibc_llflip(from_8);
        }

        if (from_8 < 0)
            return -1;
        switch (outlen) {
        case 8:
            to_8 = from_8;

            if (to_flip) {
                to_8 = flibc_llflip(to_8);
            }
            memcpy(out, &to_8, 8);
            *outdtsz = 8;
            break;
        case 4:
            if (from_8 > UINT_MAX)
                return -1;
            to_4 = from_8;

            if (to_flip) {
                to_4 = flibc_intflip(to_4);
            }
            memcpy(out, &to_4, 4);
            *outdtsz = 4;
            break;
        case 2:
            if (from_8 > USHRT_MAX)
                return -1;
            to_2 = from_8;

            if (to_flip) {
                to_2 = flibc_shortflip(to_2);
            }

            memcpy(out, &to_2, 2);
            *outdtsz = 2;
            break;
        default:
            return -1;
            break;
        }
        break;

    case 4:
        memcpy(&from_4, in, 4);

        if (from_flip) {
            from_4 = flibc_intflip(from_4);
        }

        if (from_4 < 0)
            return -1;
        switch (outlen) {
        case 8:
            to_8 = from_4;
            if (to_flip) {
                to_8 = flibc_llflip(to_8);
            }
            memcpy(out, &to_8, 8);
            *outdtsz = 8;
            break;
        case 4:
            to_4 = from_4;
            if (to_flip) {
                to_4 = flibc_intflip(to_4);
            }
            memcpy(out, &to_4, 4);
            *outdtsz = 4;
            break;
        case 2:
            if (from_4 > USHRT_MAX)
                return -1;
            to_2 = from_4;
            if (to_flip) {
                to_2 = flibc_shortflip(to_2);
            }
            memcpy(out, &to_2, 2);
            *outdtsz = 2;
            break;
        default:
            return -1;
            break;
        }
        break;

    case 2:
        memcpy(&from_2, in, 2);
        if (from_flip) {
            from_2 = flibc_shortflip(from_2);
        }

        if (from_2 < 0)
            return -1;
        switch (outlen) {
        case 8:
            to_8 = from_2;

            if (to_flip) {
                to_8 = flibc_llflip(to_8);
            }
            memcpy(out, &to_8, 8);
            *outdtsz = 8;
            break;
        case 4:
            to_4 = from_2;

            if (to_flip) {
                to_4 = flibc_intflip(to_4);
            }

            memcpy(out, &to_4, 4);
            *outdtsz = 4;
            break;
        case 2:
            to_2 = from_2;

            if (to_flip) {
                to_2 = flibc_shortflip(to_2);
            }

            memcpy(out, &to_2, 2);
            *outdtsz = 2;
            break;
        default:
            return -1;
            break;
        }
        break;

    default:
        return -1;
        break;
    }

    return 0;
}

TYPES_INLINE int CLIENT_INT_to_CLIENT_INT(const void *in, int inlen,
                                          const struct field_conv_opts *inopts,
                                          blob_buffer_t *inblob, void *out,
                                          int outlen, int *outdtsz,
                                          const struct field_conv_opts *outopts,
                                          blob_buffer_t *outblob)
{
    signed long long from_8;
    signed int from_4;
    signed short from_2;
    int from_flip = 0;

    signed long long to_8;
    signed int to_4;
    signed short to_2;
    int to_flip = 0;

    /* Determine if the from-data is little endian. */
    CHECK_FLIP(inopts, from_flip);

    /* Determine if the to-data is little endian. */
    CHECK_FLIP(outopts, to_flip);

    *outdtsz = 0;

    switch (inlen) {
    case 8:
        memcpy(&from_8, in, 8);
        if (from_flip) {
            from_8 = flibc_llflip(from_8);
        }

        switch (outlen) {
        case 8:
            to_8 = from_8;

            if (to_flip) {
                to_8 = flibc_llflip(to_8);
            }
            memcpy(out, &to_8, 8);
            *outdtsz = 8;
            break;
        case 4:
            if (from_8 > INT_MAX || from_8 < INT_MIN)
                return -1;
            to_4 = from_8;

            if (to_flip) {
                to_4 = flibc_intflip(to_4);
            }
            memcpy(out, &to_4, 4);
            *outdtsz = 4;
            break;
        case 2:
            if (from_8 > SHRT_MAX || from_8 < SHRT_MIN)
                return -1;
            to_2 = from_8;

            if (to_flip) {
                to_2 = flibc_shortflip(to_2);
            }
            memcpy(out, &to_2, 2);
            *outdtsz = 2;
            break;
        default:
            return -1;
            break;
        }
        break;

    case 4:
        memcpy(&from_4, in, 4);
        if (from_flip) {
            from_4 = flibc_intflip(from_4);
        }

        switch (outlen) {
        case 8:
            to_8 = from_4;

            if (to_flip) {
                to_8 = flibc_llflip(to_8);
            }
            memcpy(out, &to_8, 8);
            *outdtsz = 8;
            break;
        case 4:
            to_4 = from_4;

            if (to_flip) {
                to_4 = flibc_intflip(to_4);
            }
            memcpy(out, &to_4, 4);
            *outdtsz = 4;
            break;
        case 2:
            if (from_4 > SHRT_MAX || from_4 < SHRT_MIN)
                return -1;
            to_2 = from_4;

            if (to_flip) {
                to_2 = flibc_shortflip(to_2);
            }
            memcpy(out, &to_2, 2);
            *outdtsz = 2;
            break;
        default:
            return -1;
            break;
        }
        break;

    case 2:
        memcpy(&from_2, in, 2);
        if (from_flip) {
            from_2 = flibc_shortflip(from_2);
        }

        switch (outlen) {
        case 8:
            to_8 = from_2;

            if (to_flip) {
                to_8 = flibc_llflip(to_8);
            }
            memcpy(out, &to_8, 8);
            *outdtsz = 8;
            break;
        case 4:
            to_4 = from_2;

            if (to_flip) {
                to_4 = flibc_intflip(to_4);
            }
            memcpy(out, &to_4, 4);
            *outdtsz = 4;
            break;
        case 2:
            to_2 = from_2;

            if (to_flip) {
                to_2 = flibc_shortflip(to_2);
            }
            memcpy(out, &to_2, 2);
            *outdtsz = 2;
            break;
        default:
            return -1;
            break;
        }
        break;

    default:
        return -1;
        break;
    }

    return 0;
}

TYPES_INLINE int CLIENT_INT_to_CLIENT_REAL(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    signed long long from_8;
    signed int from_4;
    signed short from_2;
    int from_flip = 0;
    int to_flip = 0;

    double to_8;
    float to_4;

    /* Determine if the from-data is little endian. */
    CHECK_FLIP(inopts, from_flip);

    /* Determine if the to-data is little endian. */
    CHECK_FLIP(outopts, to_flip);

    *outdtsz = 0;

    switch (inlen) {
    case 8:
        memcpy(&from_8, in, 8);
        if (from_flip) {
            from_8 = flibc_llflip(from_8);
        }

        switch (outlen) {
        case 8:
            if (REAL_OUTOFRANGE(from_8, DBL_MAX))
                return -1;
            to_8 = from_8;

            if (to_flip) {
                to_8 = flibc_dblflip(to_8);
            }
            memcpy(out, &to_8, 8);
            *outdtsz = 8;
            break;
        case 4:
            if (REAL_OUTOFRANGE(from_8, FLT_MAX))
                return -1;
            to_4 = from_8;

            if (to_flip) {
                to_4 = flibc_floatflip(to_4);
            }
            memcpy(out, &to_4, 4);
            *outdtsz = 4;
            break;
        default:
            return -1;
            break;
        }
        break;

    case 4:
        memcpy(&from_4, in, 4);

        if (from_flip) {
            from_4 = flibc_intflip(from_4);
        }

        switch (outlen) {
        case 8:
            if (REAL_OUTOFRANGE(from_4, DBL_MAX))
                return -1;
            to_8 = from_4;

            if (to_flip) {
                to_8 = flibc_dblflip(to_8);
            }

            memcpy(out, &to_8, 8);
            *outdtsz = 8;
            break;
        case 4:
            if (REAL_OUTOFRANGE(from_4, FLT_MAX))
                return -1;
            to_4 = from_4;
            if (to_flip) {
                to_4 = flibc_floatflip(to_4);
            }
            memcpy(out, &to_4, 4);
            *outdtsz = 4;
            break;
        default:
            return -1;
            break;
        }
        break;

    case 2:
        memcpy(&from_2, in, 2);

        if (from_flip) {
            from_2 = flibc_shortflip(from_2);
        }

        switch (outlen) {
        case 8:
            if (REAL_OUTOFRANGE(from_2, DBL_MAX))
                return -1;
            to_8 = (double)from_2;

            if (to_flip) {
                to_8 = flibc_dblflip(to_8);
            }

            memcpy(out, &to_8, outlen);
            *outdtsz = 8;
            break;

        case 4:
            if (REAL_OUTOFRANGE(from_2, FLT_MAX))
                return -1;
            to_4 = (float)from_2;

            if (to_flip) {
                to_4 = flibc_floatflip(to_4);
            }

            memcpy(out, &to_4, outlen);
            *outdtsz = 4;
            break;

        default:
            break;
        }
        break;

    default:
        return -1;
        break;
    }

    return 0;
}

TYPES_INLINE int CLIENT_INT_to_CLIENT_CSTR(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    signed long long from_8;
    signed int from_4;
    signed short from_2;
    int rc;
    int from_flip = 0;

    char *to;

    int i;

    to = (char *)out;

    /* Determine if the from-data is little endian. */
    CHECK_FLIP(inopts, from_flip);

    *outdtsz = 0;

    switch (inlen) {
    case 8:
        memcpy(&from_8, in, 8);
        if (from_flip) {
            from_8 = flibc_llflip(from_8);
        }
        rc = snprintf(out, outlen, "%lld", from_8);
        break;

    case 4:
        memcpy(&from_4, in, 4);
        if (from_flip) {
            from_4 = flibc_intflip(from_4);
        }

        rc = snprintf(out, outlen, "%d", from_4);
        break;

    case 2:
        memcpy(&from_2, in, 2);
        if (from_flip) {
            from_2 = flibc_shortflip(from_2);
        }
        rc = snprintf(out, outlen, "%hd", from_2);
        break;

    default:
        return -1;
        break;
    }

    if (rc >= outlen)
        return -1;

    /* zap end to be nulls past the null byte */
    for (i = 0; to[i] != '\0'; i++)
        ;
    *outdtsz = i;
    for (; i < outlen; i++)
        to[i] = '\0';

    return 0;
}

TYPES_INLINE int CLIENT_INT_to_CLIENT_PSTR(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    signed long long from_8;
    signed int from_4;
    signed short from_2;
    int rc;
    int from_flip = 0;

    char *to;

    char *outbuf;

    int i;

    /* Determine if the from-data is little endian. */
    CHECK_FLIP(inopts, from_flip);

    outbuf = alloca(outlen + 1);

    to = (char *)out;

    *outdtsz = 0;

    switch (inlen) {
    case 8:
        memcpy(&from_8, in, 8);
        if (from_flip) {
            from_8 = flibc_llflip(from_8);
        }

        rc = snprintf(outbuf, outlen + 1, "%lld", from_8);
        break;

    case 4:
        memcpy(&from_4, in, 4);

        if (from_flip) {
            from_4 = flibc_intflip(from_4);
        }

        rc = snprintf(outbuf, outlen + 1, "%d", from_4);
        break;

    case 2:
        memcpy(&from_2, in, 2);

        if (from_flip) {
            from_2 = flibc_shortflip(from_2);
        }

        rc = snprintf(outbuf, outlen + 1, "%hd", from_2);
        break;

    default:
        return -1;
        break;
    }

    if (rc > outlen) {
        return -1;
    }

    memcpy(out, outbuf, outlen);

    /* zap end to be spaces past the null byte */
    for (i = 0; to[i] != '\0'; i++)
        ;
    for (; i < outlen; i++)
        to[i] = ' ';
    *outdtsz = outlen;
    return 0;
}

TYPES_INLINE int CLIENT_INT_to_CLIENT_PSTR2(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    signed long long from_8;
    signed int from_4;
    signed short from_2;
    int rc;
    int from_flip = 0;

    char *to;

    char *outbuf;

    int i;

    CHECK_FLIP(inopts, from_flip);

    outbuf = alloca(outlen + 1);

    to = (char *)out;
    *outdtsz = 0;
    switch (inlen) {
    case 8:
        memcpy(&from_8, in, 8);

        if (from_flip) {
            from_8 = flibc_llflip(from_8);
        }

        rc = snprintf(outbuf, outlen + 1, "%lld", from_8);
        break;

    case 4:
        memcpy(&from_4, in, 4);

        if (from_flip) {
            from_4 = flibc_intflip(from_4);
        }

        rc = snprintf(outbuf, outlen + 1, "%d", from_4);
        break;

    case 2:
        memcpy(&from_2, in, 2);

        if (from_flip) {
            from_2 = flibc_shortflip(from_2);
        }

        rc = snprintf(outbuf, outlen + 1, "%hd", from_2);
        break;

    default:
        return -1;
        break;
    }

    if (rc > outlen) {
        return -1;
    }

    memcpy(out, outbuf, outlen);

    /* zap end to be spaces past the null byte */
    for (i = 0; to[i] != '\0'; i++)
        ;
    *outdtsz = i;
    for (; i < outlen; i++)
        to[i] = ' ';

    return 0;
}

TYPES_INLINE int CLIENT_INT_to_CLIENT_BYTEARRAY(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    return -1;
}

TYPES_INLINE int CLIENT_REAL_to_CLIENT_UINT(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    double from_8;
    float from_4;

    unsigned long long to_8;
    unsigned int to_4;
    unsigned short to_2;

    int from_flip = 0;
    int to_flip = 0;

    /* Determine if the from-data is little endian. */
    CHECK_FLIP(inopts, from_flip);

    /* Determine if the to-data is little endian. */
    CHECK_FLIP(outopts, to_flip);

    *outdtsz = 0;

    switch (inlen) {
    case 8:
        memcpy(&from_8, in, 8);

        if (from_flip) {
            from_8 = flibc_dblflip(from_8);
        }

        switch (outlen) {
        case 8:
            if (from_8 > FLIBC_DBL_ULLONG_MAX)
                return -1;
            if (from_8 < 0)
                return -1;
            to_8 = from_8;

            if (to_flip) {
                to_8 = flibc_llflip(to_8);
            }
            memcpy(out, &to_8, 8);
            *outdtsz = 8;
            break;
        case 4:
            if (from_8 > FLIBC_DBL_UINT_MAX)
                return -1;
            if (from_8 < 0)
                return -1;
            to_4 = from_8;

            if (to_flip) {
                to_4 = flibc_intflip(to_4);
            }
            memcpy(out, &to_4, 4);
            *outdtsz = 4;
            break;
        case 2:
            if (from_8 > FLIBC_DBL_USHRT_MAX)
                return -1;
            if (from_8 < 0)
                return -1;
            to_2 = from_8;

            if (to_flip) {
                to_2 = flibc_shortflip(to_2);
            }
            memcpy(out, &to_2, 2);
            *outdtsz = 2;
            break;

        default:
            return -1;
            break;
        }
        break;

    case 4:
        memcpy(&from_4, in, 4);

        if (from_flip) {
            from_4 = flibc_floatflip(from_4);
        }

        switch (outlen) {
        case 8:
            if (from_4 > FLIBC_FLT_ULLONG_MAX)
                return -1;
            if (from_4 < 0)
                return -1;
            to_8 = from_4;

            if (to_flip) {
                to_8 = flibc_llflip(to_8);
            }
            memcpy(out, &to_8, 8);
            *outdtsz = 8;
            break;
        case 4:
            if (from_4 > FLIBC_FLT_UINT_MAX)
                return -1;
            if (from_4 < 0)
                return -1;
            to_4 = from_4;

            if (to_flip) {
                to_4 = flibc_intflip(to_4);
            }
            memcpy(out, &to_4, 4);
            *outdtsz = 4;
            break;
        case 2:
            if (from_4 > FLIBC_FLT_USHRT_MAX)
                return -1;
            if (from_4 < 0)
                return -1;
            to_2 = from_4;

            if (to_flip) {
                to_2 = flibc_shortflip(to_2);
            }
            memcpy(out, &to_2, 2);
            *outdtsz = 2;
            break;

        default:
            return -1;
            break;
        }
        break;

    default:
        return -1;
        break;
    }

    return 0;
}

TYPES_INLINE int CLIENT_REAL_to_CLIENT_INT(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    double from_8;
    float from_4;

    signed long long to_8;
    signed int to_4;
    signed short to_2;

    int from_flip = 0;
    int to_flip = 0;

    /* Determine if the from-data is little endian. */
    CHECK_FLIP(inopts, from_flip);

    /* Determine if the to-data is little endian. */
    CHECK_FLIP(outopts, to_flip);

    *outdtsz = 0;

    switch (inlen) {
    case 8:
        memcpy(&from_8, in, 8);

        if (from_flip) {
            from_8 = flibc_dblflip(from_8);
        }

        switch (outlen) {
        case 8:
            if (from_8 > FLIBC_DBL_LLONG_MAX)
                return -1;
            if (from_8 < FLIBC_DBL_LLONG_MIN)
                return -1;
            to_8 = from_8;

            if (to_flip) {
                to_8 = flibc_llflip(to_8);
            }
            memcpy(out, &to_8, 8);
            *outdtsz = 8;
            break;
        case 4:
            if (from_8 > FLIBC_DBL_INT_MAX)
                return -1;
            if (from_8 < FLIBC_DBL_INT_MIN)
                return -1;
            to_4 = from_8;

            if (to_flip) {
                to_4 = flibc_intflip(to_4);
            }
            memcpy(out, &to_4, 4);
            *outdtsz = 4;
            break;
        case 2:
            if (from_8 > FLIBC_DBL_SHRT_MAX)
                return -1;
            if (from_8 < FLIBC_DBL_SHRT_MIN)
                return -1;
            to_2 = from_8;

            if (to_flip) {
                to_2 = flibc_shortflip(to_2);
            }
            memcpy(out, &to_2, 2);
            *outdtsz = 2;
            break;

        default:
            return -1;
            break;
        }
        break;

    case 4:
        memcpy(&from_4, in, 4);

        if (from_flip) {
            from_4 = flibc_floatflip(from_4);
        }

        switch (outlen) {
        case 8:
            if (from_4 > FLIBC_FLT_LLONG_MAX)
                return -1;
            if (from_4 < FLIBC_FLT_LLONG_MIN)
                return -1;
            to_8 = from_4;

            if (to_flip) {
                to_8 = flibc_llflip(to_8);
            }
            memcpy(out, &to_8, 8);
            *outdtsz = 8;
            break;
        case 4:
            if (from_4 > FLIBC_FLT_INT_MAX)
                return -1;
            if (from_4 < FLIBC_FLT_INT_MIN)
                return -1;
            to_4 = from_4;

            if (to_flip) {
                to_4 = flibc_intflip(to_4);
            }
            memcpy(out, &to_4, 4);
            *outdtsz = 4;
            break;
        case 2:
            if (from_4 > FLIBC_FLT_SHRT_MAX)
                return -1;
            if (from_4 < FLIBC_FLT_SHRT_MIN)
                return -1;
            to_2 = from_4;

            if (to_flip) {
                to_2 = flibc_shortflip(to_2);
            }

            memcpy(out, &to_2, 2);
            *outdtsz = 2;
            break;

        default:
            return -1;
            break;
        }
        break;

    default:
        return -1;
        break;
    }

    return 0;
}

TYPES_INLINE int CLIENT_REAL_to_CLIENT_CSTR(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    double from_8;
    float from_4;
    int rc;
    int from_flip = 0;

    char *to;

    int i;
    *outdtsz = 0;

    /* Determine if the from-data is little endian. */
    CHECK_FLIP(inopts, from_flip);

    to = (char *)out;

    switch (inlen) {
    case 8:
        memcpy(&from_8, in, 8);
        if (from_flip) {
            from_8 = flibc_dblflip(from_8);
        }

        rc = flibc_snprintf_dbl(out, outlen, from_8);
        break;

    case 4:
        memcpy(&from_4, in, 4);

        if (from_flip) {
            from_4 = flibc_floatflip(from_4);
        }

        rc = flibc_snprintf_dbl(out, outlen, from_4);
        break;

    default:
        return -1;
        break;
    }

    if (rc)
        return -1;

    /* zap end to be nulls past the null byte */
    for (i = 0; to[i] != '\0'; i++)
        ;
    *outdtsz = i;

    for (; i < outlen; i++)
        to[i] = '\0';

    return 0;
}

TYPES_INLINE int CLIENT_REAL_to_CLIENT_PSTR(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    double from_8;
    float from_4;
    int rc;
    int from_flip = 0;

    char *to;

    char *outbuf;

    int i;

    outbuf = alloca(outlen + 1);

    /* Determine if the from-data is little endian. */
    CHECK_FLIP(inopts, from_flip);

    to = (char *)out;
    *outdtsz = 0;
    switch (inlen) {
    case 8:
        memcpy(&from_8, in, 8);
        if (from_flip) {
            from_8 = flibc_dblflip(from_8);
        }

        rc = flibc_snprintf_dbl(outbuf, outlen + 1, from_8);
        break;

    case 4:
        memcpy(&from_4, in, 4);

        if (from_flip) {
            from_4 = flibc_floatflip(from_4);
        }

        rc = flibc_snprintf_dbl(outbuf, outlen + 1, from_4);
        break;

    default:
        return -1;
        break;
    }

    if (rc)
        return -1;

    /* why not flibc_snprintf_dbl() straight to out above? Because it doesn't
     * need to be null terminated?  */
    memcpy(out, outbuf, outlen);

    /* zap end to be spaces past the null byte */
    for (i = 0; to[i] != '\0'; i++)
        ;
    for (; i < outlen; i++)
        to[i] = ' ';
    *outdtsz = outlen;
    return 0;
}

TYPES_INLINE int CLIENT_REAL_to_CLIENT_PSTR2(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    double from_8;
    float from_4;
    int rc;

    char *to;

    char *outbuf;

    int from_flip = 0;
    int i;
    *outdtsz = 0;
    outbuf = alloca(outlen + 1);

    to = (char *)out;

    /* Determine if the from-data is little endian. */
    CHECK_FLIP(inopts, from_flip);

    switch (inlen) {
    case 8:
        memcpy(&from_8, in, 8);

        if (from_flip) {
            from_8 = flibc_dblflip(from_8);
        }

        rc = flibc_snprintf_dbl(outbuf, outlen + 1, from_8);
        break;

    case 4:
        memcpy(&from_4, in, 4);

        if (from_flip) {
            from_4 = flibc_floatflip(from_4);
        }

        rc = flibc_snprintf_dbl(outbuf, outlen + 1, from_4);
        break;

    default:
        return -1;
        break;
    }

    if (rc)
        return -1;

    memcpy(out, outbuf, outlen);

    /* zap end to be spaces past the null byte */
    for (i = 0; to[i] != '\0'; i++)
        ;
    *outdtsz = i;
    for (; i < outlen; i++)
        to[i] = ' ';

    return 0;
}

TYPES_INLINE int CLIENT_REAL_to_CLIENT_BYTEARRAY(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    return -1;
}

TYPES_INLINE int CLIENT_REAL_to_CLIENT_REAL(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    float from_4, to_4;
    double from_8, to_8;

    *outdtsz = 0;

    int from_flip = 0;
    int to_flip = 0;

    /* Determine if the from-data is little endian. */
    CHECK_FLIP(inopts, from_flip);

    /* Determine if the to-data is little endian. */
    CHECK_FLIP(outopts, to_flip);

    switch (inlen) {
    case 4:
        memcpy(&from_4, in, inlen);

        if (from_flip) {
            from_4 = flibc_floatflip(from_4);
        }

        from_8 = (double)from_4;
        break;
    case 8:
        memcpy(&from_8, in, inlen);

        if (from_flip) {
            from_8 = flibc_dblflip(from_8);
        }

        break;
    default:
        return -1;
    }

    switch (outlen) {
    case 4:
        if (REAL_OUTOFRANGE(from_8, FLT_MAX))
            return -1;
        to_4 = (float)from_8;

        if (to_flip) {
            to_4 = flibc_floatflip(to_4);
        }

        memcpy(out, &to_4, outlen);
        *outdtsz = 4;
        break;
    case 8:
        to_8 = from_8;

        if (to_flip) {
            to_8 = flibc_dblflip(to_8);
        }
        memcpy(out, &to_8, outlen);
        *outdtsz = 8;
        break;
    default:
        return -1;
    }
    return 0;
}

TYPES_INLINE int CLIENT_UINT_to_SERVER_UINT(
    const void *in, int inlen, int isnull, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    unsigned long long from_8;
    unsigned int from_4;
    unsigned short from_2;
    int from_flip = 0;

    unsigned long long to_8;
    unsigned int to_4;
    unsigned short to_2;

    /* Determine if the from-data is little endian. */
    CHECK_FLIP(inopts, from_flip);

    /* Never store server-side data in little-endian. */

    *outdtsz = 0;

    if (isnull) {
        set_null(out, outlen);
        return 0;
    }

    switch (inlen) {
    case 8:
        memcpy(&from_8, in, 8);

        if (from_flip) {
            from_8 = flibc_llflip(from_8);
        }

        switch (outlen) {
        case 9:
            to_8 = from_8;
            to_8 = flibc_htonll(to_8);
            set_data(out, &to_8, 9);
            *outdtsz = 9;
            break;
        case 5:
            if (from_8 > UINT_MAX)
                return -1;
            to_4 = from_8;
            to_4 = htonl(to_4);
            set_data(out, &to_4, 5);
            *outdtsz = 5;
            break;
        case 3:
            if (from_8 > USHRT_MAX)
                return -1;
            to_2 = from_8;
            to_2 = htons(to_2);
            set_data(out, &to_2, 3);
            *outdtsz = 3;
            break;
        default:
            return -1;
            break;
        }
        break;

    case 4:
        memcpy(&from_4, in, 4);

        if (from_flip) {
            from_4 = flibc_intflip(from_4);
        }

        switch (outlen) {
        case 9:
            to_8 = from_4;
            to_8 = flibc_htonll(to_8);
            set_data(out, &to_8, outlen);
            *outdtsz = 9;
            break;
        case 5:
            to_4 = from_4;
            to_4 = htonl(to_4);
            set_data(out, &to_4, outlen);
            *outdtsz = 5;
            break;
        case 3:
            if (from_4 > USHRT_MAX)
                return -1;
            to_2 = from_4;
            to_2 = htons(to_2);
            set_data(out, &to_2, outlen);
            *outdtsz = 3;
            break;
        default:
            return -1;
            break;
        }
        break;

    case 2:
        memcpy(&from_2, in, 2);

        if (from_flip) {
            from_2 = flibc_shortflip(from_2);
        }

        switch (outlen) {
        case 9:
            to_8 = from_2;
            to_8 = flibc_htonll(to_8);
            set_data(out, &to_8, outlen);
            *outdtsz = 9;
            break;
        case 5:
            to_4 = from_2;
            to_4 = htonl(to_4);
            set_data(out, &to_4, outlen);
            *outdtsz = 5;
            break;
        case 3:
            to_2 = from_2;
            to_2 = htons(to_2);
            set_data(out, &to_2, outlen);
            *outdtsz = 3;
            break;
        default:
            return -1;
            break;
        }
        break;

    default:
        return -1;
        break;
    }

    return 0;
}

TYPES_INLINE int CLIENT_CSTR_to_CLIENT_UINT(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    unsigned long long to_8;
    unsigned int to_4;
    unsigned short to_2;
    char *endptr;
    int rc;
    int to_flip = 0;

    /* Determine if the to-data is little endian. */
    CHECK_FLIP(outopts, to_flip);

    /*   (from manpage)
         Because 0 and ULONG_MAX are returned on error and  are  also
         valid  returns  on  success, an application wishing to check
         for error situations  should  set  errno  to  0,  then  call
         strtoul(), then check errno and if it is non-zero, assume an
         error has occurred.
         */
    *outdtsz = 0;
    rc = cstrlenlim(in, inlen);
    if (rc >= inlen) /* not a valid C string of len inlen */
        return -1;
    if (rc == 0 && gbl_empty_strings_dont_convert_to_numbers)
        return -1;

    errno = 0;
    to_8 = strtoull(in, &endptr, 0);
    if (errno)
        return -1;
    if (!isspace(*endptr) && *endptr != '\0')
        return -1;

    switch (outlen) {
    case 8:
        if (to_flip) {
            to_8 = flibc_llflip(to_8);
        }
        memcpy(out, &to_8, outlen);
        *outdtsz = 8;
        break;
    case 4:
        if (to_8 > UINT_MAX)
            return -1;
        to_4 = (unsigned int)to_8;
        if (to_flip) {
            to_4 = flibc_intflip(to_4);
        }
        memcpy(out, &to_4, outlen);
        *outdtsz = 4;
        break;
    case 2:
        if (to_8 > USHRT_MAX)
            return -1;
        to_2 = (unsigned short)to_8;
        if (to_flip) {
            to_2 = flibc_shortflip(to_2);
        }

        memcpy(out, &to_2, outlen);
        *outdtsz = 2;
        break;
    default:
        return -1;
        break;
    }

    return 0;
}

TYPES_INLINE int CLIENT_CSTR_to_CLIENT_INT(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    long long to_8;
    int to_4;
    short to_2;
    char *endptr;
    int rc;
    *outdtsz = 0;
    int to_flip = 0;

    /* Determine if the to-data is little endian. */
    CHECK_FLIP(outopts, to_flip);

    rc = cstrlenlim(in, inlen);
    if (rc >= inlen) /* not a valid C string of len inlen */
        return -1;

    if (rc == 0 && gbl_empty_strings_dont_convert_to_numbers)
        return -1;

    errno = 0;

    to_8 = strtoll(in, &endptr, 0);
    if (errno)
        return -1;
    if (!isspace(*endptr) && *endptr != '\0')
        return -1;

    switch (outlen) {
    case 8:
        if (to_flip) {
            to_8 = flibc_llflip(to_8);
        }
        memcpy(out, &to_8, outlen);
        *outdtsz = 8;
        break;
    case 4:
        if (to_8 > INT_MAX || to_8 < INT_MIN)
            return -1;
        to_4 = (int)to_8;
        if (to_flip) {
            to_4 = flibc_intflip(to_4);
        }
        memcpy(out, &to_4, outlen);
        *outdtsz = 4;
        break;
    case 2:
        if (to_8 > SHRT_MAX || to_8 < SHRT_MIN)
            return -1;
        to_2 = (short)to_8;
        if (to_flip) {
            to_2 = flibc_shortflip(to_2);
        }
        memcpy(out, &to_2, outlen);
        *outdtsz = 2;
        break;
    default:
        return -1;
        break;
    }

    return 0;
}

TYPES_INLINE int CLIENT_CSTR_to_CLIENT_REAL(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    double to_8;
    float to_4;
    char *endptr;
    int rc;
    int to_flip = 0;

    /* Determine if the to-data is little endian. */
    CHECK_FLIP(outopts, to_flip);

    *outdtsz = 0;

    rc = cstrlenlim(in, inlen);
    if (rc >= inlen) /* not a valid C string of len inlen */
        return -1;
    if (rc == 0 && gbl_empty_strings_dont_convert_to_numbers)
        return -1;

    errno = 0;
    to_8 = strtod(in, &endptr);
    if (errno)
        return -1;
    if (*endptr != '\0' && !isspace(*endptr))
        return -1;

    switch (outlen) {
    case 8:

        if (to_flip) {
            to_8 = flibc_dblflip(to_8);
        }
        memcpy(out, &to_8, outlen);
        *outdtsz = 8;
        break;
    case 4:
        if (REAL_OUTOFRANGE(to_8, FLT_MAX))
            return -1;
        to_4 = (float)to_8;

        if (to_flip) {
            to_4 = flibc_floatflip(to_4);
        }
        memcpy(out, &to_4, outlen);
        *outdtsz = 4;
        break;
    default:
        return -1;
    }

    return 0;
}

/* for copying strings:
   a) if it fits, copy it regardless of in/out size mismatch
   b) if it doesnt, return error */
TYPES_INLINE int CLIENT_CSTR_to_CLIENT_CSTR(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    int len;
    len = cstrlenlim((char *)in, inlen);
    *outdtsz = 0;
    if (len < 0 || len >= inlen || len >= outlen)
        return -1;
    memset(out, 0, outlen);
    strncpy((char *)out, (char *)in, len);
    *outdtsz = strlen((char *)out);
    return 0;
}

TYPES_INLINE int CLIENT_CSTR_to_CLIENT_PSTR(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    int len;
    len = cstrlenlim((char *)in, inlen);
    *outdtsz = 0;
    if (len < 0 || len >= inlen || len > outlen)
        return -1;
    memset(out, ' ', outlen);
    strncpy((char *)out, (char *)in, len);
    *outdtsz = outlen;
    return 0;
}

TYPES_INLINE int CLIENT_CSTR_to_CLIENT_PSTR2(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    int len;
    *outdtsz = 0;
    len = cstrlenlim((char *)in, inlen);
    if (len < 0 || len >= inlen || len > outlen)
        return -1;
    memset(out, ' ', outlen);
    strncpy((char *)out, (char *)in, len);
    *outdtsz = len;
    return 0;
}

TYPES_INLINE int CLIENT_PSTR2_to_CLIENT_CSTR(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    int len;
    *outdtsz = 0;
    len = pstr2lenlim(in, inlen);
    if (len < 0 || len >= outlen)
        return -1;
    memset(out, 0, outlen);
    strncpy(out, in, len);
    *outdtsz = len;
    return 0;
}

TYPES_INLINE int CLIENT_PSTR2_to_CLIENT_INT(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    char *cstr;
    int rc;

    *outdtsz = 0;
    cstr = alloca(inlen + 1);
    rc = CLIENT_PSTR2_to_CLIENT_CSTR(in, inlen, inopts, inblob, cstr, inlen + 1,
                                     outdtsz, outopts, outblob);
    if (rc == -1)
        return rc;
    return CLIENT_CSTR_to_CLIENT_INT(cstr, inlen + 1, inopts, inblob, out,
                                     outlen, outdtsz, outopts, outblob);
}

/* NOTE: I cheated a little here: CLIENT_PSTR->* conversions convert to
 * CLIENT_CSTR first. */
TYPES_INLINE int CLIENT_PSTR2_to_CLIENT_UINT(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    char *cstr;
    int rc;
    *outdtsz = 0;
    cstr = alloca(inlen + 1);
    rc = CLIENT_PSTR2_to_CLIENT_CSTR(in, inlen, inopts, inblob, cstr, inlen + 1,
                                     outdtsz, outopts, outblob);
    if (rc == -1)
        return rc;
    return CLIENT_CSTR_to_CLIENT_UINT(cstr, inlen + 1, inopts, inblob, out,
                                      outlen, outdtsz, outopts, outblob);
}

TYPES_INLINE int CLIENT_PSTR2_to_CLIENT_REAL(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    char *cstr;
    int rc;
    *outdtsz = 0;
    cstr = alloca(inlen + 1);
    rc = CLIENT_PSTR2_to_CLIENT_CSTR(in, inlen, inopts, inblob, cstr, inlen + 1,
                                     outdtsz, outopts, outblob);
    if (rc == -1)
        return rc;
    return CLIENT_CSTR_to_CLIENT_REAL(cstr, inlen + 1, inopts, inblob, out,
                                      outlen, outdtsz, outopts, outblob);
}

TYPES_INLINE int CLIENT_PSTR2_to_CLIENT_PSTR(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    int len;
    *outdtsz = 0;
    len = pstr2lenlim(in, inlen);
    if (len == -1 || len > outlen)
        return -1;
    memset(out, ' ', outlen);
    *outdtsz = outlen;
    strncpy(out, in, len);
    return 0;
}

TYPES_INLINE int CLIENT_PSTR2_to_CLIENT_PSTR2(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    int len;
    *outdtsz = 0;
    len = pstr2lenlim(in, inlen);
    if (len == -1 || len > outlen)
        return -1;
    *outdtsz = len;
    strncpy(out, in, len);
    return 0;
}

TYPES_INLINE int CLIENT_PSTR_to_CLIENT_CSTR(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    int len;
    *outdtsz = 0;
    len = pstrlenlim(in, inlen);
    if (len < 0 || len >= outlen)
        return -1;
    memset(out, 0, outlen);
    strncpy(out, in, len);
    *outdtsz = len;
    return 0;
}

/* NOTE: I cheated a little here: CLIENT_PSTR->* conversions convert to
 * CLIENT_CSTR first. */
TYPES_INLINE int CLIENT_PSTR_to_CLIENT_UINT(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    char *cstr;
    int rc;
    *outdtsz = 0;
    cstr = alloca(inlen + 1);
    rc = CLIENT_PSTR_to_CLIENT_CSTR(in, inlen, inopts, inblob, cstr, inlen + 1,
                                    outdtsz, outopts, outblob);
    if (rc == -1)
        return rc;
    return CLIENT_CSTR_to_CLIENT_UINT(cstr, inlen + 1, inopts, inblob, out,
                                      outlen, outdtsz, outopts, outblob);
}

TYPES_INLINE int CLIENT_PSTR_to_CLIENT_INT(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    char *cstr;
    int rc;
    *outdtsz = 0;
    cstr = alloca(inlen + 1);
    rc = CLIENT_PSTR_to_CLIENT_CSTR(in, inlen, inopts, inblob, cstr, inlen + 1,
                                    outdtsz, outopts, outblob);
    if (rc == -1)
        return rc;
    return CLIENT_CSTR_to_CLIENT_INT(cstr, inlen + 1, inopts, inblob, out,
                                     outlen, outdtsz, outopts, outblob);
}

TYPES_INLINE int CLIENT_PSTR_to_CLIENT_REAL(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    char *cstr;
    int rc;
    *outdtsz = 0;
    cstr = alloca(inlen + 1);
    rc = CLIENT_PSTR_to_CLIENT_CSTR(in, inlen, inopts, inblob, cstr, inlen + 1,
                                    outdtsz, outopts, outblob);
    if (rc == -1)
        return rc;
    return CLIENT_CSTR_to_CLIENT_REAL(cstr, inlen + 1, inopts, inblob, out,
                                      outlen, outdtsz, outopts, outblob);
}

TYPES_INLINE int CLIENT_PSTR_to_CLIENT_PSTR(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    int len;
    *outdtsz = 0;
    len = pstrlenlim(in, inlen);
    if (len == -1 || len > outlen)
        return -1;
    memset(out, ' ', outlen);
    strncpy(out, in, len);
    *outdtsz = outlen;
    return 0;
}

TYPES_INLINE int CLIENT_PSTR_to_CLIENT_PSTR2(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    int len;
    *outdtsz = 0;
    len = pstrlenlim(in, inlen);
    if (len == -1 || len > outlen)
        return -1;
    *outdtsz = len;
    strncpy(out, in, len);
    return 0;
}

/* can't convert to/from byte arrays. they are by definition data that does not
 * get converted */
TYPES_INLINE int CLIENT_PSTR2_to_CLIENT_BYTEARRAY(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    return -1;
}

TYPES_INLINE int CLIENT_PSTR_to_CLIENT_BYTEARRAY(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    return -1;
}

TYPES_INLINE int CLIENT_CSTR_to_CLIENT_BYTEARRAY(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    return -1;
}

TYPES_INLINE int CLIENT_BYTEARRAY_to_CLIENT_UINT(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    return -1;
}

TYPES_INLINE int CLIENT_BYTEARRAY_to_CLIENT_INT(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    return -1;
}

TYPES_INLINE int CLIENT_BYTEARRAY_to_CLIENT_REAL(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    return -1;
}

TYPES_INLINE int CLIENT_BYTEARRAY_to_CLIENT_CSTR(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    return -1;
}

TYPES_INLINE int CLIENT_BYTEARRAY_to_CLIENT_PSTR(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    return -1;
}

TYPES_INLINE int CLIENT_BYTEARRAY_to_CLIENT_PSTR2(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    return -1;
}

TYPES_INLINE int CLIENT_BYTEARRAY_to_CLIENT_BYTEARRAY(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    return bytearray_copy(in, inlen, inopts, inblob, out, outlen, outdtsz,
                          outopts, outblob);
}

/* blobs don't convert to the other types since this layer does not have access
 * to the blob data.
 * Note: this is also #defined as CLIENT_VUTF8_to_CLIENT_VUTF8 so make sure any
 * changes work for both */
TYPES_INLINE int CLIENT_BLOB_to_CLIENT_BLOB(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    if (inlen < sizeof(client_blob_tp) || outlen < sizeof(client_blob_tp))
        return -1;

    if (inblob && outblob) {
        memcpy(outblob, inblob, sizeof(blob_buffer_t));
        bzero(inblob, sizeof(blob_buffer_t));
    }
    memcpy(out, in, sizeof(client_blob_tp));
    *outdtsz = sizeof(client_blob_tp);
    return 0;
}
TYPES_INLINE int CLIENT_UINT_to_CLIENT_BLOB(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    return -1;
}
TYPES_INLINE int CLIENT_INT_to_CLIENT_BLOB(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    return -1;
}
TYPES_INLINE int CLIENT_REAL_to_CLIENT_BLOB(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    return -1;
}
TYPES_INLINE int CLIENT_CSTR_to_CLIENT_BLOB(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    return -1;
}
TYPES_INLINE int CLIENT_PSTR_to_CLIENT_BLOB(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    return -1;
}
TYPES_INLINE int CLIENT_BYTEARRAY_to_CLIENT_BLOB(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    return -1;
}
TYPES_INLINE int CLIENT_PSTR2_to_CLIENT_BLOB(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    return -1;
}
TYPES_INLINE int CLIENT_BLOB_to_CLIENT_UINT(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    return -1;
}
TYPES_INLINE int CLIENT_BLOB_to_CLIENT_INT(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    return -1;
}
TYPES_INLINE int CLIENT_BLOB_to_CLIENT_REAL(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    return -1;
}
TYPES_INLINE int CLIENT_BLOB_to_CLIENT_CSTR(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    return -1;
}
TYPES_INLINE int CLIENT_BLOB_to_CLIENT_PSTR(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    return -1;
}
TYPES_INLINE int CLIENT_BLOB_to_CLIENT_BYTEARRAY(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    return -1;
}
TYPES_INLINE int CLIENT_BLOB_to_CLIENT_PSTR2(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    return -1;
}

/**
 * Finds out where the input vutf8 string is stored and then determines where it
 * should be copied and copies it. Doesn't deal with NULLs.
 * @param len       length of the data (vutf8 string including NUL byte) that
 *                  needs to be copied
 * @param in        pointer to the beginning of where data can be stored in the
 *                  in buffer (ie after on disk header or client blob struct)
 * @param in_len    length of the in buffer
 * @param out       pointer to the beginning of where data can be stored in the
 *                  in buffer (ie after on disk header or client blob struct)
 * @param out_len   length of the out buffer
 * @param inblob    in blob
 * @param outblob   out blob
 * @param outdtsz   the amount of data that is copied to in buffer or out buffer
 *                  (if any) is added to the int that this points to
 * @return 0 on success !0 otherwise
 */
static TYPES_INLINE int vutf8_convert(int len, const void *in, int in_len,
                                      void *out, int out_len,
                                      blob_buffer_t *inblob,
                                      blob_buffer_t *outblob, int *outdtsz)
{
    int valid_len;

    /* if the string was too large to be stored in the in buffer and won't fit
     * in the out buffer, then the string will just be transfered from one blob
     * to another */
    if (len > in_len && len > out_len) {

        if (!outblob)
            return -1;

        /* This only copies if we've passed in both an inblob and outblob, but
         * like
         * our blob-conversion code, it will always return success, whether or
         * not
         * this copy takes place.  The calling code expects this behavior. */
        if (inblob && outblob && inblob->exists) {
            /* validate input blob */
            assert(inblob->length == len);

            if (utf8_validate(inblob->data, inblob->length, &valid_len) ||
                valid_len != len - 1) {
                return -1;
            }

            memcpy(outblob, inblob, sizeof(blob_buffer_t));
            bzero(inblob, sizeof(blob_buffer_t));
            outblob->collected = 1;
        }

        return 0;
    }

    /* if the string was small enough to be stored in the in buffer and will fit
     * in the out buffer, then the string needs to be copied directly from the
     * in buffer to the out buffer */
    if (len <= in_len && len <= out_len) {

        if (inblob && inblob->exists) {
            logmsg(LOGMSG_ERROR, "vutf8_convert: bogus inblob\n");
            return -1;
        }

        /* if the string isn't empty, validate the string and make sure its
         * length matches len (minus 1 for the NUL byte) */
        if (len > 0 &&
            (utf8_validate(in, len, &valid_len) || valid_len != len - 1))
            return -1;

        memcpy(out, in, len);
        *outdtsz += len;

        if (outblob) {
            outblob->length = 0;
            outblob->exists = 0;
            outblob->collected = 1;
        }
    }

    /* if the string was small enough to be stored in the in buffer but won't
     * fit in the out buffer, then the string needs to be copied from the in
     * buffer to a new out blob */
    else if (len <= in_len) {
        int valid_len;

        if (outblob) {
            if (len > gbl_blob_sz_thresh_bytes)
                outblob->data = comdb2_bmalloc(blobmem, len);
            else
                outblob->data = malloc(len);

            if (outblob->data == NULL) {
                logmsg(LOGMSG_ERROR, "vutf8_convert: malloc %u failed\n", len);
                return -1;
            }

            /* if the string isn't empty, validate the string and make sure its
             * length matches len (minus 1 for the NUL byte) */
            if (len > 0 &&
                (utf8_validate(in, len, &valid_len) || valid_len != len - 1))
                return -1;

            memcpy(outblob->data, in, len);

            outblob->length = len;
            outblob->exists = 1;
            outblob->collected = 1;
        } else {
            return -1;
        }
    }

    /* if the string wasn't small enough to be stored in the in buffer but will
     * fit in the out buffer, then the string needs to be copied from the in
     * blob to the out buffer */
    else /* len <= out_len */
    {
        int valid_len;

        if (inblob) {
            if (!inblob->exists || !inblob->data) {
                logmsg(LOGMSG_ERROR, "vutf8_convert: missing inblob\n");
                return -1;
            }

            /* if the string isn't empty, validate the string and make sure its
             * length matches len (minus 1 for the NUL byte) */
            if (len > 0 && (utf8_validate(inblob->data, len, &valid_len) ||
                            valid_len != len - 1))
                return -1;

            memcpy(out, inblob->data, len);
            *outdtsz += len;

            free(inblob->data);
            bzero(inblob, sizeof(blob_buffer_t));

            if (outblob) {
                outblob->collected = 1;
                outblob->length = 0;
                outblob->exists = 0;
            }
        }
    }
    return 0;
}

static TYPES_INLINE int CLIENT_VUTF8_to_CLIENT_VUTF8(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    const client_blob_tp *blob = in;

    if (inlen < sizeof(client_blob_tp) || outlen < sizeof(client_blob_tp))
        return -1;

    memcpy(out, in, sizeof(client_blob_tp));
    *outdtsz = sizeof(client_blob_tp);

    /* copy the actual data around */
    return vutf8_convert(
        blob->length, (const char *)in + sizeof(client_blob_tp),
        inlen - sizeof(client_blob_tp), (char *)out + sizeof(client_blob_tp),
        outlen - sizeof(client_blob_tp), inblob, outblob, outdtsz);
}

#if 0
TYPES_INLINE int CLIENT_UINT_to_SERVER_UINT(const void *in, int inlen, int innull, void *out, int outlen, int * outdtsz, const struct field_conv_opts *outopts, blob_buffer_t *outblob) {
    unsigned short from_2, to_t; 
    unsigned int from_4, to_4;
    unsigned long long from_8, to_8;

    if (innull) {
        set_null(out, outlen);
        return 0;
    }

    switch (inlen) {
        case 2:
            memcpy(&from_2, in, inlen);
            from_8 = (unsigned long long) from_2;
            break;
        case 4:
            memcpy(&from_4, in, inlen);
            from_8 = (unsigned long long) from_8;
            break;
        case 8:
            memcpy(&from_8, in, inlen);
            break;
        default:
            return -1;
    }

    switch (outlen) {
        case 3:
            if (from_8 > USHRT_MAX)
                return -1;
            to_2 = (unsigned short) from_8;
            set_data(out, &to_2, outlen);
            break;
        case 5:
            if (from_8 > UINT_MAX)
                return -1;
            to_4 = (unsigned int) from_8;
            set_data(out, &to_4, outlen);
            break;
        case 9:
            set_data(out, &to_8, outlen);
            break;
        default:
            return -1;
    }
    return 0;
}
#endif

TYPES_INLINE int CLIENT_INT_to_SERVER_BINT(
    const void *in, int inlen, int innull, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    if (innull) {
        set_null(out, outlen);
        return 0;
    }

    int16_t from_2, to_2;
    int32_t from_4, to_4;
    int64_t from_8, to_8;
    int from_flip = 0;

    CHECK_FLIP(inopts, from_flip);

    switch (inlen) {
    case 2:
        memcpy(&from_2, in, inlen);
        if (from_flip) {
            from_2 = flibc_shortflip(from_2);
        }
        from_8 = (int64_t)from_2;
        break;
    case 4:
        memcpy(&from_4, in, inlen);
        if (from_flip) {
            from_4 = flibc_intflip(from_4);
        }
        from_8 = (int64_t)from_4;
        break;
    case 8:
        memcpy(&from_8, in, inlen);
        if (from_flip) {
            from_8 = flibc_llflip(from_8);
        }
        break;
    default:
        return -1;
    }

    uint8_t hdr = 0;
    bset(&hdr, data_bit);

    switch (outlen) {
    case 3:
        if (from_8 > INT16_MAX) {
            if (inopts && inopts->flags & FLD_CONV_TRUNCATE) {
                // make it compare greater than INT16_MAX
                from_8 = INT16_MAX;
                bset(&hdr, truncate_bit);
            } else {
                return -1;
            }
        } else if (from_8 < INT16_MIN) {
            if (inopts && inopts->flags & FLD_CONV_TRUNCATE) {
                // make it compare less than INT16_MIN
                from_8 = INT16_MIN;
                hdr = 0;
                bset(&hdr, truncate_bit);
            } else {
                return -1;
            }
        }
        from_2 = (int16_t)from_8;
        int2_to_int2b(from_2, (int2b *)&to_2);
        to_2 = htons(to_2);
        set_data_int(out, &to_2, outlen, hdr);
        break;
    case 5:
        if (from_8 > INT32_MAX) {
            if (inopts && inopts->flags & FLD_CONV_TRUNCATE) {
                from_8 = INT32_MAX;
                bset(&hdr, truncate_bit);
            } else {
                return -1;
            }
        } else if (from_8 < INT32_MIN) {
            if (inopts && inopts->flags & FLD_CONV_TRUNCATE) {
                from_8 = INT32_MIN;
                hdr = 0;
                bset(&hdr, truncate_bit);
            } else {
                return -1;
            }
        }
        from_4 = (int32_t)from_8;
        int4_to_int4b(from_4, (int4b *)&to_4);
        to_4 = htonl(to_4);
        set_data_int(out, &to_4, outlen, hdr);
        break;
    case 9:
        int8_to_int8b(from_8, (int8b *)&to_8);
        to_8 = flibc_htonll(to_8);
        set_data(out, &to_8, outlen);
        break;
    default:
        return -1;
    }
    *outdtsz = outlen;
    return 0;
}

TYPES_INLINE int CLIENT_REAL_to_SERVER_BREAL(
    const void *in, int inlen, int innull, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    if (innull) {
        set_null(out, outlen);
        return 0;
    }

    float from_4;
    double from_8;

    unsigned long to_4;
    unsigned long long to_8;

    int from_flip = 0;

    /* Determine if the from-data is little endian. */
    CHECK_FLIP(inopts, from_flip);

    switch (inlen) {
    case 4:
        memcpy(&from_4, in, 4);
        if (from_flip) {
            from_4 = flibc_floatflip(from_4);
        }
        /* DON'T GO THROUGH DOUBLE WHEN GOING FLOAT->FLOAT! */
        if (outlen != 5)
            from_8 = (double)from_4;
        break;
    case 8:
        memcpy(&from_8, in, 8);
        if (from_flip) {
            from_8 = flibc_dblflip(from_8);
        }
        break;
    default:
        return -1;
    }

    uint8_t hdr = 0;
    bset(&hdr, data_bit);
    switch (outlen) {
    case 5:
        /* avoid float->double->float conversion */
        if (inlen != 4) {
            if (REAL_OUTOFRANGE(from_8, FLT_MAX)) {
                if (inopts && inopts->flags & FLD_CONV_TRUNCATE) {
                    if (from_8 > FLT_MAX) {
                        from_8 = FLT_MAX;
                        bset(&hdr, truncate_bit);
                    } else if (from_8 < -FLT_MAX) {
                        from_8 = -FLT_MAX;
                        hdr = 0;
                        bset(&hdr, truncate_bit);
                    } else {
                        return -1;
                    }
                } else {
                    return -1;
                }
            }
            from_4 = (float)from_8;
        }
        ieee4_to_ieee4b(from_4, (int4b *)&to_4);
        to_4 = htonl(to_4);
        set_data_int(out, &to_4, outlen, hdr);
        break;
    case 9:
        ieee8_to_ieee8b(from_8, (int8b *)&to_8);
        to_8 = flibc_htonll(to_8);
        set_data(out, &to_8, outlen);
        break;
    default:
        return -1;
    }
    *outdtsz = outlen;
    return 0;
}

TYPES_INLINE int CLIENT_CSTR_to_SERVER_BCSTR(
    const void *in, int inlen, int innull, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    int len;
    int rc;

    *outdtsz = 0;
    if (innull) {
        set_null(out, outlen);
        return 0;
    }

    /* validate client CSTR */

    rc = validate_cstr(in, inlen);
    if (rc != 0)
        return -1;

    len = cstrlenlim(in, inlen);
    if (len == -1 || len > outlen - 1)
        return -1;

    memset(out, 0, outlen);
    set_data(out, in, len + 1);
    *outdtsz = len + 1;
    return 0;
}

TYPES_INLINE int CLIENT_PSTR_to_SERVER_BCSTR(
    const void *in, int inlen, int innull, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    int len;
    int rc;

    *outdtsz = 0;
    if (innull) {
        set_null(out, outlen);
        return 0;
    }

    /* validate client PSTR */
    rc = validate_pstr(in, inlen);
    if (rc != 0)
        return -1;

    len = pstrlenlim(in, inlen);
    if (len == -1 || len > outlen - 1)
        return -1;

    memset(out, 0, outlen);
    set_data(out, in, len + 1);
    *outdtsz = len + 1;
    return 0;
}

TYPES_INLINE int CLIENT_PSTR2_to_SERVER_BCSTR(
    const void *in, int inlen, int innull, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    if (innull) {
        set_null(out, outlen);
        return 0;
    }

    int rc = validate_pstr(in, inlen);
    if (rc != 0)
        return -1;

    int len = pstr2lenlim(in, inlen);
    const int olen = len; // original len
    if (len == -1)
        return -1;

    uint8_t hdr = 0;
    bset(&hdr, data_bit);
    if (len > outlen - 1) {
        if (inopts && inopts->flags & FLD_CONV_TRUNCATE) {
            len = outlen - 1;
        } else {
            return -1;
        }
    }
    ++len;
    set_data_int(out, in, len, hdr);
    memset(out + len, 0, outlen - len);
    *outdtsz = len;

    if (olen > outlen - 1) {
        // Can't just set truncate bit
        // 1. aaa
        // 2. zzz
        // aaaa should compare > aaa but < zzz
        // Setting truncate bit makes it > zzz
        // Walk backwards and increment first byte not 0xff
        if (inopts->step == 1) {
            uint8_t *a = (uint8_t *)out;
            uint8_t *b = a + len - 1;
            while (b > a) {
                if (*b != 0xff) {
                    ++(*b);
                    break;
                }
                --b;
            }
            if (b == a) {
                // All data bytes were 0xff
                // Ok to set truncate bit now
                bset(out, truncate_bit);
            }
        }
    }
    return 0;
}

TYPES_INLINE int CLIENT_BYTEARRAY_to_SERVER_BYTEARRAY(
    const void *in, int inlen, int innull, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    if (innull) {
        set_null(out, outlen);
    } else {
        int rc;
        rc = bytearray_copy(in, inlen, inopts, inblob, ((char *)out) + 1,
                            outlen - 1, outdtsz, outopts, outblob);
        if (-1 == rc)
            return -1;
        /* set data bit */
        set_data(out, in, 1);
        (*outdtsz)++;
    }
    return 0;
}

TYPES_INLINE int SERVER_UINT_to_CLIENT_UINT(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    unsigned long long from_8;
    unsigned int from_4;
    unsigned short from_2;

    unsigned int to_4;
    unsigned short to_2;
    int to_flip = 0;

    void *cint;

    /* Determine if the to-data is little endian. */
    CHECK_FLIP(outopts, to_flip);

    cint = (void *)(((char *)in) + 1);

    *outdtsz = 0;

    if (stype_is_null(in)) {
        *outnull = 1;
        return 0;
    }
    *outnull = 0;

    switch (inlen) {
    case 3:
        memcpy(&from_2, cint, sizeof(from_2));
        from_2 = ntohs(from_2);
        from_8 = (unsigned long long)from_2;
        break;
    case 5:
        memcpy(&from_4, cint, sizeof(from_4));
        from_4 = ntohl(from_4);
        from_8 = (unsigned long long)from_4;
        break;
    case 9:
        memcpy(&from_8, cint, sizeof(from_8));
        from_8 = flibc_ntohll(from_8);
        break;
    default:
        return -1;
    }

    switch (outlen) {
    case 2:
        if (from_8 > USHRT_MAX)
            return -1;
        to_2 = (unsigned short)from_8;

        if (to_flip) {
            to_2 = flibc_shortflip(to_2);
        }
        memcpy(out, &to_2, outlen);
        *outdtsz = 2;
        break;
    case 4:
        if (from_8 > UINT_MAX)
            return -1;
        to_4 = (unsigned int)from_8;

        if (to_flip) {
            to_4 = flibc_intflip(to_4);
        }
        memcpy(out, &to_4, outlen);
        *outdtsz = 4;
        break;
    case 8:

        if (to_flip) {
            from_8 = flibc_llflip(from_8);
        }
        memcpy(out, &from_8, outlen);
        *outdtsz = 8;
        break;
    default:
        return -1;
    }

    return 0;
}

TYPES_INLINE int SERVER_BINT_to_CLIENT_INT(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    long long from_8;
    int from_4;
    short from_2;

    long long from_8b;
    int from_4b;
    short from_2b;
    int to_flip = 0;

    int *to_4;
    short *to_2;

    void *cint;

    /* Determine if the to-data is little endian. */
    CHECK_FLIP(outopts, to_flip);

    cint = (void *)(((char *)in) + 1);
    *outdtsz = 0;
    if (stype_is_null(in)) {
        *outnull = 1;
        return 0;
    }

    *outnull = 0;

    switch (inlen) {
    case 3:
        memcpy(&from_2b, cint, sizeof(from_2));
        from_2b = ntohs(from_2b);
        int2b_to_int2(from_2b, &from_2);
        from_8 = (long long)from_2;
        break;
    case 5:
        memcpy(&from_4b, cint, sizeof(from_4));
        from_4b = ntohl(from_4b);
        int4b_to_int4(from_4b, (comdb2_int4 *)&from_4);
        from_8 = (long long)from_4;
        break;
    case 9:
        memcpy(&from_8b, cint, sizeof(from_8));
        from_8b = flibc_ntohll(from_8b);
        int8b_to_int8(from_8b, (comdb2_int8 *)&from_8);
        break;
    default:
        return -1;
    }

    /* from_8 contains unbiased client value now */
    switch (outlen) {
    case 2:
        if (from_8 < SHRT_MIN || from_8 > SHRT_MAX)
            return -1;
        to_2 = out;
        *to_2 = (short)from_8;

        if (to_flip) {
            *to_2 = flibc_shortflip(*to_2);
        }
        *outdtsz = 2;
        break;
    case 4:
        if (from_8 < INT_MIN || from_8 > INT_MAX)
            return -1;
        to_4 = out;
        *to_4 = (int)from_8;

        if (to_flip) {
            *to_4 = flibc_intflip(*to_4);
        }
        *outdtsz = 4;
        break;
    case 8:
        if (to_flip) {
            from_8 = flibc_llflip(from_8);
        }
        memcpy(out, &from_8, outlen);
        *outdtsz = 8;
        break;
    default:
        return -1;
    }
    return 0;
}

TYPES_INLINE int SERVER_BREAL_to_CLIENT_REAL(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    float from_4;
    double from_8;
    ieee4b from_4b;
    ieee8b from_8b;

    float to_4;
    void *cin;
    int to_flip = 0;
    *outdtsz = 0;

    /* Determine if the to-data is little endian. */
    CHECK_FLIP(outopts, to_flip);

    cin = (void *)(((char *)in) + 1);

    if (stype_is_null(in)) {
        *outnull = 1;
        return 0;
    }
    *outnull = 0;

    switch (inlen) {
    case 5:
        *(uint32_t *)&from_4b = ntohl(*(uint32_t *)cin);
        ieee4b_to_ieee4(from_4b, &from_4);
        if (outlen != 4)
            from_8 = (double)from_4;
        break;
    case 9:
        *(uint64_t *)&from_8b = flibc_ntohll(*(uint64_t *)cin);
        ieee8b_to_ieee8(from_8b, &from_8);
        break;
    default:
        return -1;
    }

    switch (outlen) {
    case 4:
        /* we skip converting float->double->float
           to avoid any representation mismatches
         */
        if (inlen != 5) {
            if (REAL_OUTOFRANGE(from_8, FLT_MAX))
                return -1;
            to_4 = (float)from_8;
        } else
            to_4 = from_4;

        if (to_flip) {
            to_4 = flibc_floatflip(to_4);
        }
        memcpy(out, &to_4, outlen);
        *outdtsz = 4;
        break;
    case 8:
        if (to_flip) {
            from_8 = flibc_dblflip(from_8);
        }
        memcpy(out, &from_8, outlen);
        *outdtsz = 8;
        break;
    default:
        return -1;
    }
    return 0;
}

TYPES_INLINE int SERVER_BCSTR_to_CLIENT_CSTR(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    int len;
    void *cin;

    *outdtsz = 0;

    if (stype_is_null(in)) {
        *outnull = 1;
        return 0;
    }
    *outnull = 0;

    cin = (void *)(((char *)in) + 1);
    len = cstrlenlim(cin, inlen - 1);
    if (len == -1 || len >= outlen)
        return -1;
    memset(out, 0, outlen);
    strncpy(out, cin, len);
    *outdtsz = len;
    return 0;
}

TYPES_INLINE int SERVER_BCSTR_to_CLIENT_PSTR(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    int len;
    void *cin;

    *outdtsz = 0;

    if (stype_is_null(in)) {
        *outnull = 1;
        return 0;
    }
    *outnull = 0;

    cin = (void *)(((char *)in) + 1);
    len = cstrlenlim(cin, inlen - 1);
    if (len == -1 || len > outlen)
        return -1;
    memset(out, ' ', outlen);
    strncpy(out, cin, len);
    *outdtsz = outlen;
    return 0;
}

TYPES_INLINE int SERVER_BCSTR_to_CLIENT_PSTR2(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    int len;
    void *cin;

    *outdtsz = 0;

    if (stype_is_null(in)) {
        *outnull = 1;
        return 0;
    }
    *outnull = 0;

    cin = (void *)(((char *)in) + 1);
    len = cstrlenlim(cin, inlen - 1);
    if (len == -1 || len > outlen)
        return -1;
    memset(out, ' ', outlen);
    strncpy(out, cin, len);
    *outdtsz = len;
    return 0;
}

TYPES_INLINE int SERVER_BYTEARRAY_to_CLIENT_BYTEARRAY(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    if (stype_is_null(in)) {
        *outnull = 1;
        return 0;
    } else {
        *outnull = 0;
        void *cin = (void *)(((char *)in) + 1);
        return bytearray_copy(cin, inlen - 1, inopts, inblob, out, outlen,
                              outdtsz, outopts, outblob);
    }
}

/* bytearray types do not convert to other types */
TYPES_INLINE int SERVER_BCSTR_to_CLIENT_BYTEARRAY(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    return -1;
}
TYPES_INLINE int SERVER_BINT_to_CLIENT_BYTEARRAY(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    return -1;
}
TYPES_INLINE int SERVER_BREAL_to_CLIENT_BYTEARRAY(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    return -1;
}
TYPES_INLINE int SERVER_BYTEARRAY_to_CLIENT_CSTR(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    return -1;
}

TYPES_INLINE int SERVER_BYTEARRAY_to_CLIENT_PSTR2(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    return -1;
}

TYPES_INLINE int SERVER_BYTEARRAY_to_CLIENT_INT(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    return -1;
}
TYPES_INLINE int SERVER_BYTEARRAY_to_CLIENT_PSTR(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    return -1;
}
TYPES_INLINE int SERVER_BYTEARRAY_to_CLIENT_REAL(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    return -1;
}
TYPES_INLINE int SERVER_BYTEARRAY_to_CLIENT_UINT(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    return -1;
}
TYPES_INLINE int SERVER_UINT_to_CLIENT_BYTEARRAY(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    return -1;
}

/* Again, cheat here: convert to client versions first, then call client
   conversion routines.
   DO_SERVER_TO_CLIENT does both conversions. */

#define DO_SERVER_TO_CLIENT(sfrom, sto, cto)                                   \
    void *tmpbuf;                                                              \
    tmpbuf = alloca(inlen);                                                    \
    *outnull = 0;                                                              \
    SERVER_##sfrom##_to_CLIENT_##sto(in, inlen, inopts, inblob, tmpbuf,        \
                                     inlen - 1, outnull, outdtsz, outopts,     \
                                     outblob);                                 \
    if (*outnull) return 0;                                                    \
    return CLIENT_##sto##_to_CLIENT_##cto(tmpbuf, inlen - 1, outopts, inblob,  \
                                          out, outlen, outdtsz, outopts,       \
                                          outblob);

#define DO_SERVER_STR_TO_CLIENT(sfrom, sto, cto)                               \
    void *tmpbuf;                                                              \
    tmpbuf = alloca(inlen);                                                    \
    *outnull = 0;                                                              \
    SERVER_##sfrom##_to_CLIENT_##sto(in, inlen, inopts, inblob, tmpbuf, inlen, \
                                     outnull, outdtsz, outopts, outblob);      \
    if (*outnull) return 0;                                                    \
    return CLIENT_##sto##_to_CLIENT_##cto(tmpbuf, inlen, outopts, inblob, out, \
                                          outlen, outdtsz, outopts, outblob);

TYPES_INLINE int SERVER_BCSTR_to_CLIENT_INT(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    DO_SERVER_STR_TO_CLIENT(BCSTR, CSTR, INT)
}

TYPES_INLINE int SERVER_BCSTR_to_CLIENT_REAL(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    DO_SERVER_STR_TO_CLIENT(BCSTR, CSTR, REAL)
}

TYPES_INLINE int SERVER_BCSTR_to_CLIENT_UINT(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    DO_SERVER_STR_TO_CLIENT(BCSTR, CSTR, UINT)
}

TYPES_INLINE int SERVER_BINT_to_CLIENT_CSTR(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    DO_SERVER_TO_CLIENT(BINT, INT, CSTR)
}

TYPES_INLINE int SERVER_BINT_to_CLIENT_PSTR(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    DO_SERVER_TO_CLIENT(BINT, INT, PSTR)
}

TYPES_INLINE int SERVER_BINT_to_CLIENT_PSTR2(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    DO_SERVER_TO_CLIENT(BINT, INT, PSTR2)
}

TYPES_INLINE int SERVER_BINT_to_CLIENT_REAL(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    DO_SERVER_TO_CLIENT(BINT, INT, REAL)
}

TYPES_INLINE int SERVER_BINT_to_CLIENT_UINT(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    DO_SERVER_TO_CLIENT(BINT, INT, UINT)
}

TYPES_INLINE int SERVER_BREAL_to_CLIENT_CSTR(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    DO_SERVER_TO_CLIENT(BREAL, REAL, CSTR)
}

TYPES_INLINE int SERVER_BREAL_to_CLIENT_INT(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    DO_SERVER_TO_CLIENT(BREAL, REAL, INT)
}

TYPES_INLINE int SERVER_BREAL_to_CLIENT_PSTR2(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    DO_SERVER_TO_CLIENT(BREAL, REAL, PSTR2)
}

TYPES_INLINE int SERVER_BREAL_to_CLIENT_PSTR(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    DO_SERVER_TO_CLIENT(BREAL, REAL, PSTR)
}

TYPES_INLINE int SERVER_BREAL_to_CLIENT_UINT(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    DO_SERVER_TO_CLIENT(BREAL, REAL, UINT)
}

TYPES_INLINE int SERVER_UINT_to_CLIENT_CSTR(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    DO_SERVER_TO_CLIENT(UINT, UINT, CSTR)
}

TYPES_INLINE int SERVER_UINT_to_CLIENT_INT(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    DO_SERVER_TO_CLIENT(UINT, UINT, INT)
}

TYPES_INLINE int SERVER_UINT_to_CLIENT_PSTR(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    DO_SERVER_TO_CLIENT(UINT, UINT, PSTR)
}

TYPES_INLINE int SERVER_UINT_to_CLIENT_PSTR2(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    DO_SERVER_TO_CLIENT(UINT, UINT, PSTR2)
}

TYPES_INLINE int SERVER_UINT_to_CLIENT_REAL(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    DO_SERVER_TO_CLIENT(UINT, UINT, REAL)
}

TYPES_INLINE int CLIENT_BYTEARRAY_to_SERVER_BCSTR(
    const void *in, int inlen, int isnull, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    return -1;
}
TYPES_INLINE int CLIENT_BYTEARRAY_to_SERVER_BINT(
    const void *in, int inlen, int isnull, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    return -1;
}
TYPES_INLINE int CLIENT_BYTEARRAY_to_SERVER_BREAL(
    const void *in, int inlen, int isnull, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    return -1;
}
TYPES_INLINE int CLIENT_BYTEARRAY_to_SERVER_UINT(
    const void *in, int inlen, int isnull, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    return -1;
}
TYPES_INLINE int CLIENT_CSTR_to_SERVER_BYTEARRAY(
    const void *in, int inlen, int isnull, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    return -1;
}
TYPES_INLINE int CLIENT_INT_to_SERVER_BYTEARRAY(
    const void *in, int inlen, int isnull, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    return -1;
}
TYPES_INLINE int CLIENT_PSTR_to_SERVER_BYTEARRAY(
    const void *in, int inlen, int isnull, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    return -1;
}
TYPES_INLINE int CLIENT_PSTR2_to_SERVER_BYTEARRAY(
    const void *in, int inlen, int isnull, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    return -1;
}
TYPES_INLINE int CLIENT_REAL_to_SERVER_BYTEARRAY(
    const void *in, int inlen, int isnull, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    return -1;
}

#ifdef _LINUX_SOURCE
#define UNSET_FLAG_ON_LINUX() inval.flags &= ~FLD_CONV_LENDIAN
#else
#define UNSET_FLAG_ON_LINUX()
#endif

/* Cheat: we have conversion routines between client and server similar types
   (int->bint).
   Convert to convertable client types first. DO_CLIENT_TO_SERVER macro does
   both conversions. */
#define DO_CLIENT_TO_SERVER(cfrom, cto, sto)                                   \
    void *tmpbuf;                                                              \
    int rc;                                                                    \
    if (isnull) {                                                              \
        set_null(out, outlen);                                                 \
        return 0;                                                              \
    } else {                                                                   \
        struct field_conv_opts *inlcl;                                         \
        struct field_conv_opts inval;                                          \
        if (inopts) {                                                          \
            inval = *inopts;                                                   \
            inval.flags &= ~FLD_CONV_LENDIAN;                                  \
            inlcl = &inval;                                                    \
            UNSET_FLAG_ON_LINUX();                                             \
        } else                                                                 \
            inlcl = NULL;                                                      \
        /* If we're on linux, we already flipped the data to be native.  Don't \
         * flip it again. */                                                   \
        tmpbuf = alloca(outlen - 1);                                           \
        rc = CLIENT_##cfrom##_to_CLIENT_##cto(in, inlen, inopts, inblob,       \
                                              tmpbuf, outlen - 1, outdtsz,     \
                                              outopts, outblob);               \
        if (rc == -1)                                                          \
            return -1;                                                         \
        rc = CLIENT_##cto##_to_SERVER_##sto(tmpbuf, outlen - 1, isnull, inlcl, \
                                            inblob, out, outlen, outdtsz,      \
                                            outopts, outblob);                 \
    }                                                                          \
    return rc;

#define DO_CLIENT_TO_SERVER_STR(cfrom, cto, sto)                               \
    void *tmpbuf;                                                              \
    int rc;                                                                    \
    if (isnull) {                                                              \
        set_null(out, outlen);                                                 \
        return 0;                                                              \
    } else {                                                                   \
        tmpbuf = alloca(outlen + 1);                                           \
        rc = CLIENT_##cfrom##_to_CLIENT_##cto(in, inlen, inopts, inblob,       \
                                              tmpbuf, outlen + 1, outdtsz,     \
                                              outopts, outblob);               \
        if (rc == -1)                                                          \
            return -1;                                                         \
        rc = CLIENT_##cto##_to_##SERVER_##sto(tmpbuf, outlen + 1, isnull,      \
                                              inopts, inblob, out, outlen,     \
                                              outdtsz, outopts, outblob);      \
    }                                                                          \
    return rc;

TYPES_INLINE int CLIENT_CSTR_to_SERVER_BINT(
    const void *in, int inlen, int isnull, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    DO_CLIENT_TO_SERVER(CSTR, INT, BINT);
}

TYPES_INLINE int CLIENT_CSTR_to_SERVER_BREAL(
    const void *in, int inlen, int isnull, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    DO_CLIENT_TO_SERVER(CSTR, REAL, BREAL);
}

TYPES_INLINE int CLIENT_CSTR_to_SERVER_UINT(
    const void *in, int inlen, int isnull, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    DO_CLIENT_TO_SERVER(CSTR, UINT, UINT);
}

TYPES_INLINE int CLIENT_INT_to_SERVER_BCSTR(
    const void *in, int inlen, int isnull, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    DO_CLIENT_TO_SERVER_STR(INT, CSTR, BCSTR);
}

TYPES_INLINE int CLIENT_INT_to_SERVER_BREAL(
    const void *in, int inlen, int isnull, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    DO_CLIENT_TO_SERVER(INT, REAL, BREAL);
}

TYPES_INLINE int CLIENT_INT_to_SERVER_UINT(
    const void *in, int inlen, int isnull, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    if (isnull) {
        set_null(out, outlen);
        return 0;
    }

    int64_t from_8;
    int32_t from_4;
    int16_t from_2;
    int from_flip = 0;

    uint64_t to_8;
    uint32_t to_4;
    uint16_t to_2;

    CHECK_FLIP(inopts, from_flip);

    uint8_t hdr = 0;
    bset(&hdr, data_bit);

    switch (inlen) {
    case 2:
        memcpy(&from_2, in, inlen);
        if (from_flip) {
            from_2 = flibc_shortflip(from_2);
        }
        from_8 = (int64_t)from_2;
        break;
    case 4:
        memcpy(&from_4, in, inlen);
        if (from_flip) {
            from_4 = flibc_intflip(from_4);
        }
        from_8 = (int64_t)from_4;
        break;
    case 8:
        memcpy(&from_8, in, inlen);
        if (from_flip) {
            from_8 = flibc_llflip(from_8);
        }
        break;
    default:
        return -1;
    }

    if (from_8 < 0) {
        if (inopts && inopts->flags & FLD_CONV_TRUNCATE) {
            from_8 = 0;
            hdr = 0;
            bset(&hdr, truncate_bit);
        } else {
            return -1;
        }
    }

    switch (outlen) {
    case 3:
        if (from_8 > UINT16_MAX) {
            if (inopts && inopts->flags & FLD_CONV_TRUNCATE) {
                from_8 = UINT16_MAX;
                bset(&hdr, truncate_bit);
            } else {
                return -1;
            }
        }
        to_2 = (uint16_t)from_8;
        to_2 = htons(to_2);
        set_data_int(out, &to_2, outlen, hdr);
        break;
    case 5:
        if (from_8 > UINT32_MAX) {
            if (inopts && inopts->flags & FLD_CONV_TRUNCATE) {
                from_8 = UINT32_MAX;
                bset(&hdr, truncate_bit);
            } else {
                return -1;
            }
        }
        to_4 = (uint32_t)from_8;
        to_4 = htonl(to_4);
        set_data_int(out, &to_4, outlen, hdr);
        break;
    case 9:
        to_8 = (uint64_t)from_8;
        to_8 = flibc_htonll(to_8);
        set_data_int(out, &to_8, outlen, hdr);
        break;
    default:
        return -1;
        break;
    }
    *outdtsz = outlen;

    return 0;
}

TYPES_INLINE int CLIENT_PSTR_to_SERVER_BINT(
    const void *in, int inlen, int isnull, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    DO_CLIENT_TO_SERVER(PSTR, INT, BINT);
}

TYPES_INLINE int CLIENT_PSTR_to_SERVER_BREAL(
    const void *in, int inlen, int isnull, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    DO_CLIENT_TO_SERVER(PSTR, REAL, BREAL);
}

TYPES_INLINE int CLIENT_PSTR_to_SERVER_UINT(
    const void *in, int inlen, int isnull, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    DO_CLIENT_TO_SERVER(PSTR, UINT, UINT);
}

TYPES_INLINE int CLIENT_PSTR2_to_SERVER_UINT(
    const void *in, int inlen, int isnull, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    DO_CLIENT_TO_SERVER(PSTR2, UINT, UINT);
}

TYPES_INLINE int CLIENT_PSTR2_to_SERVER_BINT(
    const void *in, int inlen, int isnull, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    DO_CLIENT_TO_SERVER(PSTR2, INT, BINT);
}

TYPES_INLINE int CLIENT_PSTR2_to_SERVER_BREAL(
    const void *in, int inlen, int isnull, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    DO_CLIENT_TO_SERVER(PSTR2, REAL, BREAL);
}

TYPES_INLINE int CLIENT_REAL_to_SERVER_BCSTR(
    const void *in, int inlen, int isnull, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    DO_CLIENT_TO_SERVER_STR(REAL, CSTR, BCSTR);
}

TYPES_INLINE int CLIENT_REAL_to_SERVER_BINT(
    const void *in, int inlen, int isnull, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    DO_CLIENT_TO_SERVER(REAL, INT, BINT);
}

TYPES_INLINE int CLIENT_REAL_to_SERVER_UINT(
    const void *in, int inlen, int isnull, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    DO_CLIENT_TO_SERVER(REAL, UINT, UINT);
}

TYPES_INLINE int CLIENT_UINT_to_SERVER_BINT(
    const void *in, int inlen, int isnull, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    DO_CLIENT_TO_SERVER(UINT, INT, BINT);
}

TYPES_INLINE int CLIENT_UINT_to_SERVER_BREAL(
    const void *in, int inlen, int isnull, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    DO_CLIENT_TO_SERVER(UINT, REAL, BREAL);
}

TYPES_INLINE int CLIENT_UINT_to_SERVER_BCSTR(
    const void *in, int inlen, int isnull, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    DO_CLIENT_TO_SERVER_STR(UINT, CSTR, BCSTR);
}

TYPES_INLINE int CLIENT_UINT_to_SERVER_BYTEARRAY(
    const void *in, int inlen, int isnull, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    DO_CLIENT_TO_SERVER(UINT, BYTEARRAY, BYTEARRAY);
}

/* bytearray is not convertable */
TYPES_INLINE int SERVER_BINT_to_SERVER_BYTEARRAY(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    return -1;
}
TYPES_INLINE int SERVER_BREAL_to_SERVER_BYTEARRAY(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    return -1;
}
TYPES_INLINE int SERVER_BYTEARRAY_to_SERVER_BCSTR(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    return -1;
}
TYPES_INLINE int SERVER_BYTEARRAY_to_SERVER_BINT(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    return -1;
}
TYPES_INLINE int SERVER_BYTEARRAY_to_SERVER_BREAL(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    return -1;
}
TYPES_INLINE int SERVER_BCSTR_to_SERVER_BYTEARRAY(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    return -1;
}
TYPES_INLINE int SERVER_UINT_to_SERVER_BYTEARRAY(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    return -1;
}

TYPES_INLINE int SERVER_BYTEARRAY_to_SERVER_BYTEARRAY(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    if (stype_is_null(in)) {
        /* NULL->NULL is always ok regardless of relative sizes */
        set_null(out, outlen);
        *outdtsz = outlen;
        return 0;
    }
    /* note that this will magically copy the flags byte too since that is
     * included in the size */
    return bytearray_copy(in, inlen, inopts, inblob, out, outlen, outdtsz,
                          outopts, outblob);
}

/* blobs are not convertible because we don't have access to the blob data. */
TYPES_INLINE int CLIENT_UINT_to_SERVER_BLOB(
    const void *in, int inlen, int isnull, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    return -1;
}
TYPES_INLINE int CLIENT_INT_to_SERVER_BLOB(
    const void *in, int inlen, int isnull, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    return -1;
}
TYPES_INLINE int CLIENT_REAL_to_SERVER_BLOB(
    const void *in, int inlen, int isnull, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    return -1;
}
TYPES_INLINE int CLIENT_CSTR_to_SERVER_BLOB(
    const void *in, int inlen, int isnull, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    /* HACK HACK HACK! - to support writing blobs from sql, allow null strings
     * to convert to null blobs. */
    if (isnull) {
        if (outblob) {
            bzero(outblob, sizeof(blob_buffer_t));
            outblob->collected = 1;
        }
        set_null(out, outlen);
        return 0;
    }
    return -1;
}
TYPES_INLINE int CLIENT_PSTR_to_SERVER_BLOB(
    const void *in, int inlen, int isnull, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    return -1;
}

TYPES_INLINE int CLIENT_BYTEARRAY_to_SERVER_BLOB(
    const void *in, int inlen, int isnull, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    int tmp;
    if (isnull) {
        set_null(out, outlen);
    } else {
        if (outblob) {
            if (inlen) {
                if (inlen > gbl_blob_sz_thresh_bytes)
                    outblob->data = comdb2_bmalloc(blobmem, inlen);
                else
                    outblob->data = malloc(inlen);
                if (!outblob->data) {
                    logmsg(LOGMSG_ERROR, "CLIENT_BYTEARRAY_to_SERVER_BLOB: "
                                    "malloc %d failed\n",
                            inlen);
                    return -1;
                }
            } else
                outblob->data = NULL;

            outblob->exists = 1;
            outblob->length = inlen;
            if (inlen)
                memcpy(outblob->data, in, inlen);
            outblob->collected = 1;
        }
        tmp = htonl(inlen);
        set_data(out, &tmp, BLOB_ON_DISK_LEN);
        *outdtsz = BLOB_ON_DISK_LEN;
    }
    /* I used to print warning trace if we get here with data but no blob
     * to store it in.  However, prefaulting can hit this condition, so now
     * we fail to transfer the blob data silently. */
    /*
       else {
       fprintf(stderr, "CLIENT_BYTEARRAY_to_SERVER_BLOB: no blob!\n");
       return -1;
       }
       */
    return 0;
}
TYPES_INLINE int CLIENT_PSTR2_to_SERVER_BLOB(
    const void *in, int inlen, int isnull, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    return -1;
}
TYPES_INLINE int CLIENT_BLOB_to_SERVER_UINT(
    const void *in, int inlen, int isnull, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    return -1;
}
TYPES_INLINE int CLIENT_BLOB_to_SERVER_BINT(
    const void *in, int inlen, int isnull, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    return -1;
}
TYPES_INLINE int CLIENT_BLOB_to_SERVER_BREAL(
    const void *in, int inlen, int isnull, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    return -1;
}
TYPES_INLINE int CLIENT_BLOB_to_SERVER_BCSTR(
    const void *in, int inlen, int isnull, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    return -1;
}
TYPES_INLINE int CLIENT_BLOB_to_SERVER_BYTEARRAY(
    const void *in, int inlen, int isnull, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    return -1;
}

/* preserve nullness and length of blob data */
TYPES_INLINE int CLIENT_BLOB_to_SERVER_BLOB(
    const void *in, int inlen, int isnull, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    const client_blob_tp *blob = in;

#ifdef _SUN_SOURCE
    client_blob_tp scratch;
    memcpy(&scratch, in, sizeof(client_blob_tp));
    blob = &scratch;
#endif

    if (inlen < sizeof(client_blob_tp) || outlen < BLOB_ON_DISK_LEN)
        return -1;

    if (inblob && outblob) {
        memcpy(outblob, inblob, sizeof(blob_buffer_t));
        bzero(inblob, sizeof(blob_buffer_t));
        outblob->collected = 1;
    }
    if (isnull || !blob->notnull) {
        set_null(out, outlen);
        if (outblob && outblob->exists) {
            /* shouldn't happen */
            logmsg(LOGMSG_ERROR, "CLIENT_BLOB_to_SERVER_BLOB: bogus blob!\n");
            return -1;
        }
        return 0;
    } else {
        set_data(out, &blob->length, BLOB_ON_DISK_LEN);
        *outdtsz = BLOB_ON_DISK_LEN;
        if (outblob && !outblob->exists) {
            /* shouldn't happen */
            logmsg(LOGMSG_ERROR, "CLIENT_BLOB_to_SERVER_BLOB: missing blob!\n");
            return -1;
        }
        return 0;
    }
}

/* preserve nullness and length of blob data */
TYPES_INLINE int CLIENT_BLOB_to_SERVER_BLOB2(
    const void *in, int inlen, int isnull, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    const client_blob_tp *blob = in;

#ifdef _SUN_SOURCE
    client_blob_tp scratch;
    memcpy(&scratch, in, sizeof(client_blob_tp));
    blob = &scratch;
#endif

    if (inlen < sizeof(client_blob_tp) || outlen < BLOB_ON_DISK_LEN)
        return -1;

    if (inblob && outblob) {
        memcpy(outblob, inblob, sizeof(blob_buffer_t));
        bzero(inblob, sizeof(blob_buffer_t));
        outblob->collected = 1;
    }
    if (isnull || !blob->notnull) {
        set_null(out, outlen);
        if (outblob && outblob->exists) {
            /* shouldn't happen */
            logmsg(LOGMSG_ERROR, "CLIENT_BLOB_to_SERVER_BLOB: bogus blob!\n");
            return -1;
        }
        return 0;
    } else {
        set_data(out, &blob->length, BLOB_ON_DISK_LEN);
        *outdtsz = BLOB_ON_DISK_LEN;
        if (outblob && !outblob->exists) {
            /* shouldn't happen */
            logmsg(LOGMSG_ERROR, "CLIENT_BLOB_to_SERVER_BLOB: missing blob!\n");
            return -1;
        }
        return 0;
    }
}

TYPES_INLINE int CLIENT_BYTEARRAY_to_SERVER_BLOB2(
    const void *in, int inlen, int isnull, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    int tmp;
    if (outlen < VUTF8_ON_DISK_LEN)
        return -1;

    if (isnull) {
        set_null(out, outlen);
    }

/* validate the string and get its length (not including any NUL), does
 * NOT require string to be NUL terminated */
#if 0     
    if(utf8_validate(in, inlen, &inlen))
        return -1;
#endif

    /* if the string isn't empty, add 1 to its length for the NUL byte,
     * otherwise give it 0 length */
    inlen = (inlen > 0) ? inlen : 0;
    tmp = htonl(inlen);
    set_data(out, &tmp, BLOB_ON_DISK_LEN);
    *outdtsz = BLOB_ON_DISK_LEN;

    /* if there is enough room in the out buffer to store string */
    if (inlen <= outlen - BLOB_ON_DISK_LEN) {
        if (inlen > 0) {
            char *cout = (char *)out;
            /* manually copy string data and append NUL byte because the input
             * data may not be NUL terminated */
            memcpy(cout + BLOB_ON_DISK_LEN, in, inlen);
            *outdtsz += inlen;
        }
    } else if (outblob) {
        if (inlen > gbl_blob_sz_thresh_bytes)
            outblob->data = comdb2_bmalloc(blobmem, inlen);
        else
            outblob->data = malloc(inlen);

        if (outblob->data == NULL) {
            logmsg(LOGMSG_ERROR, 
                    "CLIENT_BYTEARRAY_to_SERVER_BLOB2: malloc %u failed\n",
                    inlen);
            return -1;
        }

        /* manually copy string data and append NUL byte because the input data
         * may not be NUL terminated */
        memcpy(outblob->data, in, inlen);

        outblob->length = inlen;
        outblob->exists = 1;
        outblob->collected = 1;
    }

    return 0;
}

TYPES_INLINE int CLIENT_BYTEARRAY_to_SERVER_VUTF8(
    const void *in, int inlen, int isnull, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    int tmp;
    if (outlen < VUTF8_ON_DISK_LEN)
        return -1;

    if (isnull) {
        set_null(out, outlen);
    }

    /* validate the string and get its length (not including any NUL), does
      * NOT require string to be NUL terminated */
    if (utf8_validate(in, inlen, &inlen))
        return -1;

    /* if the string isn't empty, add 1 to its length for the NUL byte,
     * otherwise give it 0 length */
    inlen = (inlen > 0) ? inlen + 1 : 0;
    tmp = htonl(inlen);
    set_data(out, &tmp, VUTF8_ON_DISK_LEN);
    *outdtsz = VUTF8_ON_DISK_LEN;

    /* if there is enough room in the out buffer to store string */
    if (inlen <= outlen - VUTF8_ON_DISK_LEN) {
        if (inlen > 0) {
            char *cout = (char *)out;
            /* manually copy string data and append NUL byte because the input
             * data may not be NUL terminated */
            memcpy(cout + VUTF8_ON_DISK_LEN, in, inlen - 1);
            cout[VUTF8_ON_DISK_LEN + inlen - 1] = '\0';
            *outdtsz += inlen;
        }
    } else if (outblob) {
        if (inlen <= 0) {
            /* shouldn't happen */
            logmsg(LOGMSG_ERROR, "CLIENT_PSTR2_to_SERVER_VUTF8: invalid string "
                            "length: %d\n",
                    inlen);
            return -1;
        }

        if (inlen > gbl_blob_sz_thresh_bytes)
            outblob->data = comdb2_bmalloc(blobmem, inlen);
        else
            outblob->data = malloc(inlen);

        if (outblob->data == NULL) {
            logmsg(LOGMSG_ERROR, "CLIENT_PSTR2_to_SERVER_VUTF8: malloc %u failed\n",
                    inlen);
            return -1;
        }

        /* manually copy string data and append NUL byte because the input data
         * may not be NUL terminated */
        memcpy(outblob->data, in, inlen - 1);
        outblob->data[inlen - 1] = '\0';

        outblob->length = inlen;
        outblob->exists = 1;
        outblob->collected = 1;
    }

    return 0;
}

/* CLIENT_CSTR_to_SERVER_VUTF8 is #defined'd to this function so it must work
 * for both pstr2s and cstrs */
static TYPES_INLINE int CLIENT_PSTR2_to_SERVER_VUTF8(
    const void *in, int inlen, int isnull, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    int tmp;
    if (outlen < VUTF8_ON_DISK_LEN)
        return -1;

    if (isnull) {
        set_null(out, outlen);
        return 0;
    }

    /* validate the string and get its length (not including any NUL), does
     * NOT require string to be NUL terminated */
    if (utf8_validate(in, inlen, &inlen))
        return -1;

    /* if the string isn't empty, add 1 to its length for the NUL byte,
     * otherwise give it 0 length */
    inlen = (inlen > 0) ? inlen + 1 : 0;
    tmp = htonl(inlen);
    set_data(out, &tmp, VUTF8_ON_DISK_LEN);
    *outdtsz = VUTF8_ON_DISK_LEN;

    /*TODO can't use this here because doesn't handle non-NUL term case */
    /*return vutf8_convert(inlen,*/
    /*(const char *)in, inlen,*/
    /*(char *)out + VUTF8_ON_DISK_LEN, outlen - VUTF8_ON_DISK_LEN,*/
    /*NULL |+inblob+|, outblob,*/
    /*outdtsz);*/

    /* if there is enough room in the out buffer to store string */
    if (inlen <= outlen - VUTF8_ON_DISK_LEN) {
        if (inlen > 0) {
            char *cout = (char *)out;
            /* manually copy string data and append NUL byte because the input
             * data may not be NUL terminated */
            memcpy(cout + VUTF8_ON_DISK_LEN, in, inlen - 1);
            cout[VUTF8_ON_DISK_LEN + inlen - 1] = '\0';
            *outdtsz += inlen;
        }
    } else if (outblob) {
        if (inlen <= 0) {
            /* shouldn't happen */
            logmsg(LOGMSG_ERROR, "CLIENT_PSTR2_to_SERVER_VUTF8: invalid string "
                            "length: %d\n",
                    inlen);
            return -1;
        }

        if (inlen > gbl_blob_sz_thresh_bytes)
            outblob->data = comdb2_bmalloc(blobmem, inlen);
        else
            outblob->data = malloc(inlen);

        if (outblob->data == NULL) {
            logmsg(LOGMSG_ERROR, "CLIENT_PSTR2_to_SERVER_VUTF8: malloc %u failed\n",
                    inlen);
            return -1;
        }

        /* manually copy string data and append NUL byte because the input data
         * may not be NUL terminated */
        memcpy(outblob->data, in, inlen - 1);
        outblob->data[inlen - 1] = '\0';

        outblob->length = inlen;
        outblob->exists = 1;
        outblob->collected = 1;
    }

    return 0;
}

#define CLIENT_CSTR_to_SERVER_VUTF8 CLIENT_PSTR2_to_SERVER_VUTF8

static TYPES_INLINE int CLIENT_VUTF8_to_SERVER_VUTF8(
    const void *in, int inlen, int isnull, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    client_blob_tp blob;
    const client_blob_tp *binblob = in;
    int tmp;

    blob.notnull = ntohl(binblob->notnull);
    blob.length = ntohl(binblob->length);

    if (inlen < sizeof(client_blob_tp) || outlen < VUTF8_ON_DISK_LEN)
        return -1;

    if (isnull || !blob.notnull) {
        set_null(out, outlen);
        if (inblob && inblob->exists) {
            /* shouldn't happen */
            logmsg(LOGMSG_ERROR, "CLIENT_VUTF8_to_SERVER_VUTF8: bogus blob!\n");
            return -1;
        }
        return 0;
    }

    tmp = htonl(blob.length);
    set_data(out, &tmp, VUTF8_ON_DISK_LEN);
    *outdtsz = VUTF8_ON_DISK_LEN;

    /* copy the actual data around */
    return vutf8_convert(blob.length, (const char *)in + sizeof(client_blob_tp),
                         inlen - sizeof(client_blob_tp),
                         (char *)out + VUTF8_ON_DISK_LEN,
                         outlen - VUTF8_ON_DISK_LEN, inblob, outblob, outdtsz);
}

TYPES_INLINE int SERVER_UINT_to_CLIENT_BLOB(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    return -1;
}
TYPES_INLINE int SERVER_BINT_to_CLIENT_BLOB(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    return -1;
}
TYPES_INLINE int SERVER_BREAL_to_CLIENT_BLOB(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    return -1;
}
TYPES_INLINE int SERVER_BCSTR_to_CLIENT_BLOB(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    return -1;
}
TYPES_INLINE int SERVER_BYTEARRAY_to_CLIENT_BLOB(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    return -1;
}
TYPES_INLINE int SERVER_BLOB_to_CLIENT_UINT(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    return -1;
}
TYPES_INLINE int SERVER_BLOB_to_CLIENT_INT(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    return -1;
}
TYPES_INLINE int SERVER_BLOB_to_CLIENT_REAL(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    return -1;
}
TYPES_INLINE int SERVER_BLOB_to_CLIENT_CSTR(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    return -1;
}
TYPES_INLINE int SERVER_BLOB_to_CLIENT_PSTR(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    return -1;
}
TYPES_INLINE int SERVER_BLOB_to_CLIENT_BYTEARRAY(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    return -1;
}
TYPES_INLINE int SERVER_BLOB_to_CLIENT_PSTR2(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    return -1;
}

TYPES_INLINE int SERVER_BLOB2_to_CLIENT_UINT(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    return -1;
}
TYPES_INLINE int SERVER_BLOB2_to_CLIENT_INT(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    return -1;
}
TYPES_INLINE int SERVER_BLOB2_to_CLIENT_REAL(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    return -1;
}
TYPES_INLINE int SERVER_BLOB2_to_CLIENT_CSTR(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    return -1;
}
TYPES_INLINE int SERVER_BLOB2_to_CLIENT_PSTR(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    return -1;
}
TYPES_INLINE int SERVER_BLOB2_to_CLIENT_BYTEARRAY(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    return -1;
}
TYPES_INLINE int SERVER_BLOB2_to_CLIENT_PSTR2(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    return -1;
}

TYPES_INLINE int SERVER_BLOB2_to_CLIENT_BLOB(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    int tmp;
    const char *cin = in;
    client_blob_tp *blob = out;

#ifdef _SUN_SOURCE
    client_blob_tp scratch;
    blob = &scratch;
#endif

    if (inlen < BLOB_ON_DISK_LEN || outlen < sizeof(client_blob_tp))
        return -1;

    blob->padding0 = 0;
    blob->padding1 = 0;

    if (inblob && outblob) {
        memcpy(outblob, inblob, sizeof(blob_buffer_t));
        bzero(inblob, sizeof(blob_buffer_t));
    }

    if (stype_is_null(in)) {
        *outnull = 1;
        *outdtsz = 0;
        blob->notnull = 0;
        blob->length = 0;

#ifdef _SUN_SOURCE
        memcpy(out, blob, sizeof(client_blob_tp));
#endif

        if (outblob && outblob->exists) {
            /* shouldn't happen */
            logmsg(LOGMSG_ERROR, "SERVER_BLOB2_to_CLIENT_BLOB: bogus blob!\n");
            return -1;
        }
        return 0;
    }

    *outnull = 0;
    blob->notnull = htonl(1);
    /* server & client are both already big-endian */
    memcpy(&blob->length, cin + 1, sizeof(int));

#ifdef _SUN_SOURCE
    memcpy(out, blob, sizeof(client_blob_tp));
#endif

    *outdtsz = 16;
    if (outblob && !outblob->exists) {
        /* shouldn't happen */
        logmsg(LOGMSG_ERROR, "SERVER_BLOB2_to_CLIENT_BLOB: missing blob!\n");
        return -1;
    }
    return 0;
}

/* nullness and length of blob data get copied. */
TYPES_INLINE int SERVER_BLOB_to_CLIENT_BLOB(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    int tmp;
    const char *cin = in;
    client_blob_tp *blob = out;

#ifdef _SUN_SOURCE
    client_blob_tp scratch;
    blob = &scratch;
#endif

    if (inlen < BLOB_ON_DISK_LEN || outlen < sizeof(client_blob_tp))
        return -1;

    blob->padding0 = 0;
    blob->padding1 = 0;

    if (inblob && outblob) {
        memcpy(outblob, inblob, sizeof(blob_buffer_t));
        bzero(inblob, sizeof(blob_buffer_t));
    }

    if (stype_is_null(in)) {
        *outnull = 1;
        *outdtsz = 0;
        blob->notnull = 0;
        blob->length = 0;

#ifdef _SUN_SOURCE
        memcpy(out, blob, sizeof(client_blob_tp));
#endif

        if (outblob && outblob->exists) {
            /* shouldn't happen */
            logmsg(LOGMSG_ERROR, "SERVER_BLOB_to_CLIENT_BLOB: bogus blob!\n");
            return -1;
        }
        return 0;
    }

    *outnull = 0;
    blob->notnull = htonl(1);
    /* server & client are both already big-endian */
    memcpy(&blob->length, cin + 1, sizeof(int));

#ifdef _SUN_SOURCE
    memcpy(out, blob, sizeof(client_blob_tp));
#endif

    *outdtsz = 16;
    if (outblob && !outblob->exists) {
        /* shouldn't happen */
        logmsg(LOGMSG_ERROR, "SERVER_BLOB_to_CLIENT_BLOB: missing blob!\n");
        return -1;
    }
    return 0;
}

static TYPES_INLINE int SERVER_VUTF8_to_CLIENT_VUTF8(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    client_blob_tp *blob = out;
    int tmp;
    const char *cin = (const char *)in;
    int bloblen;

    if (inlen < VUTF8_ON_DISK_LEN || outlen < sizeof(client_blob_tp))
        return -1;

    blob->padding0 = 0;
    blob->padding1 = 0;

    if (stype_is_null(in)) {
        *outnull = 1;
        *outdtsz = 0;
        blob->notnull = 0;
        blob->length = 0;
        if (inblob && inblob->exists) {
            /* shouldn't happen */
            logmsg(LOGMSG_ERROR, "SERVER_VUTF8_to_CLIENT_VUTF8: bogus blob!\n");
            return -1;
        }
        return 0;
    }

    *outnull = 0;
    blob->notnull = 1;
    memcpy(&tmp, cin + 1, sizeof(int));
    /* blob->length is unsigned and may be in the non-native endianness */
    memcpy(&blob->length, &tmp, sizeof(blob->length));
    bloblen = ntohl(blob->length);
    *outdtsz = sizeof(client_blob_tp);

    /* copy the actual data around */
    return vutf8_convert(
        bloblen, cin + VUTF8_ON_DISK_LEN, inlen - VUTF8_ON_DISK_LEN,
        (char *)out + sizeof(client_blob_tp), outlen - sizeof(client_blob_tp),
        inblob, outblob, outdtsz);
}

#define DO_SERVER_TO_SERVER(sfrom, cfrom, sto)                                 \
    void *tmpbuf;                                                              \
    int isnull;                                                                \
    int rc;                                                                    \
    if (stype_is_null(in)) {                                                   \
        set_null(out, outlen);                                                 \
        return 0;                                                              \
    }                                                                          \
    tmpbuf = alloca(inlen);                                                    \
    rc = SERVER_##sfrom##_to_CLIENT_##cfrom(in, inlen, inopts, inblob, tmpbuf, \
                                            inlen - 1, &isnull, outdtsz,       \
                                            outopts, outblob);                 \
    if (rc)                                                                    \
        return -1;                                                             \
    return CLIENT_##cfrom##_to_SERVER_##sto(tmpbuf, inlen - 1, isnull, inopts, \
                                            inblob, out, outlen, outdtsz,      \
                                            outopts, outblob);

/* TODO what is the difference between this macro and DO_SERVER_TO_SERVER? this
 * one allocates one less byte, but it both macros pass inlen-1 as tmpbuf's
 * size? and why would the STR version of the macro have a smaller buffer? I
 * think they're both wrong, the one above should alloca(inlen-1) and then pass
 * that as the buffers size (which it already does) and this one should
 * alloca(inlen) and pass inlen as it's size (the odh byte will be able to store
 * the NUL byte) */
#define DO_SERVER_TO_SERVER_STR(sfrom, cfrom, sto)                             \
    void *tmpbuf;                                                              \
    int isnull;                                                                \
    int rc;                                                                    \
    if (stype_is_null(in)) {                                                   \
        set_null(out, outlen);                                                 \
        return 0;                                                              \
    }                                                                          \
    tmpbuf = alloca(inlen - 1);                                                \
    rc = SERVER_##sfrom##_to_CLIENT_##cfrom(in, inlen, inopts, inblob, tmpbuf, \
                                            inlen - 1, &isnull, outdtsz,       \
                                            outopts, outblob);                 \
    if (rc)                                                                    \
        return -1;                                                             \
    return CLIENT_##cfrom##_to_SERVER_##sto(tmpbuf, inlen - 1, isnull, inopts, \
                                            inblob, out, outlen, outdtsz,      \
                                            outopts, outblob);

/* Covert to corresponding client type first, then to needed server type. */
TYPES_INLINE int SERVER_BINT_to_SERVER_BCSTR(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    DO_SERVER_TO_SERVER(BINT, INT, BCSTR);
}

TYPES_INLINE int SERVER_BINT_to_SERVER_BINT(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    if (inlen == outlen) {
        memcpy(out, in, inlen);
        return 0;
    }
    /* dumb, but should work */
    DO_SERVER_TO_SERVER(BINT, INT, BINT);
}

TYPES_INLINE int SERVER_BINT_to_SERVER_BREAL(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    DO_SERVER_TO_SERVER(BINT, INT, BREAL);
}

TYPES_INLINE int SERVER_BREAL_to_SERVER_BCSTR(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    DO_SERVER_TO_SERVER_STR(BREAL, REAL, BCSTR);
}

TYPES_INLINE int SERVER_BREAL_to_SERVER_BINT(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    DO_SERVER_TO_SERVER(BREAL, REAL, BINT);
}

TYPES_INLINE int SERVER_BREAL_to_SERVER_BREAL(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    if (inlen == outlen) {
        memcpy(out, in, inlen);
        return 0;
    }
    DO_SERVER_TO_SERVER(BREAL, REAL, BREAL);
}

TYPES_INLINE int SERVER_BINT_to_SERVER_UINT(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    DO_SERVER_TO_SERVER(BINT, INT, UINT);
}

TYPES_INLINE int SERVER_BREAL_to_SERVER_UINT(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    DO_SERVER_TO_SERVER(BREAL, REAL, UINT);
}

TYPES_INLINE int SERVER_BCSTR_to_SERVER_UINT(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    DO_SERVER_TO_SERVER(BCSTR, PSTR, UINT);
}

TYPES_INLINE int SERVER_BYTEARRAY_to_SERVER_UINT(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    *outdtsz = 0;
    return -1;
}

TYPES_INLINE int SERVER_BCSTR_to_SERVER_BCSTR(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    int len;

    if (stype_is_null(in)) {
        set_null(out, outlen);
        return 0;
    }

    len = cstrlenlim((char *)in + 1, inlen - 1);
    if (len > outlen - 1)
        return -1;
    memset(out, 0, outlen);
    memcpy(out, in, len + 1);
    return 0;
}

TYPES_INLINE int SERVER_BCSTR_to_SERVER_BINT(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    DO_SERVER_TO_SERVER(BCSTR, PSTR, BINT);
}

TYPES_INLINE int SERVER_BCSTR_to_SERVER_BREAL(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    DO_SERVER_TO_SERVER(BCSTR, PSTR, BREAL);
}

TYPES_INLINE int SERVER_UINT_to_SERVER_BCSTR(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    DO_SERVER_TO_SERVER_STR(UINT, UINT, BCSTR);
}

TYPES_INLINE int SERVER_UINT_to_SERVER_BINT(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    DO_SERVER_TO_SERVER(UINT, UINT, BINT);
}

TYPES_INLINE int SERVER_UINT_to_SERVER_BREAL(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    DO_SERVER_TO_SERVER(UINT, UINT, BREAL);
}

TYPES_INLINE int SERVER_UINT_to_SERVER_UINT(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    DO_SERVER_TO_SERVER(UINT, UINT, UINT);
}

TYPES_INLINE int SERVER_BLOB_to_SERVER_BLOB(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    if (inlen < BLOB_ON_DISK_LEN || outlen < BLOB_ON_DISK_LEN)
        return -1;

    if (inblob && outblob) {
        memcpy(outblob, inblob, sizeof(blob_buffer_t));
        bzero(inblob, sizeof(blob_buffer_t));
    }
    memcpy(out, in, BLOB_ON_DISK_LEN);
    *outdtsz = BLOB_ON_DISK_LEN;
    return 0;
}

/* blobs don't convert easily - we don't have access to the blob data at this
 * level */
TYPES_INLINE int SERVER_UINT_to_SERVER_BLOB(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    return -1;
}
TYPES_INLINE int SERVER_BINT_to_SERVER_BLOB(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    return -1;
}
TYPES_INLINE int SERVER_BREAL_to_SERVER_BLOB(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    return -1;
}
TYPES_INLINE int SERVER_BCSTR_to_SERVER_BLOB(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    return -1;
}
TYPES_INLINE int SERVER_BLOB_to_SERVER_UINT(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    return -1;
}
TYPES_INLINE int SERVER_BLOB_to_SERVER_BINT(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    return -1;
}
TYPES_INLINE int SERVER_BLOB_to_SERVER_BREAL(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    return -1;
}
TYPES_INLINE int SERVER_BLOB_to_SERVER_BCSTR(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    return -1;
}

TYPES_INLINE int SERVER_BLOB2_to_SERVER_UINT(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    return -1;
}
TYPES_INLINE int SERVER_BLOB2_to_SERVER_BINT(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    return -1;
}
TYPES_INLINE int SERVER_BLOB2_to_SERVER_BREAL(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    return -1;
}
TYPES_INLINE int SERVER_BLOB2_to_SERVER_BCSTR(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    return -1;
}

static TYPES_INLINE int SERVER_VUTF8_to_SERVER_BCSTR(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    const char *cin = (const char *)in;
    int len, tmp;

    if (inlen < VUTF8_ON_DISK_LEN)
        return -1;

    if (stype_is_null(in)) {
        set_null(out, outlen);
        if (inblob && inblob->exists) {
            /* shouldn't happen */
            logmsg(LOGMSG_ERROR, "SERVER_VUTF8_to_SERVER_BCSTR: bogus vutf8!\n");
            return -1;
        }
        return 0;
    } else {
        /* set data bit */
        set_data(out, in, 1);
    }

    memcpy(&tmp, cin + 1, sizeof(int));
    len = ntohl(tmp);

    *outdtsz = 1;
    /* copy the actual data around */
    return vutf8_convert(len, cin + VUTF8_ON_DISK_LEN,
                         inlen - VUTF8_ON_DISK_LEN, (char *)out + 1, outlen - 1,
                         inblob, NULL, outdtsz);
}

static TYPES_INLINE int SERVER_VUTF8_to_SERVER_VUTF8(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    const char *cin = (const char *)in;
    int len, tmp;

    if (inlen < VUTF8_ON_DISK_LEN || outlen < VUTF8_ON_DISK_LEN)
        return -1;

    if (stype_is_null(in)) {
        set_null(out, outlen);
        if (inblob && inblob->exists) {
            /* shouldn't happen */
            logmsg(LOGMSG_ERROR, "SERVER_VUTF8_to_SERVER_VUTF8: bogus blob!\n");
            return -1;
        }
        return 0;
    }

    if (inblob == NULL && outblob == NULL && inlen == outlen) {
        memcpy(out, in, inlen);
        return 0;
    }

    memcpy(out, in, VUTF8_ON_DISK_LEN);
    *outdtsz = VUTF8_ON_DISK_LEN;

    memcpy(&tmp, cin + 1, sizeof(int));
    len = ntohl(tmp);

    /* copy the actual data around */
    return vutf8_convert(len, cin + VUTF8_ON_DISK_LEN,
                         inlen - VUTF8_ON_DISK_LEN,
                         (char *)out + VUTF8_ON_DISK_LEN,
                         outlen - VUTF8_ON_DISK_LEN, inblob, outblob, outdtsz);
}

static TYPES_INLINE int blob2_convert(int len, const void *in, int in_len,
                                      void *out, int out_len,
                                      blob_buffer_t *inblob,
                                      blob_buffer_t *outblob, int *outdtsz)
{
    int valid_len;

    /* if the string was too large to be stored in the in buffer and won't fit
     * in the out buffer, then the string will just be transfered from one blob
     * to another */
    if (len > in_len && len > out_len) {

        /* This only copies if we've passed in both an inblob and outblob, but
         * like
         * our blob-conversion code, it will always return success, whether or
         * not
         * this copy takes place.  The calling code expects this behavior. */
        if (inblob && outblob && inblob->exists) {
            /* validate input blob */
            assert(inblob->length == len);

            memcpy(outblob, inblob, sizeof(blob_buffer_t));
            bzero(inblob, sizeof(blob_buffer_t));
            outblob->collected = 1;
        }

        return 0;
    }

    /* if the string was small enough to be stored in the in buffer and will fit
     * in the out buffer, then the string needs to be copied directly from the
     * in buffer to the out buffer */
    if (len <= in_len && len <= out_len) {

        if (inblob && inblob->exists) {
            logmsg(LOGMSG_ERROR, "blob2_convert: bogus inblob\n");
            return -1;
        }

        memcpy(out, in, len);
        *outdtsz += len;

        if (outblob) {
            outblob->length = 0;
            outblob->exists = 0;
            outblob->collected = 1;
        }
    }

    /* if the string was small enough to be stored in the in buffer but won't
     * fit in the out buffer, then the string needs to be copied from the in
     * buffer to a new out blob */
    else if (len <= in_len) {
        int valid_len;

        if (outblob) {
            if (len > gbl_blob_sz_thresh_bytes)
                outblob->data = comdb2_bmalloc(blobmem, len);
            else
                outblob->data = malloc(len);

            if (outblob->data == NULL) {
                logmsg(LOGMSG_ERROR, "blob2_convert: malloc %u failed\n", len);
                return -1;
            }

            memcpy(outblob->data, in, len);

            outblob->length = len;
            outblob->exists = 1;
            outblob->collected = 1;
        }
    }

    /* if the string wasn't small enough to be stored in the in buffer but will
     * fit in the out buffer, then the string needs to be copied from the in
     * blob to the out buffer */
    else /* len <= out_len */
    {
        int valid_len;

        if (inblob) {
            if (!inblob->exists || !inblob->data) {
                logmsg(LOGMSG_ERROR, "blob2_convert: missing inblob\n");
                return -1;
            }

            memcpy(out, inblob->data, len);
            *outdtsz += len;

            free(inblob->data);
            bzero(inblob, sizeof(blob_buffer_t));

            if (outblob) {
                outblob->collected = 1;
                outblob->length = 0;
                outblob->exists = 0;
            }
        }
    }
    return 0;
}

static TYPES_INLINE int SERVER_BYTEARRAY_to_SERVER_BLOB(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    const char *cin = (const char *)in + 1;

    int len, tmp;

    if (outlen < BLOB_ON_DISK_LEN)
        return -1;

    if (stype_is_null(in)) {
        set_null(out, outlen);
        return 0;
    }

    /* if the string isn't empty, add 1 to its length for the NUL byte,
    * otherwise give it 0 length */
    tmp = htonl(inlen - 1);
    set_data(out, &tmp, BLOB_ON_DISK_LEN);
    *outdtsz = BLOB_ON_DISK_LEN;

    /* if there is enough room in the out buffer to store */
    if (inlen <= outlen - BLOB_ON_DISK_LEN + 1) {
        if (inlen > 0) {
            char *cout = (char *)out;
            /* manually copy string data and append NUL byte because the input
             * data may not be NUL terminated */
            memcpy(cout + BLOB_ON_DISK_LEN, cin, inlen - 1);
            *outdtsz += (inlen - 1);
        }
    } else if (outblob) {
        if (inlen > gbl_blob_sz_thresh_bytes)
            outblob->data = comdb2_bmalloc(blobmem, inlen - 1);
        else
            outblob->data = malloc(inlen - 1);

        if (outblob->data == NULL) {
            logmsg(LOGMSG_ERROR, 
                    "CLIENT_BYTEARRAY_to_SERVER_BLOB2: malloc %u failed\n",
                    inlen);
            return -1;
        }

        /* manually copy string data and append NUL byte because the input data
         * may not be NUL terminated */
        memcpy(outblob->data, cin, inlen - 1);

        outblob->length = inlen - 1;
        outblob->exists = 1;
        outblob->collected = 1;
    }
    return 0;
}

static TYPES_INLINE int SERVER_BLOB_to_SERVER_BYTEARRAY(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    const char *cin = (const char *)in;

    int len, tmp;

    if (inlen < BLOB_ON_DISK_LEN)
        return -1;

    if (stype_is_null(in)) {
        set_null(out, outlen);
        return 0;
    }

    memcpy(&tmp, cin + 1, sizeof(int));
    len = ntohl(tmp);

    if (len > outlen)
        return -1;

    if (len <= inlen - BLOB_ON_DISK_LEN) {
        int rc;
        rc = bytearray_copy(in + BLOB_ON_DISK_LEN, len, inopts, NULL,
                            ((char *)out) + 1, outlen - 1, outdtsz, outopts,
                            outblob);
        /* set data bit */
        set_data(out, in, 1);
        if (-1 == rc)
            return -1;
    } else {
        if (inblob) {
            if (!inblob->exists || !inblob->data) {
                logmsg(LOGMSG_ERROR, "blob2_convert: missing inblob\n");
                return -1;
            }
            int rc;
            rc = bytearray_copy(inblob->data, inblob->length, inopts, NULL,
                                ((char *)out) + 1, outlen - 1, outdtsz, outopts,
                                outblob);
            /* set data bit */
            set_data(out, in, 1);
            if (-1 == rc)
                return -1;

        } else {
            return -1;
        }
    }
    return 0;
}

static TYPES_INLINE int SERVER_BLOB2_to_SERVER_BLOB(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    const char *cin = (const char *)in;
    int len, tmp;

    if (inlen < BLOB_ON_DISK_LEN || outlen < BLOB_ON_DISK_LEN)
        return -1;

    if (stype_is_null(in)) {
        set_null(out, outlen);
        if (inblob && inblob->exists) {
            /* shouldn't happen */
            logmsg(LOGMSG_ERROR, "SERVER_BLOB2_to_SERVER_BLOB: bogus blob!\n");
            return -1;
        }
        return 0;
    }

    memcpy(out, in, BLOB_ON_DISK_LEN);
    *outdtsz = BLOB_ON_DISK_LEN;

    memcpy(&tmp, cin + 1, sizeof(int));
    len = ntohl(tmp);

    /* copy the actual data around */
    return blob2_convert(len, cin + BLOB_ON_DISK_LEN, inlen - BLOB_ON_DISK_LEN,
                         (char *)out + BLOB_ON_DISK_LEN,
                         outlen - BLOB_ON_DISK_LEN, inblob, outblob, outdtsz);
}

static TYPES_INLINE int SERVER_BLOB2_to_SERVER_BLOB2(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    const char *cin = (const char *)in;
    int len, tmp;

    if (inlen < BLOB_ON_DISK_LEN || outlen < BLOB_ON_DISK_LEN)
        return -1;

    if (stype_is_null(in)) {
        set_null(out, outlen);
        if (inblob && inblob->exists) {
            /* shouldn't happen */
            logmsg(LOGMSG_ERROR, "SERVER_VUTF8_to_SERVER_VUTF8: bogus blob!\n");
            return -1;
        }
        return 0;
    }

    memcpy(out, in, BLOB_ON_DISK_LEN);
    *outdtsz = BLOB_ON_DISK_LEN;

    memcpy(&tmp, cin + 1, sizeof(int));
    len = ntohl(tmp);

    /* copy the actual data around */
    return blob2_convert(len, cin + BLOB_ON_DISK_LEN, inlen - BLOB_ON_DISK_LEN,
                         (char *)out + BLOB_ON_DISK_LEN,
                         outlen - BLOB_ON_DISK_LEN, inblob, outblob, outdtsz);
}

/* datetime conversions*/

/* TMP BROKEN DATETIME */
extern int gbl_allowbrokendatetime;
void datetime_panic_button()
{
    logmsg(LOGMSG_FATAL, "\n\nTHIS DATABASE CONTAINS BROKEN DATETIME FIELDS.\n");
    logmsg(LOGMSG_FATAL, "THIS IS A TEMPORARY PROBLEM THAT NEEDS TO BE ADDRESSED.\n");
    exit(1);
}

static int _splitReal_3(const double in, long long *sec, unsigned short *frac);
static int _splitReal_6(const double in, long long *sec, unsigned int *frac);

/* some forwards to resolve warnings */
#define S2S_FUNKY_ARGS                                                         \
    const void *in, int inlen, const struct field_conv_opts *inopts,           \
        blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,            \
        const struct field_conv_opts *outopts, blob_buffer_t *outblob

int SERVER_BREAL_to_SERVER_DATETIME(S2S_FUNKY_ARGS);

static TYPES_INLINE int SERVER_UINT_to_SERVER_DATETIME(S2S_FUNKY_ARGS);
static TYPES_INLINE int SERVER_BINT_to_SERVER_DATETIME(S2S_FUNKY_ARGS);
static TYPES_INLINE int SERVER_DATETIME_to_SERVER_UINT(S2S_FUNKY_ARGS);
static TYPES_INLINE int SERVER_DATETIME_to_SERVER_BINT(S2S_FUNKY_ARGS);
static TYPES_INLINE int SERVER_DATETIME_to_SERVER_BREAL(S2S_FUNKY_ARGS);

int SERVER_BREAL_to_SERVER_DATETIMEUS(S2S_FUNKY_ARGS);

static TYPES_INLINE int SERVER_UINT_to_SERVER_DATETIMEUS(S2S_FUNKY_ARGS);
static TYPES_INLINE int SERVER_BINT_to_SERVER_DATETIMEUS(S2S_FUNKY_ARGS);
static TYPES_INLINE int SERVER_DATETIMEUS_to_SERVER_UINT(S2S_FUNKY_ARGS);
static TYPES_INLINE int SERVER_DATETIMEUS_to_SERVER_BINT(S2S_FUNKY_ARGS);
static TYPES_INLINE int SERVER_DATETIMEUS_to_SERVER_BREAL(S2S_FUNKY_ARGS);

#define S2C_FUNKY_ARGS                                                         \
    const void *in, int inlen, const struct field_conv_opts *inopts,           \
        blob_buffer_t *inblob, void *out, int outlen, int *outnull,            \
        int *outdtsz, const struct field_conv_opts *outopts,                   \
        blob_buffer_t *outblob

static TYPES_INLINE int SERVER_to_CLIENT_NO_CONV(S2C_FUNKY_ARGS) { return -1; }

#define SERVER_DATETIME_to_CLIENT_BYTEARRAY SERVER_to_CLIENT_NO_CONV
#define SERVER_DATETIME_to_CLIENT_BLOB SERVER_to_CLIENT_NO_CONV

#define SERVER_BYTEARRAY_to_CLIENT_DATETIME SERVER_to_CLIENT_NO_CONV
#define SERVER_BLOB_to_CLIENT_DATETIME SERVER_to_CLIENT_NO_CONV
#define SERVER_BLOB2_to_CLIENT_DATETIME SERVER_to_CLIENT_NO_CONV

#define SERVER_DATETIMEUS_to_CLIENT_BYTEARRAY SERVER_to_CLIENT_NO_CONV
#define SERVER_DATETIMEUS_to_CLIENT_BLOB SERVER_to_CLIENT_NO_CONV

#define SERVER_BYTEARRAY_to_CLIENT_DATETIMEUS SERVER_to_CLIENT_NO_CONV
#define SERVER_BLOB_to_CLIENT_DATETIMEUS SERVER_to_CLIENT_NO_CONV
#define SERVER_BLOB2_to_CLIENT_DATETIMEUS SERVER_to_CLIENT_NO_CONV

#define SERVER_DATETIMEUS_to_CLIENT_INT SERVER_DATETIME_to_CLIENT_INT
#define SERVER_DATETIMEUS_to_CLIENT_UINT SERVER_DATETIME_to_CLIENT_UINT

/* we gonna abuse CLIENT_intish_to_CLIENT_intish for int-size interpretation */
TYPES_INLINE int SERVER_DATETIME_to_CLIENT_INT(S2C_FUNKY_ARGS)
{

    /* TMP BROKEN DATETIME */
    if (*(char *)in == 0) {
        return SERVER_UINT_to_CLIENT_INT(in, 9, NULL, NULL, out, outlen,
                outnull, outdtsz, outopts, NULL);
    }

    /* good datetime */
    return SERVER_BINT_to_CLIENT_INT(in, 9, NULL, NULL, out, outlen, outnull,
                                     outdtsz, outopts, NULL);
}

static TYPES_INLINE int SERVER_DATETIME_to_CLIENT_UINT(S2C_FUNKY_ARGS)
{

    /* TMP BROKEN DATETIME */
    if (*(char *)in == 0) {
        return SERVER_UINT_to_CLIENT_UINT(in, 9, NULL, NULL, out, outlen,
                outnull, outdtsz, outopts, NULL);
    }

    /* good datetime */
    return SERVER_BINT_to_CLIENT_UINT(in, 9, NULL, NULL, out, outlen, outnull,
                                      outdtsz, outopts, NULL);
}

#define SRV_TP(abbr) server_##abbr##_t
#define CLT_TP(abbr) cdb2_client_##abbr##_t
#define POW10_EVAL(exp) 1E##exp
#define POW10(exp) POW10_EVAL(exp)
#define IPOW10(exp) ((long long)POW10(exp))

/* Convert server datetime/datetimeus to client real */
#define SERVER_DT_to_CLIENT_REAL_func_body(dt, UDT, prec, frac, fracdt)        \
    SRV_TP(dt) sdt;                                                            \
    double tmp;                                                                \
    const uint8_t *p_in = in;                                                  \
    const uint8_t *p_in_end = (const uint8_t *)in + inlen;                     \
                                                                               \
    /* null? */                                                                \
    if (stype_is_null(in)) {                                                   \
        *outnull = 1;                                                          \
        return 0;                                                              \
    }                                                                          \
                                                                               \
    /* unpack */                                                               \
    if (!server_##dt##_get(&sdt, p_in, p_in_end))                              \
        return -1;                                                             \
                                                                               \
    /* TMP BROKEN DATETIME */                                                  \
    if (*(char *)in == 0) {                                                    \
    } /* good datetime */                                                      \
    else                                                                       \
        int8b_to_int8(sdt.sec, (comdb2_int8 *)&sdt.sec);                       \
                                                                               \
    tmp = sdt.sec + ((double)(sdt.frac % IPOW10(prec)) / POW10(prec));         \
    tmp = flibc_htond(tmp);                                                    \
                                                                               \
    if (CLIENT_REAL_to_CLIENT_REAL(&tmp, sizeof(tmp), NULL, NULL, out, outlen, \
                                   outdtsz, outopts, NULL))                    \
        return -1;                                                             \
    *outnull = 0;                                                              \
                                                                               \
    return 0;                                                                  \
/* END OF SERVER_DT_to_CLIENT_REAL_func_body */

int SERVER_DATETIME_to_CLIENT_REAL(S2C_FUNKY_ARGS)
{
    SERVER_DT_to_CLIENT_REAL_func_body(datetime, DATETIME, 3, msec,
                                       unsigned short);
}

int SERVER_DATETIMEUS_to_CLIENT_REAL(S2C_FUNKY_ARGS)
{
    SERVER_DT_to_CLIENT_REAL_func_body(datetimeus, DATETIMEUS, 6, usec,
                                       unsigned int);
}

/* Convert server datetime/datetimeus to client datetime/datetimeus */
#define SERVER_DT_to_CLIENT_DT_func_body(from_dt, FROM_UDT, from_prec,         \
                                         from_frac, from_fracdt, to_dt, TO_DT, \
                                         to_prec, to_frac, to_fracdt)          \
    db_time_t sec = 0;                                                         \
    SRV_TP(from_dt) sdt;                                                       \
    CLT_TP(to_dt) cdt;                                                         \
    struct field_conv_opts_tz *tzopts = (struct field_conv_opts_tz *)outopts;  \
                                                                               \
    const uint8_t *p_in = in;                                                  \
    const uint8_t *p_in_end = (const uint8_t *)in + inlen;                     \
                                                                               \
    uint8_t *p_out = out;                                                      \
    const uint8_t *p_out_start = out;                                          \
    const uint8_t *p_out_end = (const uint8_t *)out + outlen;                  \
                                                                               \
    int to_little = 0;                                                         \
                                                                               \
    /* paranoia testing */                                                     \
    if (!outopts || !(outopts->flags & FLD_CONV_TZONE))                        \
        return -1;                                                             \
    if (!in || !out) {                                                         \
        return -1;                                                             \
    }                                                                          \
                                                                               \
    /* null? */                                                                \
    if (stype_is_null(in)) {                                                   \
        *outnull = 1;                                                          \
        return 0;                                                              \
    }                                                                          \
                                                                               \
    /* unpack */                                                               \
    if (!server_##from_dt##_get(&sdt, p_in, p_in_end)) {                       \
        return -1;                                                             \
    }                                                                          \
                                                                               \
    /* Determine if the to-data is little endian. */                           \
    if (outopts && outopts->flags & FLD_CONV_LENDIAN) {                        \
        to_little = 1;                                                         \
    }                                                                          \
                                                                               \
    /* TMP BROKEN DATETIME */                                                  \
    if (*(char *)in == 0) {                                                    \
    } /* good datetime */                                                      \
    else {                                                                     \
        int8b_to_int8(sdt.sec, (comdb2_int8 *)&sdt.sec);                       \
    }                                                                          \
                                                                               \
    /* convert */                                                              \
    if (db_time2struct(tzopts->tzname, &sdt.sec, (struct tm *)&cdt.tm)) {      \
        return -1;                                                             \
    }                                                                          \
                                                                               \
    /* fill in */                                                              \
    memcpy(cdt.tzname, tzopts->tzname, DB_MAX_TZNAMEDB);                       \
    /* can be easily optimized by compiler */                                  \
    cdt.to_frac = sdt.from_frac * IPOW10(to_prec) / IPOW10(from_prec);         \
                                                                               \
    if (!(p_out = client_##to_dt##_put_switch(&cdt, p_out, p_out_end,          \
                                              to_little))) {                   \
        return -1;                                                             \
    }                                                                          \
                                                                               \
    *outdtsz = p_out - p_out_start;                                            \
    *outnull = 0;                                                              \
                                                                               \
    return 0;                                                                  \
/* END OF SERVER_DT_to_CLIENT_DT_func_body */

int SERVER_DATETIME_to_CLIENT_DATETIME(S2C_FUNKY_ARGS)
{
    SERVER_DT_to_CLIENT_DT_func_body(
        datetime, DATETIME, 3, msec, unsigned short, /* from */
        datetime, DATETIME, 3, msec, unsigned short /* to   */);
}

int SERVER_DATETIMEUS_to_CLIENT_DATETIME(S2C_FUNKY_ARGS)
{
    if (gbl_forbid_datetime_truncation)
        return -1;
    SERVER_DT_to_CLIENT_DT_func_body(
        datetimeus, DATETIMEUS, 6, usec, unsigned int, /* from */
        datetime, DATETIME, 3, msec, unsigned short /* to   */);
}

int SERVER_DATETIME_to_CLIENT_DATETIMEUS(S2C_FUNKY_ARGS)
{
    if (gbl_forbid_datetime_promotion)
        return -1;
    SERVER_DT_to_CLIENT_DT_func_body(
        datetime, DATETIME, 3, msec, unsigned short, /* from */
        datetimeus, DATETIMEUS, 6, usec, unsigned int /* to   */);
}

int SERVER_DATETIMEUS_to_CLIENT_DATETIMEUS(S2C_FUNKY_ARGS)
{
    SERVER_DT_to_CLIENT_DT_func_body(
        datetimeus, DATETIMEUS, 6, usec, unsigned int, /* from */
        datetimeus, DATETIMEUS, 6, usec, unsigned int /* to   */);
}

#define SRV_TYPE_TO_CLT_DATETIME(fff)                                          \
    server_datetime_t sdt;                                                     \
    int rc = 0;                                                                \
    int tmp = 0;                                                               \
                                                                               \
    bzero(&sdt, sizeof(sdt));                                                  \
                                                                               \
    rc = SERVER_##fff##_to_SERVER_DATETIME(in, inlen, inopts, inblob, &sdt,    \
                                           sizeof(sdt), &tmp, NULL, NULL);     \
    if (rc)                                                                    \
        return rc;                                                             \
                                                                               \
    return SERVER_DATETIME_to_CLIENT_DATETIME(&sdt, sizeof(sdt), NULL, NULL,   \
                                              out, outlen, outnull, outdtsz,   \
                                              outopts, outblob);

int SERVER_UINT_to_CLIENT_DATETIME(S2C_FUNKY_ARGS)
{
    SRV_TYPE_TO_CLT_DATETIME(UINT);
}

int SERVER_BINT_to_CLIENT_DATETIME(S2C_FUNKY_ARGS)
{
    SRV_TYPE_TO_CLT_DATETIME(BINT);
}

int SERVER_BREAL_to_CLIENT_DATETIME(S2C_FUNKY_ARGS)
{
    SRV_TYPE_TO_CLT_DATETIME(BREAL);
}

#define SRV_TYPE_TO_CLT_DATETIMEUS(fff)                                        \
    server_datetimeus_t sdt;                                                   \
    int rc = 0;                                                                \
    int tmp = 0;                                                               \
                                                                               \
    bzero(&sdt, sizeof(sdt));                                                  \
                                                                               \
    rc = SERVER_##fff##_to_SERVER_DATETIMEUS(in, inlen, inopts, inblob, &sdt,  \
                                             sizeof(sdt), &tmp, NULL, NULL);   \
    if (rc)                                                                    \
        return rc;                                                             \
                                                                               \
    return SERVER_DATETIMEUS_to_CLIENT_DATETIMEUS(&sdt, sizeof(sdt), NULL,     \
                                                  NULL, out, outlen, outnull,  \
                                                  outdtsz, outopts, outblob);

int SERVER_UINT_to_CLIENT_DATETIMEUS(S2C_FUNKY_ARGS)
{
    SRV_TYPE_TO_CLT_DATETIMEUS(UINT);
}

int SERVER_BINT_to_CLIENT_DATETIMEUS(S2C_FUNKY_ARGS)
{
    SRV_TYPE_TO_CLT_DATETIMEUS(BINT);
}

int SERVER_BREAL_to_CLIENT_DATETIMEUS(S2C_FUNKY_ARGS)
{
    SRV_TYPE_TO_CLT_DATETIMEUS(BREAL);
}

static TYPES_INLINE int is_valid_days(int year, int month, int days)
{
    int lims[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

    if (year % 4 == 0) {
        if (year % 100 || (year % 400 == 0))
            lims[2]++;
    }

    return (days >= 1) && (days <= lims[month]);
}

static int twochar2int(char *str)
{
    if (isdigit(str[0]) && isdigit(str[1]))
        return (str[0] - '0') * 10 + str[1] - '0';

    return -1;
}

/* TODO: handle +offset format, and validate the TZNAME */
static int get_tzname(char *dst, char *src, int len)
{

    int tzname_len =
        ((len) < (DB_MAX_TZNAMEDB - 1)) ? len : (DB_MAX_TZNAMEDB - 1);
    int i;

    for (i = 0; i < tzname_len && src[i] == ' '; i++)
        ;

    /* i must have thought real hard to come up w/ this code...
       for(i =0;i<tzname_len; i++)
       if ( src[i] == ' ') {
       for(;i<tzname_len && src[i] == ' ';i++);
       break;
       } else break;
       */

    if (tzname_len > i) {
        strncpy(dst, src + i, tzname_len - i);
        dst[tzname_len] = 0;
    }

    /* we allow no-tzname datetime strings */

    return 0;
}

static int getInt(char *in, int len, int *offset, int *ltoken, int minltoken,
                  int maxltoken, char *seps, int *sep_skipped)
{

    char *token = NULL;
    int ret = -1;
    char bckp = 0;
    int origlen = len;

    *ltoken = 0;
    *sep_skipped = 0;
    if ((*offset + minltoken) > len || !isdigit(in[*offset])) {
        /*fprintf(stderr,"getInt returning -1 because *offset+minltoken >
         * len:\n");*/
        /*fprintf(stderr,"*offset: %d minltoken: %d len: %d\n", *offset,
         * minltoken, len);*/
        return ret;
    }

    /* accept at most maxltoken digits */
    if (len > (*offset + maxltoken))
        len = *offset + maxltoken;

    token = segtokx(in, len, offset, ltoken, seps);
    if (*ltoken >= minltoken && *ltoken <= maxltoken) {

        bckp = in[*offset];
        in[*offset] = 0; /* temporary asciiz*/
        ret = atoi(token);
        in[*offset] = bckp;
    }

    if (*offset < origlen) {
        if (strchr(seps, in[*offset])) {
            (*offset)++;
            *sep_skipped = 1;
        }
    }

    return ret;
}

/* Return 1 if the string might be a usec datetime */
static size_t is_usec_dt(const char *str, int len)
{
    int n = 0, isnum;
    const char *dot, *stop;

    /* find the last dot */
    for (dot = NULL, stop = str; (stop - str) < len && *stop != '\0'; ++stop)
        if (*stop == '.')
            dot = stop;

    /* no dot then it's definitely not a usec datetime */
    if (dot == NULL)
        return 0;

    /* check if the left side is a number */
    for (isnum = 1; str != dot; ++str)
        if (!isdigit(*str)) {
            isnum = 0;
            break;
        }

    /* get the number of digits to the right */
    while (++dot < stop && isdigit(*dot))
        ++n;

    return isnum ? (n > DTTZ_PREC_MSEC) : (n >= DTTZ_PREC_USEC);
}

/*
   if the string contains only nymerics, it is an epoch seconds,
   otherwise it is a ISO8601 datetime representation
   */
static int _isISO8601(const char *str, int len)
{
    int i;
    int nodot = 0;

    for (i = 0; i < len && str[i]; i++) {
        if (str[i] == '.') {
            if (nodot)
                return 1;
            nodot = 1;
        } else if (str[i] < '0' || str[i] > '9')
            return 1;
        if (!isalnum(str[i]) && str[i] != '-' && str[i] != ':' &&
            str[i] != '.') {
            /* invalid character in datetime string */
            return -1;
        }
    }
    return 0;
}

#define string2structtm_ISO_func_body(dt, UDT, minlen, prec, frac, fracdt)     \
    int offset = 0;                                                            \
    int ltoken = 0;                                                            \
    char *token = NULL;                                                        \
    int year = 0, month = 0, day = 0, hh = 0, mm = 0, ss = 0, sss = 0;         \
    int skipped = 0;                                                           \
    int sign = 1;                                                              \
                                                                               \
    enum {                                                                     \
        CONV_GEN_ERR = -1,                                                     \
        CONV_WRONG_FRMT = -2,                                                  \
        CONV_WRONG_WDAY = -3,                                                  \
        CONV_WRONG_MON = -4,                                                   \
        CONV_WRONG_MDAY = -5,                                                  \
        CONV_WRONG_HOUR = -6,                                                  \
        CONV_WRONG_MIN = -7,                                                   \
        CONV_WRONG_SEC = -8,                                                   \
        CONV_WRONG_MSEC = -9,                                                  \
        CONV_WRONG_TIME = -10,                                                 \
        CONV_WRONG_TZNM = -11,                                                 \
        CONV_WRONG_YEAR = -12,                                                 \
        CONV_WRONG_USEC = -13,                                                 \
    };                                                                         \
                                                                               \
    /* maybe we should remove this */                                          \
    if (_isISO8601(in, len) <= 0) {                                            \
        logmsg(LOGMSG_ERROR,                                                   \
                "Called string2structtm_ISO with a non ISO8601 string!\n");    \
        return CONV_WRONG_YEAR;                                                \
    }                                                                          \
                                                                               \
    if (in[0] == '-') {                                                        \
        sign = -1;                                                             \
        in++;                                                                  \
        if (!in)                                                               \
            return -1;                                                         \
    } else                                                                     \
        sign = 1;                                                              \
                                                                               \
    /*get year*/                                                               \
    year = getInt(in, len, &offset, &ltoken, 1, 4, "-/", &skipped);            \
    if (ltoken < 1 || ltoken > 4)                                              \
        return CONV_WRONG_YEAR;                                                \
                                                                               \
    year *= sign;                                                              \
                                                                               \
    /*get month*/                                                              \
    month = getInt(in, len, &offset, &ltoken, 1, 2, "-/", &skipped);           \
    if (!ltoken || ltoken > 2)                                                 \
        return CONV_WRONG_MON;                                                 \
    /* (month<1 || month>12)*/                                                 \
                                                                               \
    /*get day*/                                                                \
    day = getInt(in, len, &offset, &ltoken, 1, 2, "-T", &skipped);             \
    if (!ltoken)                                                               \
        return CONV_WRONG_MDAY;                                                \
    /*(day <1 || !is_valid_days(year, month, day))*/                           \
                                                                               \
    /* get time */                                                             \
    hh = getInt(in, len, &offset, &ltoken, 1, 2, ":", &skipped);               \
    if (ltoken) {                                                              \
        /* hh>=0 hh<=23 */                                                     \
                                                                               \
        mm = getInt(in, len, &offset, &ltoken, 1, 2, ":", &skipped);           \
        if (ltoken) {                                                          \
            /* mm>=0 && mm<60 */                                               \
                                                                               \
            ss = getInt(in, len, &offset, &ltoken, 1, 2, ":. ", &skipped);     \
            if (ltoken) {                                                      \
                /* ss >=0 && ss<60 */                                          \
                                                                               \
                /* check if this is space, which marks the end of time info;   \
                 */                                                            \
                /* it will follow the TZ */                                    \
                if (in[offset - 1] == '.') {                                   \
                                                                               \
                    sss = getInt(in, len, &offset, &ltoken, minlen, prec, "",  \
                                 &skipped);                                    \
                    if (!ltoken && minlen == prec)                             \
                        return -1; /* strict mode */                           \
                    if (!ltoken)                                               \
                        sss = 0;                                               \
                                                                               \
                    /* lets skip redundant digits here */                      \
                    if (!skipped) {                                            \
                        while (offset < len && isdigit(in[offset]))            \
                            offset++;                                          \
                    }                                                          \
                } else {                                                       \
                    if (in[offset - 1] == ' ')                                 \
                        offset--; /*incremented afterwards*/                   \
                    sss = 0;                                                   \
                }                                                              \
                                                                               \
            } else                                                             \
                ss = 0;                                                        \
        } else                                                                 \
            mm = 0;                                                            \
    } else                                                                     \
        hh = 0;                                                                \
                                                                               \
    /* fill in the blanks*/                                                    \
    out->tm.tm_year = year - 1900;                                             \
    out->tm.tm_mon = month - 1;                                                \
    out->tm.tm_mday = day;                                                     \
    out->tm.tm_hour = hh;                                                      \
    out->tm.tm_min = mm;                                                       \
    out->tm.tm_sec = ss;                                                       \
    out->frac = sss;                                                           \
                                                                               \
    if (len == offset) {                                                       \
        out->tzname[0] = 0;                                                    \
        return 0; /* tolerate for now no TZ, convopts will make up for it*/    \
    }                                                                          \
    if ((offset + 1 == len) && (in[offset] == 'z' || in[offset] == 'Z')) {     \
        /* Zulu notation */                                                    \
        strncpy(out->tzname, "UTC", 4);                                        \
        return 0;                                                              \
    }                                                                          \
                                                                               \
    if (in[offset] && in[offset] != ' ')                                       \
        return -1;                                                             \
                                                                               \
    offset++; /*jump over separator*/                                          \
                                                                               \
    return get_tzname(out->tzname, in + offset, len - offset);                 \
/* END OF string2structtm_ISO_func_body */

int string2structdatetime_ISO(char *in, int len, cdb2_client_datetime_t *out)
{
    string2structtm_ISO_func_body(datetime, DATETIME, 1, 3, msec,
                                  unsigned short);
}

int string2structdatetimeus_ISO(char *in, int len,
                                cdb2_client_datetimeus_t *out)
{
    string2structtm_ISO_func_body(datetimeus, DATETIMEUS, 6, 6, usec,
                                  unsigned int);
}

#define structtm2string_ISO_func_body(dt, UDT, prec, frac, fracdt)             \
    if (in->tm.tm_year + 1900 > 9999 || in->tm.tm_year + 1900 < -9999)         \
        return -1;                                                             \
    snprintf(out, outlen, "%4.4d-%2.2u-%2.2uT%2.2u%2.2u%2.2u.%*.*u %s",        \
             in->tm.tm_year + 1900, in->tm.tm_mon + 1, in->tm.tm_mday,         \
             in->tm.tm_hour, in->tm.tm_min, in->tm.tm_sec, prec, prec,         \
             in->frac, in->tzname);                                            \
    return 0;                                                                  \
/* END OF structm2string_ISO_func_body */

int structdatetime2string_ISO(cdb2_client_datetime_t *in, char *out, int outlen)
{
    structtm2string_ISO_func_body(datetime, DATETIME, 3, msec, unsigned short);
}

static int structdatetimeus2string_ISO(cdb2_client_datetimeus_t *in, char *out,
                                       int outlen)
{
    structtm2string_ISO_func_body(datetimeus, DATETIMEUS, 6, usec,
                                  unsigned int);
}

#define SERVER_BCSTR_to_CLIENT_DT_func_body(dt, UDT, prec, frac, fracdt)       \
    int rc = 0;                                                                \
    int to_little = 0;                                                         \
                                                                               \
    /* paranoia tests */                                                       \
    if (!outopts || !(outopts->flags & FLD_CONV_TZONE))                        \
        return -1;                                                             \
    if (!in || !out)                                                           \
        return -1;                                                             \
                                                                               \
    /* null? */                                                                \
    if (stype_is_null(in)) {                                                   \
        *outnull = 1;                                                          \
        return 0;                                                              \
    }                                                                          \
                                                                               \
    /* Determine if the to-data is little endian. */                           \
    if (outopts && outopts->flags & FLD_CONV_LENDIAN) {                        \
        to_little = 1;                                                         \
    }                                                                          \
                                                                               \
    rc = _isISO8601((char *)in + 1, inlen - 1);                                \
    if (rc > 0) {                                                              \
        CLT_TP(dt) cdt;                                                        \
        int ret;                                                               \
                                                                               \
        uint8_t *p_out = out;                                                  \
        const uint8_t *p_out_start = out;                                      \
        const uint8_t *p_out_end = (const uint8_t *)out + outlen;              \
                                                                               \
        if ((ret = string2struct##dt##_ISO(((char *)in) + 1, inlen - 1,        \
                                           &cdt)) != 0)                        \
            return ret;                                                        \
                                                                               \
        if (!cdt.tzname[0])                                                    \
            strncpy(cdt.tzname,                                                \
                    ((struct field_conv_opts_tz *)outopts)->tzname,            \
                    sizeof(cdt.tzname));                                       \
        /* IF CLIENT THE TIMEZONELESS FIELD WAS STORED IN A DIFFERENT TIMEZONE \
         */                                                                    \
        /* THE PROVIDED CLIENT_DATETIME IS WRONG. THIS IS AS MUCH WE CAN DO */ \
        /* GIVEN THE AMOUNT OF INFORMATION WE HAVE */                          \
                                                                               \
        /* all is good, fill the user value */                                 \
        if (!(p_out = client_##dt##_put_switch(&cdt, p_out, p_out_end,         \
                                               to_little))) {                  \
            return -1;                                                         \
        }                                                                      \
        *outdtsz = p_out - p_out_start;                                        \
        *outnull = 0;                                                          \
                                                                               \
        return 0;                                                              \
    } else if (rc == 0) {                                                      \
        /* we have here a epoch seconds and a fractional part as msec or usec  \
         */                                                                    \
        double tmp;                                                            \
        SRV_TP(dt) sdt;                                                        \
                                                                               \
        bzero(&sdt, sizeof(sdt));                                              \
        bset(&sdt, data_bit);                                                  \
                                                                               \
        tmp = atof((char *)in + 1);                                            \
                                                                               \
        if (_splitReal_##prec(tmp, &sdt.sec, &sdt.frac))                       \
            return -1;                                                         \
                                                                               \
        int8_to_int8b(sdt.sec, (int8b *)&sdt.sec);                             \
        sdt.sec = flibc_htonll(sdt.sec);                                       \
        sdt.frac = (sizeof(fracdt) == sizeof(uint16_t)) ? htons(sdt.frac)      \
                                                        : htonl(sdt.frac);     \
                                                                               \
        return SERVER_##UDT##_to_CLIENT_##UDT(&sdt, sizeof(sdt), NULL, NULL,   \
                                              out, outlen, outnull, outdtsz,   \
                                              outopts, NULL);                  \
    } else {                                                                   \
        /*error */                                                             \
        return -1;                                                             \
    }                                                                          \
/* END OF SERVER_BCSTR_to_CLIENT_DT_func_body */

int SERVER_BCSTR_to_CLIENT_DATETIME(S2C_FUNKY_ARGS)
{
    SERVER_BCSTR_to_CLIENT_DT_func_body(datetime, DATETIME, 3, msec,
                                        unsigned short);
}

int SERVER_BCSTR_to_CLIENT_DATETIMEUS(S2C_FUNKY_ARGS)
{
    SERVER_BCSTR_to_CLIENT_DT_func_body(datetimeus, DATETIMEUS, 6, usec,
                                        unsigned int);
}

#define SERVER_DT_to_CLIENT_CSTR_func_body(dt, UDT, prec, frac, fracdt)        \
    int ret;                                                                   \
    CLT_TP(dt) cdt;                                                            \
    uint8_t dt_buf[sizeof(cdt)];                                               \
    int outdtsz_dup;                                                           \
    int outnull_dup;                                                           \
    int to_little = 0;                                                         \
                                                                               \
    uint8_t *p_dt_buf = dt_buf;                                                \
    const uint8_t *p_dt_buf_end = dt_buf + sizeof(dt_buf);                     \
                                                                               \
    /* null? */                                                                \
    if (stype_is_null(in)) {                                                   \
        *outnull = 1;                                                          \
        return 0;                                                              \
    }                                                                          \
                                                                               \
    /* Determine if the to-data is little endian. */                           \
    if (outopts && outopts->flags & FLD_CONV_LENDIAN) {                        \
        to_little = 1;                                                         \
    }                                                                          \
                                                                               \
    ret = SERVER_##UDT##_to_CLIENT_##UDT(in, inlen, inopts, NULL, &dt_buf,     \
                                         sizeof(dt_buf), &outnull_dup,         \
                                         &outdtsz_dup, outopts, NULL);         \
    if (ret) {                                                                 \
        return ret;                                                            \
    }                                                                          \
                                                                               \
    if (!client_##dt##_get_switch(&cdt, p_dt_buf, p_dt_buf_end, to_little))    \
        return -1;                                                             \
                                                                               \
    ret = struct##dt##2string_ISO(&cdt, out, outlen);                          \
    if (ret) {                                                                 \
        return ret;                                                            \
    }                                                                          \
    *outdtsz = strlen(out) + 1;                                                \
    *outnull = 0;                                                              \
                                                                               \
    return 0;                                                                  \
/* END OF SERVER_DT_to_CLIENT_CSTR_func_body */

int SERVER_DATETIME_to_CLIENT_CSTR(S2C_FUNKY_ARGS)
{
    SERVER_DT_to_CLIENT_CSTR_func_body(datetime, DATETIME, 3, msec,
                                       unsigned short);
}

int SERVER_DATETIMEUS_to_CLIENT_CSTR(S2C_FUNKY_ARGS)
{
    SERVER_DT_to_CLIENT_CSTR_func_body(datetimeus, DATETIMEUS, 6, usec,
                                       unsigned int);
}

#define SERVER_DT_to_CLIENT_PSTR_func_body(dt, UDT, prec, frac, fracdt)        \
    int ret = 0;                                                               \
                                                                               \
    ret = SERVER_##UDT##_to_CLIENT_CSTR(in, inlen, inopts, NULL, out, outlen,  \
                                        outnull, outdtsz, outopts, NULL);      \
    if (ret)                                                                   \
        return ret;                                                            \
    if (*outnull)                                                              \
        return 0;                                                              \
                                                                               \
    /* convert out from CSTR to PSTR */                                        \
    ((char *)out)[*outdtsz - 1] = ' ';                                         \
    return 0;                                                                  \
/* END OF SERVER_DT_to_CLIENT_PSTR_func_body */

int SERVER_DATETIME_to_CLIENT_PSTR(S2C_FUNKY_ARGS)
{
    SERVER_DT_to_CLIENT_PSTR_func_body(datetime, DATETIME, 3, msec,
                                       unsigned short);
}

int SERVER_DATETIMEUS_to_CLIENT_PSTR(S2C_FUNKY_ARGS)
{
    SERVER_DT_to_CLIENT_PSTR_func_body(datetimeus, DATETIMEUS, 6, usec,
                                       unsigned int);
}

#define SERVER_DT_to_CLIENT_PSTR2_func_body(dt, UDT, prec, frac, fracdt)       \
    int ret = 0;                                                               \
                                                                               \
    ret = SERVER_##UDT##_to_CLIENT_CSTR(in, inlen, inopts, NULL, out, outlen,  \
                                        outnull, outdtsz, outopts, NULL);      \
    if (ret)                                                                   \
        return ret;                                                            \
    if (*outnull)                                                              \
        return 0;                                                              \
                                                                               \
    /* convert out from CSTR to PSTR */                                        \
    for (ret = *outdtsz - 1; ret < outlen; ret++)                              \
        ((char *)out)[ret] = ' ';                                              \
    return 0;                                                                  \
/* END OF SERVER_DT_to_CLIENT_PSTR2_func_body */

int SERVER_DATETIME_to_CLIENT_PSTR2(S2C_FUNKY_ARGS)
{
    SERVER_DT_to_CLIENT_PSTR2_func_body(datetime, DATETIME, 3, msec,
                                        unsigned short);
}

int SERVER_DATETIMEUS_to_CLIENT_PSTR2(S2C_FUNKY_ARGS)
{
    SERVER_DT_to_CLIENT_PSTR2_func_body(datetimeus, DATETIMEUS, 6, usec,
                                        unsigned int);
}

#define C2S_FUNKY_ARGS                                                         \
    const void *in, int inlen, int isnull,                                     \
        const struct field_conv_opts *inopts, blob_buffer_t *inblob,           \
        void *out, int outlen, int *outdtsz,                                   \
        const struct field_conv_opts *outopts, blob_buffer_t *outblob

static TYPES_INLINE int CLIENT_to_SERVER_NO_CONV(C2S_FUNKY_ARGS) { return -1; }

#define CLIENT_DATETIME_to_SERVER_BYTEARRAY CLIENT_to_SERVER_NO_CONV
#define CLIENT_DATETIME_to_SERVER_BLOB CLIENT_to_SERVER_NO_CONV
#define CLIENT_DATETIME_to_SERVER_BCSTR CLIENT_to_SERVER_NO_CONV
#define CLIENT_BYTEARRAY_to_SERVER_DATETIME CLIENT_to_SERVER_NO_CONV
#define CLIENT_BLOB_to_SERVER_DATETIME CLIENT_to_SERVER_NO_CONV

#define CLIENT_DATETIMEUS_to_SERVER_BYTEARRAY CLIENT_to_SERVER_NO_CONV
#define CLIENT_DATETIMEUS_to_SERVER_BLOB CLIENT_to_SERVER_NO_CONV
#define CLIENT_DATETIMEUS_to_SERVER_BCSTR CLIENT_to_SERVER_NO_CONV
#define CLIENT_BYTEARRAY_to_SERVER_DATETIMEUS CLIENT_to_SERVER_NO_CONV
#define CLIENT_BLOB_to_SERVER_DATETIMEUS CLIENT_to_SERVER_NO_CONV

int datetime_check_range(long long secs, int fracs)
{
    if (gbl_mifid2_datetime_range) {
        if (secs < -377673580801ll || secs > 253402300799ll) {
            fprintf(stderr, "%s:%d Value %lld %d\n", __func__, __LINE__, secs,
                    fracs);
            return -1;
        }
    } else {
        if (!debug_switch_unlimited_datetime_range() &&
            (secs < -377705030401ll || secs > 253402214400ll))
            return -1;
    }

    return 0;
}

#define CLIENT_DT_to_SERVER_DT_func_body(from_dt, FROM_UDT, from_prec,         \
                                         from_frac, from_fracdt, to_dt,        \
                                         TO_UDT, to_prec, to_frac, to_fracdt)  \
    CLT_TP(from_dt) cdt;                                                       \
    SRV_TP(to_dt) sdt;                                                         \
    struct tm tm = {0};                                                        \
    struct field_conv_opts_tz *tzopts = (struct field_conv_opts_tz *)inopts;   \
    char *actualtzname = NULL;                                                 \
                                                                               \
    uint8_t *p_in = (uint8_t *)in;                                             \
    const uint8_t *p_in_start = (const uint8_t *)in;                           \
    const uint8_t *p_in_end = (const uint8_t *)in + inlen;                     \
                                                                               \
    uint8_t *p_out = out;                                                      \
    const uint8_t *p_out_start = out;                                          \
    const uint8_t *p_out_end = (const uint8_t *)out + outlen;                  \
    int from_little = 0;                                                       \
    int from_ext_tm = 0;                                                       \
                                                                               \
    /* paranoia testing */                                                     \
    if (!in || !out)                                                           \
        return -1;                                                             \
                                                                               \
    /* null? */                                                                \
    if (isnull) {                                                              \
        set_null(out, outlen);                                                 \
        return 0;                                                              \
    }                                                                          \
                                                                               \
    /* Determine if the to-data is little endian. */                           \
    if (inopts && inopts->flags & FLD_CONV_LENDIAN) {                          \
        from_little = 1;                                                       \
    }                                                                          \
                                                                               \
    /* unpack */                                                               \
    if (!client_##from_dt##_get_switch(&cdt, p_in, p_in_end, from_little)) {   \
        return -1;                                                             \
    }                                                                          \
                                                                               \
    if (!cdt.tzname[0] && (!inopts || !(inopts->flags & FLD_CONV_TZONE))) {    \
        return -1;                                                             \
    }                                                                          \
    actualtzname = (cdt.tzname[0])                                             \
                       ? cdt.tzname                                            \
                       : ((struct field_conv_opts_tz *)inopts)->tzname;        \
                                                                               \
    /* convert */                                                              \
    memcpy(&tm, &cdt.tm, sizeof(cdt.tm));                                      \
    if ((sdt.sec = db_struct2time(actualtzname, &tm)) == -1) {                 \
        return -1;                                                             \
    }                                                                          \
                                                                               \
    /* check if the time was correct in the first place */                     \
    if (cdt.tm.tm_isdst != tm.tm_isdst) {                                      \
        /* we've been corrected by library, correct and do it again */         \
        memcpy(&tm, &cdt.tm, sizeof(struct tm));                               \
        tm.tm_isdst = (tm.tm_isdst) ? 0 : 1;                                   \
                                                                               \
        if ((sdt.sec = db_struct2time(actualtzname, &tm)) == -1) {             \
            return -1;                                                         \
        }                                                                      \
    }                                                                          \
                                                                               \
    /* fill in */                                                              \
    sdt.flag = 0;                                                              \
    bset(&sdt, data_bit);                                                      \
                                                                               \
    /*normalize*/                                                              \
    sdt.to_frac = (cdt.from_frac * IPOW10(to_prec) / IPOW10(from_prec)) %      \
                  IPOW10(to_prec);                                             \
    sdt.sec += (cdt.from_frac * IPOW10(to_prec) / IPOW10(from_prec)) /         \
               IPOW10(to_prec);                                                \
                                                                               \
    if (datetime_check_range(sdt.sec, sdt.to_frac))                            \
        return -1;                                                             \
                                                                               \
    int8_to_int8b(sdt.sec, (int8b *)&sdt.sec);                                 \
    if (!(p_out = server_##to_dt##_put(&sdt, p_out, p_out_end))) {             \
        return -1;                                                             \
    }                                                                          \
    *outdtsz = p_out - p_out_start;                                            \
                                                                               \
    return 0;                                                                  \
/* END OF CLIENT_DT_to_SERVER_DT_func_body */

int CLIENT_DATETIME_to_SERVER_DATETIME(C2S_FUNKY_ARGS)
{
    CLIENT_DT_to_SERVER_DT_func_body(
        datetime, DATETIME, 3, msec, unsigned short, /* from */
        datetime, DATETIME, 3, msec, unsigned short /* to   */);
}

int CLIENT_DATETIMEUS_to_SERVER_DATETIME(C2S_FUNKY_ARGS)
{
    if (gbl_forbid_datetime_truncation)
        return -1;
    CLIENT_DT_to_SERVER_DT_func_body(
        datetimeus, DATETIMEUS, 6, usec, unsigned int, /* from */
        datetime, DATETIME, 3, msec, unsigned short /* to   */);
}

int CLIENT_DATETIME_to_SERVER_DATETIMEUS(C2S_FUNKY_ARGS)
{
    if (gbl_forbid_datetime_promotion)
        return -1;
    CLIENT_DT_to_SERVER_DT_func_body(
        datetime, DATETIME, 3, msec, unsigned short, /* from */
        datetimeus, DATETIMEUS, 6, usec, unsigned int /* to   */);
}

int CLIENT_DATETIMEUS_to_SERVER_DATETIMEUS(C2S_FUNKY_ARGS)
{
    CLIENT_DT_to_SERVER_DT_func_body(
        datetimeus, DATETIMEUS, 6, usec, unsigned int, /* from */
        datetimeus, DATETIMEUS, 6, usec, unsigned int /* to   */);
}

#define CLT_DATETIME_TO_SRV_TYPE(to)                                           \
    server_datetime_t sdt;                                                     \
    int tmp;                                                                   \
    int rc;                                                                    \
                                                                               \
    rc = CLIENT_DATETIME_to_SERVER_DATETIME(in, inlen, isnull, inopts, inblob, \
                                            &sdt, sizeof(sdt), &tmp, NULL,     \
                                            NULL);                             \
    if (rc)                                                                    \
        return rc;                                                             \
                                                                               \
    return SERVER_DATETIME_to_SERVER_##to(&sdt, sizeof(sdt), NULL, NULL, out,  \
                                          outlen, outdtsz, outopts, outblob);

static TYPES_INLINE int CLIENT_DATETIME_to_SERVER_UINT(C2S_FUNKY_ARGS)
{

    CLT_DATETIME_TO_SRV_TYPE(UINT);
}

static TYPES_INLINE int CLIENT_DATETIME_to_SERVER_BINT(C2S_FUNKY_ARGS)
{

    CLT_DATETIME_TO_SRV_TYPE(BINT);
}

static TYPES_INLINE int CLIENT_DATETIME_to_SERVER_BREAL(C2S_FUNKY_ARGS)
{

    CLT_DATETIME_TO_SRV_TYPE(BREAL);
}

#define CLT_DATETIMEUS_TO_SRV_TYPE(to)                                         \
    server_datetimeus_t sdt;                                                   \
    int tmp;                                                                   \
    int rc;                                                                    \
                                                                               \
    rc = CLIENT_DATETIMEUS_to_SERVER_DATETIMEUS(in, inlen, isnull, inopts,     \
                                                inblob, &sdt, sizeof(sdt),     \
                                                &tmp, NULL, NULL);             \
    if (rc)                                                                    \
        return rc;                                                             \
                                                                               \
    return SERVER_DATETIMEUS_to_SERVER_##to(&sdt, sizeof(sdt), NULL, NULL,     \
                                            out, outlen, outdtsz, outopts,     \
                                            outblob);

static TYPES_INLINE int CLIENT_DATETIMEUS_to_SERVER_UINT(C2S_FUNKY_ARGS)
{
    CLT_DATETIMEUS_TO_SRV_TYPE(UINT);
}

static TYPES_INLINE int CLIENT_DATETIMEUS_to_SERVER_BINT(C2S_FUNKY_ARGS)
{
    CLT_DATETIMEUS_TO_SRV_TYPE(BINT);
}

static TYPES_INLINE int CLIENT_DATETIMEUS_to_SERVER_BREAL(C2S_FUNKY_ARGS)
{
    CLT_DATETIMEUS_TO_SRV_TYPE(BREAL);
}

#define CLIENT_CSTR_to_SERVER_DT_func_body(dt, UDT, prec, frac, fracdt)        \
    int rc = 0;                                                                \
                                                                               \
    /* paranoia tests */                                                       \
    /*if( !inopts || !(inopts->flags & FLD_CONV_TZONE)) return -1;*/           \
    if (!in || !out)                                                           \
        return -1;                                                             \
                                                                               \
    /* null? */                                                                \
    if (isnull) {                                                              \
        set_null(out, outlen);                                                 \
        return 0;                                                              \
    }                                                                          \
                                                                               \
    /* patch: the string could contain actually a number */                    \
    rc = _isISO8601((char *)in, inlen);                                        \
    if (rc > 0) {                                                              \
        int ret;                                                               \
        CLT_TP(dt) cdt;                                                        \
        uint8_t cdt_buf[sizeof(cdt)];                                          \
        struct field_conv_opts_tz tzopts;                                      \
                                                                               \
        uint8_t *p_buf = cdt_buf;                                              \
        uint8_t *p_cdt_buf_start = cdt_buf;                                    \
        const uint8_t *p_cdt_buf_end = cdt_buf + sizeof(cdt_buf);              \
                                                                               \
        bzero(&cdt, sizeof(cdt));                                              \
                                                                               \
        if ((ret = string2struct##dt##_ISO((char *)in, inlen, &cdt)) != 0) {   \
            return ret;                                                        \
        }                                                                      \
                                                                               \
        if (!cdt.tzname[0] &&                                                  \
            (!inopts || !(inopts->flags & FLD_CONV_TZONE))) {                  \
            return -1;                                                         \
        }                                                                      \
                                                                               \
        if (!cdt.tzname[0])                                                    \
            strncpy(cdt.tzname, ((struct field_conv_opts_tz *)inopts)->tzname, \
                    sizeof(cdt.tzname));                                       \
                                                                               \
        if (inopts)                                                            \
            memcpy(&tzopts, inopts, sizeof(*inopts));                          \
                                                                               \
        else                                                                   \
            bzero(&tzopts, sizeof(tzopts));                                    \
                                                                               \
        tzopts.flags |= FLD_CONV_TZONE;                                        \
                                                                               \
        /* Remove the little-endian. */                                        \
        tzopts.flags &= ~(FLD_CONV_LENDIAN);                                   \
                                                                               \
        memcpy(tzopts.tzname, cdt.tzname, DB_MAX_TZNAMEDB);                    \
                                                                               \
        if (!(p_buf = client_##dt##_put(&cdt, cdt_buf, p_cdt_buf_end))) {      \
            return -1;                                                         \
        }                                                                      \
                                                                               \
        if ((ret = CLIENT_##UDT##_to_SERVER_##UDT(                             \
                 &cdt_buf, sizeof(cdt_buf), 0,                                 \
                 (struct field_conv_opts *)&tzopts, NULL, out, outlen,         \
                 outdtsz, outopts, NULL)) != 0) {                              \
            return ret;                                                        \
        }                                                                      \
                                                                               \
        /* hack to determine the correct setting for the tm.tm_isdst flag*/    \
        if (!cdt.tm.tm_isdst) {                                                \
            return 0;                                                          \
        }                                                                      \
                                                                               \
        /* time was DST, and cdt is now changed, time transform again this */  \
        /* time with DST set*/                                                 \
        bzero(&cdt, sizeof(cdt));                                              \
        if ((ret = string2struct##dt##_ISO((char *)in, inlen, &cdt)) != 0) {   \
            return ret;                                                        \
        }                                                                      \
        cdt.tm.tm_isdst = 1;                                                   \
        if (!cdt.tzname[0])                                                    \
            strncpy(cdt.tzname, ((struct field_conv_opts_tz *)inopts)->tzname, \
                    sizeof(cdt.tzname));                                       \
                                                                               \
        if (!(p_buf = client_##dt##_put(&cdt, cdt_buf, p_cdt_buf_end))) {      \
            return -1;                                                         \
        }                                                                      \
                                                                               \
        rc = CLIENT_##UDT##_to_SERVER_##UDT(                                   \
            &cdt_buf, sizeof(cdt_buf), 0, (struct field_conv_opts *)&tzopts,   \
            NULL, out, outlen, outdtsz, outopts, NULL);                        \
                                                                               \
        return rc;                                                             \
    } else if (rc == 0) {                                                      \
        /* we have here a epoch seconds and a fractional part as msec */       \
        double tmp = 0;                                                        \
        SRV_TP(dt) sdt;                                                        \
        char nulterm[inlen + 1];                                               \
                                                                               \
        uint8_t *p_out = out;                                                  \
        const uint8_t *p_out_start = out;                                      \
        const uint8_t *p_out_end = (const uint8_t *)out + outlen;              \
                                                                               \
        if (outlen != sizeof(SRV_TP(dt)))                                      \
            return -1;                                                         \
                                                                               \
        memcpy(nulterm, in, inlen);                                            \
        nulterm[inlen] = 0;                                                    \
        tmp = atof(nulterm);                                                   \
        if (_splitReal_##prec(tmp, &sdt.sec, &sdt.frac))                       \
            return -1;                                                         \
                                                                               \
        /* THIS ALWAYS WRITES CORRECT DATETIMES */                             \
        sdt.flag = 0;                                                          \
        bset(&sdt, data_bit);                                                  \
                                                                               \
        if (datetime_check_range(sdt.sec, sdt.frac))                           \
            return -1;                                                         \
                                                                               \
        int8_to_int8b(sdt.sec, (int8b *)&sdt.sec);                             \
        if (!(p_out = server_##dt##_put(&sdt, p_out, p_out_end))) {            \
            return -1;                                                         \
        }                                                                      \
        *outdtsz = p_out - p_out_start;                                        \
                                                                               \
        return 0;                                                              \
    } else {                                                                   \
        /* error */                                                            \
        return -1;                                                             \
    }                                                                          \
/* END OF CLIENT_CSTR_to_SERVER_DT_func_body */

int CLIENT_CSTR_to_SERVER_DATETIME(C2S_FUNKY_ARGS)
{
    CLIENT_CSTR_to_SERVER_DT_func_body(datetime, DATETIME, 3, msec,
                                       unsigned short);
}

int CLIENT_CSTR_to_SERVER_DATETIMEUS(C2S_FUNKY_ARGS)
{
    CLIENT_CSTR_to_SERVER_DT_func_body(datetimeus, DATETIMEUS, 6, usec,
                                       unsigned int);
}

int CLIENT_PSTR_to_SERVER_DATETIME(C2S_FUNKY_ARGS)
{
    return CLIENT_CSTR_to_SERVER_DATETIME(in, inlen, isnull, inopts, inblob,
                                          out, outlen, outdtsz, outopts,
                                          outblob);
}

int CLIENT_PSTR_to_SERVER_DATETIMEUS(C2S_FUNKY_ARGS)
{
    return CLIENT_CSTR_to_SERVER_DATETIMEUS(in, inlen, isnull, inopts, inblob,
                                            out, outlen, outdtsz, outopts,
                                            outblob);
}

int CLIENT_PSTR2_to_SERVER_DATETIME(C2S_FUNKY_ARGS)
{
    return CLIENT_CSTR_to_SERVER_DATETIME(in, inlen, isnull, inopts, inblob,
                                          out, outlen, outdtsz, outopts,
                                          outblob);
}

int CLIENT_PSTR2_to_SERVER_DATETIMEUS(C2S_FUNKY_ARGS)
{
    return CLIENT_CSTR_to_SERVER_DATETIMEUS(in, inlen, isnull, inopts, inblob,
                                            out, outlen, outdtsz, outopts,
                                            outblob);
}

#define CLIENT_UINT_to_SERVER_DT_func_body(dt, UDT, prec, frac, fracdt)        \
    int rc = 0;                                                                \
    char *tmp = alloca(inlen + 1);                                             \
    int outtmp;                                                                \
    db_time_t dbtime;                                                          \
                                                                               \
    if (!tmp)                                                                  \
        return -1;                                                             \
                                                                               \
    rc = CLIENT_UINT_to_CLIENT_INT(in, inlen, inopts, inblob, &dbtime,         \
                                   sizeof(dbtime), &outtmp, NULL, NULL);       \
    if (isnull || rc)                                                          \
        return rc;                                                             \
    dbtime = flibc_ntohll(dbtime);                                             \
    if (datetime_check_range(dbtime, 0))                                       \
        return -1;                                                             \
                                                                               \
    rc = CLIENT_UINT_to_SERVER_UINT(in, inlen, isnull, inopts, inblob, tmp,    \
                                    inlen + 1, &outtmp, NULL, NULL);           \
    if (isnull || rc)                                                          \
        return rc;                                                             \
                                                                               \
    rc = SERVER_UINT_to_SERVER_##UDT(tmp, outtmp, NULL, NULL, out, outlen,     \
                                     outdtsz, outopts, outblob);               \
                                                                               \
    return rc;                                                                 \
/* END OF CLIENT_UINT_to_SERVER_DT_func_body */

static TYPES_INLINE int CLIENT_UINT_to_SERVER_DATETIME(C2S_FUNKY_ARGS)
{
    CLIENT_UINT_to_SERVER_DT_func_body(datetime, DATETIME, 3, msec,
                                       unsigned short);
}

static TYPES_INLINE int CLIENT_UINT_to_SERVER_DATETIMEUS(C2S_FUNKY_ARGS)
{
    CLIENT_UINT_to_SERVER_DT_func_body(datetimeus, DATETIMEUS, 6, usec,
                                       unsigned int);
}

#define CLIENT_INT_to_SERVER_DT_func_body(dt, UDT, prec, frac, fracdt)         \
    int rc = 0;                                                                \
    char *tmp = alloca(inlen + 1);                                             \
    int outtmp;                                                                \
    db_time_t dbtime;                                                          \
                                                                               \
    if (!tmp)                                                                  \
        return -1;                                                             \
                                                                               \
    rc = CLIENT_INT_to_CLIENT_INT(in, inlen, inopts, inblob, &dbtime,          \
                                  sizeof(dbtime), &outtmp, NULL, NULL);        \
    if (isnull || rc)                                                          \
        return rc;                                                             \
    dbtime = flibc_ntohll(dbtime);                                             \
    if (datetime_check_range(dbtime, 0))                                       \
        return -1;                                                             \
                                                                               \
    rc = CLIENT_INT_to_SERVER_BINT(in, inlen, isnull, inopts, inblob, tmp,     \
                                   inlen + 1, &outtmp, NULL, NULL);            \
    if (isnull || rc)                                                          \
        return rc;                                                             \
                                                                               \
    rc = SERVER_BINT_to_SERVER_##UDT(tmp, outtmp, NULL, NULL, out, outlen,     \
                                     outdtsz, outopts, outblob);               \
                                                                               \
    return rc;                                                                 \
/* END OF CLIENT_INT_to_SERVER_DT_func_body */

TYPES_INLINE int CLIENT_INT_to_SERVER_DATETIME(C2S_FUNKY_ARGS)
{
    CLIENT_INT_to_SERVER_DT_func_body(datetime, DATETIME, 3, msec,
                                      unsigned short);
}

TYPES_INLINE int CLIENT_INT_to_SERVER_DATETIMEUS(C2S_FUNKY_ARGS)
{
    CLIENT_INT_to_SERVER_DT_func_body(datetimeus, DATETIMEUS, 6, usec,
                                      unsigned int);
}

#define check_server_dt_func_body(dt, UDT, prec, frac, fracdt)                 \
    SRV_TP(dt) sdt;                                                            \
    const uint8_t *p_in;                                                       \
    const uint8_t *p_in_end;                                                   \
                                                                               \
    /* range validation */                                                     \
    p_in = (const uint8_t *)out;                                               \
    p_in_end = (const uint8_t *)out + sizeof(sdt);                             \
    if (!server_##dt##_get(&sdt, p_in, p_in_end))                              \
        return -1;                                                             \
    int8b_to_int8(sdt.sec, (comdb2_int8 *)&sdt.sec);                           \
                                                                               \
    if (datetime_check_range(sdt.sec, sdt.frac))                               \
        return -1;                                                             \
    return 0;
/* END OF check_server_dt_func_body */

int check_server_datetime(void *out)
{
    check_server_dt_func_body(datetime, DATETIME, 3, msec, unsigned short);
}

int check_server_datetimeus(void *out)
{
    check_server_dt_func_body(datetimeus, DATETIMEUS, 6, usec, unsigned int);
}

#define CLIENT_REAL_to_SERVER_DT_func_body(dt, UDT, prec, frac, fracdt)        \
    int rc = 0;                                                                \
    char *tmp = alloca(inlen + 1);                                             \
    int outtmp;                                                                \
                                                                               \
    if (!tmp)                                                                  \
        return -1;                                                             \
                                                                               \
    rc = CLIENT_REAL_to_SERVER_BREAL(in, inlen, isnull, inopts, inblob, tmp,   \
                                     inlen + 1, &outtmp, NULL, NULL);          \
    if (isnull || rc)                                                          \
        return rc;                                                             \
                                                                               \
    rc = SERVER_BREAL_to_SERVER_##UDT(tmp, outtmp, NULL, NULL, out, outlen,    \
                                      outdtsz, outopts, outblob);              \
                                                                               \
    if (check_server_##dt(out))                                                \
        return -1;                                                             \
    return rc;                                                                 \
/* END OF CLIENT_REAL_to_SERVER_DT_func_body */

int CLIENT_REAL_to_SERVER_DATETIME(C2S_FUNKY_ARGS)
{
    CLIENT_REAL_to_SERVER_DT_func_body(datetime, DATETIME, 3, msec,
                                       unsigned short);
}

int CLIENT_REAL_to_SERVER_DATETIMEUS(C2S_FUNKY_ARGS)
{
    CLIENT_REAL_to_SERVER_DT_func_body(datetimeus, DATETIMEUS, 6, usec,
                                       unsigned int);
}

#define C2C_FUNKY_ARGS                                                         \
    const void *in, int inlen, const struct field_conv_opts *inopts,           \
        blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,            \
        const struct field_conv_opts *outopts, blob_buffer_t *outblob

static TYPES_INLINE int CLIENT_to_CLIENT_NO_CONV(C2C_FUNKY_ARGS)
{
    *outdtsz = 0;
    return -1;
}

#define CLIENT_DATETIME_to_CLIENT_UINT CLIENT_to_CLIENT_NO_CONV
#define CLIENT_DATETIME_to_CLIENT_INT CLIENT_to_CLIENT_NO_CONV
#define CLIENT_DATETIME_to_CLIENT_REAL CLIENT_to_CLIENT_NO_CONV
#define CLIENT_DATETIME_to_CLIENT_CSTR CLIENT_to_CLIENT_NO_CONV
#define CLIENT_DATETIME_to_CLIENT_PSTR CLIENT_to_CLIENT_NO_CONV
#define CLIENT_DATETIME_to_CLIENT_BYTEARRAY CLIENT_to_CLIENT_NO_CONV
#define CLIENT_DATETIME_to_CLIENT_PSTR2 CLIENT_to_CLIENT_NO_CONV
#define CLIENT_DATETIME_to_CLIENT_BLOB CLIENT_to_CLIENT_NO_CONV
#define CLIENT_UINT_to_CLIENT_DATETIME CLIENT_to_CLIENT_NO_CONV
#define CLIENT_INT_to_CLIENT_DATETIME CLIENT_to_CLIENT_NO_CONV
#define CLIENT_REAL_to_CLIENT_DATETIME CLIENT_to_CLIENT_NO_CONV
#define CLIENT_CSTR_to_CLIENT_DATETIME CLIENT_to_CLIENT_NO_CONV
#define CLIENT_PSTR_to_CLIENT_DATETIME CLIENT_to_CLIENT_NO_CONV
#define CLIENT_BYTEARRAY_to_CLIENT_DATETIME CLIENT_to_CLIENT_NO_CONV
#define CLIENT_PSTR2_to_CLIENT_DATETIME CLIENT_to_CLIENT_NO_CONV
#define CLIENT_BLOB_to_CLIENT_DATETIME CLIENT_to_CLIENT_NO_CONV

#define CLIENT_DATETIMEUS_to_CLIENT_UINT CLIENT_to_CLIENT_NO_CONV
#define CLIENT_DATETIMEUS_to_CLIENT_INT CLIENT_to_CLIENT_NO_CONV
#define CLIENT_DATETIMEUS_to_CLIENT_REAL CLIENT_to_CLIENT_NO_CONV
#define CLIENT_DATETIMEUS_to_CLIENT_CSTR CLIENT_to_CLIENT_NO_CONV
#define CLIENT_DATETIMEUS_to_CLIENT_PSTR CLIENT_to_CLIENT_NO_CONV
#define CLIENT_DATETIMEUS_to_CLIENT_BYTEARRAY CLIENT_to_CLIENT_NO_CONV
#define CLIENT_DATETIMEUS_to_CLIENT_PSTR2 CLIENT_to_CLIENT_NO_CONV
#define CLIENT_DATETIMEUS_to_CLIENT_BLOB CLIENT_to_CLIENT_NO_CONV
#define CLIENT_UINT_to_CLIENT_DATETIMEUS CLIENT_to_CLIENT_NO_CONV
#define CLIENT_INT_to_CLIENT_DATETIMEUS CLIENT_to_CLIENT_NO_CONV
#define CLIENT_REAL_to_CLIENT_DATETIMEUS CLIENT_to_CLIENT_NO_CONV
#define CLIENT_CSTR_to_CLIENT_DATETIMEUS CLIENT_to_CLIENT_NO_CONV
#define CLIENT_PSTR_to_CLIENT_DATETIMEUS CLIENT_to_CLIENT_NO_CONV
#define CLIENT_BYTEARRAY_to_CLIENT_DATETIMEUS CLIENT_to_CLIENT_NO_CONV
#define CLIENT_PSTR2_to_CLIENT_DATETIMEUS CLIENT_to_CLIENT_NO_CONV
#define CLIENT_BLOB_to_CLIENT_DATETIMEUS CLIENT_to_CLIENT_NO_CONV

#define CLIENT_DT_to_CLIENT_DT_func_body(from_dt, FROM_UDT, from_prec,         \
                                         from_frac, from_fracdt, to_dt,        \
                                         TO_UDT, to_prec, to_frac, to_fracdt)  \
    struct field_conv_opts_tz *tzopts = (struct field_conv_opts_tz *)inopts;   \
                                                                               \
    struct field_conv_opts_tz *tzoutopts =                                     \
        (struct field_conv_opts_tz *)outopts;                                  \
                                                                               \
    if (!inopts || !outopts || !(inopts->flags & FLD_CONV_TZONE) ||            \
        !(outopts->flags & FLD_CONV_TZONE))                                    \
        /* we need TZ information */                                           \
        return -1;                                                             \
                                                                               \
    /* if the timezones are the same, just copy it over */                     \
    if (!strncmp(tzopts->tzname, tzoutopts->tzname, DB_MAX_TZNAMEDB)) {        \
        memcpy(out, in, inlen);                                                \
        *outdtsz = inlen;                                                      \
        return 0;                                                              \
    }                                                                          \
                                                                               \
    /* different TZones, the way to convert is through server representation   \
     */                                                                        \
    {                                                                          \
        SRV_TP(to_dt) tmp;                                                     \
        int tmplen;                                                            \
        int outnull;                                                           \
                                                                               \
        if (CLIENT_##FROM_UDT##_to_SERVER_##TO_UDT(in, inlen, 0, inopts, NULL, \
                                                   &tmp, sizeof(tmp), &tmplen, \
                                                   NULL, NULL))                \
            return -1;                                                         \
                                                                               \
        return SERVER_##TO_UDT##_to_CLIENT_##TO_UDT(                           \
            &tmp, sizeof(tmp), NULL, NULL, out, outlen, &outnull, outdtsz,     \
            outopts, NULL);                                                    \
    }
/* END OF CLIENT_DT_to_CLIENT_DT_func_body */

static TYPES_INLINE int CLIENT_DATETIME_to_CLIENT_DATETIME(C2C_FUNKY_ARGS)
{
    CLIENT_DT_to_CLIENT_DT_func_body(
        datetime, DATETIME, 3, msec, unsigned short, /* from */
        datetime, DATETIME, 3, msec, unsigned short /* to   */);
}

static TYPES_INLINE int CLIENT_DATETIMEUS_to_CLIENT_DATETIME(C2C_FUNKY_ARGS)
{
    if (gbl_forbid_datetime_truncation)
        return -1;
    CLIENT_DT_to_CLIENT_DT_func_body(
        datetimeus, DATETIMEUS, 6, usec, unsigned int, /* from */
        datetime, DATETIME, 3, msec, unsigned short /* to   */);
}

static TYPES_INLINE int CLIENT_DATETIME_to_CLIENT_DATETIMEUS(C2C_FUNKY_ARGS)
{
    if (gbl_forbid_datetime_promotion)
        return -1;
    CLIENT_DT_to_CLIENT_DT_func_body(
        datetime, DATETIME, 3, msec, unsigned short, /* from */
        datetimeus, DATETIMEUS, 6, usec, unsigned int /* to   */);
}

static TYPES_INLINE int CLIENT_DATETIMEUS_to_CLIENT_DATETIMEUS(C2C_FUNKY_ARGS)
{
    CLIENT_DT_to_CLIENT_DT_func_body(
        datetimeus, DATETIMEUS, 6, usec, unsigned int, /* from */
        datetimeus, DATETIMEUS, 6, usec, unsigned int /* to   */);
}

static TYPES_INLINE int SERVER_to_SERVER_NO_CONV(S2S_FUNKY_ARGS) { return -1; }

#define SERVER_BYTEARRAY_to_SERVER_DATETIME SERVER_to_SERVER_NO_CONV
#define SERVER_BYTEARRAY_to_SERVER_DATETIMEUS SERVER_to_SERVER_NO_CONV
#define SERVER_BLOB_to_SERVER_DATETIME SERVER_to_SERVER_NO_CONV
#define SERVER_BLOB_to_SERVER_DATETIMEUS SERVER_to_SERVER_NO_CONV
#define SERVER_BLOB2_to_SERVER_DATETIME SERVER_to_SERVER_NO_CONV
#define SERVER_BLOB2_to_SERVER_DATETIMEUS SERVER_to_SERVER_NO_CONV
#define SERVER_BLOB2_to_SERVER_BYTEARRAY SERVER_BLOB_to_SERVER_BYTEARRAY

#define SERVER_DATETIME_to_SERVER_BCSTR SERVER_to_SERVER_NO_CONV
#define SERVER_DATETIME_to_SERVER_BYTEARRAY SERVER_to_SERVER_NO_CONV
#define SERVER_DATETIME_to_SERVER_BLOB SERVER_to_SERVER_NO_CONV

#define SERVER_DATETIMEUS_to_SERVER_BCSTR SERVER_to_SERVER_NO_CONV
#define SERVER_DATETIMEUS_to_SERVER_BYTEARRAY SERVER_to_SERVER_NO_CONV
#define SERVER_DATETIMEUS_to_SERVER_BLOB SERVER_to_SERVER_NO_CONV

#define SERVER_UINT_to_SERVER_BLOB2 SERVER_to_SERVER_NO_CONV
#define SERVER_BINT_to_SERVER_BLOB2 SERVER_to_SERVER_NO_CONV
#define SERVER_BREAL_to_SERVER_BLOB2 SERVER_to_SERVER_NO_CONV
#define SERVER_BCSTR_to_SERVER_BLOB2 SERVER_to_SERVER_NO_CONV
#define SERVER_BYTEARRAY_to_SERVER_BLOB2 SERVER_BYTEARRAY_to_SERVER_BLOB
#define SERVER_BLOB_to_SERVER_BLOB2 SERVER_to_SERVER_NO_CONV
#define SERVER_DATETIME_to_SERVER_BLOB2 SERVER_to_SERVER_NO_CONV
#define SERVER_DATETIMEUS_to_SERVER_BLOB2 SERVER_to_SERVER_NO_CONV
#define SERVER_INTVYM_to_SERVER_BLOB2 SERVER_to_SERVER_NO_CONV
#define SERVER_INTVDS_to_SERVER_BLOB2 SERVER_to_SERVER_NO_CONV
#define SERVER_INTVDSUS_to_SERVER_BLOB2 SERVER_to_SERVER_NO_CONV
#define SERVER_VUTF8_to_SERVER_BLOB2 SERVER_to_SERVER_NO_CONV
#define SERVER_DECIMAL_to_SERVER_BLOB2 SERVER_to_SERVER_NO_CONV

static TYPES_INLINE int SERVER_DATETIME_to_SERVER_DATETIME(S2S_FUNKY_ARGS)
{

    memcpy(out, in, inlen);

    /* TMP BROKEN DATETIME */
    if (*(char *)in == 0) {
        /* ALWAYS WRITE CORRECT DATETIMES*/
        bset(out, data_bit);
        /* TODO huh? we mark it fixed but we don't actually swizzle the bit
         * inside the seconds member? */
    }

    /* good datetime */
    *outdtsz = inlen;
    return 0;
}

static TYPES_INLINE int SERVER_DATETIMEUS_to_SERVER_DATETIMEUS(S2S_FUNKY_ARGS)
{
    return SERVER_DATETIME_to_SERVER_DATETIME(
        in, inlen, inopts, inblob, out, outlen, outdtsz, outopts, outblob);
}

#define SERVER_DT_to_SERVER_DT_func_body(from_dt, FROM_UDT, from_prec,         \
                                         from_frac, from_fracdt, to_dt,        \
                                         TO_UDT, to_prec, to_frac, to_fracdt)  \
    SRV_TP(from_dt) sdt;                                                       \
    SRV_TP(to_dt) tmp;                                                         \
    const uint8_t *p_in = in;                                                  \
    const uint8_t *p_in_end = (const uint8_t *)in + inlen;                     \
    uint8_t *p_out = out;                                                      \
    const uint8_t *p_out_start = out;                                          \
    const uint8_t *p_out_end = (const uint8_t *)out + outlen;                  \
                                                                               \
    /* null? */                                                                \
    if (stype_is_null(in)) {                                                   \
        set_null(out, outlen);                                                 \
        return 0;                                                              \
    }                                                                          \
                                                                               \
    /* unpack */                                                               \
    if (!server_##from_dt##_get(&sdt, p_in, p_in_end))                         \
        return -1;                                                             \
                                                                               \
    /* TMP BROKEN DATETIME */                                                  \
    if (*(char *)in == 0) {                                                    \
        /* ALWAYS WRITE CORRECT DATETIMES*/                                    \
        bset(out, data_bit);                                                   \
    }                                                                          \
                                                                               \
    tmp.flag = sdt.flag;                                                       \
    tmp.sec = sdt.sec;                                                         \
    tmp.to_frac = sdt.from_frac * POW10(to_prec) / POW10(from_prec);           \
                                                                               \
    if (!(p_out = server_##to_dt##_put(&tmp, p_out, p_out_end))) {             \
        return -1;                                                             \
    }                                                                          \
                                                                               \
    *outdtsz = p_out - p_out_start;                                            \
    return 0;                                                                  \
/* END OF SERVER_DT_to_SERVER_DT_func_body */

static TYPES_INLINE int SERVER_DATETIME_to_SERVER_DATETIMEUS(S2S_FUNKY_ARGS)
{
    if (gbl_forbid_datetime_ms_us_s2s)
        return -1;
    SERVER_DT_to_SERVER_DT_func_body(
        datetime, DATETIME, 3, msec, unsigned short, /* from */
        datetimeus, DATETIMEUS, 6, usec, unsigned int /* to   */);
}

static TYPES_INLINE int SERVER_DATETIMEUS_to_SERVER_DATETIME(S2S_FUNKY_ARGS)
{
    if (gbl_forbid_datetime_ms_us_s2s)
        return -1;
    SERVER_DT_to_SERVER_DT_func_body(
        datetimeus, DATETIMEUS, 6, usec, unsigned int, /* from */
        datetime, DATETIME, 3, msec, unsigned short /* to   */);
}

#define SERVER_UINT_to_SERVER_DT_func_body(dt, UDT, prec, frac, fracdt)        \
    int rc = 0;                                                                \
    SRV_TP(dt) *sdt = (SRV_TP(dt) *)out;                                       \
                                                                               \
    if (sizeof(SRV_TP(dt)) != outlen)                                          \
        return -1;                                                             \
                                                                               \
    rc = SERVER_UINT_to_SERVER_BINT(in, inlen, inopts, inblob, out,            \
                                    sizeof(sdt->sec) + 1, outdtsz, outopts,    \
                                    outblob);                                  \
    if (rc)                                                                    \
        return rc;                                                             \
                                                                               \
    bzero(&sdt->frac, sizeof(sdt->frac));                                      \
    *outdtsz = sizeof(SRV_TP(dt));                                             \
                                                                               \
    if (check_server_##dt(out))                                                \
        return -1;                                                             \
                                                                               \
    return 0;                                                                  \
/* END OF SERVER_UINT_to_SERVER_DT_func_body */

static TYPES_INLINE int SERVER_UINT_to_SERVER_DATETIME(S2S_FUNKY_ARGS)
{
    SERVER_UINT_to_SERVER_DT_func_body(datetime, DATETIME, 3, msec,
                                       unsigned short);
}

static TYPES_INLINE int SERVER_UINT_to_SERVER_DATETIMEUS(S2S_FUNKY_ARGS)
{
    SERVER_UINT_to_SERVER_DT_func_body(datetimeus, DATETIMEUS, 6, usec,
                                       unsigned int);
}

#define SERVER_BINT_to_SERVER_DT_func_body(dt, UDT, prec, frac, fracdt)        \
    int rc = 0;                                                                \
    SRV_TP(dt) *sdt = (SRV_TP(dt) *)out;                                       \
                                                                               \
    if (sizeof(SRV_TP(dt)) != outlen)                                          \
        return -1;                                                             \
                                                                               \
    rc = SERVER_BINT_to_SERVER_BINT(in, inlen, inopts, inblob, out,            \
                                    sizeof(sdt->sec) + 1, outdtsz, outopts,    \
                                    outblob);                                  \
    if (rc)                                                                    \
        return rc;                                                             \
                                                                               \
    bzero(&sdt->frac, sizeof(sdt->frac));                                      \
    *outdtsz = sizeof(SRV_TP(dt));                                             \
                                                                               \
    if (check_server_##dt(out))                                                \
        return -1;                                                             \
                                                                               \
    return 0;                                                                  \
/* END OF SERVER_BINT_to_SERVER_DT_func_body */

static TYPES_INLINE int SERVER_BINT_to_SERVER_DATETIME(S2S_FUNKY_ARGS)
{
    SERVER_BINT_to_SERVER_DT_func_body(datetime, DATETIME, 3, msec,
                                       unsigned short);
}

static TYPES_INLINE int SERVER_BINT_to_SERVER_DATETIMEUS(S2S_FUNKY_ARGS)
{
    SERVER_BINT_to_SERVER_DT_func_body(datetimeus, DATETIMEUS, 6, usec,
                                       unsigned int);
}

#define SERVER_BREAL_to_SERVER_DT_func_body(dt, UDT, prec, frac, fracdt)       \
    int rc = 0;                                                                \
    SRV_TP(dt) sdt;                                                            \
    double tmp = 0.0;                                                          \
    int outnull = 0;                                                           \
                                                                               \
    uint8_t *p_out = out;                                                      \
    const uint8_t *p_out_start = out;                                          \
    const uint8_t *p_out_end = (const uint8_t *)out + outlen;                  \
                                                                               \
    if (sizeof(SRV_TP(dt)) != outlen)                                          \
        return -1;                                                             \
                                                                               \
    if (stype_is_null(in)) {                                                   \
        set_null(out, outlen);                                                 \
        return 0;                                                              \
    }                                                                          \
                                                                               \
    /* Replace 'outopts' with 'NULL' to prevent little-endian conversion. */   \
    rc = SERVER_BREAL_to_CLIENT_REAL(in, inlen, inopts, inblob, &tmp,          \
                                     sizeof(tmp), &outnull, outdtsz, NULL,     \
                                     outblob);                                 \
    if (rc || outnull)                                                         \
        return rc;                                                             \
                                                                               \
    sdt.flag = *(char *)in; /* propagate the flag*/                            \
                                                                               \
    tmp = flibc_ntohd(tmp);                                                    \
    if (_splitReal_##prec(tmp, &sdt.sec, &sdt.frac))                           \
        return -1;                                                             \
    if (datetime_check_range(sdt.sec, sdt.frac))                               \
        return -1;                                                             \
    int8_to_int8b(sdt.sec, (int8b *)&sdt.sec);                                 \
    if (!(p_out = server_##dt##_put(&sdt, p_out, p_out_end)))                  \
        return -1;                                                             \
    *outdtsz = p_out - p_out_start;                                            \
    return 0;                                                                  \
/* END OF SERVER_BREAL_to_SERVER_DT_func_body */

int SERVER_BREAL_to_SERVER_DATETIME(S2S_FUNKY_ARGS)
{
    SERVER_BREAL_to_SERVER_DT_func_body(datetime, DATETIME, 3, msec,
                                        unsigned short);
}

int SERVER_BREAL_to_SERVER_DATETIMEUS(S2S_FUNKY_ARGS)
{
    SERVER_BREAL_to_SERVER_DT_func_body(datetimeus, DATETIMEUS, 6, usec,
                                        unsigned int);
}

#define SERVER_BCSTR_to_SERVER_DT_func_body(dt, UDT, prec, frac, fracdt)       \
    /* null? */                                                                \
    if (stype_is_null(in)) {                                                   \
        set_null(out, outlen);                                                 \
        return 0;                                                              \
    }                                                                          \
                                                                               \
    *((char *)out) = *((char *)in); /* flag propagation */                     \
    return CLIENT_CSTR_to_SERVER_##UDT(((char *)in) + 1, inlen - 1, 0, inopts, \
                                       NULL, (char *)out + 1, outlen - 1,      \
                                       outdtsz, outopts, NULL);                \
/* END OF SERVER_BCSTR_to_SERVER_DT_func_body */

static TYPES_INLINE int SERVER_BCSTR_to_SERVER_DATETIME(S2S_FUNKY_ARGS)
{
    SERVER_BCSTR_to_SERVER_DT_func_body(datetime, DATETIME, 3, msec,
                                        unsigned short);
}

static TYPES_INLINE int SERVER_BCSTR_to_SERVER_DATETIMEUS(S2S_FUNKY_ARGS)
{
    SERVER_BCSTR_to_SERVER_DT_func_body(datetimeus, DATETIMEUS, 6, usec,
                                        unsigned int);
}

#define SERVER_DT_to_SERVER_UINT_func_body(dt, UDT, prec, frac, fracdt)        \
    if (inlen != sizeof(SRV_TP(dt)))                                           \
        return -1;                                                             \
                                                                               \
    /* TMP BROKEN DATETIME */                                                  \
    if (*(char *)in == 0) {                                                    \
            return SERVER_UINT_to_SERVER_UINT(in, 9, inopts, inblob, out,      \
                                              outlen, outdtsz, outopts,        \
                                              outblob);                        \
    }                                                                          \
                                                                               \
    /* good datetime */                                                        \
    return SERVER_BINT_to_SERVER_UINT(in, 9, inopts, inblob, out, outlen,      \
                                      outdtsz, outopts, outblob);              \
/* END OF SERVER_DT_to_SERVER_UINT_func_body */

static TYPES_INLINE int SERVER_DATETIME_to_SERVER_UINT(S2S_FUNKY_ARGS)
{
    SERVER_DT_to_SERVER_UINT_func_body(datetime, DATETIME, 3, msec,
                                       unsigned short);
}

static TYPES_INLINE int SERVER_DATETIMEUS_to_SERVER_UINT(S2S_FUNKY_ARGS)
{
    SERVER_DT_to_SERVER_UINT_func_body(datetimeus, DATETIMEUS, 6, usec,
                                       unsigned int);
}

#define SERVER_DT_to_SERVER_BINT_func_body(dt, UDT, prec, frac, fracdt)        \
    if (inlen != sizeof(SRV_TP(dt)))                                           \
        return -1;                                                             \
                                                                               \
    /* null? */                                                                \
    if (stype_is_null(in)) {                                                   \
        set_null(out, outlen);                                                 \
        return 0;                                                              \
    }                                                                          \
                                                                               \
    /* TMP BROKEN DATETIME */                                                  \
    if (*(char *)in == 0) {                                                    \
            return SERVER_UINT_to_SERVER_BINT(in, 9, inopts, inblob, out,      \
                                              outlen, outdtsz, outopts,        \
                                              outblob);                        \
    }                                                                          \
                                                                               \
    /* good datetime */                                                        \
    return SERVER_BINT_to_SERVER_BINT(in, 9, inopts, inblob, out, outlen,      \
                                      outdtsz, outopts, outblob);              \
/* END OF SERVER_DT_to_SERVER_BINT_func_body */

static TYPES_INLINE int SERVER_DATETIME_to_SERVER_BINT(S2S_FUNKY_ARGS)
{
    SERVER_DT_to_SERVER_BINT_func_body(datetime, DATETIME, 3, msec,
                                       unsigned short);
}

static TYPES_INLINE int SERVER_DATETIMEUS_to_SERVER_BINT(S2S_FUNKY_ARGS)
{
    SERVER_DT_to_SERVER_BINT_func_body(datetimeus, DATETIMEUS, 6, usec,
                                       unsigned int);
}

#define SERVER_DT_to_SERVER_BREAL_func_body(dt, UDT, prec, frac, fracdt)       \
    SRV_TP(dt) sdt;                                                            \
    double tmp;                                                                \
    const uint8_t *p_in = in;                                                  \
    const uint8_t *p_in_end = (const uint8_t *)in + inlen;                     \
                                                                               \
    /* null? */                                                                \
    if (stype_is_null(in)) {                                                   \
        set_null(out, outlen);                                                 \
        return 0;                                                              \
    }                                                                          \
                                                                               \
    /* unpack */                                                               \
    if (!server_##dt##_get(&sdt, p_in, p_in_end))                              \
        return -1;                                                             \
                                                                               \
    /* TMP BROKEN DATETIME */                                                  \
    if (*(char *)in == 0) {                                                    \
    } else                                                                     \
        /* good datetime */                                                    \
        int8b_to_int8(sdt.sec, (comdb2_int8 *)&sdt.sec);                       \
                                                                               \
    tmp = sdt.sec + ((double)sdt.frac) / POW10(prec);                          \
    tmp = flibc_htond(tmp);                                                    \
    return CLIENT_REAL_to_SERVER_BREAL(&tmp, sizeof(tmp), 0, NULL, NULL, out,  \
                                       outlen, outdtsz, outopts, outblob);     \
/* END OF SERVER_DT_to_SERVER_BREAL_func_body */

static int SERVER_DATETIME_to_SERVER_BREAL(S2S_FUNKY_ARGS)
{
    SERVER_DT_to_SERVER_BINT_func_body(datetime, DATETIME, 3, msec,
                                       unsigned short);
}

static int SERVER_DATETIMEUS_to_SERVER_BREAL(S2S_FUNKY_ARGS)
{
    SERVER_DT_to_SERVER_BINT_func_body(datetimeus, DATETIMEUS, 6, usec,
                                       unsigned int);
}

/* interval conversions */
enum intv_enum {
    INTV_YM = INTV_YM_TYPE,
    INTV_DS = INTV_DS_TYPE,
    INTV_DSUS = INTV_DSUS_TYPE,
    INTV_DEC = INTV_DECIMAL_TYPE
};

#define S2D(sec) ((int)((sec) / (24 * 3600)))
#define S2H(sec) ((int)(((sec) / 3600) % 24))
#define S2M(sec) ((int)(((sec) / 60) % 60))
#define S2S(sec) ((int)((sec) % 60))

#define _intv_srv2string_func_body(dt, UDT, prec, frac, fracdt)                \
    SRV_TP(dt) cin;                                                            \
    uint8_t *p_buf = (uint8_t *)in;                                            \
    uint8_t *p_buf_end = (p_buf + sizeof(SRV_TP(dt)));                         \
    char sign = 1;                                                             \
    long long sec;                                                             \
    fracdt frac;                                                               \
                                                                               \
    server_##dt##_get(&cin, p_buf, p_buf_end);                                 \
                                                                               \
    /* around aligment issues */                                               \
    memcpy(&sec, &cin.sec, sizeof(sec));                                       \
    int8b_to_int8(sec, (comdb2_int8 *)&sec);                                   \
    memcpy(&frac, &cin.frac, sizeof(frac));                                    \
                                                                               \
    if (sec < 0) {                                                             \
        sign = -1;                                                             \
        sec = -sec;                                                            \
        if (frac) {                                                            \
            sec--;                                                             \
        }                                                                      \
    }                                                                          \
                                                                               \
    snprintf(out, outlen - 1, "%s%u %2.2u:%2.2u:%2.2u.%*.*u",                  \
             (sign == -1) ? "- " : "", S2D(sec), S2H(sec), S2M(sec), S2S(sec), \
             prec, prec, frac);                                                \
    out[outlen - 1] = 0;                                                       \
/* END OF _intv_srv2string_func_body */

/* low level conversion routines, no arg check; this is resposability of the
 * caller */
static int _intv_srv2string(const void *in, const enum intv_enum type,
                            char *out, const int outlen)
{

    switch (type) {
    case INTV_YM: {
        server_intv_ym_t cin; /* (server_intv_ym_t*)in; */
        uint8_t *p_buf = (uint8_t *)in;
        uint8_t *p_buf_end = (p_buf + sizeof(server_intv_ym_t));
        int months = 0;
        char sign = 1;

        server_intv_ym_get(&cin, p_buf, p_buf_end);
        memcpy(&months, &cin.months, sizeof(months));
        int4b_to_int4(months, (comdb2_int4 *)&months);

        if (months < 0) {
            sign = -1;
            months = -months;
        }

        snprintf(out, outlen - 1, "%s%u-%2.2u", (sign == -1) ? "- " : "",
                 months / 12, months % 12);
        out[outlen - 1] = 0;
    } break;
    case INTV_DS: {
        _intv_srv2string_func_body(intv_ds, INTVDS, 3, msec, unsigned short);
    } break;
    case INTV_DSUS: {
        _intv_srv2string_func_body(intv_dsus, INTVDSUS, 6, usec, unsigned int);
    } break;
    default:
        return -1;
    }

    return 0;
}

#define _intv_clt2string_func_body(dt, UDT, prec, frac, fracdt)                \
    CLT_TP(dt) cin;                                                            \
    uint8_t *p_buf = (uint8_t *)in;                                            \
    uint8_t *p_buf_end = p_buf + sizeof(CLT_TP(dt));                           \
                                                                               \
    if (inopts && inopts->flags & FLD_CONV_LENDIAN) {                          \
        client_##dt##_little_get(&cin, p_buf, p_buf_end);                      \
    } else {                                                                   \
        client_##dt##_get(&cin, p_buf, p_buf_end);                             \
    }                                                                          \
                                                                               \
    snprintf(out, outlen - 1, "%s%u %2.2u:%2.2u:%2.2u.%*.*u",                  \
             (cin.sign == -1) ? "- " : "", cin.days, cin.hours, cin.mins,      \
             cin.sec, prec, prec, cin.frac);                                   \
    out[outlen - 1] = 0;                                                       \
/* END OF _intv_clt2string_func_body */

static int _intv_clt2string(const void *in,
                            const struct field_conv_opts *inopts,
                            const enum intv_enum type, char *out,
                            const int outlen)
{

    switch (type) {
    case INTV_YM: {
        cdb2_client_intv_ym_t cin;
        uint8_t *p_buf = (uint8_t *)in;
        uint8_t *p_buf_end = p_buf + sizeof(cdb2_client_intv_ym_t);

        if (inopts && inopts->flags & FLD_CONV_LENDIAN) {
            client_intv_ym_little_get(&cin, p_buf, p_buf_end);
        } else {
            client_intv_ym_get(&cin, p_buf, p_buf_end);
        }

        snprintf(out, outlen - 1, "%s%u-%2.2u", (cin.sign == -1) ? "- " : "",
                 cin.years, cin.months);
        out[outlen - 1] = 0;
    } break;
    case INTV_DS: {
        _intv_clt2string_func_body(intv_ds, INTVDS, 3, msec, unsigned short);
    } break;
    case INTV_DSUS: {
        _intv_clt2string_func_body(intv_dsus, INTVDSUS, 6, usec, unsigned int);
    } break;
    default:
        return -1;
    }

    return 0;
}

static const char *skipsp(const char *str)
{
    while (str && str[0] == ' ')
        str++;
    return str;
}

#define _string2intv_srv_func_body(dt, UDT, prec, frac, fracdt)                \
    p_buf = (uint8_t *)out;                                                    \
    p_buf_end = p_buf + sizeof(SRV_TP(dt));                                    \
    bzero(&dt, sizeof(dt));                                                    \
    bset(&dt.flag, data_bit); /* oh, but the nulls */                          \
    if (str_to_interval(in, len, &tp, &n0, &n1, &ids, &sign))                  \
        return -1;                                                             \
    else if (tp == 1)                                                          \
        ds_to_secs_and_##frac##s(&ids, &n0, &n1);                              \
    else if (tp != 2)                                                          \
        return -1;                                                             \
    frac = (prec == DTTZ_PREC_MSEC) ? n1 / 1000 : n1;                          \
    sec = n0 * sign;                                                           \
    int8_to_int8b(sec, (int8b *)&sec);                                         \
    memcpy(&dt.sec, &sec, sizeof(dt.sec));                                     \
    memcpy(&dt.frac, &frac, sizeof(dt.frac));                                  \
    server_##dt##_put(&dt, p_buf, p_buf_end);                                  \
/* END OF _string2intv_srv_func_body */

static void ds_to_secs_and_msecs(const intv_ds_t *ds, uint64_t *sec,
                                 uint64_t *msec);
static void ds_to_secs_and_usecs(const intv_ds_t *ds, uint64_t *sec,
                                 uint64_t *usec);
static int _string2intv_srv(const char *in, enum intv_enum type, void *out)
{
    if (!in)
        return -1;
    uint64_t n0, n1;
    uint8_t *p_buf, *p_buf_end;
    unsigned int months;
    long long sec;
    unsigned short msec;
    unsigned int usec;
    intv_ds_t ids;
    server_intv_ym_t ym;
    server_intv_ds_t intv_ds;
    server_intv_dsus_t intv_dsus;
    int sign, tp = type, len = strlen(in);
    switch (type) {
    case INTV_YM:
        p_buf = (uint8_t *)out;
        p_buf_end = p_buf + sizeof(server_intv_ym_t);
        bzero(&ym, sizeof(ym));
        bset(&ym.flag, data_bit); /* oh, but the nulls */
        if (str_to_interval(in, len, &tp, &n0, &n1, NULL, &sign))
            return -1;
        else if (tp == 0)
            months = ((n0 * 12) + n1) * sign;
        else if (tp == 2)
            months = n0 * sign;
        else
            return -1;
        int4_to_int4b(months, (int4b *)&months);
        memcpy(&ym.months, &months, sizeof(ym.months));
        server_intv_ym_put(&ym, p_buf, p_buf_end);
        break;
    case INTV_DS:
        _string2intv_srv_func_body(intv_ds, INTVDS, 3, msec, unsigned short);
        break;
    case INTV_DSUS:
        _string2intv_srv_func_body(intv_dsus, INTVDSUS, 6, usec, unsigned int);
        /* post-check: enforce 6-digit fraction */
        if (tp == 1 && ids.prec != DTTZ_PREC_USEC)
            return -1;
        break;
    default:
        return -1;
    }
    return 0;
}

#define _intv_srv2int_func_body(dt, UDT, prec, frac, fracdt)                   \
    SRV_TP(dt) cin;                                                            \
    uint8_t *p_buf = (uint8_t *)in;                                            \
    uint8_t *p_buf_end = p_buf + sizeof(SRV_TP(dt));                           \
                                                                               \
    /* convert to host ordering */                                             \
    server_##dt##_get(&cin, p_buf, p_buf_end);                                 \
    memcpy(&tmp, &cin.sec, sizeof(tmp));                                       \
    int8b_to_int8(tmp, (comdb2_int8 *)&tmp);                                   \
    tmp = flibc_htonll(tmp);                                                   \
/* END OF _intv_srv2int_func_body */
static int _intv_srv2int(const void *in, const enum intv_enum type,
                         long long *out)
{

    long long tmp = 0;
    switch (type) {
    case INTV_YM: {
        server_intv_ym_t cin;
        uint8_t *p_buf = (uint8_t *)in;
        uint8_t *p_buf_end = p_buf + sizeof(server_intv_ym_t);
        long long tmpmon;
        int months = 0;

        /* convert to host ordering */
        server_intv_ym_get(&cin, p_buf, p_buf_end);
        memcpy(&months, &cin.months, sizeof(months));
        int4b_to_int4(months, (comdb2_int4 *)&months);
        tmpmon = (long long)months;
        tmp = flibc_htonll(tmpmon);
    } break;
    case INTV_DS: {
        _intv_srv2int_func_body(intv_ds, INTVDS, 3, msec, unsigned short);
    } break;
    case INTV_DSUS: {
        _intv_srv2int_func_body(intv_dsus, INTVDSUS, 6, usec, unsigned int);
    } break;
    default:
        return -1;
    }

    memcpy(out, &tmp, sizeof(tmp));

    return 0;
}

#define _int2intv_srv_func_body(dt, UDT, prec, frac, fracdt)                   \
    SRV_TP(dt) cout;                                                           \
    uint8_t *p_buf = (uint8_t *)out;                                           \
    uint8_t *p_buf_end = p_buf + sizeof(SRV_TP(dt));                           \
    long long sec = flibc_ntohll(in);                                          \
    fracdt frac = 0;                                                           \
                                                                               \
    bzero(&cout, sizeof(cout));                                                \
    bset(&cout.flag, data_bit); /* oh, but the nulls */                        \
                                                                               \
    int8_to_int8b(sec, (int8b *)&sec);                                         \
    cout.sec = sec;                                                            \
    cout.frac = frac;                                                          \
                                                                               \
    server_##dt##_put(&cout, p_buf, p_buf_end);
/* END OF _int2intv_srv_func_body */
/* in input parameter to these functions is expeced to be in network byte order
 */
static int _int2intv_srv(const long long in, const enum intv_enum type,
                         void *out)
{

    switch (type) {
    case INTV_YM: {
        server_intv_ym_t cout;
        uint8_t *p_buf = (uint8_t *)out;
        uint8_t *p_buf_end = p_buf + sizeof(server_intv_ym_t);
        int months = flibc_ntohll(in);

        bzero(&cout, sizeof(cout));
        bset(&cout.flag, data_bit); /* oh, but the nulls */

        int4_to_int4b(months, (int4b *)&months);
        cout.months = months;
        server_intv_ym_put(&cout, p_buf, p_buf_end);
    } break;
    case INTV_DS: {
        _int2intv_srv_func_body(intv_ds, INTVDS, 3, msec, unsigned short);
    } break;
    case INTV_DSUS: {
        _int2intv_srv_func_body(intv_dsus, INTVDSUS, 6, usec, unsigned int);
    } break;
    default:
        return -1;
    }

    return 0;
}

#define _intv_srv2real_func_body(dt, UDT, prec, frac, fracdt)                  \
    SRV_TP(dt) cin;                                                            \
    uint8_t *p_buf = (uint8_t *)in;                                            \
    uint8_t *p_buf_end = p_buf + sizeof(SRV_TP(dt));                           \
    long long sec = 0;                                                         \
    fracdt frac = 0;                                                           \
                                                                               \
    server_##dt##_get(&cin, p_buf, p_buf_end);                                 \
    memcpy(&sec, &cin.sec, sizeof(sec));                                       \
    int8b_to_int8(sec, (comdb2_int8 *)&sec);                                   \
    memcpy(&frac, &cin.frac, sizeof(frac));                                    \
                                                                               \
    tmp = sec + (double)frac / POW10(prec);                                    \
/* END OF _intv_srv2string_func_body */

static int _intv_srv2real(const void *in, const enum intv_enum type,
                          double *out)
{

    double tmp = 0;

    switch (type) {
    case INTV_YM: {
        server_intv_ym_t cin;
        uint8_t *p_buf = (uint8_t *)in;
        uint8_t *p_buf_end = p_buf + sizeof(server_intv_ym_t);
        int months = 0;

        server_intv_ym_get(&cin, p_buf, p_buf_end);
        memcpy(&months, &cin.months, sizeof(months));
        int4b_to_int4(months, (comdb2_int4 *)&months);
        tmp = months;
    } break;
    case INTV_DS: {
        _intv_srv2real_func_body(intv_ds, INTVDS, 3, msec, unsigned short);
    } break;
    case INTV_DSUS: {
        _intv_srv2real_func_body(intv_dsus, INTVDSUS, 6, usec, unsigned int);
    } break;
    default:
        return -1;
    }

    tmp = flibc_htond(tmp);
    memcpy(out, &tmp, sizeof(*out));

    return 0;
}

#define _intv_clt2real_func_body(dt, UDT, prec, frac, fracdt)                  \
    CLT_TP(dt) cin;                                                            \
    uint8_t *p_buf = (uint8_t *)in;                                            \
    uint8_t *p_buf_end = (uint8_t *)in + sizeof(CLT_TP(dt));                   \
                                                                               \
    bzero(&cin, sizeof(cin));                                                  \
                                                                               \
    if (inopts && inopts->flags & FLD_CONV_LENDIAN) {                          \
        client_##dt##_little_get(&cin, p_buf, p_buf_end);                      \
    } else {                                                                   \
        client_##dt##_get(&cin, p_buf, p_buf_end);                             \
    }                                                                          \
                                                                               \
    tmp = cin.sign * (double)(cin.days * 24 * 3600 + cin.hours * 3600 +        \
                              cin.mins * 60 + cin.sec) +                       \
          (double)cin.frac / POW10(prec);                                      \
/* END OF _intv_clt2real_func_body */
static int _intv_clt2real(const void *in, const struct field_conv_opts *inopts,
                          const enum intv_enum type, double *out)
{

    double tmp = 0;

    switch (type) {
    case INTV_YM: {
        cdb2_client_intv_ym_t cin;
        uint8_t *p_buf = (uint8_t *)in;
        uint8_t *p_buf_end = p_buf + sizeof(cdb2_client_intv_ym_t);

        bzero(&cin, sizeof(cin));

        if (inopts && inopts->flags & FLD_CONV_LENDIAN) {
            client_intv_ym_little_get(&cin, p_buf, p_buf_end);
        } else {
            client_intv_ym_get(&cin, p_buf, p_buf_end);
        }

        tmp = cin.sign * (double)(12 * cin.years + cin.months);
    } break;
    case INTV_DS: {
        _intv_clt2real_func_body(intv_ds, INTVDS, 3, msec, unsigned short);
    } break;
    case INTV_DSUS: {
        _intv_clt2real_func_body(intv_dsus, INTVDSUS, 6, usec, unsigned int);
    } break;
    default:
        return -1;
    }

    tmp = flibc_htond(tmp);
    memcpy(out, &tmp, sizeof(*out));

    return 0;
}

#define _splitReal_func_body(dt, UDT, prec, frac, fracdt)                      \
    long long tmp = 0, divisor = IPOW10(prec) * 10, power10 = IPOW10(prec);    \
                                                                               \
    if (in >= (double)(LLONG_MAX / divisor + 5) ||                             \
        in <= (double)(LLONG_MIN / divisor - 5))                               \
        return -1;                                                             \
                                                                               \
    if (in >= 0) {                                                             \
        tmp = (in * divisor + 5); /* rounding */                               \
                                                                               \
        *sec = tmp / divisor;                                                  \
        *frac_sec = (tmp % divisor) / 10;                                      \
    } else {                                                                   \
        tmp = (in * divisor - 5);                                              \
        if ((tmp % divisor) / 10 == 0) {                                       \
            *sec = tmp / divisor;                                              \
            *frac_sec = 0;                                                     \
        } else {                                                               \
            *sec = tmp / divisor - 1;                                          \
            *frac_sec = power10 + ((tmp % divisor) / 10);                      \
        }                                                                      \
    }                                                                          \
                                                                               \
    return 0;
/* END OF _splitReal_func_body */

static int _splitReal_3(const double in, long long *sec,
                        unsigned short *frac_sec)
{
    _splitReal_func_body(datetime, DATETIME, 3, msec, unsigned short);
}

static int _splitReal_6(const double in, long long *sec, unsigned int *frac_sec)
{
    _splitReal_func_body(datetime, DATETIME, 6, usec, unsigned int);
}

static int _real2intv_srv(const double in, const enum intv_enum type, void *out)
{

    unsigned short msec = 0;
    unsigned int usec = 0;
    long long sec = 0;
    int rc = 0;

    switch (type) {
    case INTV_DS:
        rc = _splitReal_3(flibc_ntohd(in), &sec, &msec);
        if (rc)
            return rc;

        rc = _int2intv_srv(flibc_htonll(sec), type, out);
        if (rc)
            return rc;

        msec = htons(msec);
        memcpy(&((server_intv_ds_t *)out)->msec, &msec,
               sizeof(((server_intv_ds_t *)out)->msec));
        break;
    case INTV_DSUS:
        rc = _splitReal_6(flibc_ntohd(in), &sec, &usec);
        if (rc)
            return rc;

        rc = _int2intv_srv(flibc_htonll(sec), type, out);
        if (rc)
            return rc;

        usec = htonl(usec);
        memcpy(&((server_intv_dsus_t *)out)->usec, &usec,
               sizeof(((server_intv_dsus_t *)out)->usec));
        break;
    default:
        rc = _splitReal_3(flibc_ntohd(in), &sec, &msec);
        if (rc)
            return rc;

        rc = _int2intv_srv(flibc_htonll(sec), type, out);
        if (rc)
            return rc;
        break;
    }

    return 0;
}

/* some forwards */
int SERVER_UINT_to_SERVER_INTVYM(S2S_FUNKY_ARGS);
int SERVER_UINT_to_SERVER_INTVDS(S2S_FUNKY_ARGS);
int SERVER_UINT_to_SERVER_INTVDSUS(S2S_FUNKY_ARGS);
int SERVER_BINT_to_SERVER_INTVYM(S2S_FUNKY_ARGS);
int SERVER_BINT_to_SERVER_INTVDS(S2S_FUNKY_ARGS);
int SERVER_BINT_to_SERVER_INTVDSUS(S2S_FUNKY_ARGS);
int SERVER_BREAL_to_SERVER_INTVYM(S2S_FUNKY_ARGS);
int SERVER_BREAL_to_SERVER_INTVDS(S2S_FUNKY_ARGS);
int SERVER_BREAL_to_SERVER_INTVDSUS(S2S_FUNKY_ARGS);
int SERVER_BCSTR_to_SERVER_INTVYM(S2S_FUNKY_ARGS);
int SERVER_BCSTR_to_SERVER_INTVDS(S2S_FUNKY_ARGS);
int SERVER_BCSTR_to_SERVER_INTVDSUS(S2S_FUNKY_ARGS);
int SERVER_INTVYM_to_CLIENT_INTVYM(S2C_FUNKY_ARGS);
int SERVER_INTVDS_to_CLIENT_INTVDS(S2C_FUNKY_ARGS);
int SERVER_INTVDSUS_to_CLIENT_INTVDS(S2C_FUNKY_ARGS);
int SERVER_INTVDS_to_CLIENT_INTVDSUS(S2C_FUNKY_ARGS);
int SERVER_INTVDSUS_to_CLIENT_INTVDSUS(S2C_FUNKY_ARGS);

/* client to client - not needed now, not supported */
#define CLIENT_BLOB_to_CLIENT_INTVDS CLIENT_to_CLIENT_NO_CONV
#define CLIENT_BLOB_to_CLIENT_INTVDSUS CLIENT_to_CLIENT_NO_CONV
#define CLIENT_BLOB_to_CLIENT_INTVYM CLIENT_to_CLIENT_NO_CONV
#define CLIENT_BLOB_to_CLIENT_VUTF8 CLIENT_to_CLIENT_NO_CONV
#define CLIENT_BYTEARRAY_to_CLIENT_INTDS CLIENT_to_CLIENT_NO_CONV
#define CLIENT_BYTEARRAY_to_CLIENT_INTDSUS CLIENT_to_CLIENT_NO_CONV
#define CLIENT_BYTEARRAY_to_CLIENT_INTVYM CLIENT_to_CLIENT_NO_CONV
#define CLIENT_BYTEARRAY_to_CLIENT_VUTF8 CLIENT_to_CLIENT_NO_CONV
#define CLIENT_CSTR_to_CLIENT_INTVDS CLIENT_to_CLIENT_NO_CONV
#define CLIENT_CSTR_to_CLIENT_INTVDSUS CLIENT_to_CLIENT_NO_CONV
#define CLIENT_CSTR_to_CLIENT_INTVYM CLIENT_to_CLIENT_NO_CONV
#define CLIENT_CSTR_to_CLIENT_VUTF8 CLIENT_to_CLIENT_NO_CONV
#define CLIENT_DATETIME_to_CLIENT_INTVDS CLIENT_to_CLIENT_NO_CONV
#define CLIENT_DATETIME_to_CLIENT_INTVDSUS CLIENT_to_CLIENT_NO_CONV
#define CLIENT_DATETIME_to_CLIENT_INTVYM CLIENT_to_CLIENT_NO_CONV
#define CLIENT_DATETIME_to_CLIENT_VUTF8 CLIENT_to_CLIENT_NO_CONV
#define CLIENT_DATETIMEUS_to_CLIENT_INTVDS CLIENT_to_CLIENT_NO_CONV
#define CLIENT_DATETIMEUS_to_CLIENT_INTVDSUS CLIENT_to_CLIENT_NO_CONV
#define CLIENT_DATETIMEUS_to_CLIENT_INTVYM CLIENT_to_CLIENT_NO_CONV
#define CLIENT_DATETIMEUS_to_CLIENT_VUTF8 CLIENT_to_CLIENT_NO_CONV
#define CLIENT_INTVDS_to_CLIENT_BLOB CLIENT_to_CLIENT_NO_CONV
#define CLIENT_INTVDS_to_CLIENT_BYTEARRAY CLIENT_to_CLIENT_NO_CONV
#define CLIENT_INTVDS_to_CLIENT_CSTR CLIENT_to_CLIENT_NO_CONV
#define CLIENT_INTVDS_to_CLIENT_DATETIME CLIENT_to_CLIENT_NO_CONV
#define CLIENT_INTVDS_to_CLIENT_DATETIMEUS CLIENT_to_CLIENT_NO_CONV
#define CLIENT_INTVDS_to_CLIENT_INT CLIENT_to_CLIENT_NO_CONV
#define CLIENT_INTVDS_to_CLIENT_INTVDS CLIENT_to_CLIENT_NO_CONV
#define CLIENT_INTVDS_to_CLIENT_INTVDSUS CLIENT_to_CLIENT_NO_CONV
#define CLIENT_INTVDS_to_CLIENT_INTVYM CLIENT_to_CLIENT_NO_CONV
#define CLIENT_INTVDS_to_CLIENT_PSTR CLIENT_to_CLIENT_NO_CONV
#define CLIENT_INTVDS_to_CLIENT_PSTR2 CLIENT_to_CLIENT_NO_CONV
#define CLIENT_INTVDS_to_CLIENT_REAL CLIENT_to_CLIENT_NO_CONV
#define CLIENT_INTVDS_to_CLIENT_UINT CLIENT_to_CLIENT_NO_CONV
#define CLIENT_INTVDS_to_CLIENT_VUTF8 CLIENT_to_CLIENT_NO_CONV
#define CLIENT_INTVDSUS_to_CLIENT_BLOB CLIENT_to_CLIENT_NO_CONV
#define CLIENT_INTVDSUS_to_CLIENT_BYTEARRAY CLIENT_to_CLIENT_NO_CONV
#define CLIENT_INTVDSUS_to_CLIENT_CSTR CLIENT_to_CLIENT_NO_CONV
#define CLIENT_INTVDSUS_to_CLIENT_DATETIME CLIENT_to_CLIENT_NO_CONV
#define CLIENT_INTVDSUS_to_CLIENT_DATETIMEUS CLIENT_to_CLIENT_NO_CONV
#define CLIENT_INTVDSUS_to_CLIENT_INT CLIENT_to_CLIENT_NO_CONV
#define CLIENT_INTVDSUS_to_CLIENT_INTVDS CLIENT_to_CLIENT_NO_CONV
#define CLIENT_INTVDSUS_to_CLIENT_INTVDSUS CLIENT_to_CLIENT_NO_CONV
#define CLIENT_INTVDSUS_to_CLIENT_INTVYM CLIENT_to_CLIENT_NO_CONV
#define CLIENT_INTVDSUS_to_CLIENT_PSTR CLIENT_to_CLIENT_NO_CONV
#define CLIENT_INTVDSUS_to_CLIENT_PSTR2 CLIENT_to_CLIENT_NO_CONV
#define CLIENT_INTVDSUS_to_CLIENT_REAL CLIENT_to_CLIENT_NO_CONV
#define CLIENT_INTVDSUS_to_CLIENT_UINT CLIENT_to_CLIENT_NO_CONV
#define CLIENT_INTVDSUS_to_CLIENT_VUTF8 CLIENT_to_CLIENT_NO_CONV
#define CLIENT_INTVYM_to_CLIENT_BLOB CLIENT_to_CLIENT_NO_CONV
#define CLIENT_INTVYM_to_CLIENT_BYTEARRAY CLIENT_to_CLIENT_NO_CONV
#define CLIENT_INTVYM_to_CLIENT_CSTR CLIENT_to_CLIENT_NO_CONV
#define CLIENT_INTVYM_to_CLIENT_DATETIME CLIENT_to_CLIENT_NO_CONV
#define CLIENT_INTVYM_to_CLIENT_DATETIMEUS CLIENT_to_CLIENT_NO_CONV
#define CLIENT_INTVYM_to_CLIENT_INT CLIENT_to_CLIENT_NO_CONV
#define CLIENT_INTVYM_to_CLIENT_INTVDS CLIENT_to_CLIENT_NO_CONV
#define CLIENT_INTVYM_to_CLIENT_INTVDSUS CLIENT_to_CLIENT_NO_CONV
#define CLIENT_INTVYM_to_CLIENT_INTVYM CLIENT_to_CLIENT_NO_CONV
#define CLIENT_INTVYM_to_CLIENT_PSTR CLIENT_to_CLIENT_NO_CONV
#define CLIENT_INTVYM_to_CLIENT_PSTR2 CLIENT_to_CLIENT_NO_CONV
#define CLIENT_INTVYM_to_CLIENT_REAL CLIENT_to_CLIENT_NO_CONV
#define CLIENT_INTVYM_to_CLIENT_UINT CLIENT_to_CLIENT_NO_CONV
#define CLIENT_INTVYM_to_CLIENT_VUTF8 CLIENT_to_CLIENT_NO_CONV
#define CLIENT_INT_to_CLIENT_INTVDS CLIENT_to_CLIENT_NO_CONV
#define CLIENT_INT_to_CLIENT_INTVDSUS CLIENT_to_CLIENT_NO_CONV
#define CLIENT_INT_to_CLIENT_INTVYM CLIENT_to_CLIENT_NO_CONV
#define CLIENT_INT_to_CLIENT_VUTF8 CLIENT_to_CLIENT_NO_CONV
#define CLIENT_PSTR2_to_CLIENT_INTVDS CLIENT_to_CLIENT_NO_CONV
#define CLIENT_PSTR2_to_CLIENT_INTVDSUS CLIENT_to_CLIENT_NO_CONV
#define CLIENT_PSTR2_to_CLIENT_INTVYM CLIENT_to_CLIENT_NO_CONV
#define CLIENT_PSTR2_to_CLIENT_VUTF8 CLIENT_to_CLIENT_NO_CONV
#define CLIENT_PSTR_to_CLIENT_INTVDS CLIENT_to_CLIENT_NO_CONV
#define CLIENT_PSTR_to_CLIENT_INTVDSUS CLIENT_to_CLIENT_NO_CONV
#define CLIENT_PSTR_to_CLIENT_INTVYM CLIENT_to_CLIENT_NO_CONV
#define CLIENT_PSTR_to_CLIENT_VUTF8 CLIENT_to_CLIENT_NO_CONV
#define CLIENT_REAL_to_CLIENT_INTVDS CLIENT_to_CLIENT_NO_CONV
#define CLIENT_REAL_to_CLIENT_INTVDSUS CLIENT_to_CLIENT_NO_CONV
#define CLIENT_REAL_to_CLIENT_INTVYM CLIENT_to_CLIENT_NO_CONV
#define CLIENT_REAL_to_CLIENT_VUTF8 CLIENT_to_CLIENT_NO_CONV
#define CLIENT_UINT_to_CLIENT_INTVDS CLIENT_to_CLIENT_NO_CONV
#define CLIENT_UINT_to_CLIENT_INTVDSUS CLIENT_to_CLIENT_NO_CONV
#define CLIENT_UINT_to_CLIENT_INTVYM CLIENT_to_CLIENT_NO_CONV
#define CLIENT_UINT_to_CLIENT_VUTF8 CLIENT_to_CLIENT_NO_CONV
#define CLIENT_VUTF8_to_CLIENT_BLOB CLIENT_to_CLIENT_NO_CONV
#define CLIENT_VUTF8_to_CLIENT_BYTEARRAY CLIENT_to_CLIENT_NO_CONV
#define CLIENT_VUTF8_to_CLIENT_CSTR CLIENT_to_CLIENT_NO_CONV
#define CLIENT_VUTF8_to_CLIENT_DATETIME CLIENT_to_CLIENT_NO_CONV
#define CLIENT_VUTF8_to_CLIENT_DATETIMEUS CLIENT_to_CLIENT_NO_CONV
#define CLIENT_VUTF8_to_CLIENT_INT CLIENT_to_CLIENT_NO_CONV
#define CLIENT_VUTF8_to_CLIENT_INTVDS CLIENT_to_CLIENT_NO_CONV
#define CLIENT_VUTF8_to_CLIENT_INTVDSUS CLIENT_to_CLIENT_NO_CONV
#define CLIENT_VUTF8_to_CLIENT_INTVYM CLIENT_to_CLIENT_NO_CONV
#define CLIENT_VUTF8_to_CLIENT_PSTR CLIENT_to_CLIENT_NO_CONV
#define CLIENT_VUTF8_to_CLIENT_PSTR2 CLIENT_to_CLIENT_NO_CONV
#define CLIENT_VUTF8_to_CLIENT_REAL CLIENT_to_CLIENT_NO_CONV
#define CLIENT_VUTF8_to_CLIENT_UINT CLIENT_to_CLIENT_NO_CONV

/* server to client */
#define SERVER_BCSTR_to_CLIENT_VUTF8 SERVER_to_CLIENT_NO_CONV
#define SERVER_BINT_to_CLIENT_VUTF8 SERVER_to_CLIENT_NO_CONV
#define SERVER_BLOB_to_CLIENT_INTVDS SERVER_to_CLIENT_NO_CONV
#define SERVER_BLOB_to_CLIENT_INTVDSUS SERVER_to_CLIENT_NO_CONV
#define SERVER_BLOB_to_CLIENT_INTVYM SERVER_to_CLIENT_NO_CONV
#define SERVER_BLOB_to_CLIENT_VUTF8 SERVER_to_CLIENT_NO_CONV
#define SERVER_BLOB2_to_CLIENT_INTVDS SERVER_to_CLIENT_NO_CONV
#define SERVER_BLOB2_to_CLIENT_INTVDSUS SERVER_to_CLIENT_NO_CONV
#define SERVER_BLOB2_to_CLIENT_INTVYM SERVER_to_CLIENT_NO_CONV
#define SERVER_BLOB2_to_CLIENT_VUTF8 SERVER_to_CLIENT_NO_CONV
#define SERVER_BREAL_to_CLIENT_VUTF8 SERVER_to_CLIENT_NO_CONV
#define SERVER_BYTEARRAY_to_CLIENT_INTVDS SERVER_to_CLIENT_NO_CONV
#define SERVER_BYTEARRAY_to_CLIENT_INTVDSUS SERVER_to_CLIENT_NO_CONV
#define SERVER_BYTEARRAY_to_CLIENT_INTVYM SERVER_to_CLIENT_NO_CONV
#define SERVER_BYTEARRAY_to_CLIENT_VUTF8 SERVER_to_CLIENT_NO_CONV
#define SERVER_DATETIME_to_CLIENT_INTVDS SERVER_to_CLIENT_NO_CONV
#define SERVER_DATETIME_to_CLIENT_INTVDSUS SERVER_to_CLIENT_NO_CONV
#define SERVER_DATETIME_to_CLIENT_INTVYM SERVER_to_CLIENT_NO_CONV
#define SERVER_DATETIME_to_CLIENT_VUTF8 SERVER_to_CLIENT_NO_CONV
#define SERVER_DATETIMEUS_to_CLIENT_INTVDS SERVER_to_CLIENT_NO_CONV
#define SERVER_DATETIMEUS_to_CLIENT_INTVDSUS SERVER_to_CLIENT_NO_CONV
#define SERVER_DATETIMEUS_to_CLIENT_INTVYM SERVER_to_CLIENT_NO_CONV
#define SERVER_DATETIMEUS_to_CLIENT_VUTF8 SERVER_to_CLIENT_NO_CONV
#define SERVER_INTVDS_to_CLIENT_BLOB SERVER_to_CLIENT_NO_CONV
#define SERVER_INTVDS_to_CLIENT_BYTEARRAY SERVER_to_CLIENT_NO_CONV
#define SERVER_INTVDS_to_CLIENT_DATETIME SERVER_to_CLIENT_NO_CONV
#define SERVER_INTVDS_to_CLIENT_DATETIMEUS SERVER_to_CLIENT_NO_CONV
#define SERVER_INTVDS_to_CLIENT_INTVYM SERVER_to_CLIENT_NO_CONV
#define SERVER_INTVDS_to_CLIENT_VUTF8 SERVER_to_CLIENT_NO_CONV
#define SERVER_INTVDSUS_to_CLIENT_BLOB SERVER_to_CLIENT_NO_CONV
#define SERVER_INTVDSUS_to_CLIENT_BYTEARRAY SERVER_to_CLIENT_NO_CONV
#define SERVER_INTVDSUS_to_CLIENT_DATETIME SERVER_to_CLIENT_NO_CONV
#define SERVER_INTVDSUS_to_CLIENT_DATETIMEUS SERVER_to_CLIENT_NO_CONV
#define SERVER_INTVDSUS_to_CLIENT_INTVYM SERVER_to_CLIENT_NO_CONV
#define SERVER_INTVDSUS_to_CLIENT_VUTF8 SERVER_to_CLIENT_NO_CONV
#define SERVER_INTVYM_to_CLIENT_BLOB SERVER_to_CLIENT_NO_CONV
#define SERVER_INTVYM_to_CLIENT_BYTEARRAY SERVER_to_CLIENT_NO_CONV
#define SERVER_INTVYM_to_CLIENT_DATETIME SERVER_to_CLIENT_NO_CONV
#define SERVER_INTVYM_to_CLIENT_DATETIMEUS SERVER_to_CLIENT_NO_CONV
#define SERVER_INTVYM_to_CLIENT_INTVDS SERVER_to_CLIENT_NO_CONV
#define SERVER_INTVYM_to_CLIENT_INTVDSUS SERVER_to_CLIENT_NO_CONV
#define SERVER_INTVYM_to_CLIENT_VUTF8 SERVER_to_CLIENT_NO_CONV
#define SERVER_UINT_to_CLIENT_VUTF8 SERVER_to_CLIENT_NO_CONV
#define SERVER_VUTF8_to_CLIENT_BLOB SERVER_to_CLIENT_NO_CONV
#define SERVER_VUTF8_to_CLIENT_BYTEARRAY SERVER_to_CLIENT_NO_CONV
#define SERVER_VUTF8_to_CLIENT_CSTR SERVER_to_CLIENT_NO_CONV
#define SERVER_VUTF8_to_CLIENT_DATETIME SERVER_to_CLIENT_NO_CONV
#define SERVER_VUTF8_to_CLIENT_DATETIMEUS SERVER_to_CLIENT_NO_CONV
#define SERVER_VUTF8_to_CLIENT_INT SERVER_to_CLIENT_NO_CONV
#define SERVER_VUTF8_to_CLIENT_INTVDS SERVER_to_CLIENT_NO_CONV
#define SERVER_VUTF8_to_CLIENT_INTVDSUS SERVER_to_CLIENT_NO_CONV
#define SERVER_VUTF8_to_CLIENT_INTVYM SERVER_to_CLIENT_NO_CONV
#define SERVER_VUTF8_to_CLIENT_PSTR SERVER_to_CLIENT_NO_CONV
#define SERVER_VUTF8_to_CLIENT_PSTR2 SERVER_to_CLIENT_NO_CONV
#define SERVER_VUTF8_to_CLIENT_REAL SERVER_to_CLIENT_NO_CONV
#define SERVER_VUTF8_to_CLIENT_UINT SERVER_to_CLIENT_NO_CONV

#define SERVER_DECIMAL_to_CLIENT_BLOB SERVER_to_CLIENT_NO_CONV
#define SERVER_DECIMAL_to_CLIENT_BYTEARRAY SERVER_to_CLIENT_NO_CONV
#define SERVER_DECIMAL_to_CLIENT_DATETIME SERVER_to_CLIENT_NO_CONV
#define SERVER_DECIMAL_to_CLIENT_DATETIMEUS SERVER_to_CLIENT_NO_CONV
#define SERVER_DECIMAL_to_CLIENT_INT SERVER_to_CLIENT_NO_CONV
#define SERVER_DECIMAL_to_CLIENT_INTVDS SERVER_to_CLIENT_NO_CONV
#define SERVER_DECIMAL_to_CLIENT_INTVDSUS SERVER_to_CLIENT_NO_CONV
#define SERVER_DECIMAL_to_CLIENT_INTVYM SERVER_to_CLIENT_NO_CONV
#define SERVER_DECIMAL_to_CLIENT_PSTR SERVER_to_CLIENT_NO_CONV
#define SERVER_DECIMAL_to_CLIENT_PSTR2 SERVER_to_CLIENT_NO_CONV
#define SERVER_DECIMAL_to_CLIENT_REAL SERVER_to_CLIENT_NO_CONV
#define SERVER_DECIMAL_to_CLIENT_UINT SERVER_to_CLIENT_NO_CONV
#define SERVER_DECIMAL_to_CLIENT_VUTF8 SERVER_to_CLIENT_NO_CONV

static void shift_coefficient_to_ondisk(int coefbytes, char *coef, int *tlzero)
{
    int tailzero = 0;
    int i;
    int j;

    for (i = 0; i < coefbytes && coef[i] == 0; i++)
        ;

    if (i >= coefbytes) /* all zeros */
    {
        *tlzero = 0;
        return;
    }

    if (coef[i] & 0x0F0) {
        /* moving around bytes */
        for (j = 0; (j + i) < coefbytes; j++) {
            coef[j] = coef[j + i];
            if (coef[j]) {
                tailzero = (coef[j + i] & 0x0F) ? 0 : 1;
            } else {
                tailzero += 2;
            }
        }
        tailzero--; /*counted sign tail here */
    } else {
        /* moving halfs */
        for (j = 0; (j + i) < (coefbytes - 1); j++) {
            coef[j] =
                ((coef[j + i] & 0x0F) << 4) | ((coef[j + i + 1] & 0x0F0) >> 4);
            if (coef[j]) {
                tailzero = (coef[j + i + 1] & 0xF0) ? 0 : 1;
            } else {
                tailzero += 2;
            }
        }
    }
    if (j < coefbytes) {
        bzero(&coef[j], coefbytes - j); /* zero the tail */
    }

    *tlzero = tailzero;
}

/**
 * Returns -1 for corruption
 *         0 for non-zero move
 *         1 for zero numbers
 */
int shift_coefficient_to_dfp(int coefbytes, char *coef, int *pmoved,
                             short tailzero)
{
    int i;
    int j;
    int moved;

    /* compute the digits and update exponent */
    if (coefbytes == DECSINGLE_PACKED_COEF) {
        if (coef[coefbytes - 1] & 0x0F0) {
            *pmoved = 2 * coefbytes - 1;
            /* nothing to shift */
            return 0;
        }
    }

    for (i = coefbytes - 2; i >= 0 && coef[i] == 0; i--)
        ;

    if (i < 0) {
        *pmoved = 0;
        return 1; /* this is zero */
    }

    moved = 2 * i + ((coef[i] & 0x0F) ? 2 : 1);

    if (moved + tailzero > 2 * coefbytes - 1) {
        logmsg(LOGMSG_ERROR, "%s:%d detected corrupted decimal\n", __func__,
                __LINE__);
        *pmoved = 0;
        return -1;
    }

    if (!(coef[i] & 0x0F)) {
        if (tailzero % 2 == 0) {
            for (j = coefbytes - 1 - ((tailzero + 1) / 2); i >= 0 && i != j;
                 i--, j--) {
                coef[j] = coef[i];
                coef[i] = 0;
            }
        } else {
            /* move like the other */
            for (j = coefbytes - 1 - ((tailzero + 1) / 2); i > 0; i--, j--) {
                coef[j] = (coef[i - 1] << 4) | ((coef[i] >> 4) & 0x0F);
            }
            coef[j--] = (coef[i] >> 4) & 0x0F;
            if (j >= 0) {
                bzero(coef, j + 1);
            }
        }
    } else {
        if (tailzero % 2 == 0) {
            coef[coefbytes - 1 - ((tailzero + 1) / 2)] |= (coef[i] << 4);
            coef[i] &= 0x0F0;
            for (j = coefbytes - 1 - ((tailzero + 1) / 2) - 1; i > 0;
                 i--, j--) {
                coef[j] = (coef[i - 1] << 4) | ((coef[i] >> 4) & 0x0F);
            }
            coef[j--] = (coef[i] >> 4) & 0x0F;
            if (j >= 0) {
                bzero(&coef[0], j + 1);
            }
        } else {
            for (j = coefbytes - 1 - ((tailzero + 1) / 2); i >= 0 && i != j;
                 i--, j--) {
                coef[j] = coef[i];
                coef[i] = 0;
            }
        }
    }

    *pmoved = moved;
    return 0;
}

int make_order_decimal32(server_decimal32_t *pdec32)
{
    int adj_exp;
    int i;

    /* compute the digits and update exponent */
    for (i = 0; i < DECSINGLE_PACKED_COEF && pdec32->coef[i] == 0; i++)
        ;

    if (i < DECSINGLE_PACKED_COEF) {
        /* this is not zero */
        int j;
        int tailzero = 0;

        /* use new format, correct sorting */
        adj_exp = 2 * DECSINGLE_PACKED_COEF -
                  (2 * i + ((pdec32->coef[i] & 0x0F0) ? 0 : 1)) -
                  1 /*sign nibble*/;

        shift_coefficient_to_ondisk(DECSINGLE_PACKED_COEF, (char *)pdec32->coef,
                                    &tailzero);

        /* update exponent */
        pdec32->exp += adj_exp;
        pdec32->exp ^= 0x080;

        /* save tail */
        tailzero++; /* mark the new format  */
        if (tailzero > 7) {
            tailzero = 7;
        }
        pdec32->coef[DECSINGLE_PACKED_COEF - 1] |= tailzero;

    } else {
        pdec32->coef[DECSINGLE_PACKED_COEF - 1] = 1;
        pdec32->exp ^= 0x080;
    }

    /* exponent ready */
    if (!pdec32->sign) {
        pdec32->exp ^= 0x0FF;
        for (i = 0; i < DECSINGLE_PACKED_COEF; i++)
            pdec32->coef[i] ^= 0x0FF;
    }

    return 0;
}

int unmake_order_decimal32(server_decimal32_t *pdec32, char *decimals,
                           int *exponent)
{
    int adj_exp = 0;
    int i;
    char exp;

    exp = pdec32->exp;

    if (pdec32->sign == 0) {
        exp ^= 0x0FF;
        for (i = 0; i < sizeof(pdec32->coef); i++)
            decimals[i] = pdec32->coef[i] ^ 0x0FF;
    } else {
        memcpy(decimals, pdec32->coef, sizeof(pdec32->coef));
    }

    if ((decimals[DECSINGLE_PACKED_COEF - 1] & 0x0F)) {
        int moved;
        short quantum;

        /* extract the quantum */
        quantum = 0;
        *((char *)&quantum + 1) = decimals[DECSINGLE_PACKED_COEF - 1] & 0x0F;
        quantum = ntohs(quantum);
        quantum--;
        assert(quantum >= 0 && quantum < 8);

        i = shift_coefficient_to_dfp(DECSINGLE_PACKED_COEF, decimals, &moved,
                                     quantum);

        if (i < 0)
            return -1;

        if (i == 0) {
            /* non-zero, we have moved "moved" digits to the right, therefore
               decrement exponent by "moved";
               the quantum provides additiona zeros that appear in integer
               coefficient,
               so substract that from exponent to keep the same numeric value */
            adj_exp = quantum;

            /* exponent */
            exp ^= 0x080;
            *exponent = exp - adj_exp - moved;
        } else {
            /* this is a zero; quantum is basically 1, while
               the exponent contains the quantum */
            exp ^= 0x080;
            *exponent = exp;
        }
    } else {
        /* compute the digits and update exponent */
        for (i = 0; i < DECSINGLE_PACKED_COEF && decimals[i] == 0; i++)
            ;

        if (i < DECSINGLE_PACKED_COEF) {
            adj_exp =
                DECSINGLE_Pmax - (2 * i + ((decimals[i] & 0x0F0) ? 0 : 1));

            /* out of range */
            if ((exp + DECSINGLE_Pmax - 1) < (DECSINGLE_Emin + adj_exp)) {
                logmsg(LOGMSG_ERROR, "%s:%d cannot make decimal 32 orderable adj=%d "
                                "crt_ext=%d\n",
                        __FILE__, __LINE__, adj_exp, exp);

                return -1;
            }

            exp -= adj_exp;
            *exponent = (signed short)exp;
        } else {
            *exponent = (signed short)exp;
        }
    }

    decimals[DECSINGLE_PACKED_COEF - 1] &= 0x0F0;
    if (pdec32->sign == 0) {
        decimals[DECSINGLE_PACKED_COEF - 1] |= DECPMINUS;
    } else {
        decimals[DECSINGLE_PACKED_COEF - 1] |= DECPPLUS;
    }

    return 0;
}

int make_order_decimal64(server_decimal64_t *pdec64, int exponent)
{
    short adj_exp;
    int i;
    comdb2_int2 tmp;
    int2b exp;

    if (bdb_attr_get(thedb->bdb_attr, BDB_ATTR_REPORT_DECIMAL_CONVERSION)) {
        logmsg(LOGMSG_USER, "Dec64 make_order IN (exponent %d %x):\n", exponent,
                exponent);
        hexdump(LOGMSG_USER, (unsigned char *)pdec64, sizeof(*pdec64));
        logmsg(LOGMSG_USER, "\n");
    }

    /* compute the digits and update exponent */
    for (i = 0; i < DECDOUBLE_PACKED_COEF && pdec64->coef[i] == 0; i++)
        ;

    if (i < DECDOUBLE_PACKED_COEF) {
        int j;
        int tailzero = 0;

        /* use new format, correct sorting */
        adj_exp = 2 * DECDOUBLE_PACKED_COEF -
                  (2 * i + ((pdec64->coef[i] & 0x0F0) ? 0 : 1)) -
                  1 /*sign nibble*/;

        shift_coefficient_to_ondisk(DECDOUBLE_PACKED_COEF, (char *)pdec64->coef,
                                    &tailzero);

        /* update exponent */
        comdb2_int2 tmp = exponent + adj_exp;
        int2_to_int2b(tmp, &exp);

        /* save tail */
        tailzero++; /* mark the new format  */
        if (tailzero > 16) {
            tailzero = 16;
        }
        pdec64->coef[DECDOUBLE_PACKED_COEF - 1] = tailzero;
    } else {
        pdec64->coef[DECDOUBLE_PACKED_COEF - 1] = 1;
        tmp = exponent;
        int2_to_int2b(tmp, (int2b *)&exp);
    }

    /* exponent ready */
    exp = htons(exp);
    if (!pdec64->sign) {
        exp ^= 0x0FFFF;
    }
    memcpy((char *)&pdec64->exp, &exp, sizeof(pdec64->exp));

    /* check the sign; flip it */
    if (!pdec64->sign) {
        for (i = 0; i < DECDOUBLE_PACKED_COEF; i++)
            pdec64->coef[i] ^= 0x0FF;
    }

    if (bdb_attr_get(thedb->bdb_attr, BDB_ATTR_REPORT_DECIMAL_CONVERSION)) {
        logmsg(LOGMSG_USER, "Dec64 make_order OUT:\n");
        hexdump(LOGMSG_USER, (unsigned char *)pdec64, sizeof(*pdec64));
        logmsg(LOGMSG_USER, "\n");
    }

    return 0;
}

int dec64_exponent_is_outrageous(server_decimal64_t *pdec64, char *decimals)
{
    /* 0 decimals were able to insert exponents > 15, missing for %16=0 the mark
     * that it is a new format */
    int i;

    for (i = 0; i < sizeof(pdec64->coef) && pdec64->coef[i] == 0; i++)
        ;

    if (i >= sizeof(pdec64->coef) - 1) {
        unsigned char highnibble = (*(char *)(&pdec64->exp)) & 0x0F0;

        /* check range for negative exponents; first nibble is 7 for new format!
         */
        if ((highnibble & 0x080) == 0 && (highnibble != 0x070)) {
            return 0;
        }
        /* check range for positive exponents; first nibble is 8 for new format!
         */
        if ((highnibble & 0x080) && (highnibble != 0x080)) {
            return 0;
        }
        return 1;
    }

    return 0;
}

int unmake_order_decimal64(server_decimal64_t *pdec64, char *decimals,
                           int *exponent)
{
    comdb2_int2 tmp;
    int2b exp;
    int adj_exp = 0;
    int i;

    if (bdb_attr_get(thedb->bdb_attr, BDB_ATTR_REPORT_DECIMAL_CONVERSION)) {
        logmsg(LOGMSG_USER, "Dec64 unmake_order IN (exponent %d %x):\n", *exponent,
                *exponent);
        hexdump(LOGMSG_USER, (unsigned char *)decimals, DECDOUBLE_PACKED_COEF);
        logmsg(LOGMSG_USER, "\n");
    }

    memcpy(&exp, &pdec64->exp, sizeof(exp));

    if (pdec64->sign == 0) {
        exp ^= 0x0FFFF;
        for (i = 0; i < sizeof(pdec64->coef); i++)
            decimals[i] = pdec64->coef[i] ^ 0x0FF;
    } else {
        memcpy(decimals, pdec64->coef, sizeof(pdec64->coef));
    }

    exp = ntohs(exp);

    if (decimals[DECDOUBLE_PACKED_COEF - 1] & 0x0FF) {
        int moved;
        short quantum;

        /* extract the quantum */
        quantum = 0;
        *((char *)&quantum + 1) = decimals[DECDOUBLE_PACKED_COEF - 1];
        quantum = ntohs(quantum);
        quantum--;
        assert(quantum >= 0 && quantum <= 16);

        /* need tp remove the quantum from decimals */
        decimals[DECDOUBLE_PACKED_COEF - 1] = 0x0;

        if (0) {
        bug1:
            quantum = 0;
        }

        i = shift_coefficient_to_dfp(DECDOUBLE_PACKED_COEF, decimals, &moved,
                                     quantum);

        if (i < 0)
            return -1;

        if (i == 0) {
            adj_exp = quantum;

            /* exponent */
            int2b_to_int2(exp, &tmp);
            *exponent = tmp - adj_exp - moved;
        } else {
#if 0
            decimals[DECDOUBLE_PACKED_COEF - 1] = 0;

            /* coef is 0, set the exponent such that it will preserve quantum */
            if (exp == 0) {
                *exponent = (-1) * quantum;
            } else {
                int2b_to_int2(exp, &tmp);
                *exponent = tmp;
            }
#endif
            int2b_to_int2(exp, &tmp);
            *exponent = tmp;
        }
    } else {
        /* this is a workaround a buggy version; quantum was > 15! */
        if (dec64_exponent_is_outrageous(pdec64, decimals)) {
            goto bug1;
        }

        /* non-sortable version */
        for (i = 0; i < DECDOUBLE_PACKED_COEF && decimals[i] == 0; i++)
            ;

        if (i < DECDOUBLE_PACKED_COEF) {
            adj_exp =
                DECDOUBLE_Pmax - (2 * i + ((decimals[i] & 0x0F0) ? 0 : 1));

            /* out of range */
            if ((exp + DECDOUBLE_Pmax - 1) < (DECDOUBLE_Emin + adj_exp)) {
                logmsg(LOGMSG_ERROR, "%s:%d cannot make decimal 64 orderable adj=%d "
                                "crt_ext=%d\n",
                        __FILE__, __LINE__, adj_exp, exp);

                *exponent = (signed short)exp;
                return -1;
            }

            exp -= adj_exp;
            *exponent = (signed short)exp;
        } else {
            *exponent = (signed short)exp;
        }
    }

    /* recreate decNumber sign */
    decimals[DECDOUBLE_PACKED_COEF - 1] &= 0x0F0;
    if (pdec64->sign == 0) {
        decimals[DECDOUBLE_PACKED_COEF - 1] |= DECPMINUS;
    } else {
        decimals[DECDOUBLE_PACKED_COEF - 1] |= DECPPLUS;
    }

    if (bdb_attr_get(thedb->bdb_attr, BDB_ATTR_REPORT_DECIMAL_CONVERSION)) {
        logmsg(LOGMSG_USER, "Dec64 unmake_order out (exponent %d %x):\n", *exponent,
                *exponent);
        hexdump(LOGMSG_USER, (unsigned char *)decimals, DECDOUBLE_PACKED_COEF);
        logmsg(LOGMSG_USER, "\n");
    }

    return 0;
}

int make_order_decimal128(server_decimal128_t *pdec128, int exponent)
{
    short adj_exp;
    int i;
    comdb2_int2 tmp;
    int2b exp;

    if (bdb_attr_get(thedb->bdb_attr, BDB_ATTR_REPORT_DECIMAL_CONVERSION)) {
        logmsg(LOGMSG_USER, "Dec128 make_order IN (exponent %d %x):\n", exponent,
                exponent);
        hexdump(LOGMSG_USER, (unsigned char *)pdec128, sizeof(*pdec128));
        logmsg(LOGMSG_USER, "\n");
    }

    /* TODO: make this sortable */
    /* compute the digits and update exponent */
    for (i = 0; i < DECQUAD_PACKED_COEF && pdec128->coef[i] == 0; i++)
        ;

    if (i < DECQUAD_PACKED_COEF) {
        int j;
        int tailzero = 0;

        /* use new format, correct sorting */
        adj_exp = 2 * DECQUAD_PACKED_COEF -
                  (2 * i + ((pdec128->coef[i] & 0x0F0) ? 0 : 1)) -
                  1 /*sign nibble*/;

        shift_coefficient_to_ondisk(DECQUAD_PACKED_COEF, (char *)pdec128->coef,
                                    &tailzero);

        /* update exponent */
        comdb2_int2 tmp = exponent + adj_exp;
        int2_to_int2b(tmp, &exp);

        /* save tail */
        tailzero++; /* mark the new format  */
        if (tailzero > 34) {
            tailzero = 34;
        }
        pdec128->coef[DECQUAD_PACKED_COEF - 1] = tailzero;
    } else {
        /* this is a zero; exp has the quantum, keep it */
        pdec128->coef[DECQUAD_PACKED_COEF - 1] = 1;
        tmp = exponent;
        int2_to_int2b(tmp, (int2b *)&exp);
    }

    /* exponent ready */
    exp = htons(exp);
    if (!pdec128->sign) {
        exp ^= 0x0FFFF;
    }
    memcpy((char *)&pdec128->exp, &exp, sizeof(pdec128->exp));

    /* check the sign; flip it */
    if (!pdec128->sign) {
        for (i = 0; i < DECQUAD_PACKED_COEF; i++)
            pdec128->coef[i] ^= 0x0FF;
    }

    if (bdb_attr_get(thedb->bdb_attr, BDB_ATTR_REPORT_DECIMAL_CONVERSION)) {
        logmsg(LOGMSG_USER, "Dec128 make_order OUT:\n");
        hexdump(LOGMSG_USER, (unsigned char *)pdec128, sizeof(*pdec128));
        logmsg(LOGMSG_USER, "\n");
    }

    return 0;
}

int unmake_order_decimal128(server_decimal128_t *pdec128, char *decimals,
                            int *exponent)
{
    comdb2_int2 tmp;
    int2b exp;
    int adj_exp = 0;
    int i;

    if (bdb_attr_get(thedb->bdb_attr, BDB_ATTR_REPORT_DECIMAL_CONVERSION)) {
        logmsg(LOGMSG_USER, "Dec128 unmake_order IN (exponent %d %x):\n", *exponent,
                *exponent);
        hexdump(LOGMSG_USER, (unsigned char *)decimals, DECQUAD_PACKED_COEF);
        logmsg(LOGMSG_USER, "\n");
    }

    memcpy(&exp, &pdec128->exp, sizeof(exp));

    if (pdec128->sign == 0) {
        exp ^= 0x0FFFF;
        for (i = 0; i < sizeof(pdec128->coef); i++)
            decimals[i] = pdec128->coef[i] ^ 0x0FF;
    } else {
        memcpy(decimals, pdec128->coef, sizeof(pdec128->coef));
    }

    exp = ntohs(exp);

    if (decimals[DECQUAD_PACKED_COEF - 1] & 0x0FF) {
        int moved;
        short quantum;

        /* extract the quantum */
        quantum = 0;
        *((char *)&quantum + 1) = decimals[DECQUAD_PACKED_COEF - 1];
        quantum = ntohs(quantum);
        quantum--;
        assert(quantum >= 0 && quantum < 34);

        /* need tp remove the quantum from decimals */
        decimals[DECQUAD_PACKED_COEF - 1] = 0x0;

        i = shift_coefficient_to_dfp(DECQUAD_PACKED_COEF, decimals, &moved,
                                     quantum);

        if (i < 0)
            return -1;

        if (i == 0) {
            adj_exp = quantum;

            /* exponent */
            int2b_to_int2(exp, &tmp);
            *exponent = tmp - adj_exp - moved;
        } else {
#if 0
            decimals[DECQUAD_PACKED_COEF - 1] = 0;

            /* coef is 0, set the exponent such that it will preserve quantum */
            if (exp == 0) {
                *exponent = (-1) * quantum;
            } else {
                int2b_to_int2(exp, &tmp);
                *exponent = tmp;
            }
#endif
            int2b_to_int2(exp, &tmp);
            *exponent = tmp;
        }
    } else {
        /* non-sortable version */
        for (i = 0; i < DECQUAD_PACKED_COEF && decimals[i] == 0; i++)
            ;

        if (i < DECQUAD_PACKED_COEF) {
            adj_exp = DECQUAD_Pmax - (2 * i + ((decimals[i] & 0x0F0) ? 0 : 1));

            /* out of range */
            if ((exp + DECQUAD_Pmax - 1) < (DECQUAD_Emin + adj_exp)) {
                logmsg(LOGMSG_ERROR, "%s:%d cannot make decimal 128 orderable "
                                "adj=%d crt_ext=%d\n",
                        __FILE__, __LINE__, adj_exp, exp);

                *exponent = (signed short)exp;
                return -1;
            }
            exp -= adj_exp;
            *exponent = (signed short)exp;
        } else {
            *exponent = (signed short)exp;
        }
    }

    /* recreate decNumber sign */
    decimals[DECQUAD_PACKED_COEF - 1] &= 0x0F0;
    if (pdec128->sign == 0) {
        decimals[DECQUAD_PACKED_COEF - 1] |= DECPMINUS;
    } else {
        decimals[DECQUAD_PACKED_COEF - 1] |= DECPPLUS;
    }

    if (bdb_attr_get(thedb->bdb_attr, BDB_ATTR_REPORT_DECIMAL_CONVERSION)) {
        logmsg(LOGMSG_USER, "Dec128 unmake_order out (exponent %d %x):\n",
                *exponent, *exponent);
        hexdump(LOGMSG_USER, (unsigned char *)decimals, DECQUAD_PACKED_COEF);
        logmsg(LOGMSG_USER, "\n");
    }

    return 0;
}

static void decimal32_ondisk_to_single(server_decimal32_t *pdec32,
                                       decSingle *dn)
{
    char decimals[DECSINGLE_PACKED_COEF];
    int exponent;
    int sign;
    int rc = 0;
    char mask;

    if (pdec32->coef[DECSINGLE_PACKED_COEF - 1] & 0x0F) {
        mask = 0x080;
    } else {
        mask = 0;
    }

    /* NaN */
    if (pdec32->exp == (0x7c ^ mask) || pdec32->exp == (0x7e ^ mask)) {
        exponent = (pdec32->exp ^ mask) << 24; /* preserve signaling bit */
        bzero(decimals, sizeof(decimals));
    }
    /* Infinite? */
    else if ((pdec32->sign == 1 && pdec32->exp == (0x78 ^ mask)) ||
             (pdec32->sign == 0 && pdec32->exp == ((0x78 ^ mask) & 0x0FF))) {
        exponent = DECFLOAT_Inf;
        bzero(decimals, sizeof(decimals));

        if (pdec32->sign) {
            decimals[sizeof(decimals) - 1] |= DECPPLUS;
        } else {
            decimals[sizeof(decimals) - 1] |= DECPMINUS;
        }
    } else {
        /* comdb2 normalized */
        unmake_order_decimal32(pdec32, (char *)decimals, &exponent);
    }

    if (exponent < DECSINGLE_Emin - 7 || exponent > DECSINGLE_Emax + 7) {
        logmsg(LOGMSG_ERROR, "%s; format issues with decimal32: exponent=%d\n",
                __func__, exponent);
        hexdump(LOGMSG_USER, (unsigned char *)pdec32, sizeof(*pdec32));
        logmsg(LOGMSG_USER, "\n");
        exponent ^= 0x080;
    }

    decSingleFromPacked(dn, exponent, (uint8_t *)decimals);
}

static void decimal64_ondisk_to_double(server_decimal64_t *pdec64,
                                       decDouble *dn)
{
    char decimals[DECDOUBLE_PACKED_COEF];
    int exponent;
    int sign;
    int rc = 0;

    bzero(decimals, sizeof(decimals));

    /* Infinite? */
    if ((pdec64->sign == 1 && pdec64->exp == htons(0x7800)) ||
        (pdec64->sign == 0 && pdec64->exp == (htons(0x7800) ^ 0x0FFFF))) {
        exponent = DECFLOAT_Inf;

        if (pdec64->sign) {
            decimals[sizeof(decimals) - 1] |= DECPPLUS;
        } else {
            decimals[sizeof(decimals) - 1] |= DECPMINUS;
        }
    }
    /* NaN */
    else if ((pdec64->exp == htons(0x7c00) || pdec64->exp == htons(0x7e00))) {
        exponent = ntohs(pdec64->exp) << 16; /* preserve signaling bit */
    } else {
        /* comdb2 normalized */
        unmake_order_decimal64(pdec64, (char *)decimals, &exponent);

        if (exponent < DECDOUBLE_Emin - 16 || exponent > DECDOUBLE_Emax + 16) {
            comdb2_int2 tmp;
            logmsg(LOGMSG_ERROR, "%s; format issues with decimal64: exponent=%d\n",
                    __func__, exponent);
            hexdump(LOGMSG_ERROR, (unsigned char *)pdec64, sizeof(*pdec64));
            logmsg(LOGMSG_ERROR, "\n");
            int2b_to_int2(exponent, &tmp);
            exponent = tmp;
        }
    }

    decDoubleFromPacked(dn, exponent, (uint8_t *)decimals);
}

void decimal128_ondisk_to_quad(server_decimal128_t *pdec128, decQuad *dn)
{
    char decimals[DECQUAD_PACKED_COEF];
    int exponent;
    int sign;
    int rc = 0;

    bzero(decimals, sizeof(decimals));

    /* Infinite? */
    if ((pdec128->sign == 1 && pdec128->exp == htons(0x7800)) ||
        (pdec128->sign == 0 && pdec128->exp == (htons(0x7800) ^ 0x0FFFF))) {
        exponent = DECFLOAT_Inf;

        if (pdec128->sign) {
            decimals[sizeof(decimals) - 1] |= DECPPLUS;
        } else {

            decimals[sizeof(decimals) - 1] |= DECPMINUS;
        }
    }
    /* NaN */
    else if ((pdec128->exp == htons(0x7c00) || pdec128->exp == htons(0x7e00))) {
        exponent = ntohs(pdec128->exp) << 16; /* preserve signaling bit */
    } else {
        /* comdb2 normalized */
        unmake_order_decimal128(pdec128, (char *)decimals, &exponent);

        if (exponent < DECQUAD_Emin - 34 || exponent > DECQUAD_Emax + 34) {
            comdb2_int2 tmp;
            logmsg(LOGMSG_USER, "%s; format issues with decimal128: exponent=%d\n",
                    __func__, exponent);
            hexdump(LOGMSG_USER, (unsigned char *)pdec128, sizeof(*pdec128));
            logmsg(LOGMSG_USER, "\n");
            int2b_to_int2(exponent, &tmp);
            exponent = tmp;
        }
    }

    decQuadFromPacked(dn, exponent, (uint8_t *)decimals);
}

int decimal_ondisk_to_sqlite(const void *in, int len, decQuad *dn, int *outnull)
{
    decSingle sing;
    decDouble doub;

    if (stype_is_null(in)) {
        *outnull = 1;
        return 0;
    }

    switch (len) {
    case sizeof(server_decimal32_t):
        decimal32_ondisk_to_single((server_decimal32_t *)in, &sing);

        decSingleToWider(&sing, &doub);
        decDoubleToWider(&doub, dn);

        break;

    case sizeof(server_decimal64_t):
        decimal64_ondisk_to_double((server_decimal64_t *)in, &doub);

        decDoubleToWider(&doub, dn);

        break;

    case sizeof(server_decimal128_t):
        decimal128_ondisk_to_quad((server_decimal128_t *)in, dn);
        break;

    default:
        abort();
    }

#if 0
   {
      char aaa[1024];

      decQuadToString( dn, aaa);

      fprintf( stderr, "%s\n", aaa);
   }
#endif

    return 0;
}

int SERVER_DECIMAL_to_CLIENT_CSTR(const void *in, int inlen,
                                  const struct field_conv_opts *inopts,
                                  blob_buffer_t *inblob, void *out, int outlen,
                                  int *outnull, int *outdtsz,
                                  const struct field_conv_opts *outopts,
                                  blob_buffer_t *outblob)
{
    decSingle dfp_single;
    decDouble dfp_double;
    decQuad dfp_quad;
    int rc;

    if (stype_is_null(in)) {
        *outnull = 1;
        return 0;
    }

    decContextTestEndian(0);

    *outnull = 0;

    switch (inlen) {
    case sizeof(server_decimal32_t):

        if (outlen < DFP_32_MAX_STR)
            return -1;

        decimal32_ondisk_to_single((server_decimal32_t *)in, &dfp_single);

        decSingleToString(&dfp_single, (char *)out);
        break;
    case sizeof(server_decimal64_t):

        if (outlen < DFP_64_MAX_STR)
            return -1;

        decimal64_ondisk_to_double((server_decimal64_t *)in, &dfp_double);

        decDoubleToString(&dfp_double, (char *)out);
        break;
    case sizeof(server_decimal128_t):

        if (outlen < DFP_128_MAX_STR)
            return -1;

        decimal128_ondisk_to_quad((server_decimal128_t *)in, &dfp_quad);

        decQuadToString(&dfp_quad, (char *)out);
        break;
    }

    return 0;
}

#define SINTV_TO_CLT(from, to)                                                 \
    void *tmpbuf;                                                              \
    int rc;                                                                    \
    if (stype_is_null(in)) {                                                   \
        *outnull = 1;                                                          \
        return 0;                                                              \
    } else {                                                                   \
        tmpbuf = alloca(sizeof(double));                                       \
        rc = _intv_srv2real(in, from, tmpbuf);                                 \
        if (rc == -1)                                                          \
            return -1;                                                         \
        *outnull = 0;                                                          \
        rc = CLIENT_REAL_to_CLIENT_##to(tmpbuf, sizeof(double), NULL, NULL,    \
                                        out, outlen, outdtsz, outopts,         \
                                        outblob);                              \
    }                                                                          \
    return rc

#define SRV_TYPE_TO_CINTV(size, from, to)                                      \
    void *tmpbuf;                                                              \
    int rc;                                                                    \
    if (stype_is_null(in)) {                                                   \
        *outnull = 1;                                                          \
        return 0;                                                              \
    } else {                                                                   \
        tmpbuf = alloca(size);                                                 \
        rc = SERVER_##from##_to_SERVER_##to(in, inlen, inopts, inblob, tmpbuf, \
                                            size, outdtsz, NULL, NULL);        \
        if (rc == -1)                                                          \
            return -1;                                                         \
        rc = SERVER_##to##_to_CLIENT_##to(tmpbuf, size, NULL, NULL, out,       \
                                          outlen, outnull, outdtsz, outopts,   \
                                          outblob);                            \
    }                                                                          \
    return rc;

int SERVER_UINT_to_CLIENT_INTVYM(S2C_FUNKY_ARGS)
{

    SRV_TYPE_TO_CINTV(sizeof(server_intv_ym_t), UINT, INTVYM);
}

int SERVER_UINT_to_CLIENT_INTVDS(S2C_FUNKY_ARGS)
{

    SRV_TYPE_TO_CINTV(sizeof(server_intv_ds_t), UINT, INTVDS);
}

int SERVER_UINT_to_CLIENT_INTVDSUS(S2C_FUNKY_ARGS)
{

    SRV_TYPE_TO_CINTV(sizeof(server_intv_dsus_t), UINT, INTVDSUS);
}

int SERVER_BINT_to_CLIENT_INTVYM(S2C_FUNKY_ARGS)
{

    SRV_TYPE_TO_CINTV(sizeof(server_intv_ym_t), BINT, INTVYM);
}

int SERVER_BINT_to_CLIENT_INTVDS(S2C_FUNKY_ARGS)
{

    SRV_TYPE_TO_CINTV(sizeof(server_intv_ds_t), BINT, INTVDS);
}

int SERVER_BINT_to_CLIENT_INTVDSUS(S2C_FUNKY_ARGS)
{

    SRV_TYPE_TO_CINTV(sizeof(server_intv_dsus_t), BINT, INTVDSUS);
}

int SERVER_BREAL_to_CLIENT_INTVYM(S2C_FUNKY_ARGS)
{

    SRV_TYPE_TO_CINTV(sizeof(server_intv_ym_t), BREAL, INTVYM);
}

int SERVER_BREAL_to_CLIENT_INTVDS(S2C_FUNKY_ARGS)
{

    SRV_TYPE_TO_CINTV(sizeof(server_intv_ds_t), BREAL, INTVDS);
}

int SERVER_BREAL_to_CLIENT_INTVDSUS(S2C_FUNKY_ARGS)
{

    SRV_TYPE_TO_CINTV(sizeof(server_intv_dsus_t), BREAL, INTVDSUS);
}

int SERVER_BCSTR_to_CLIENT_INTVYM(S2C_FUNKY_ARGS)
{

    SRV_TYPE_TO_CINTV(sizeof(server_intv_ym_t), BCSTR, INTVYM);
}

int SERVER_BCSTR_to_CLIENT_INTVDS(S2C_FUNKY_ARGS)
{

    SRV_TYPE_TO_CINTV(sizeof(server_intv_ds_t), BCSTR, INTVDS);
}

int SERVER_BCSTR_to_CLIENT_INTVDSUS(S2C_FUNKY_ARGS)
{

    SRV_TYPE_TO_CINTV(sizeof(server_intv_dsus_t), BCSTR, INTVDSUS);
}

int SERVER_INTVYM_to_CLIENT_UINT(S2C_FUNKY_ARGS)
{

    SINTV_TO_CLT(INTV_YM, UINT);
}

int SERVER_INTVYM_to_CLIENT_INT(S2C_FUNKY_ARGS) { SINTV_TO_CLT(INTV_YM, INT); }

int SERVER_INTVYM_to_CLIENT_REAL(S2C_FUNKY_ARGS)
{

    SINTV_TO_CLT(INTV_YM, REAL);
}

int SERVER_INTVYM_to_CLIENT_CSTR(S2C_FUNKY_ARGS)
{

    int rc;
    if (stype_is_null(in)) {
        *outnull = 1;
        return 0;
    }

    *outnull = 0;

    rc = _intv_srv2string(in, INTV_YM, out, outlen);
    if (!rc)
        *outdtsz = strlen(out) + 1;

    return rc;
}

int SERVER_INTVYM_to_CLIENT_PSTR(S2C_FUNKY_ARGS)
{

    int ret = 0;

    ret = SERVER_INTVYM_to_CLIENT_CSTR(in, inlen, inopts, NULL, out, outlen,
                                       outnull, outdtsz, outopts, NULL);
    if (ret)
        return ret;
    if (*outnull)
        return 0;

    /* convert out from CSTR to PSTR */
    ((char *)out)[*outdtsz - 1] = ' ';

    return 0;
}

int SERVER_INTVYM_to_CLIENT_PSTR2(S2C_FUNKY_ARGS)
{

    int ret = 0;

    ret = SERVER_INTVYM_to_CLIENT_CSTR(in, inlen, inopts, NULL, out, outlen,
                                       outnull, outdtsz, outopts, NULL);
    if (ret)
        return ret;
    if (*outnull)
        return 0;

    /* convert out from CSTR to PSTR2 */
    for (ret = *outdtsz - 1; ret < outlen; ret++)
        ((char *)out)[ret] = ' ';
    *outdtsz = outlen;

    return 0;
}

int SERVER_INTVYM_to_CLIENT_INTVYM(S2C_FUNKY_ARGS)
{

    server_intv_ym_t cin;
    cdb2_client_intv_ym_t cout;
    uint8_t *p_buf_in = (uint8_t *)in;
    uint8_t *p_buf_in_end = (uint8_t *)in + inlen;
    uint8_t *p_buf_out = (uint8_t *)out;
    uint8_t *p_buf_out_end = (uint8_t *)out + outlen;
    int months = 0, sign;

    if (stype_is_null(in)) {
        *outnull = 1;
        return 0;
    }

    *outnull = 0;

    if (!(server_intv_ym_get(&cin, p_buf_in, p_buf_in_end))) {
        logmsg(LOGMSG_ERROR, "%s: line %d server_intv_ym_get returns NULL???\n",
                __func__, __LINE__);
        return -1;
    }

    /* server to client */
    memcpy(&months, &cin.months, sizeof(months));
    int4b_to_int4(months, (comdb2_int4 *)&months);

    sign = months < 0 ? -1 : 1;

    bzero(&cout, sizeof(cout));

    months *= sign;
    cout.sign = sign;
    cout.months = months % 12;
    cout.years = months / 12;

    if (outopts && outopts->flags & FLD_CONV_LENDIAN) {
        if (!(client_intv_ym_little_put(&cout, p_buf_out, p_buf_out_end))) {
            logmsg(LOGMSG_ERROR, 
                    "%s: line %d client_intv_ym_little_put returns NULL???\n",
                    __func__, __LINE__);
            return -1;
        }
    } else {
        if (!(client_intv_ym_put(&cout, p_buf_out, p_buf_out_end))) {
            logmsg(LOGMSG_ERROR, 
                    "%s: line %d client_intv_ym_put returns NULL???\n",
                    __func__, __LINE__);
            return -1;
        }
    }

    *outdtsz = sizeof(cdb2_client_intv_ym_t);

    return 0;
}

int SERVER_INTVDS_to_CLIENT_UINT(S2C_FUNKY_ARGS)
{

    SINTV_TO_CLT(INTV_DS, UINT);
}

int SERVER_INTVDSUS_to_CLIENT_UINT(S2C_FUNKY_ARGS)
{

    SINTV_TO_CLT(INTV_DSUS, UINT);
}

int SERVER_INTVDS_to_CLIENT_INT(S2C_FUNKY_ARGS) { SINTV_TO_CLT(INTV_DS, INT); }

int SERVER_INTVDSUS_to_CLIENT_INT(S2C_FUNKY_ARGS)
{

    SINTV_TO_CLT(INTV_DSUS, INT);
}

int SERVER_INTVDS_to_CLIENT_REAL(S2C_FUNKY_ARGS)
{

    SINTV_TO_CLT(INTV_DS, REAL);
}

int SERVER_INTVDSUS_to_CLIENT_REAL(S2C_FUNKY_ARGS)
{

    SINTV_TO_CLT(INTV_DSUS, REAL);
}

int SERVER_INTVDS_to_CLIENT_CSTR(S2C_FUNKY_ARGS)
{

    int rc;
    if (stype_is_null(in)) {
        *outnull = 1;
        return 0;
    }

    *outnull = 0;

    rc = _intv_srv2string(in, INTV_DS, out, outlen);
    if (!rc)
        *outdtsz = strlen(out) + 1;

    return rc;
}

int SERVER_INTVDSUS_to_CLIENT_CSTR(S2C_FUNKY_ARGS)
{

    int rc;
    if (stype_is_null(in)) {
        *outnull = 1;
        return 0;
    }

    *outnull = 0;

    rc = _intv_srv2string(in, INTV_DSUS, out, outlen);
    if (!rc)
        *outdtsz = strlen(out) + 1;

    return rc;
}

int SERVER_INTVDS_to_CLIENT_PSTR(S2C_FUNKY_ARGS)
{

    int ret = 0;

    ret = SERVER_INTVDS_to_CLIENT_CSTR(in, inlen, inopts, NULL, out, outlen,
                                       outnull, outdtsz, outopts, NULL);
    if (ret)
        return ret;
    if (*outnull)
        return 0;

    /* convert out from CSTR to PSTR */
    ((char *)out)[*outdtsz - 1] = ' ';

    return 0;
}

int SERVER_INTVDSUS_to_CLIENT_PSTR(S2C_FUNKY_ARGS)
{

    int ret = 0;

    ret = SERVER_INTVDSUS_to_CLIENT_CSTR(in, inlen, inopts, NULL, out, outlen,
                                         outnull, outdtsz, outopts, NULL);
    if (ret)
        return ret;
    if (*outnull)
        return 0;

    /* convert out from CSTR to PSTR */
    ((char *)out)[*outdtsz - 1] = ' ';

    return 0;
}

int SERVER_INTVDS_to_CLIENT_PSTR2(S2C_FUNKY_ARGS)
{

    int ret = 0;

    ret = SERVER_INTVDS_to_CLIENT_CSTR(in, inlen, inopts, NULL, out, outlen,
                                       outnull, outdtsz, outopts, NULL);
    if (ret)
        return ret;
    if (*outnull)
        return 0;

    /* convert out from CSTR to PSTR2 */
    for (ret = *outdtsz - 1; ret < outlen; ret++)
        ((char *)out)[ret] = ' ';
    *outdtsz = outlen;

    return 0;
}

int SERVER_INTVDSUS_to_CLIENT_PSTR2(S2C_FUNKY_ARGS)
{

    int ret = 0;

    ret = SERVER_INTVDSUS_to_CLIENT_CSTR(in, inlen, inopts, NULL, out, outlen,
                                         outnull, outdtsz, outopts, NULL);
    if (ret)
        return ret;
    if (*outnull)
        return 0;

    /* convert out from CSTR to PSTR2 */
    for (ret = *outdtsz - 1; ret < outlen; ret++)
        ((char *)out)[ret] = ' ';
    *outdtsz = outlen;

    return 0;
}

#define SERVER_INTVDST_to_CLIENT_INTVDST_func_body(                            \
    from_dt, FROM_UDT, from_prec, from_frac, from_fracdt, to_dt, TO_UDT,       \
    to_prec, to_frac, to_fracdt)                                               \
    SRV_TP(from_dt) cin;                                                       \
    CLT_TP(to_dt) cout;                                                        \
                                                                               \
    /* pointers to raw buffers */                                              \
    uint8_t *p_buf_in = (uint8_t *)in;                                         \
    uint8_t *p_buf_in_end = (uint8_t *)in + inlen;                             \
    uint8_t *p_buf_out = (uint8_t *)out;                                       \
    uint8_t *p_buf_out_end = (uint8_t *)out + outlen;                          \
    long long sec = 0;                                                         \
    from_fracdt from_frac = 0;                                                 \
                                                                               \
    if (stype_is_null(in)) {                                                   \
        *outnull = 1;                                                          \
        return 0;                                                              \
    }                                                                          \
                                                                               \
    *outnull = 0;                                                              \
                                                                               \
    if (!server_##from_dt##_get(&cin, p_buf_in, p_buf_in_end)) {               \
        logmsg(LOGMSG_ERROR,                                                   \
                "%s: line %d server_" #from_dt "_get returns NULL???\n",       \
                __func__, __LINE__);                                           \
        return -1;                                                             \
    }                                                                          \
                                                                               \
    memcpy(&sec, &cin.sec, sizeof(sec));                                       \
    memcpy(&from_frac, &cin.from_frac, sizeof(from_frac));                     \
                                                                               \
    /* we need to use these so change them to host-order */                    \
    int8b_to_int8(sec, (comdb2_int8 *)&sec);                                   \
                                                                               \
    /* zap this */                                                             \
    bzero(&cout, sizeof(cout));                                                \
                                                                               \
    /* client to server while normalizing */                                   \
    cout.sign = (sec < 0) ? -1 : 1;                                            \
    if (cout.sign == -1) {                                                     \
        sec = -sec;                                                            \
        if (from_frac) {                                                       \
            sec--;                                                             \
        }                                                                      \
    }                                                                          \
                                                                               \
    /* put in network order */                                                 \
    cout.to_frac = cin.from_frac * IPOW10(to_prec) / IPOW10(from_prec);        \
                                                                               \
    cout.sec = S2S(sec);                                                       \
    cout.mins = S2M(sec);                                                      \
    cout.hours = S2H(sec);                                                     \
    cout.days = S2D(sec);                                                      \
                                                                               \
    if (outopts && outopts->flags & FLD_CONV_LENDIAN) {                        \
        if (!(client_##to_dt##_little_put(&cout, p_buf_out, p_buf_out_end))) { \
            logmsg(LOGMSG_USER,                                                \
                    "%s: line %d client_" #to_dt "_put returns NULL???\n",     \
                    __func__, __LINE__);                                       \
            return -1;                                                         \
        }                                                                      \
    } else {                                                                   \
        if (!(client_##to_dt##_put(&cout, p_buf_out, p_buf_out_end))) {        \
            logmsg(LOGMSG_USER,                                                \
                    "%s: line %d client_" #to_dt "_put returns NULL???\n",     \
                    __func__, __LINE__);                                       \
            return -1;                                                         \
        }                                                                      \
    }                                                                          \
                                                                               \
    *outdtsz = sizeof(CLT_TP(to_dt));                                          \
    return 0;
/* END OF SERVER_INTVDST_to_CLIENT_INTVDST_func_body */

int SERVER_INTVDS_to_CLIENT_INTVDS(S2C_FUNKY_ARGS)
{
    SERVER_INTVDST_to_CLIENT_INTVDST_func_body(
        intv_ds, INTVDS, 3, msec, unsigned short, /* from */
        intv_ds, INTVDS, 3, msec, unsigned short /* to   */);
}

int SERVER_INTVDS_to_CLIENT_INTVDSUS(S2C_FUNKY_ARGS)
{
    if (gbl_forbid_datetime_promotion)
        return -1;
    SERVER_INTVDST_to_CLIENT_INTVDST_func_body(
        intv_ds, INTVDS, 3, msec, unsigned short, /* from */
        intv_dsus, INTVDSUS, 6, usec, unsigned int /* to   */);
}

int SERVER_INTVDSUS_to_CLIENT_INTVDS(S2C_FUNKY_ARGS)
{
    if (gbl_forbid_datetime_truncation)
        return -1;
    SERVER_INTVDST_to_CLIENT_INTVDST_func_body(
        intv_dsus, INTVDSUS, 6, usec, unsigned int, /* from */
        intv_ds, INTVDS, 3, msec, unsigned short /* to   */);
}

int SERVER_INTVDSUS_to_CLIENT_INTVDSUS(S2C_FUNKY_ARGS)
{
    SERVER_INTVDST_to_CLIENT_INTVDST_func_body(
        intv_dsus, INTVDSUS, 6, usec, unsigned int, /* from */
        intv_dsus, INTVDSUS, 6, usec, unsigned int /* to   */);
}

/* client to server */

#define CLIENT_BLOB_to_SERVER_INTVDS CLIENT_to_SERVER_NO_CONV
#define CLIENT_BLOB_to_SERVER_INTVDSUS CLIENT_to_SERVER_NO_CONV
#define CLIENT_BLOB_to_SERVER_INTVYM CLIENT_to_SERVER_NO_CONV
#define CLIENT_BLOB_to_SERVER_VUTF8 CLIENT_to_SERVER_NO_CONV
#define CLIENT_BYTEARRAY_to_SERVER_INTVDS CLIENT_to_SERVER_NO_CONV
#define CLIENT_BYTEARRAY_to_SERVER_INTVDSUS CLIENT_to_SERVER_NO_CONV
#define CLIENT_BYTEARRAY_to_SERVER_INTVYM CLIENT_to_SERVER_NO_CONV
#define CLIENT_VUTF8_to_SERVER_BLOB2 CLIENT_to_SERVER_NO_CONV
#define CLIENT_INTVDS_to_SERVER_BLOB2 CLIENT_to_SERVER_NO_CONV
#define CLIENT_INTVDSUS_to_SERVER_BLOB2 CLIENT_to_SERVER_NO_CONV
#define CLIENT_INTVYM_to_SERVER_BLOB2 CLIENT_to_SERVER_NO_CONV
#define CLIENT_DATETIME_to_SERVER_BLOB2 CLIENT_to_SERVER_NO_CONV
#define CLIENT_DATETIME_to_SERVER_BLOB2 CLIENT_to_SERVER_NO_CONV
#define CLIENT_DATETIMEUS_to_SERVER_BLOB2 CLIENT_to_SERVER_NO_CONV
#define CLIENT_DATETIMEUS_to_SERVER_BLOB2 CLIENT_to_SERVER_NO_CONV
#define CLIENT_PSTR2_to_SERVER_BLOB2 CLIENT_to_SERVER_NO_CONV
#define CLIENT_PSTR_to_SERVER_BLOB2 CLIENT_to_SERVER_NO_CONV
#define CLIENT_CSTR_to_SERVER_BLOB2 CLIENT_to_SERVER_NO_CONV
#define CLIENT_REAL_to_SERVER_BLOB2 CLIENT_to_SERVER_NO_CONV
#define CLIENT_INT_to_SERVER_BLOB2 CLIENT_to_SERVER_NO_CONV
#define CLIENT_UINT_to_SERVER_BLOB2 CLIENT_to_SERVER_NO_CONV
#define CLIENT_DATETIME_to_SERVER_INTVDS CLIENT_to_SERVER_NO_CONV
#define CLIENT_DATETIME_to_SERVER_INTVDSUS CLIENT_to_SERVER_NO_CONV
#define CLIENT_DATETIME_to_SERVER_INTVYM CLIENT_to_SERVER_NO_CONV
#define CLIENT_DATETIME_to_SERVER_VUTF8 CLIENT_to_SERVER_NO_CONV
#define CLIENT_DATETIMEUS_to_SERVER_INTVDS CLIENT_to_SERVER_NO_CONV
#define CLIENT_DATETIMEUS_to_SERVER_INTVDSUS CLIENT_to_SERVER_NO_CONV
#define CLIENT_DATETIMEUS_to_SERVER_INTVYM CLIENT_to_SERVER_NO_CONV
#define CLIENT_DATETIMEUS_to_SERVER_VUTF8 CLIENT_to_SERVER_NO_CONV
#define CLIENT_INTVDS_to_SERVER_BLOB CLIENT_to_SERVER_NO_CONV
#define CLIENT_INTVDS_to_SERVER_BYTEARRAY CLIENT_to_SERVER_NO_CONV
#define CLIENT_INTVDS_to_SERVER_DATETIME CLIENT_to_SERVER_NO_CONV
#define CLIENT_INTVDS_to_SERVER_DATETIMEUS CLIENT_to_SERVER_NO_CONV
#define CLIENT_INTVDS_to_SERVER_INTVYM CLIENT_to_SERVER_NO_CONV
#define CLIENT_INTVDS_to_SERVER_VUTF8 CLIENT_to_SERVER_NO_CONV
#define CLIENT_INTVDSUS_to_SERVER_BLOB CLIENT_to_SERVER_NO_CONV
#define CLIENT_INTVDSUS_to_SERVER_BYTEARRAY CLIENT_to_SERVER_NO_CONV
#define CLIENT_INTVDSUS_to_SERVER_DATETIME CLIENT_to_SERVER_NO_CONV
#define CLIENT_INTVDSUS_to_SERVER_DATETIMEUS CLIENT_to_SERVER_NO_CONV
#define CLIENT_INTVDSUS_to_SERVER_INTVYM CLIENT_to_SERVER_NO_CONV
#define CLIENT_INTVDSUS_to_SERVER_VUTF8 CLIENT_to_SERVER_NO_CONV
#define CLIENT_INTVYM_to_SERVER_BLOB CLIENT_to_SERVER_NO_CONV
#define CLIENT_INTVYM_to_SERVER_BYTEARRAY CLIENT_to_SERVER_NO_CONV
#define CLIENT_INTVYM_to_SERVER_DATETIME CLIENT_to_SERVER_NO_CONV
#define CLIENT_INTVYM_to_SERVER_DATETIMEUS CLIENT_to_SERVER_NO_CONV
#define CLIENT_INTVYM_to_SERVER_INTVDS CLIENT_to_SERVER_NO_CONV
#define CLIENT_INTVYM_to_SERVER_INTVDSUS CLIENT_to_SERVER_NO_CONV
#define CLIENT_INTVYM_to_SERVER_VUTF8 CLIENT_to_SERVER_NO_CONV
#define CLIENT_INT_to_SERVER_VUTF8 CLIENT_to_SERVER_NO_CONV
#define CLIENT_PSTR_to_SERVER_VUTF8 CLIENT_to_SERVER_NO_CONV
#define CLIENT_REAL_to_SERVER_VUTF8 CLIENT_to_SERVER_NO_CONV
#define CLIENT_UINT_to_SERVER_VUTF8 CLIENT_to_SERVER_NO_CONV
#define CLIENT_VUTF8_to_SERVER_BCSTR CLIENT_to_SERVER_NO_CONV
#define CLIENT_VUTF8_to_SERVER_BINT CLIENT_to_SERVER_NO_CONV
#define CLIENT_VUTF8_to_SERVER_BLOB CLIENT_to_SERVER_NO_CONV
#define CLIENT_VUTF8_to_SERVER_BREAL CLIENT_to_SERVER_NO_CONV
#define CLIENT_VUTF8_to_SERVER_BYTEARRAY CLIENT_to_SERVER_NO_CONV
#define CLIENT_VUTF8_to_SERVER_DATETIME CLIENT_to_SERVER_NO_CONV
#define CLIENT_VUTF8_to_SERVER_DATETIMEUS CLIENT_to_SERVER_NO_CONV
#define CLIENT_VUTF8_to_SERVER_INTVDS CLIENT_to_SERVER_NO_CONV
#define CLIENT_VUTF8_to_SERVER_INTVDSUS CLIENT_to_SERVER_NO_CONV
#define CLIENT_VUTF8_to_SERVER_INTVYM CLIENT_to_SERVER_NO_CONV
#define CLIENT_VUTF8_to_SERVER_UINT CLIENT_to_SERVER_NO_CONV

#define CLIENT_UINT_to_SERVER_DECIMAL CLIENT_to_SERVER_NO_CONV
#define CLIENT_INT_to_SERVER_DECIMAL CLIENT_to_SERVER_NO_CONV
#define CLIENT_REAL_to_SERVER_DECIMAL CLIENT_to_SERVER_NO_CONV
#define CLIENT_PSTR_to_SERVER_DECIMAL CLIENT_to_SERVER_NO_CONV
#define CLIENT_BYTEARRAY_to_SERVER_DECIMAL CLIENT_to_SERVER_NO_CONV
#define CLIENT_PSTR2_to_SERVER_DECIMAL CLIENT_to_SERVER_NO_CONV
#define CLIENT_BLOB_to_SERVER_DECIMAL CLIENT_to_SERVER_NO_CONV
#define CLIENT_DATETIME_to_SERVER_DECIMAL CLIENT_to_SERVER_NO_CONV
#define CLIENT_DATETIMEUS_to_SERVER_DECIMAL CLIENT_to_SERVER_NO_CONV
#define CLIENT_INTVYM_to_SERVER_DECIMAL CLIENT_to_SERVER_NO_CONV
#define CLIENT_INTVDS_to_SERVER_DECIMAL CLIENT_to_SERVER_NO_CONV
#define CLIENT_INTVDSUS_to_SERVER_DECIMAL CLIENT_to_SERVER_NO_CONV
#define CLIENT_VUTF8_to_SERVER_DECIMAL CLIENT_to_SERVER_NO_CONV

int dfp_conv_check_status(void *pctx, char *from, char *to)
{
    decContext *ctx = (decContext *)pctx;
    uint32_t status;

    status = decContextGetStatus(ctx);
    if (decContextTestSavedStatus(status, DEC_Overflow)) {
        logmsg(LOGMSG_ERROR, "%s:%d %s cannot compress %s to %s [Overflow]\n",
                __FILE__, __LINE__, __func__, from, to);
        return -1;
    }

    if (decContextTestSavedStatus(status, DEC_Clamped)) {
        logmsg(LOGMSG_ERROR, "%s:%d %s cannot compress %s to %s [Clamped]\n",
                __FILE__, __LINE__, __func__, from, to);
        return -1;
    }

    if (decContextTestSavedStatus(status, DEC_Inexact) &&
        gbl_decimal_rounding == DEC_ROUND_NONE) {
        logmsg(LOGMSG_ERROR, "%s:%d %s cannot compress %s to %s [Rounded]\n",
                __FILE__, __LINE__, __func__, from, to);
        return -1;
    }

    if (decContextTestSavedStatus(status, DEC_Subnormal)) {
        logmsg(LOGMSG_ERROR, "%s:%d %s cannot compress %s to %s [Subnormal]\n",
                __FILE__, __LINE__, __func__, from, to);
        return -1;
    }

    if (decContextTestSavedStatus(status, DEC_Underflow)) {
        logmsg(LOGMSG_ERROR, "%s:%d %s cannot compress %s to %s [Underflow]\n",
                __FILE__, __LINE__, __func__, from, to);
        return -1;
    }

    return 0;
}

static int dfp_single_to_ondisk(decSingle *dn, server_decimal32_t *pdec32)
{
    decDouble doub;
    int sign;
    int rc = 0;
    char mask;

    bzero(pdec32, sizeof(*pdec32));
    bset(pdec32, data_bit);

    decSingleToWider(dn, &doub);

    sign = decDoubleIsSigned(&doub);

    mask = 0x080;

    /* NaN */
    if (decDoubleIsNaN(&doub)) {
        pdec32->sign = 1;
        if (decDoubleIsSignaling(&doub))
            pdec32->exp = 0x7e ^ mask;
        else
            pdec32->exp = 0x7c ^ mask;
    }
    /* Infinite? */
    else if (decDoubleIsInfinite(&doub)) {
        pdec32->exp = 0x78 ^ mask;
        if (sign) {
            pdec32->sign = 0;
            pdec32->exp ^= 0x0FF;
        } else {
            pdec32->sign = 1;
        }
    } else
    /* Good one */
    {
        int32_t exponent;
        int i;

        sign = decSingleToPacked(dn, &exponent, pdec32->coef);

        /* we can keep this shifted left */
        pdec32->coef[DECSINGLE_PACKED_COEF - 1] &= 0x0F0; /* clear the sign */
        pdec32->sign = sign ? 0 : 1;
        pdec32->exp = exponent;

        /* comdb2 normalized */
        rc = make_order_decimal32(pdec32);
        if (rc) {
            rc = -1;
            goto done;
        }
    }

done:
/* debug */
#if 0
   {
      int i=0;

      if (!pdec32->sign)
         printf("-");
      for (i=0;i<sizeof(pdec32->coef);i++)
         printf("%x", pdec32->coef[i]^((pdec32->sign)?0:0x0FF));
      printf("E%d", pdec32->exp^((pdec32->sign)?0:0x0FF));
      printf("\n");
   }
#endif

    return rc;
}

static int dfp_double_to_ondisk(decDouble *dn, server_decimal64_t *pdec64)
{
    int sign;
    int rc = 0;
    ;

    bzero(pdec64, sizeof(*pdec64));
    bset(pdec64, data_bit);

    sign = decDoubleIsSigned(dn);

    /* Infinite? */
    if (decDoubleIsInfinite(dn)) {
        pdec64->exp = htons(0x7800);

        if (sign) {
            pdec64->sign = 0;
            pdec64->exp ^= 0x0FFFF;
        } else {
            pdec64->sign = 1;
        }
        goto done;
    }

    /* NaN */
    if (decDoubleIsNaN(dn)) {
        pdec64->sign = 1;
        if (decDoubleIsSignaling(dn))
            pdec64->exp = htons(0x7e00);
        else
            pdec64->exp = htons(0x7c00);

        goto done;
    }

    /* Good one */
    {
        int32_t exponent;

        sign = decDoubleToPacked(dn, &exponent, pdec64->coef);

        /* we can keep this shifted left */
        pdec64->coef[DECDOUBLE_PACKED_COEF - 1] &= 0x0F0; /* clear the sign */
        pdec64->sign = sign ? 0 : 1;

        /* comdb2 normalized */
        rc = make_order_decimal64(pdec64, exponent);
        if (rc) {
            rc = -1;
            goto done;
        }
    }

done:
/* debug */
#if 0
   {
      int i=0;

      if (!pdec64->sign)
         printf("-");
      for (i=0;i<sizeof(pdec64->coef);i++)
         printf("%x", pdec64->coef[i]^((pdec64->sign)?0:0x0FF));
      printf("E%d", ntohs(pdec64->exp)^((pdec64->sign)?0:0x0FFFF));
      printf("\n");
   }
#endif

    return rc;
}

static int dfp_quad_to_ondisk(decQuad *dn, server_decimal128_t *pdec128)
{
    int sign;
    int rc = 0;

    bzero(pdec128, sizeof(*pdec128));
    bset(pdec128, data_bit);

    sign = decQuadIsSigned(dn);

    /* Infinite? */
    if (decQuadIsInfinite(dn)) {
        pdec128->exp = htons(0x7800);

        if (sign) {
            pdec128->sign = 0;
            pdec128->exp ^= 0x0FFFF;
        } else {
            pdec128->sign = 1;
        }
        goto done;
    }

    /* NaN */
    if (decQuadIsNaN(dn)) {
        pdec128->sign = 1;
        if (decQuadIsSignaling(dn))
            pdec128->exp = htons(0x7e00);
        else
            pdec128->exp = htons(0x7c00);

        goto done;
    }

    /* Good one */
    {
        int32_t exponent;

        sign = decQuadToPacked(dn, &exponent, pdec128->coef);

        /* we can keep this shifted left */
        pdec128->coef[DECQUAD_PACKED_COEF - 1] &= 0x0F0; /* clear the sign */
        pdec128->sign = sign ? 0 : 1;

        /* comdb2 normalized */
        rc = make_order_decimal128(pdec128, exponent);
        if (rc) {
            rc = -1;
            goto done;
        }
    }

done:
/* debug */
#if 0
   {
      int i=0;

      if (!pdec128->sign)
         printf("-");
      for (i=0;i<sizeof(pdec128->coef);i++)
         printf("%x", pdec128->coef[i]^((pdec128->sign)?0:0x0FF));
      printf("E%d", ntohs(pdec128->exp)^((pdec128->sign)?0:0x0FFFF));
      printf("\n");
   }
#endif

    return rc;
}

static int dfp_quad_to_decimal32_ondisk(decQuad *quad, void *rec, int len)
{
    server_decimal32_t *pdec32;
    int sign;
    int rc = 0;
    int mask;

    char dbg[1024];

    if (len < sizeof(*pdec32)) {
        rc = -1;
        goto done;
    }

    sign = decQuadIsSigned(quad);

    pdec32 = (server_decimal32_t *)rec;

    mask = 0x080;

    /* Infinite? */
    if (decQuadIsInfinite(quad)) {
        pdec32->exp = 0x78 ^ mask;

        if (sign) {
            pdec32->sign = 0;
            pdec32->exp ^= 0x0FF;
        } else {
            pdec32->sign = 1;
        }
        goto done;
    }

    /* NaN */
    if (decQuadIsNaN(quad)) {
        pdec32->sign = 1;
        if (decQuadIsSignaling(quad))
            pdec32->exp = 0x7e ^ mask;
        else
            pdec32->exp = 0x7c ^ mask;

        goto done;
    }

    /* Good one */
    {
        int32_t exponent;
        decContext doub_ctx;
        decContext sing_ctx;
        decDouble doub;
        decSingle sing;
        int i;

        /* first, convert Quad to Single and check for range */
        dec_ctx_init(&doub_ctx, DEC_INIT_DECIMAL64, gbl_decimal_rounding);
        dec_ctx_init(&sing_ctx, DEC_INIT_DECIMAL32, gbl_decimal_rounding);

        decDoubleFromWider(&doub, quad, &doub_ctx);
        rc = dfp_conv_check_status(&doub_ctx, "quad", "double");
        if (rc)
            goto done;

        decDoubleToString(&doub, dbg);
        /*printf("Got double %s\n", dbg);*/

        decSingleFromWider(&sing, &doub, &sing_ctx);
        rc = dfp_conv_check_status(&sing_ctx, "double", "single");
        if (rc)
            goto done;

        decSingleToString(&sing, dbg);
        /*printf("Got double %s\n", dbg);*/

        sign = decSingleToPacked(&sing, &exponent, pdec32->coef);

        /* we can keep this shifted left */
        pdec32->coef[DECSINGLE_PACKED_COEF - 1] &= 0x0F0; /* clear the sign */
        pdec32->sign = sign ? 0 : 1;
        pdec32->exp = exponent;

        /* comdb2 normalized */
        if (make_order_decimal32(pdec32)) {
            rc = -1;
            goto done;
        }

#if 0
      /* decided to keep this shifted left to save some cpu */
         pdec32->coef[0] = (decimals[0] >> 4) & 0x0F;
         pdec32->coef[0] ^= 0x0FF;
         for(i=1; i<DECSINGLE_PACKED_COEF; i++)
         {
            pdec32->coef[i] = (decimals[i-1]<<4) | ((decimals[i]>>4) & 0x0F);
            pdec32->coef[i] ^= 0x0FF;
         }
#endif
    }

done:
/* debug */
#if 0
   {
      int i=0;

      if (!pdec32->sign)
         printf("-");
      for (i=0;i<sizeof(pdec32->coef);i++)
         printf("%x", pdec32->coef[i]^((pdec32->sign)?0:0x0FF));
      printf("E%d", pdec32->exp^((pdec32->sign)?0:0x0FF));
      printf("\n");
   }
#endif

    return rc;
}

static int dfp_quad_to_decimal64_ondisk(decQuad *quad, void *rec, int len)
{
    server_decimal64_t *pdec64;
    int sign;
    int rc = 0;

    if (len < sizeof(*pdec64)) {
        rc = -1;
        goto done;
    }

    sign = decQuadIsSigned(quad);

    pdec64 = (server_decimal64_t *)rec;

    /* Infinite? */
    if (decQuadIsInfinite(quad)) {
        pdec64->exp = htons(0x7800);

        if (sign) {
            pdec64->sign = 0;
            pdec64->exp ^= 0x0FFFF;
        } else {
            pdec64->sign = 1;
        }
        goto done;
    }

    /* NaN */
    if (decQuadIsNaN(quad)) {
        pdec64->sign = 1;
        if (decQuadIsSignaling(quad))
            pdec64->exp = htons(0x7e00);
        else
            pdec64->exp = htons(0x7c00);

        goto done;
    }

    /* Good one */
    {
        int32_t exponent;
        decContext doub_ctx;
        decDouble doub;

        /* first, convert Quad to Single and check for range */
        dec_ctx_init(&doub_ctx, DEC_INIT_DECIMAL64, gbl_decimal_rounding);

        decDoubleFromWider(&doub, quad, &doub_ctx);
        rc = dfp_conv_check_status(&doub_ctx, "quad", "double");
        if (rc)
            goto done;

        sign = decDoubleToPacked(&doub, &exponent, pdec64->coef);

        /* we can keep this shifted left */
        pdec64->coef[DECDOUBLE_PACKED_COEF - 1] &= 0x0F0; /* clear the sign */
        pdec64->sign = sign ? 0 : 1;

        /* comdb2 normalized */
        if (make_order_decimal64(pdec64, exponent)) {
            rc = -1;
            goto done;
        }
    }

done:
/* debug */
#if 0
   {
      int i=0;

      if (!pdec64->sign)
         printf("-");
      for (i=0;i<sizeof(pdec64->coef);i++)
         printf("%x", pdec64->coef[i]^((pdec64->sign)?0:0x0FF));
      printf("E%d", ntohs(pdec64->exp)^((pdec64->sign)?0:0x0FFFF));
      printf("\n");
   }
#endif

    return rc;
#if 0
         for(i=0; i<DECSINGLE_PACKED_COEF-1; i++)
         {
            pdec32->coef[i] = (decimals[i]<<4) | ((decimals[i+1]>>4) & 0x0F);
            pdec32->coef[i] ^= 0x0FF;
         }
#endif
}

static int dfp_quad_to_decimal128_ondisk(decQuad *quad, void *rec, int len)
{
    server_decimal128_t *pdec128;
    int sign;
    int rc = 0;

    if (len < sizeof(*pdec128)) {
        rc = -1;
        goto done;
    }

    sign = decQuadIsSigned(quad);

    pdec128 = (server_decimal128_t *)rec;

    /* Infinite? */
    if (decQuadIsInfinite(quad)) {
        pdec128->exp = htons(0x7800);

        if (sign) {
            pdec128->sign = 0;
            pdec128->exp ^= 0x0FFFF;
        } else {
            pdec128->sign = 1;
        }
        goto done;
    }

    /* NaN */
    if (decQuadIsNaN(quad)) {
        pdec128->sign = 1;
        if (decQuadIsSignaling(quad))
            pdec128->exp = htons(0x7e00);
        else
            pdec128->exp = htons(0x7c00);

        goto done;
    }

    /* Good one */
    {
        int32_t exponent;

        sign = decQuadToPacked(quad, &exponent, pdec128->coef);

        /* we can keep this shifted left */
        pdec128->coef[DECQUAD_PACKED_COEF - 1] &= 0x0F0; /* clear the sign */
        pdec128->sign = sign ? 0 : 1;

        /* comdb2 normalized */
        if (make_order_decimal128(pdec128, exponent)) {
            rc = -1;
            goto done;
        }
    }

done:
/* debug */
#if 0
   {
      int i=0;

      if (!pdec128->sign)
         printf("-");
      for (i=0;i<sizeof(pdec128->coef);i++)
         printf("%x", pdec128->coef[i]^((pdec128->sign)?0:0x0FF));
      printf("E%d", ntohs(pdec128->exp)^((pdec128->sign)?0:0x0FFFF));
      printf("\n");
   }
#endif

    return rc;
}

int sqlite_to_decimal_ondisk(decQuad *quad, void *rec, int len)
{
    int rc = 0;

    bzero((char *)rec, len);
    bset((char *)rec, data_bit);

#if 0
    char dbgstr[DFP_128_MAX_STR];

    decQuadToString(quad, dbgstr);
    fprintf(stderr, "%s\n", dbgstr);

#endif

    switch (len) {
    case (sizeof(server_decimal32_t)):
        rc = dfp_quad_to_decimal32_ondisk(quad, rec, len);
        break;

    case (sizeof(server_decimal64_t)):
        rc = dfp_quad_to_decimal64_ondisk(quad, rec, len);
        break;

    case (sizeof(server_decimal128_t)):
        rc = dfp_quad_to_decimal128_ondisk(quad, rec, len);
        break;

    default:
        abort();
    }

    return rc;
}

int CLIENT_CSTR_to_SERVER_DECIMAL(const void *in, int inlen, int isnull,
                                  const struct field_conv_opts *inopts,
                                  blob_buffer_t *inblob, void *out, int outlen,
                                  int *outdtsz,
                                  const struct field_conv_opts *outopts,
                                  blob_buffer_t *outblob)
{
    decContext dfp_ctx;
    decSingle dfp_single;
    decDouble dfp_double;
    decQuad dfp_quad;
    int rc = 0;

    if (isnull) {
        set_null(out, outlen);
        return 0;
    }

    decContextTestEndian(0);

    switch (outlen) {
    case sizeof(server_decimal32_t):

        assert(outlen >= sizeof(server_decimal32_t));

        dec_ctx_init(&dfp_ctx, DEC_INIT_DECSINGLE, gbl_decimal_rounding);

        decSingleFromString(&dfp_single, in, &dfp_ctx);

        rc = dfp_conv_check_status(&dfp_ctx, "string", "single");
        if (rc)
            goto done;

        rc = dfp_single_to_ondisk(&dfp_single, (server_decimal32_t *)out);
        if (rc)
            goto done;

        break;

    case sizeof(server_decimal64_t):

        assert(outlen >= sizeof(server_decimal64_t));

        dec_ctx_init(&dfp_ctx, DEC_INIT_DECDOUBLE, gbl_decimal_rounding);

        decDoubleFromString(&dfp_double, in, &dfp_ctx);

        rc = dfp_conv_check_status(&dfp_ctx, "string", "double");
        if (rc)
            goto done;

        rc = dfp_double_to_ondisk(&dfp_double, (server_decimal64_t *)out);
        if (rc)
            goto done;

        break;

    case sizeof(server_decimal128_t):

        assert(outlen >= sizeof(server_decimal128_t));

        dec_ctx_init(&dfp_ctx, DEC_INIT_DECQUAD, gbl_decimal_rounding);

        decQuadFromString(&dfp_quad, in, &dfp_ctx);

        rc = dfp_conv_check_status(&dfp_ctx, "string", "quad");
        if (rc)
            goto done;

        rc = dfp_quad_to_ondisk(&dfp_quad, (server_decimal128_t *)out);
        if (rc)
            goto done;

        break;

    default:
        logmsg(LOGMSG_ERROR, "%d: bug, unknown server dfp length\n", __LINE__);
        return -1;
    }

done:
    return rc;
}

#define CLT_TO_SVR_THROUGH_SRV(cfrom, cto, sto)                                \
    void *tmpbuf;                                                              \
    int rc;                                                                    \
    if (isnull) {                                                              \
        set_null(out, outlen);                                                 \
        return 0;                                                              \
    } else {                                                                   \
        tmpbuf = alloca(inlen + 1);                                            \
        rc = CLIENT_##cfrom##_to_SERVER_##cto(in, inlen, 0, inopts, inblob,    \
                                              tmpbuf, inlen + 1, outdtsz,      \
                                              NULL, NULL);                     \
        if (rc == -1)                                                          \
            return -1;                                                         \
        rc = SERVER_##cto##_to_##SERVER_##sto(tmpbuf, inlen + 1, NULL, NULL,   \
                                              out, outlen, outdtsz, outopts,   \
                                              outblob);                        \
    }                                                                          \
    return rc;

#define CLT_TO_SVR_THROUGH_SRV_STR(cfrom, cto, sto)                            \
    void *tmpbuf;                                                              \
    int rc;                                                                    \
    if (isnull) {                                                              \
        set_null(out, outlen);                                                 \
        return 0;                                                              \
    } else {                                                                   \
        tmpbuf = alloca(inlen + 2);                                            \
        rc = CLIENT_##cfrom##_to_SERVER_##cto(in, inlen, 0, inopts, inblob,    \
                                              tmpbuf, inlen + 2, outdtsz,      \
                                              NULL, NULL);                     \
        if (rc == -1)                                                          \
            return -1;                                                         \
        rc =                                                                   \
            SERVER_##cto##_to_SERVER_##sto(tmpbuf, inlen + 2, NULL, NULL, out, \
                                           outlen, outdtsz, outopts, outblob); \
    }                                                                          \
    return rc;

#define CINTV_TO_SVR(from, to)                                                 \
    void *tmpbuf;                                                              \
    int rc;                                                                    \
    if (isnull) {                                                              \
        set_null(out, outlen);                                                 \
        return 0;                                                              \
    } else {                                                                   \
        tmpbuf = alloca(sizeof(double));                                       \
        rc = _intv_clt2real(in, inopts, from, tmpbuf);                         \
        if (rc == -1)                                                          \
            return -1;                                                         \
        rc = CLIENT_REAL_to_SERVER_##to(tmpbuf, sizeof(double), 0, NULL, NULL, \
                                        out, outlen, outdtsz, outopts,         \
                                        outblob);                              \
    }                                                                          \
    return rc;

#define CINTV_TO_SVR_STR(from)                                                 \
    void *tmpbuf;                                                              \
    int rc;                                                                    \
    if (isnull) {                                                              \
        set_null(out, outlen);                                                 \
        return 0;                                                              \
    } else {                                                                   \
        tmpbuf = alloca(outlen + 1);                                           \
        rc = _intv_clt2string(in, inopts, from, tmpbuf, outlen);               \
        if (rc == -1)                                                          \
            return -1;                                                         \
        rc = CLIENT_CSTR_to_SERVER_BCSTR(tmpbuf, outlen, 0, NULL, NULL, out,   \
                                         outlen, outdtsz, outopts, outblob);   \
    }                                                                          \
    return rc;

int CLIENT_UINT_to_SERVER_INTVYM(C2S_FUNKY_ARGS)
{

    CLT_TO_SVR_THROUGH_SRV(UINT, UINT, INTVYM);
}

int CLIENT_UINT_to_SERVER_INTVDS(C2S_FUNKY_ARGS)
{

    CLT_TO_SVR_THROUGH_SRV(UINT, UINT, INTVDS);
}

int CLIENT_UINT_to_SERVER_INTVDSUS(C2S_FUNKY_ARGS)
{

    CLT_TO_SVR_THROUGH_SRV(UINT, UINT, INTVDSUS);
}

int CLIENT_INT_to_SERVER_INTVYM(C2S_FUNKY_ARGS)
{

    CLT_TO_SVR_THROUGH_SRV(INT, BINT, INTVYM);
}

int CLIENT_INT_to_SERVER_INTVDS(C2S_FUNKY_ARGS)
{

    CLT_TO_SVR_THROUGH_SRV(INT, BINT, INTVDS);
}

int CLIENT_INT_to_SERVER_INTVDSUS(C2S_FUNKY_ARGS)
{

    CLT_TO_SVR_THROUGH_SRV(INT, BINT, INTVDSUS);
}

int CLIENT_REAL_to_SERVER_INTVYM(C2S_FUNKY_ARGS)
{

    CLT_TO_SVR_THROUGH_SRV(REAL, BREAL, INTVYM);
}

int CLIENT_REAL_to_SERVER_INTVDS(C2S_FUNKY_ARGS)
{

    CLT_TO_SVR_THROUGH_SRV(REAL, BREAL, INTVDS);
}

int CLIENT_REAL_to_SERVER_INTVDSUS(C2S_FUNKY_ARGS)
{

    CLT_TO_SVR_THROUGH_SRV(REAL, BREAL, INTVDSUS);
}

int CLIENT_CSTR_to_SERVER_INTVYM(C2S_FUNKY_ARGS)
{

    CLT_TO_SVR_THROUGH_SRV_STR(CSTR, BCSTR, INTVYM);
}

int CLIENT_CSTR_to_SERVER_INTVDS(C2S_FUNKY_ARGS)
{
    CLT_TO_SVR_THROUGH_SRV_STR(CSTR, BCSTR, INTVDS);
}

int CLIENT_CSTR_to_SERVER_INTVDSUS(C2S_FUNKY_ARGS)
{
    CLT_TO_SVR_THROUGH_SRV_STR(CSTR, BCSTR, INTVDSUS);
}

int CLIENT_PSTR_to_SERVER_INTVYM(C2S_FUNKY_ARGS)
{

    CLT_TO_SVR_THROUGH_SRV_STR(PSTR, BCSTR, INTVYM);
}

int CLIENT_PSTR_to_SERVER_INTVDS(C2S_FUNKY_ARGS)
{

    CLT_TO_SVR_THROUGH_SRV_STR(PSTR, BCSTR, INTVDS);
}

int CLIENT_PSTR_to_SERVER_INTVDSUS(C2S_FUNKY_ARGS)
{

    CLT_TO_SVR_THROUGH_SRV_STR(PSTR, BCSTR, INTVDSUS);
}

int CLIENT_PSTR2_to_SERVER_INTVYM(C2S_FUNKY_ARGS)
{

    CLT_TO_SVR_THROUGH_SRV_STR(PSTR2, BCSTR, INTVYM);
}

int CLIENT_PSTR2_to_SERVER_INTVDS(C2S_FUNKY_ARGS)
{

    CLT_TO_SVR_THROUGH_SRV_STR(PSTR2, BCSTR, INTVDS);
}

int CLIENT_PSTR2_to_SERVER_INTVDSUS(C2S_FUNKY_ARGS)
{

    CLT_TO_SVR_THROUGH_SRV_STR(PSTR2, BCSTR, INTVDSUS);
}

int CLIENT_INTVYM_to_SERVER_UINT(C2S_FUNKY_ARGS)
{

    CINTV_TO_SVR(INTV_YM, UINT);
}

int CLIENT_INTVYM_to_SERVER_BINT(C2S_FUNKY_ARGS)
{

    CINTV_TO_SVR(INTV_YM, BINT);
}

int CLIENT_INTVYM_to_SERVER_BREAL(C2S_FUNKY_ARGS)
{

    CINTV_TO_SVR(INTV_YM, BREAL);
}

int CLIENT_INTVYM_to_SERVER_BCSTR(C2S_FUNKY_ARGS) { CINTV_TO_SVR_STR(INTV_YM); }

int CLIENT_INTVYM_to_SERVER_INTVYM(C2S_FUNKY_ARGS)
{

    cdb2_client_intv_ym_t cin;
    server_intv_ym_t cout;

    /* pointers to raw buffers */
    uint8_t *p_buf_in = (uint8_t *)in;
    uint8_t *p_buf_in_end = (uint8_t *)in + inlen;
    uint8_t *p_buf_out = (uint8_t *)out;
    uint8_t *p_buf_out_end = (uint8_t *)out + outlen;

    int months = 0;

    if (isnull) {
        set_null(out, outlen);
        return 0;
    }

    if (inopts && inopts->flags & FLD_CONV_LENDIAN) {
        if (!(client_intv_ym_little_get(&cin, p_buf_in, p_buf_in_end))) {
            logmsg(LOGMSG_ERROR, "%s: line %d client_intv_ym_get returns NULL???\n",
                    __func__, __LINE__);
            return -1;
        }
    } else {
        if (!(client_intv_ym_get(&cin, p_buf_in, p_buf_in_end))) {
            logmsg(LOGMSG_ERROR, "%s: line %d client_intv_ym_get returns NULL???\n",
                    __func__, __LINE__);
            return -1;
        }
    }

    /* zero */
    bzero(&cout, sizeof(cout));

    /* server to client */
    bset(&cout.flag, data_bit);

    months = cin.sign * (long long)(cin.months + cin.years * 12);
    int4_to_int4b(months, (int4b *)&months);
    cout.months = months;

    if (!(server_intv_ym_put(&cout, p_buf_out, p_buf_out_end))) {
        logmsg(LOGMSG_ERROR, "%s: line %d server_intv_ym_put returns NULL???\n",
                __func__, __LINE__);
        return -1;
    }

    *outdtsz = sizeof(server_intv_ym_t);

    return 0;
}

int CLIENT_INTVDS_to_SERVER_UINT(C2S_FUNKY_ARGS)
{

    CINTV_TO_SVR(INTV_DS, UINT);
}

int CLIENT_INTVDSUS_to_SERVER_UINT(C2S_FUNKY_ARGS)
{

    CINTV_TO_SVR(INTV_DSUS, UINT);
}

int CLIENT_INTVDS_to_SERVER_BINT(C2S_FUNKY_ARGS)
{

    CINTV_TO_SVR(INTV_DS, BINT);
}

int CLIENT_INTVDSUS_to_SERVER_BINT(C2S_FUNKY_ARGS)
{

    CINTV_TO_SVR(INTV_DSUS, BINT);
}

int CLIENT_INTVDS_to_SERVER_BREAL(C2S_FUNKY_ARGS)
{

    CINTV_TO_SVR(INTV_DS, BREAL);
}

int CLIENT_INTVDSUS_to_SERVER_BREAL(C2S_FUNKY_ARGS)
{

    CINTV_TO_SVR(INTV_DSUS, BREAL);
}

int CLIENT_INTVDS_to_SERVER_BCSTR(C2S_FUNKY_ARGS) { CINTV_TO_SVR_STR(INTV_DS); }

int CLIENT_INTVDSUS_to_SERVER_BCSTR(C2S_FUNKY_ARGS)
{

    CINTV_TO_SVR_STR(INTV_DSUS);
}

#define CLIENT_INTVDST_to_SERVER_INTVDST_func_body(                            \
    from_dt, FROM_UDT, from_prec, from_frac, from_fracdt, to_dt, TO_UDT,       \
    to_prec, to_frac, to_fracdt)                                               \
    CLT_TP(from_dt) cin;                                                       \
    SRV_TP(to_dt) cout;                                                        \
                                                                               \
    /* pointers to raw buffers */                                              \
    uint8_t *p_buf_in = (uint8_t *)in;                                         \
    uint8_t *p_buf_in_end = (uint8_t *)in + inlen;                             \
    uint8_t *p_buf_out = (uint8_t *)out;                                       \
    uint8_t *p_buf_out_end = (uint8_t *)out + outlen;                          \
                                                                               \
    long long sec = 0;                                                         \
    from_fracdt from_frac = 0;                                                 \
                                                                               \
    if (isnull) {                                                              \
        set_null(out, outlen);                                                 \
        return 0;                                                              \
    }                                                                          \
                                                                               \
    if (inopts && inopts->flags & FLD_CONV_LENDIAN) {                          \
        if (!(client_##from_dt##_little_get(&cin, p_buf_in, p_buf_in_end))) {  \
            logmsg(LOGMSG_ERROR, "%s: line %d client_" #from_dt                \
                            "_little_get returns NULL???\n",                   \
                    __func__, __LINE__);                                       \
            return -1;                                                         \
        }                                                                      \
    } else {                                                                   \
        if (!(client_##from_dt##_get(&cin, p_buf_in, p_buf_in_end))) {         \
            logmsg(LOGMSG_ERROR, "%s: line %d client_" #from_dt "_get returns NULL???\n",   \
                    __func__, __LINE__);                                       \
            return -1;                                                         \
        }                                                                      \
    }                                                                          \
                                                                               \
    bzero(&cout, sizeof(cout));                                                \
                                                                               \
    bset(&cout.flag, data_bit);                                                \
                                                                               \
    /* client to server while normalizing */                                   \
    from_frac = cin.from_frac % IPOW10(from_prec);                             \
    sec = cin.sign * (long long)(cin.from_frac / IPOW10(from_prec) + cin.sec + \
                                 cin.mins * 60 + cin.hours * 3600 +            \
                                 cin.days * 24 * 3600);                        \
                                                                               \
    if (cin.sign == -1 && cin.from_frac) {                                     \
        sec--;                                                                 \
    }                                                                          \
                                                                               \
    int8_to_int8b(sec, (int8b *)&sec);                                         \
    cout.sec = sec;                                                            \
    cout.to_frac = from_frac * IPOW10(to_prec) / IPOW10(from_prec);            \
                                                                               \
    if (!(server_##to_dt##_put(&cout, p_buf_out, p_buf_out_end))) {            \
        logmsg(LOGMSG_ERROR, "%s: line %d server_" #to_dt "_put returns NULL???\n", \
                __func__, __LINE__);                                           \
        return -1;                                                             \
    }                                                                          \
                                                                               \
    *outdtsz = sizeof(SRV_TP(to_dt));                                          \
                                                                               \
    return 0;                                                                  \
/* END OF CLIENT_INTVDST_to_SERVER_INTVDST */

int CLIENT_INTVDS_to_SERVER_INTVDS(C2S_FUNKY_ARGS)
{
    CLIENT_INTVDST_to_SERVER_INTVDST_func_body(
        intv_ds, INTVDS, 3, msec, unsigned short, /* from */
        intv_ds, INTVDS, 3, msec, unsigned short /* to   */);
}

int CLIENT_INTVDS_to_SERVER_INTVDSUS(C2S_FUNKY_ARGS)
{
    if (gbl_forbid_datetime_promotion)
        return -1;
    CLIENT_INTVDST_to_SERVER_INTVDST_func_body(
        intv_ds, INTVDS, 3, msec, unsigned short, /* from */
        intv_dsus, INTVDSUS, 6, usec, unsigned int /* to   */);
}

int CLIENT_INTVDSUS_to_SERVER_INTVDS(C2S_FUNKY_ARGS)
{
    if (gbl_forbid_datetime_truncation)
        return -1;
    CLIENT_INTVDST_to_SERVER_INTVDST_func_body(
        intv_dsus, INTVDSUS, 6, usec, unsigned int, /* from */
        intv_ds, INTVDS, 3, msec, unsigned short /* to   */);
}

int CLIENT_INTVDSUS_to_SERVER_INTVDSUS(C2S_FUNKY_ARGS)
{
    CLIENT_INTVDST_to_SERVER_INTVDST_func_body(
        intv_dsus, INTVDSUS, 6, usec, unsigned int, /* from */
        intv_dsus, INTVDSUS, 6, usec, unsigned int /* to   */);
}

/* server to server */
#define SERVER_BINT_to_SERVER_VUTF8 SERVER_to_SERVER_NO_CONV
#define SERVER_BLOB_to_SERVER_INTVDS SERVER_to_SERVER_NO_CONV
#define SERVER_BLOB_to_SERVER_INTVDSUS SERVER_to_SERVER_NO_CONV
#define SERVER_BLOB_to_SERVER_INTVYM SERVER_to_SERVER_NO_CONV
#define SERVER_BLOB_to_SERVER_VUTF8 SERVER_to_SERVER_NO_CONV
#define SERVER_BLOB2_to_SERVER_INTVDS SERVER_to_SERVER_NO_CONV
#define SERVER_BLOB2_to_SERVER_INTVDSUS SERVER_to_SERVER_NO_CONV
#define SERVER_BLOB2_to_SERVER_INTVYM SERVER_to_SERVER_NO_CONV
#define SERVER_BLOB2_to_SERVER_VUTF8 SERVER_to_SERVER_NO_CONV
#define SERVER_BREAL_to_SERVER_VUTF8 SERVER_to_SERVER_NO_CONV
#define SERVER_BYTEARRAY_to_SERVER_INTVDS SERVER_to_SERVER_NO_CONV
#define SERVER_BYTEARRAY_to_SERVER_INTVDSUS SERVER_to_SERVER_NO_CONV
#define SERVER_BYTEARRAY_to_SERVER_INTVYM SERVER_to_SERVER_NO_CONV
#define SERVER_BYTEARRAY_to_SERVER_VUTF8 SERVER_to_SERVER_NO_CONV
#define SERVER_DATETIME_to_SERVER_INTVDS SERVER_to_SERVER_NO_CONV
#define SERVER_DATETIME_to_SERVER_INTVDSUS SERVER_to_SERVER_NO_CONV
#define SERVER_DATETIME_to_SERVER_INTVYM SERVER_to_SERVER_NO_CONV
#define SERVER_DATETIME_to_SERVER_VUTF8 SERVER_to_SERVER_NO_CONV
#define SERVER_DATETIMEUS_to_SERVER_INTVDS SERVER_to_SERVER_NO_CONV
#define SERVER_DATETIMEUS_to_SERVER_INTVDSUS SERVER_to_SERVER_NO_CONV
#define SERVER_DATETIMEUS_to_SERVER_INTVYM SERVER_to_SERVER_NO_CONV
#define SERVER_DATETIMEUS_to_SERVER_VUTF8 SERVER_to_SERVER_NO_CONV
#define SERVER_INTVDS_to_SERVER_BLOB SERVER_to_SERVER_NO_CONV
#define SERVER_INTVDS_to_SERVER_BYTEARRAY SERVER_to_SERVER_NO_CONV
#define SERVER_INTVDS_to_SERVER_DATETIME SERVER_to_SERVER_NO_CONV
#define SERVER_INTVDS_to_SERVER_DATETIMEUS SERVER_to_SERVER_NO_CONV
#define SERVER_INTVDS_to_SERVER_INTVYM SERVER_to_SERVER_NO_CONV
#define SERVER_INTVDS_to_SERVER_VUTF8 SERVER_to_SERVER_NO_CONV
#define SERVER_INTVDSUS_to_SERVER_BLOB SERVER_to_SERVER_NO_CONV
#define SERVER_INTVDSUS_to_SERVER_BYTEARRAY SERVER_to_SERVER_NO_CONV
#define SERVER_INTVDSUS_to_SERVER_DATETIME SERVER_to_SERVER_NO_CONV
#define SERVER_INTVDSUS_to_SERVER_DATETIMEUS SERVER_to_SERVER_NO_CONV
#define SERVER_INTVDSUS_to_SERVER_INTVYM SERVER_to_SERVER_NO_CONV
#define SERVER_INTVDSUS_to_SERVER_VUTF8 SERVER_to_SERVER_NO_CONV
#define SERVER_INTVYM_to_SERVER_BLOB SERVER_to_SERVER_NO_CONV
#define SERVER_INTVYM_to_SERVER_BYTEARRAY SERVER_to_SERVER_NO_CONV
#define SERVER_INTVYM_to_SERVER_DATETIME SERVER_to_SERVER_NO_CONV
#define SERVER_INTVYM_to_SERVER_DATETIMEUS SERVER_to_SERVER_NO_CONV
#define SERVER_INTVYM_to_SERVER_INTVDS SERVER_to_SERVER_NO_CONV
#define SERVER_INTVYM_to_SERVER_INTVDSUS SERVER_to_SERVER_NO_CONV
#define SERVER_INTVYM_to_SERVER_VUTF8 SERVER_to_SERVER_NO_CONV
#define SERVER_UINT_to_SERVER_VUTF8 SERVER_to_SERVER_NO_CONV
#define SERVER_VUTF8_to_SERVER_BINT SERVER_to_SERVER_NO_CONV
#define SERVER_VUTF8_to_SERVER_BLOB SERVER_to_SERVER_NO_CONV
#define SERVER_VUTF8_to_SERVER_BREAL SERVER_to_SERVER_NO_CONV
#define SERVER_VUTF8_to_SERVER_BYTEARRAY SERVER_to_SERVER_NO_CONV
#define SERVER_VUTF8_to_SERVER_DATETIME SERVER_to_SERVER_NO_CONV
#define SERVER_VUTF8_to_SERVER_DATETIMEUS SERVER_to_SERVER_NO_CONV
#define SERVER_VUTF8_to_SERVER_INTVDS SERVER_to_SERVER_NO_CONV
#define SERVER_VUTF8_to_SERVER_INTVDSUS SERVER_to_SERVER_NO_CONV
#define SERVER_VUTF8_to_SERVER_INTVYM SERVER_to_SERVER_NO_CONV
#define SERVER_VUTF8_to_SERVER_UINT SERVER_to_SERVER_NO_CONV

#define SERVER_DECIMAL_to_SERVER_BCSTR SERVER_to_SERVER_NO_CONV
#define SERVER_DECIMAL_to_SERVER_BINT SERVER_to_SERVER_NO_CONV
#define SERVER_DECIMAL_to_SERVER_BLOB SERVER_to_SERVER_NO_CONV
#define SERVER_DECIMAL_to_SERVER_BREAL SERVER_to_SERVER_NO_CONV
#define SERVER_DECIMAL_to_SERVER_BYTEARRAY SERVER_to_SERVER_NO_CONV
#define SERVER_DECIMAL_to_SERVER_DATETIME SERVER_to_SERVER_NO_CONV
#define SERVER_DECIMAL_to_SERVER_DATETIMEUS SERVER_to_SERVER_NO_CONV
#define SERVER_DECIMAL_to_SERVER_INTVDS SERVER_to_SERVER_NO_CONV
#define SERVER_DECIMAL_to_SERVER_INTVDSUS SERVER_to_SERVER_NO_CONV
#define SERVER_DECIMAL_to_SERVER_INTVYM SERVER_to_SERVER_NO_CONV
#define SERVER_DECIMAL_to_SERVER_UINT SERVER_to_SERVER_NO_CONV
#define SERVER_DECIMAL_to_SERVER_VUTF8 SERVER_to_SERVER_NO_CONV

#define SERVER_UINT_to_SERVER_DECIMAL SERVER_to_SERVER_NO_CONV
#define SERVER_BINT_to_SERVER_DECIMAL SERVER_to_SERVER_NO_CONV
#define SERVER_BREAL_to_SERVER_DECIMAL SERVER_to_SERVER_NO_CONV
#define SERVER_BCSTR_to_SERVER_DECIMAL SERVER_to_SERVER_NO_CONV
#define SERVER_BYTEARRAY_to_SERVER_DECIMAL SERVER_to_SERVER_NO_CONV
#define SERVER_BLOB_to_SERVER_DECIMAL SERVER_to_SERVER_NO_CONV
#define SERVER_BLOB2_to_SERVER_DECIMAL SERVER_to_SERVER_NO_CONV
#define SERVER_DATETIME_to_SERVER_DECIMAL SERVER_to_SERVER_NO_CONV
#define SERVER_DATETIMEUS_to_SERVER_DECIMAL SERVER_to_SERVER_NO_CONV
#define SERVER_INTVYM_to_SERVER_DECIMAL SERVER_to_SERVER_NO_CONV
#define SERVER_INTVDS_to_SERVER_DECIMAL SERVER_to_SERVER_NO_CONV
#define SERVER_INTVDSUS_to_SERVER_DECIMAL SERVER_to_SERVER_NO_CONV
#define SERVER_VUTF8_to_SERVER_DECIMAL SERVER_to_SERVER_NO_CONV

#define _SRV_TYPE_TO_CLT_TYPE(size, sfrom, cfrom)                              \
    void *tmpbuf;                                                              \
    int isnull;                                                                \
    int rc;                                                                    \
    if (stype_is_null(in)) {                                                   \
        set_null(out, outlen);                                                 \
        return 0;                                                              \
    }                                                                          \
    tmpbuf = alloca(inlen);                                                    \
    *(char *)out = 0;                                                          \
    bset(out, data_bit);                                                       \
                                                                               \
    rc = SERVER_##sfrom##_to_CLIENT_##cfrom(in, inlen, inopts, inblob, tmpbuf, \
                                            size, &isnull, outdtsz, NULL,      \
                                            NULL);                             \
    if (rc)                                                                    \
        return -1;

int SERVER_DECIMAL_to_SERVER_DECIMAL(S2S_FUNKY_ARGS)
{
    decQuad dn;
    int rc;
    int outnull;

    if (inlen != outlen)
        return -1;

    if (stype_is_null(in)) {
        memcpy(out, in, outlen);
        return 0;
    }

    rc = decimal_ondisk_to_sqlite(in, inlen, &dn, &outnull);
    if (rc)
        return rc;

    rc = sqlite_to_decimal_ondisk(&dn, out, outlen);

    return rc;
}

int SERVER_UINT_to_SERVER_INTVYM(S2S_FUNKY_ARGS)
{

    _SRV_TYPE_TO_CLT_TYPE(sizeof(long long), UINT, INT);

    /*flag propagation*/
    *(char *)out = *(char *)in;
    *outdtsz = sizeof(server_intv_ym_t);

    return _int2intv_srv(*(long long *)tmpbuf, INTV_YM, out);
}

int SERVER_UINT_to_SERVER_INTVDS(S2S_FUNKY_ARGS)
{

    _SRV_TYPE_TO_CLT_TYPE(sizeof(long long), UINT, INT);

    /*flag propagation*/
    *(char *)out = *(char *)in;
    *outdtsz = sizeof(server_intv_ds_t);

    return _int2intv_srv(*(long long *)tmpbuf, INTV_DS, out);
}

int SERVER_UINT_to_SERVER_INTVDSUS(S2S_FUNKY_ARGS)
{

    _SRV_TYPE_TO_CLT_TYPE(sizeof(long long), UINT, INT);

    /*flag propagation*/
    *(char *)out = *(char *)in;
    *outdtsz = sizeof(server_intv_dsus_t);

    return _int2intv_srv(*(long long *)tmpbuf, INTV_DSUS, out);
}

int SERVER_BINT_to_SERVER_INTVYM(S2S_FUNKY_ARGS)
{

    _SRV_TYPE_TO_CLT_TYPE(sizeof(long long), BINT, INT);

    /*flag propagation*/
    *(char *)out = *(char *)in;
    *outdtsz = sizeof(server_intv_ym_t);

    return _int2intv_srv(*(long long *)tmpbuf, INTV_YM, out);
}

int SERVER_BINT_to_SERVER_INTVDS(S2S_FUNKY_ARGS)
{

    _SRV_TYPE_TO_CLT_TYPE(sizeof(long long), BINT, INT);

    /*flag propagation*/
    *(char *)out = *(char *)in;
    *outdtsz = sizeof(server_intv_ds_t);

    return _int2intv_srv(*(long long *)tmpbuf, INTV_DS, out);
}

int SERVER_BINT_to_SERVER_INTVDSUS(S2S_FUNKY_ARGS)
{

    _SRV_TYPE_TO_CLT_TYPE(sizeof(long long), BINT, INT);

    /*flag propagation*/
    *(char *)out = *(char *)in;
    *outdtsz = sizeof(server_intv_dsus_t);

    return _int2intv_srv(*(long long *)tmpbuf, INTV_DSUS, out);
}

int SERVER_BREAL_to_SERVER_INTVYM(S2S_FUNKY_ARGS)
{

    _SRV_TYPE_TO_CLT_TYPE(sizeof(double), BREAL, REAL);

    /*flag propagation*/
    *(char *)out = *(char *)in;
    *outdtsz = sizeof(server_intv_ym_t);

    return _real2intv_srv(*(double *)tmpbuf, INTV_YM, out);
}

int SERVER_BREAL_to_SERVER_INTVDS(S2S_FUNKY_ARGS)
{

    _SRV_TYPE_TO_CLT_TYPE(sizeof(double), BREAL, REAL);

    /*flag propagation*/
    *(char *)out = *(char *)in;
    *outdtsz = sizeof(server_intv_ds_t);

    return _real2intv_srv(*(double *)tmpbuf, INTV_DS, out);
}

int SERVER_BREAL_to_SERVER_INTVDSUS(S2S_FUNKY_ARGS)
{

    _SRV_TYPE_TO_CLT_TYPE(sizeof(double), BREAL, REAL);

    /*flag propagation*/
    *(char *)out = *(char *)in;
    *outdtsz = sizeof(server_intv_dsus_t);

    return _real2intv_srv(*(double *)tmpbuf, INTV_DSUS, out);
}

int SERVER_BCSTR_to_SERVER_INTVYM(S2S_FUNKY_ARGS)
{

    if (stype_is_null(in)) {
        set_null(out, outlen);
        return 0;
    }

    /*flag propagation*/
    *(char *)out = *(char *)in;
    *outdtsz = sizeof(server_intv_ym_t);

    return _string2intv_srv((char *)in + 1, INTV_YM, out);
}

int SERVER_BCSTR_to_SERVER_INTVDS(S2S_FUNKY_ARGS)
{
    if (stype_is_null(in)) {
        set_null(out, outlen);
        return 0;
    }

    /*flag propagation*/
    *(char *)out = *(char *)in;
    *outdtsz = sizeof(server_intv_ds_t);

    return _string2intv_srv((char *)in + 1, INTV_DS, out);
}

int SERVER_BCSTR_to_SERVER_INTVDSUS(S2S_FUNKY_ARGS)
{
    if (stype_is_null(in)) {
        set_null(out, outlen);
        return 0;
    }

    /*flag propagation*/
    *(char *)out = *(char *)in;
    *outdtsz = sizeof(server_intv_dsus_t);

    return _string2intv_srv((char *)in + 1, INTV_DSUS, out);
}

#define _INTVSRV_to_SVR_TYPE(intype, totype)                                   \
    int rc;                                                                    \
    long long tmp;                                                             \
                                                                               \
    if (stype_is_null(in)) {                                                   \
        set_null(out, outlen);                                                 \
        return 0;                                                              \
    }                                                                          \
                                                                               \
    *(char *)out = *(char *)in;                                                \
    rc = _intv_srv2int(in, intype, &tmp);                                      \
    if (rc)                                                                    \
        return rc;                                                             \
                                                                               \
    return CLIENT_INT_to_SERVER_##totype(&tmp, sizeof(tmp), 0, NULL, NULL,     \
                                         out, outlen, outdtsz, outopts,        \
                                         outblob);

int SERVER_INTVYM_to_SERVER_UINT(S2S_FUNKY_ARGS)
{

    _INTVSRV_to_SVR_TYPE(INTV_YM, UINT);
}

int SERVER_INTVYM_to_SERVER_BINT(S2S_FUNKY_ARGS)
{

    _INTVSRV_to_SVR_TYPE(INTV_YM, BINT);
}

int SERVER_INTVYM_to_SERVER_BREAL(S2S_FUNKY_ARGS)
{

    _INTVSRV_to_SVR_TYPE(INTV_YM, BREAL);
}

int SERVER_INTVYM_to_SERVER_BCSTR(S2S_FUNKY_ARGS)
{

    int rc = 0;
    if (stype_is_null(in)) {
        set_null(out, outlen);
        return 0;
    }

    *(char *)out = *(char *)in;

    rc = _intv_srv2string(in, INTV_YM, (char *)out + 1, outlen - 1);
    if (rc)
        return rc;

    *outdtsz = strlen((char *)out + 1) + 2;
    return 0;
}

static TYPES_INLINE int SERVER_INTVYM_to_SERVER_INTVYM(S2S_FUNKY_ARGS)
{

    /* paranoia */
    if (inlen != outlen)
        return -1;

    memcpy(out, in, outlen);
    *outdtsz = outlen;

    return 0;
}

int SERVER_INTVDS_to_SERVER_UINT(S2S_FUNKY_ARGS)
{

    _INTVSRV_to_SVR_TYPE(INTV_DS, UINT);
}

int SERVER_INTVDSUS_to_SERVER_UINT(S2S_FUNKY_ARGS)
{

    _INTVSRV_to_SVR_TYPE(INTV_DSUS, UINT);
}

int SERVER_INTVDS_to_SERVER_BINT(S2S_FUNKY_ARGS)
{

    _INTVSRV_to_SVR_TYPE(INTV_DS, BINT);
}

int SERVER_INTVDSUS_to_SERVER_BINT(S2S_FUNKY_ARGS)
{

    _INTVSRV_to_SVR_TYPE(INTV_DSUS, BINT);
}

int SERVER_INTVDS_to_SERVER_BREAL(S2S_FUNKY_ARGS)
{

    _INTVSRV_to_SVR_TYPE(INTV_DS, BREAL);
}

int SERVER_INTVDSUS_to_SERVER_BREAL(S2S_FUNKY_ARGS)
{

    _INTVSRV_to_SVR_TYPE(INTV_DSUS, BREAL);
}

int SERVER_INTVDS_to_SERVER_BCSTR(S2S_FUNKY_ARGS)
{

    int rc = 0;

    if (stype_is_null(in)) {
        set_null(out, outlen);
        return 0;
    }

    *(char *)out = *(char *)in;

    rc = _intv_srv2string(in, INTV_DS, (char *)out + 1, outlen - 1);
    if (rc)
        return rc;

    *outdtsz = strlen((char *)out + 1) + 2;

    return 0;
}

int SERVER_INTVDSUS_to_SERVER_BCSTR(S2S_FUNKY_ARGS)
{

    int rc = 0;

    if (stype_is_null(in)) {
        set_null(out, outlen);
        return 0;
    }

    *(char *)out = *(char *)in;

    rc = _intv_srv2string(in, INTV_DSUS, (char *)out + 1, outlen - 1);
    if (rc)
        return rc;

    *outdtsz = strlen((char *)out + 1) + 2;

    return 0;
}

static TYPES_INLINE int SERVER_INTVDS_to_SERVER_INTVDS(S2S_FUNKY_ARGS)
{

    /* paranoia */
    if (inlen != outlen)
        return -1;

    memcpy(out, in, outlen);
    *outdtsz = outlen;

    return 0;
}

static TYPES_INLINE int SERVER_INTVDSUS_to_SERVER_INTVDSUS(S2S_FUNKY_ARGS)
{
    return SERVER_INTVDS_to_SERVER_INTVDS(in, inlen, inopts, inblob, out,
                                          outlen, outdtsz, outopts, outblob);
}

#define SERVER_INTVDST_to_SERVER_INTVDST_func_body(                            \
    from_dt, FROM_UDT, from_prec, from_frac, from_fracdt, to_dt, TO_UDT,       \
    to_prec, to_frac, to_fracdt)                                               \
    SRV_TP(from_dt) sintv;                                                     \
    SRV_TP(to_dt) tmp;                                                         \
    const uint8_t *p_in = in;                                                  \
    const uint8_t *p_in_end = (const uint8_t *)in + inlen;                     \
    uint8_t *p_out = out;                                                      \
    const uint8_t *p_out_start = out;                                          \
    const uint8_t *p_out_end = (const uint8_t *)out + outlen;                  \
                                                                               \
    /* null? */                                                                \
    if (stype_is_null(in)) {                                                   \
        set_null(out, outlen);                                                 \
        return 0;                                                              \
    }                                                                          \
                                                                               \
    /* unpack */                                                               \
    if (!server_##from_dt##_get(&sintv, p_in, p_in_end))                       \
        return -1;                                                             \
    tmp.flag = sintv.flag;                                                     \
    tmp.sec = sintv.sec;                                                       \
    tmp.to_frac = sintv.from_frac * POW10(to_prec) / POW10(from_prec);         \
                                                                               \
    if (!(p_out = server_##to_dt##_put(&tmp, p_out, p_out_end))) {             \
        return -1;                                                             \
    }                                                                          \
                                                                               \
    *outdtsz = p_out - p_out_start;                                            \
    return 0;                                                                  \
/* END OF SERVER_INTVDST_to_SERVER_INTVDST_func_body */

static TYPES_INLINE int SERVER_INTVDS_to_SERVER_INTVDSUS(S2S_FUNKY_ARGS)
{
    SERVER_INTVDST_to_SERVER_INTVDST_func_body(
        intv_ds, INTVDS, 3, msec, unsigned short, /* from */
        intv_dsus, INTVDSUS, 6, usec, unsigned int /* to   */);
}

static TYPES_INLINE int SERVER_INTVDSUS_to_SERVER_INTVDS(S2S_FUNKY_ARGS)
{
    SERVER_INTVDST_to_SERVER_INTVDST_func_body(
        intv_dsus, INTVDSUS, 6, usec, unsigned int, /* from */
        intv_ds, INTVDS, 3, msec, unsigned short /* to   */);
}
/*FIXME TODO XXX*/

static TYPES_INLINE int SERVER_BCSTR_to_SERVER_VUTF8(S2S_FUNKY_ARGS)
{
    char *cstr;
    int isnull;
    int rc;
    if (stype_is_null(in)) {
        set_null(out, outlen);
        return 0;
    }
    cstr = alloca(inlen + 1);
    rc = SERVER_BCSTR_to_CLIENT_CSTR(in, inlen, inopts, inblob, cstr, inlen + 1,
                                     &isnull, outdtsz, outopts, outblob);
    if (rc == -1)
        return rc;
    return CLIENT_CSTR_to_SERVER_VUTF8(cstr, (*outdtsz) + 1, isnull, inopts,
                                       inblob, out, outlen, outdtsz, outopts,
                                       outblob);
}

int (*client_to_client_convert_map[CLIENT_MAXTYPE][CLIENT_MAXTYPE])(
    const void *, int, const struct field_conv_opts *, blob_buffer_t *, void *,
    int, int *, const struct field_conv_opts *, blob_buffer_t *) = {
    /* FROM */ /* TO */
               /*                     CLIENT_MINTYPE  CLIENT_UINT
                  CLIENT_INT                      CLIENT_REAL
                  CLIENT_CSTR                      CLIENT_PSTR
                  CLIENT_BYTEARRAY                      CLIENT_PSTR2
                  CLIENT_BLOB                       CLIENT_DATETIME
                  INTERVAL_YM                        INTERVAL_DS
                  CLIENT_VUTF8                                          CLIENT_BLOB2
                  CLIENT_DATETIMEUS                          INTERVAL_DSUS */
    /* CLIENT_MINTYPE */ {NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                          NULL, NULL, NULL, NULL, NULL, NULL, NULL},
    /* CLIENT_UINT */ {NULL, CLIENT_UINT_to_CLIENT_UINT,
                       CLIENT_UINT_to_CLIENT_INT, CLIENT_UINT_to_CLIENT_REAL,
                       CLIENT_UINT_to_CLIENT_CSTR, CLIENT_UINT_to_CLIENT_PSTR,
                       CLIENT_UINT_to_CLIENT_BYTEARRAY,
                       CLIENT_UINT_to_CLIENT_PSTR2, CLIENT_UINT_to_CLIENT_BLOB,
                       CLIENT_UINT_to_CLIENT_DATETIME,
                       CLIENT_UINT_to_CLIENT_INTVYM,
                       CLIENT_UINT_to_CLIENT_INTVDS,
                       CLIENT_UINT_to_CLIENT_VUTF8, NULL,
                       CLIENT_UINT_to_CLIENT_DATETIMEUS,
                       CLIENT_UINT_to_CLIENT_INTVDSUS},
    /* CLIENT_INT */ {NULL, CLIENT_INT_to_CLIENT_UINT, CLIENT_INT_to_CLIENT_INT,
                      CLIENT_INT_to_CLIENT_REAL, CLIENT_INT_to_CLIENT_CSTR,
                      CLIENT_INT_to_CLIENT_PSTR, CLIENT_INT_to_CLIENT_BYTEARRAY,
                      CLIENT_INT_to_CLIENT_PSTR2, CLIENT_INT_to_CLIENT_BLOB,
                      CLIENT_INT_to_CLIENT_DATETIME,
                      CLIENT_INT_to_CLIENT_INTVYM, CLIENT_INT_to_CLIENT_INTVDS,
                      CLIENT_INT_to_CLIENT_VUTF8, NULL,
                      CLIENT_INT_to_CLIENT_DATETIMEUS,
                      CLIENT_INT_to_CLIENT_INTVDSUS},
    /* CLIENT_REAL */ {NULL, CLIENT_REAL_to_CLIENT_UINT,
                       CLIENT_REAL_to_CLIENT_INT, CLIENT_REAL_to_CLIENT_REAL,
                       CLIENT_REAL_to_CLIENT_CSTR, CLIENT_REAL_to_CLIENT_PSTR,
                       CLIENT_REAL_to_CLIENT_BYTEARRAY,
                       CLIENT_REAL_to_CLIENT_PSTR2, CLIENT_REAL_to_CLIENT_BLOB,
                       CLIENT_REAL_to_CLIENT_DATETIME,
                       CLIENT_REAL_to_CLIENT_INTVYM,
                       CLIENT_REAL_to_CLIENT_INTVDS,
                       CLIENT_REAL_to_CLIENT_VUTF8, NULL,
                       CLIENT_REAL_to_CLIENT_DATETIMEUS,
                       CLIENT_REAL_to_CLIENT_INTVDSUS},
    /* CLIENT_CSTR */ {NULL, CLIENT_CSTR_to_CLIENT_UINT,
                       CLIENT_CSTR_to_CLIENT_INT, CLIENT_CSTR_to_CLIENT_REAL,
                       CLIENT_CSTR_to_CLIENT_CSTR, CLIENT_CSTR_to_CLIENT_PSTR,
                       CLIENT_CSTR_to_CLIENT_BYTEARRAY,
                       CLIENT_CSTR_to_CLIENT_PSTR2, CLIENT_CSTR_to_CLIENT_BLOB,
                       CLIENT_CSTR_to_CLIENT_DATETIME,
                       CLIENT_CSTR_to_CLIENT_INTVYM,
                       CLIENT_CSTR_to_CLIENT_INTVDS,
                       CLIENT_CSTR_to_CLIENT_VUTF8, NULL,
                       CLIENT_CSTR_to_CLIENT_DATETIMEUS,
                       CLIENT_CSTR_to_CLIENT_INTVDSUS},
    /* CLIENT_PSTR */ {NULL, CLIENT_PSTR_to_CLIENT_UINT,
                       CLIENT_PSTR_to_CLIENT_INT, CLIENT_PSTR_to_CLIENT_REAL,
                       CLIENT_PSTR_to_CLIENT_CSTR, CLIENT_PSTR_to_CLIENT_PSTR,
                       CLIENT_PSTR_to_CLIENT_BYTEARRAY,
                       CLIENT_PSTR_to_CLIENT_PSTR2, CLIENT_PSTR_to_CLIENT_BLOB,
                       CLIENT_PSTR_to_CLIENT_DATETIME,
                       CLIENT_PSTR_to_CLIENT_INTVYM,
                       CLIENT_PSTR_to_CLIENT_INTVDS,
                       CLIENT_PSTR_to_CLIENT_VUTF8, NULL,
                       CLIENT_PSTR_to_CLIENT_DATETIMEUS,
                       CLIENT_PSTR_to_CLIENT_INTVDSUS},
    /* CLIENT_BYTEARRAY */ {NULL, CLIENT_BYTEARRAY_to_CLIENT_UINT,
                            CLIENT_BYTEARRAY_to_CLIENT_INT,
                            CLIENT_BYTEARRAY_to_CLIENT_REAL,
                            CLIENT_BYTEARRAY_to_CLIENT_CSTR,
                            CLIENT_BYTEARRAY_to_CLIENT_PSTR,
                            CLIENT_BYTEARRAY_to_CLIENT_BYTEARRAY,
                            CLIENT_BYTEARRAY_to_CLIENT_PSTR2,
                            CLIENT_BYTEARRAY_to_CLIENT_BLOB,
                            CLIENT_BYTEARRAY_to_CLIENT_DATETIME,
                            CLIENT_BYTEARRAY_to_CLIENT_INTVYM,
                            CLIENT_BYTEARRAY_to_CLIENT_INTDS,
                            CLIENT_BYTEARRAY_to_CLIENT_VUTF8, NULL,
                            CLIENT_BYTEARRAY_to_CLIENT_DATETIMEUS,
                            CLIENT_BYTEARRAY_to_CLIENT_INTDSUS},
    /* CLIENT_PSTR2   */ {NULL, CLIENT_PSTR2_to_CLIENT_UINT,
                          CLIENT_PSTR2_to_CLIENT_INT,
                          CLIENT_PSTR2_to_CLIENT_REAL,
                          CLIENT_PSTR2_to_CLIENT_CSTR,
                          CLIENT_PSTR2_to_CLIENT_PSTR,
                          CLIENT_PSTR2_to_CLIENT_BYTEARRAY,
                          CLIENT_PSTR2_to_CLIENT_PSTR2,
                          CLIENT_PSTR2_to_CLIENT_BLOB,
                          CLIENT_PSTR2_to_CLIENT_DATETIME,
                          CLIENT_PSTR2_to_CLIENT_INTVYM,
                          CLIENT_PSTR2_to_CLIENT_INTVDS,
                          CLIENT_PSTR2_to_CLIENT_VUTF8, NULL,
                          CLIENT_PSTR2_to_CLIENT_DATETIMEUS,
                          CLIENT_PSTR2_to_CLIENT_INTVDSUS},
    /* CLIENT_BLOB */ {NULL, CLIENT_BLOB_to_CLIENT_UINT,
                       CLIENT_BLOB_to_CLIENT_INT, CLIENT_BLOB_to_CLIENT_REAL,
                       CLIENT_BLOB_to_CLIENT_CSTR, CLIENT_BLOB_to_CLIENT_PSTR,
                       CLIENT_BLOB_to_CLIENT_BYTEARRAY,
                       CLIENT_BLOB_to_CLIENT_PSTR2, CLIENT_BLOB_to_CLIENT_BLOB,
                       CLIENT_BLOB_to_CLIENT_DATETIME,
                       CLIENT_BLOB_to_CLIENT_INTVYM,
                       CLIENT_BLOB_to_CLIENT_INTVDS,
                       CLIENT_BLOB_to_CLIENT_VUTF8, NULL,
                       CLIENT_BLOB_to_CLIENT_DATETIMEUS,
                       CLIENT_BLOB_to_CLIENT_INTVDSUS},
    /* CLIENT_DATETIME */ {NULL, CLIENT_DATETIME_to_CLIENT_UINT,
                           CLIENT_DATETIME_to_CLIENT_INT,
                           CLIENT_DATETIME_to_CLIENT_REAL,
                           CLIENT_DATETIME_to_CLIENT_CSTR,
                           CLIENT_DATETIME_to_CLIENT_PSTR,
                           CLIENT_DATETIME_to_CLIENT_BYTEARRAY,
                           CLIENT_DATETIME_to_CLIENT_PSTR2,
                           CLIENT_DATETIME_to_CLIENT_BLOB,
                           CLIENT_DATETIME_to_CLIENT_DATETIME,
                           CLIENT_DATETIME_to_CLIENT_INTVYM,
                           CLIENT_DATETIME_to_CLIENT_INTVDS,
                           CLIENT_DATETIME_to_CLIENT_VUTF8, NULL,
                           CLIENT_DATETIME_to_CLIENT_DATETIMEUS,
                           CLIENT_DATETIME_to_CLIENT_INTVDSUS},
    /* CLIENT_INTVYM */ {NULL, CLIENT_INTVYM_to_CLIENT_UINT,
                         CLIENT_INTVYM_to_CLIENT_INT,
                         CLIENT_INTVYM_to_CLIENT_REAL,
                         CLIENT_INTVYM_to_CLIENT_CSTR,
                         CLIENT_INTVYM_to_CLIENT_PSTR,
                         CLIENT_INTVYM_to_CLIENT_BYTEARRAY,
                         CLIENT_INTVYM_to_CLIENT_PSTR2,
                         CLIENT_INTVYM_to_CLIENT_BLOB,
                         CLIENT_INTVYM_to_CLIENT_DATETIME,
                         CLIENT_INTVYM_to_CLIENT_INTVYM,
                         CLIENT_INTVYM_to_CLIENT_INTVDS,
                         CLIENT_INTVYM_to_CLIENT_VUTF8, NULL,
                         CLIENT_INTVYM_to_CLIENT_DATETIMEUS,
                         CLIENT_INTVYM_to_CLIENT_INTVDSUS},
    /* CLIENT_INTVDS */ {NULL, CLIENT_INTVDS_to_CLIENT_UINT,
                         CLIENT_INTVDS_to_CLIENT_INT,
                         CLIENT_INTVDS_to_CLIENT_REAL,
                         CLIENT_INTVDS_to_CLIENT_CSTR,
                         CLIENT_INTVDS_to_CLIENT_PSTR,
                         CLIENT_INTVDS_to_CLIENT_BYTEARRAY,
                         CLIENT_INTVDS_to_CLIENT_PSTR2,
                         CLIENT_INTVDS_to_CLIENT_BLOB,
                         CLIENT_INTVDS_to_CLIENT_DATETIME,
                         CLIENT_INTVDS_to_CLIENT_INTVYM,
                         CLIENT_INTVDS_to_CLIENT_INTVDS,
                         CLIENT_INTVDS_to_CLIENT_VUTF8, NULL,
                         CLIENT_INTVDS_to_CLIENT_DATETIMEUS,
                         CLIENT_INTVDS_to_CLIENT_INTVDSUS},
    /* CLIENT_VUTF8 */ {NULL, CLIENT_VUTF8_to_CLIENT_UINT,
                        CLIENT_VUTF8_to_CLIENT_INT, CLIENT_VUTF8_to_CLIENT_REAL,
                        CLIENT_VUTF8_to_CLIENT_CSTR,
                        CLIENT_VUTF8_to_CLIENT_PSTR,
                        CLIENT_VUTF8_to_CLIENT_BYTEARRAY,
                        CLIENT_VUTF8_to_CLIENT_PSTR2,
                        CLIENT_VUTF8_to_CLIENT_BLOB,
                        CLIENT_VUTF8_to_CLIENT_DATETIME,
                        CLIENT_VUTF8_to_CLIENT_INTVYM,
                        CLIENT_VUTF8_to_CLIENT_INTVDS,
                        CLIENT_VUTF8_to_CLIENT_VUTF8, NULL,
                        CLIENT_VUTF8_to_CLIENT_DATETIMEUS,
                        CLIENT_VUTF8_to_CLIENT_INTVDSUS},
    /* CLIENT_BLOB2 */ {NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                        NULL, NULL, NULL, NULL, NULL, NULL, NULL},
    /* CLIENT_DATETIMEUS */ {NULL, CLIENT_DATETIMEUS_to_CLIENT_UINT,
                             CLIENT_DATETIMEUS_to_CLIENT_INT,
                             CLIENT_DATETIMEUS_to_CLIENT_REAL,
                             CLIENT_DATETIMEUS_to_CLIENT_CSTR,
                             CLIENT_DATETIMEUS_to_CLIENT_PSTR,
                             CLIENT_DATETIMEUS_to_CLIENT_BYTEARRAY,
                             CLIENT_DATETIMEUS_to_CLIENT_PSTR2,
                             CLIENT_DATETIMEUS_to_CLIENT_BLOB,
                             CLIENT_DATETIMEUS_to_CLIENT_DATETIME,
                             CLIENT_DATETIMEUS_to_CLIENT_INTVYM,
                             CLIENT_DATETIMEUS_to_CLIENT_INTVDS,
                             CLIENT_DATETIMEUS_to_CLIENT_VUTF8, NULL,
                             CLIENT_DATETIMEUS_to_CLIENT_DATETIMEUS,
                             CLIENT_DATETIMEUS_to_CLIENT_INTVDSUS},
    /* CLIENT_INTVDS */ {NULL, CLIENT_INTVDSUS_to_CLIENT_UINT,
                         CLIENT_INTVDSUS_to_CLIENT_INT,
                         CLIENT_INTVDSUS_to_CLIENT_REAL,
                         CLIENT_INTVDSUS_to_CLIENT_CSTR,
                         CLIENT_INTVDSUS_to_CLIENT_PSTR,
                         CLIENT_INTVDSUS_to_CLIENT_BYTEARRAY,
                         CLIENT_INTVDSUS_to_CLIENT_PSTR2,
                         CLIENT_INTVDSUS_to_CLIENT_BLOB,
                         CLIENT_INTVDSUS_to_CLIENT_DATETIME,
                         CLIENT_INTVDSUS_to_CLIENT_INTVYM,
                         CLIENT_INTVDSUS_to_CLIENT_INTVDS,
                         CLIENT_INTVDSUS_to_CLIENT_VUTF8, NULL,
                         CLIENT_INTVDSUS_to_CLIENT_DATETIMEUS,
                         CLIENT_INTVDSUS_to_CLIENT_INTVDSUS},
};
TYPES_INLINE int CLIENT_to_CLIENT(const void *in, int inlen, int intype,
                                  const struct field_conv_opts *inopts,
                                  blob_buffer_t *inblob, void *out, int outlen,
                                  int outtype, int *outdtsz,
                                  const struct field_conv_opts *outopts,
                                  blob_buffer_t *outblob)
{
    if (intype <= CLIENT_MINTYPE || intype >= CLIENT_MAXTYPE)
        return -1;

    if (outtype <= CLIENT_MINTYPE || outtype >= CLIENT_MAXTYPE)
        return -1;

    if (client_to_client_convert_map[intype][outtype] == NULL)
        return -1;

    return client_to_client_convert_map[intype][outtype](
        in, inlen, inopts, inblob, out, outlen, outdtsz, outopts, outblob);
}

int (*server_to_client_convert_map[SERVER_MAXTYPE][CLIENT_MAXTYPE])(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outnull, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob) = {
    /* FROM */ /* TO */
               /* CLIENT_MINTYPE                CLIENT_UINT
                  CLIENT_INT                      CLIENT_REAL
                  CLIENT_CSTR                      CLIENT_PSTR
                  CLIENT_BYTEARRAY                      CLIENT_PSTR2
                  CLIENT_BLOB                       CLIENT_DATETIME
                  CLIENT_INTVYM                      CLIENT_INTVDS
                  SERVER_VUTF8                                         CLIENT_BLOB2
                  CLIENT_DATETIMEUS                            CLIENT_INTVDSUS */
    /* SERVER_MINTYPE */ {NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                          NULL, NULL, NULL, NULL, NULL, NULL, NULL},
    /* SERVER_UINT */ {NULL, SERVER_UINT_to_CLIENT_UINT,
                       SERVER_UINT_to_CLIENT_INT, SERVER_UINT_to_CLIENT_REAL,
                       SERVER_UINT_to_CLIENT_CSTR, SERVER_UINT_to_CLIENT_PSTR,
                       SERVER_UINT_to_CLIENT_BYTEARRAY,
                       SERVER_UINT_to_CLIENT_PSTR2, SERVER_UINT_to_CLIENT_BLOB,
                       SERVER_UINT_to_CLIENT_DATETIME,
                       SERVER_UINT_to_CLIENT_INTVYM,
                       SERVER_UINT_to_CLIENT_INTVDS,
                       SERVER_UINT_to_CLIENT_VUTF8, NULL,
                       SERVER_UINT_to_CLIENT_DATETIMEUS,
                       SERVER_UINT_to_CLIENT_INTVDSUS},
    /* SERVER_BINT */ {NULL, SERVER_BINT_to_CLIENT_UINT,
                       SERVER_BINT_to_CLIENT_INT, SERVER_BINT_to_CLIENT_REAL,
                       SERVER_BINT_to_CLIENT_CSTR, SERVER_BINT_to_CLIENT_PSTR,
                       SERVER_BINT_to_CLIENT_BYTEARRAY,
                       SERVER_BINT_to_CLIENT_PSTR2, SERVER_BINT_to_CLIENT_BLOB,
                       SERVER_BINT_to_CLIENT_DATETIME,
                       SERVER_BINT_to_CLIENT_INTVYM,
                       SERVER_BINT_to_CLIENT_INTVDS,
                       SERVER_BINT_to_CLIENT_VUTF8, NULL,
                       SERVER_BINT_to_CLIENT_DATETIMEUS,
                       SERVER_BINT_to_CLIENT_INTVDSUS},
    /* SERVER_BREAL */ {NULL, SERVER_BREAL_to_CLIENT_UINT,
                        SERVER_BREAL_to_CLIENT_INT, SERVER_BREAL_to_CLIENT_REAL,
                        SERVER_BREAL_to_CLIENT_CSTR,
                        SERVER_BREAL_to_CLIENT_PSTR,
                        SERVER_BREAL_to_CLIENT_BYTEARRAY,
                        SERVER_BREAL_to_CLIENT_PSTR2,
                        SERVER_BREAL_to_CLIENT_BLOB,
                        SERVER_BREAL_to_CLIENT_DATETIME,
                        SERVER_BREAL_to_CLIENT_INTVYM,
                        SERVER_BREAL_to_CLIENT_INTVDS,
                        SERVER_BREAL_to_CLIENT_VUTF8, NULL,
                        SERVER_BREAL_to_CLIENT_DATETIMEUS,
                        SERVER_BREAL_to_CLIENT_INTVDSUS},
    /* SERVER_BCSTR */ {NULL, SERVER_BCSTR_to_CLIENT_UINT,
                        SERVER_BCSTR_to_CLIENT_INT, SERVER_BCSTR_to_CLIENT_REAL,
                        SERVER_BCSTR_to_CLIENT_CSTR,
                        SERVER_BCSTR_to_CLIENT_PSTR,
                        SERVER_BCSTR_to_CLIENT_BYTEARRAY,
                        SERVER_BCSTR_to_CLIENT_PSTR2,
                        SERVER_BCSTR_to_CLIENT_BLOB,
                        SERVER_BCSTR_to_CLIENT_DATETIME,
                        SERVER_BCSTR_to_CLIENT_INTVYM,
                        SERVER_BCSTR_to_CLIENT_INTVDS,
                        SERVER_BCSTR_to_CLIENT_VUTF8, NULL,
                        SERVER_BCSTR_to_CLIENT_DATETIMEUS,
                        SERVER_BCSTR_to_CLIENT_INTVDSUS},
    /* SERVER_BYTEARRAY */ {NULL, SERVER_BYTEARRAY_to_CLIENT_UINT,
                            SERVER_BYTEARRAY_to_CLIENT_INT,
                            SERVER_BYTEARRAY_to_CLIENT_REAL,
                            SERVER_BYTEARRAY_to_CLIENT_CSTR,
                            SERVER_BYTEARRAY_to_CLIENT_PSTR,
                            SERVER_BYTEARRAY_to_CLIENT_BYTEARRAY,
                            SERVER_BYTEARRAY_to_CLIENT_PSTR2,
                            SERVER_BYTEARRAY_to_CLIENT_BLOB,
                            SERVER_BYTEARRAY_to_CLIENT_DATETIME,
                            SERVER_BYTEARRAY_to_CLIENT_INTVYM,
                            SERVER_BYTEARRAY_to_CLIENT_INTVDS,
                            SERVER_BYTEARRAY_to_CLIENT_VUTF8, NULL,
                            SERVER_BYTEARRAY_to_CLIENT_DATETIMEUS,
                            SERVER_BYTEARRAY_to_CLIENT_INTVDSUS},
    /* SERVER_BLOB */ {NULL, SERVER_BLOB_to_CLIENT_UINT,
                       SERVER_BLOB_to_CLIENT_INT, SERVER_BLOB_to_CLIENT_REAL,
                       SERVER_BLOB_to_CLIENT_CSTR, SERVER_BLOB_to_CLIENT_PSTR,
                       SERVER_BLOB_to_CLIENT_BYTEARRAY,
                       SERVER_BLOB_to_CLIENT_PSTR2, SERVER_BLOB_to_CLIENT_BLOB,
                       SERVER_BLOB_to_CLIENT_DATETIME,
                       SERVER_BLOB_to_CLIENT_INTVYM,
                       SERVER_BLOB_to_CLIENT_INTVDS,
                       SERVER_BLOB_to_CLIENT_VUTF8, NULL,
                       SERVER_BLOB_to_CLIENT_DATETIMEUS,
                       SERVER_BLOB_to_CLIENT_INTVDSUS},
    /* SERVER_DATETIME*/ {NULL, SERVER_DATETIME_to_CLIENT_UINT,
                          SERVER_DATETIME_to_CLIENT_INT,
                          SERVER_DATETIME_to_CLIENT_REAL,
                          SERVER_DATETIME_to_CLIENT_CSTR,
                          SERVER_DATETIME_to_CLIENT_PSTR,
                          SERVER_DATETIME_to_CLIENT_BYTEARRAY,
                          SERVER_DATETIME_to_CLIENT_PSTR2,
                          SERVER_DATETIME_to_CLIENT_BLOB,
                          SERVER_DATETIME_to_CLIENT_DATETIME,
                          SERVER_DATETIME_to_CLIENT_INTVYM,
                          SERVER_DATETIME_to_CLIENT_INTVDS,
                          SERVER_DATETIME_to_CLIENT_VUTF8, NULL,
                          SERVER_DATETIME_to_CLIENT_DATETIMEUS,
                          SERVER_DATETIME_to_CLIENT_INTVDSUS},
    /* SERVER_INTVYM */ {NULL, SERVER_INTVYM_to_CLIENT_UINT,
                         SERVER_INTVYM_to_CLIENT_INT,
                         SERVER_INTVYM_to_CLIENT_REAL,
                         SERVER_INTVYM_to_CLIENT_CSTR,
                         SERVER_INTVYM_to_CLIENT_PSTR,
                         SERVER_INTVYM_to_CLIENT_BYTEARRAY,
                         SERVER_INTVYM_to_CLIENT_PSTR2,
                         SERVER_INTVYM_to_CLIENT_BLOB,
                         SERVER_INTVYM_to_CLIENT_DATETIME,
                         SERVER_INTVYM_to_CLIENT_INTVYM,
                         SERVER_INTVYM_to_CLIENT_INTVDS,
                         SERVER_INTVYM_to_CLIENT_VUTF8, NULL,
                         SERVER_INTVYM_to_CLIENT_DATETIMEUS,
                         SERVER_INTVYM_to_CLIENT_INTVDSUS},
    /* SERVER_INTVDS */ {NULL, SERVER_INTVDS_to_CLIENT_UINT,
                         SERVER_INTVDS_to_CLIENT_INT,
                         SERVER_INTVDS_to_CLIENT_REAL,
                         SERVER_INTVDS_to_CLIENT_CSTR,
                         SERVER_INTVDS_to_CLIENT_PSTR,
                         SERVER_INTVDS_to_CLIENT_BYTEARRAY,
                         SERVER_INTVDS_to_CLIENT_PSTR2,
                         SERVER_INTVDS_to_CLIENT_BLOB,
                         SERVER_INTVDS_to_CLIENT_DATETIME,
                         SERVER_INTVDS_to_CLIENT_INTVYM,
                         SERVER_INTVDS_to_CLIENT_INTVDS,
                         SERVER_INTVDS_to_CLIENT_VUTF8, NULL,
                         SERVER_INTVDS_to_CLIENT_DATETIMEUS,
                         SERVER_INTVDS_to_CLIENT_INTVDSUS},
    /* SERVER_VUTF8 */ {NULL, SERVER_VUTF8_to_CLIENT_UINT,
                        SERVER_VUTF8_to_CLIENT_INT, SERVER_VUTF8_to_CLIENT_REAL,
                        SERVER_VUTF8_to_CLIENT_CSTR,
                        SERVER_VUTF8_to_CLIENT_PSTR,
                        SERVER_VUTF8_to_CLIENT_BYTEARRAY,
                        SERVER_VUTF8_to_CLIENT_PSTR2,
                        SERVER_VUTF8_to_CLIENT_BLOB,
                        SERVER_VUTF8_to_CLIENT_DATETIME,
                        SERVER_VUTF8_to_CLIENT_INTVYM,
                        SERVER_VUTF8_to_CLIENT_INTVDS,
                        SERVER_VUTF8_to_CLIENT_VUTF8, NULL,
                        SERVER_VUTF8_to_CLIENT_DATETIMEUS,
                        SERVER_VUTF8_to_CLIENT_INTVDSUS},
    /* SERVER_DECIMAL */ {NULL, SERVER_DECIMAL_to_CLIENT_UINT,
                          SERVER_DECIMAL_to_CLIENT_INT,
                          SERVER_DECIMAL_to_CLIENT_REAL,
                          SERVER_DECIMAL_to_CLIENT_CSTR,
                          SERVER_DECIMAL_to_CLIENT_PSTR,
                          SERVER_DECIMAL_to_CLIENT_BYTEARRAY,
                          SERVER_DECIMAL_to_CLIENT_PSTR2,
                          SERVER_DECIMAL_to_CLIENT_BLOB,
                          SERVER_DECIMAL_to_CLIENT_DATETIME,
                          SERVER_DECIMAL_to_CLIENT_INTVYM,
                          SERVER_DECIMAL_to_CLIENT_INTVDS,
                          SERVER_DECIMAL_to_CLIENT_VUTF8, NULL,
                          SERVER_DECIMAL_to_CLIENT_DATETIMEUS,
                          SERVER_DECIMAL_to_CLIENT_INTVDSUS},
    /* SERVER_BLOB2 */ {NULL, SERVER_BLOB2_to_CLIENT_UINT,
                        SERVER_BLOB2_to_CLIENT_INT, SERVER_BLOB2_to_CLIENT_REAL,
                        SERVER_BLOB2_to_CLIENT_CSTR,
                        SERVER_BLOB2_to_CLIENT_PSTR,
                        SERVER_BLOB2_to_CLIENT_BYTEARRAY,
                        SERVER_BLOB2_to_CLIENT_PSTR2,
                        SERVER_BLOB2_to_CLIENT_BLOB,
                        SERVER_BLOB2_to_CLIENT_DATETIME,
                        SERVER_BLOB2_to_CLIENT_INTVYM,
                        SERVER_BLOB2_to_CLIENT_INTVDS,
                        SERVER_BLOB2_to_CLIENT_VUTF8, NULL,
                        SERVER_BLOB2_to_CLIENT_DATETIMEUS,
                        SERVER_BLOB2_to_CLIENT_INTVDSUS},
    /* SERVER_DATETIMEUS*/ {NULL, SERVER_DATETIMEUS_to_CLIENT_UINT,
                            SERVER_DATETIMEUS_to_CLIENT_INT,
                            SERVER_DATETIMEUS_to_CLIENT_REAL,
                            SERVER_DATETIMEUS_to_CLIENT_CSTR,
                            SERVER_DATETIMEUS_to_CLIENT_PSTR,
                            SERVER_DATETIMEUS_to_CLIENT_BYTEARRAY,
                            SERVER_DATETIMEUS_to_CLIENT_PSTR2,
                            SERVER_DATETIMEUS_to_CLIENT_BLOB,
                            SERVER_DATETIMEUS_to_CLIENT_DATETIME,
                            SERVER_DATETIMEUS_to_CLIENT_INTVYM,
                            SERVER_DATETIMEUS_to_CLIENT_INTVDS,
                            SERVER_DATETIMEUS_to_CLIENT_VUTF8, NULL,
                            SERVER_DATETIMEUS_to_CLIENT_DATETIMEUS,
                            SERVER_DATETIMEUS_to_CLIENT_INTVDSUS},
    /* SERVER_INTVDS */ {NULL, SERVER_INTVDSUS_to_CLIENT_UINT,
                         SERVER_INTVDSUS_to_CLIENT_INT,
                         SERVER_INTVDSUS_to_CLIENT_REAL,
                         SERVER_INTVDSUS_to_CLIENT_CSTR,
                         SERVER_INTVDSUS_to_CLIENT_PSTR,
                         SERVER_INTVDSUS_to_CLIENT_BYTEARRAY,
                         SERVER_INTVDSUS_to_CLIENT_PSTR2,
                         SERVER_INTVDSUS_to_CLIENT_BLOB,
                         SERVER_INTVDSUS_to_CLIENT_DATETIME,
                         SERVER_INTVDSUS_to_CLIENT_INTVYM,
                         SERVER_INTVDSUS_to_CLIENT_INTVDS,
                         SERVER_INTVDSUS_to_CLIENT_VUTF8, NULL,
                         SERVER_INTVDSUS_to_CLIENT_DATETIMEUS,
                         SERVER_INTVDSUS_to_CLIENT_INTVDSUS},
};
TYPES_INLINE int SERVER_to_CLIENT(const void *in, int inlen, int intype,
                                  const struct field_conv_opts *inopts,
                                  blob_buffer_t *inblob, int flags, void *out,
                                  int outlen, int outtype, int *isnull,
                                  int *outdtsz,
                                  const struct field_conv_opts *outopts,
                                  blob_buffer_t *outblob)
{
    int ridx;
    int rc;
    if (intype < SERVER_MINTYPE || intype > SERVER_MAXTYPE)
        return -1;

    if (outtype < CLIENT_MINTYPE || outtype > CLIENT_MAXTYPE)
        return -1;

    ridx = intype - SERVER_MINTYPE;
    rc = server_to_client_convert_map[ridx][outtype](in, inlen, inopts, inblob,
                                                     out, outlen, isnull,
                                                     outdtsz, outopts, outblob);

    return rc;
}

int (*client_to_server_convert_map[CLIENT_MAXTYPE][SERVER_MAXTYPE])(
    const void *in, int inlen, int isnull, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob) = {
    /* FROM */ /* TO */
               /*                       SERVER_MINTYPE SERVER_UINT
                  SERVER_BINT                      SERVER_BREAL
                  SERVER_BCSTR                      SERVER_BYTEARRAY
                  SERVER_BLOB                        SERVER_DATETIME
                  SERVER_INTVYM                      SERVER_INTVDS
                  SERVER_VUTF8                        SERVER_DECIMAL     SERVER_BLOB2
                  SERVER_DATETIMEUS                     SERVER_INTVDSUS */
    /* CLIENT_MINTYPE   */ {NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                            NULL, NULL, NULL, NULL, NULL, NULL},
    /* CLIENT_UINT      */ {NULL, CLIENT_UINT_to_SERVER_UINT,
                            CLIENT_UINT_to_SERVER_BINT,
                            CLIENT_UINT_to_SERVER_BREAL,
                            CLIENT_UINT_to_SERVER_BCSTR,
                            CLIENT_UINT_to_SERVER_BYTEARRAY,
                            CLIENT_UINT_to_SERVER_BLOB,
                            CLIENT_UINT_to_SERVER_DATETIME,
                            CLIENT_UINT_to_SERVER_INTVYM,
                            CLIENT_UINT_to_SERVER_INTVDS,
                            CLIENT_UINT_to_SERVER_VUTF8,
                            CLIENT_UINT_to_SERVER_DECIMAL,
                            CLIENT_UINT_to_SERVER_BLOB2,
                            CLIENT_UINT_to_SERVER_DATETIMEUS,
                            CLIENT_UINT_to_SERVER_INTVDSUS},
    /* CLIENT_INT       */ {NULL, CLIENT_INT_to_SERVER_UINT,
                            CLIENT_INT_to_SERVER_BINT,
                            CLIENT_INT_to_SERVER_BREAL,
                            CLIENT_INT_to_SERVER_BCSTR,
                            CLIENT_INT_to_SERVER_BYTEARRAY,
                            CLIENT_INT_to_SERVER_BLOB,
                            CLIENT_INT_to_SERVER_DATETIME,
                            CLIENT_INT_to_SERVER_INTVYM,
                            CLIENT_INT_to_SERVER_INTVDS,
                            CLIENT_INT_to_SERVER_VUTF8,
                            CLIENT_INT_to_SERVER_DECIMAL,
                            CLIENT_INT_to_SERVER_BLOB2,
                            CLIENT_INT_to_SERVER_DATETIMEUS,
                            CLIENT_INT_to_SERVER_INTVDSUS},
    /* CLIENT_REAL      */ {NULL, CLIENT_REAL_to_SERVER_UINT,
                            CLIENT_REAL_to_SERVER_BINT,
                            CLIENT_REAL_to_SERVER_BREAL,
                            CLIENT_REAL_to_SERVER_BCSTR,
                            CLIENT_REAL_to_SERVER_BYTEARRAY,
                            CLIENT_REAL_to_SERVER_BLOB,
                            CLIENT_REAL_to_SERVER_DATETIME,
                            CLIENT_REAL_to_SERVER_INTVYM,
                            CLIENT_REAL_to_SERVER_INTVDS,
                            CLIENT_REAL_to_SERVER_VUTF8,
                            CLIENT_REAL_to_SERVER_DECIMAL,
                            CLIENT_REAL_to_SERVER_BLOB2,
                            CLIENT_REAL_to_SERVER_DATETIMEUS,
                            CLIENT_REAL_to_SERVER_INTVDSUS},
    /* CLIENT_CSTR      */ {NULL, CLIENT_CSTR_to_SERVER_UINT,
                            CLIENT_CSTR_to_SERVER_BINT,
                            CLIENT_CSTR_to_SERVER_BREAL,
                            CLIENT_CSTR_to_SERVER_BCSTR,
                            CLIENT_CSTR_to_SERVER_BYTEARRAY,
                            CLIENT_CSTR_to_SERVER_BLOB,
                            CLIENT_CSTR_to_SERVER_DATETIME,
                            CLIENT_CSTR_to_SERVER_INTVYM,
                            CLIENT_CSTR_to_SERVER_INTVDS,
                            CLIENT_CSTR_to_SERVER_VUTF8,
                            CLIENT_CSTR_to_SERVER_DECIMAL,
                            CLIENT_CSTR_to_SERVER_BLOB2,
                            CLIENT_CSTR_to_SERVER_DATETIMEUS,
                            CLIENT_CSTR_to_SERVER_INTVDSUS},
    /* CLIENT_PSTR      */ {NULL, CLIENT_PSTR_to_SERVER_UINT,
                            CLIENT_PSTR_to_SERVER_BINT,
                            CLIENT_PSTR_to_SERVER_BREAL,
                            CLIENT_PSTR_to_SERVER_BCSTR,
                            CLIENT_PSTR_to_SERVER_BYTEARRAY,
                            CLIENT_PSTR_to_SERVER_BLOB,
                            CLIENT_PSTR_to_SERVER_DATETIME,
                            CLIENT_PSTR_to_SERVER_INTVYM,
                            CLIENT_PSTR_to_SERVER_INTVDS,
                            CLIENT_PSTR_to_SERVER_VUTF8,
                            CLIENT_PSTR_to_SERVER_DECIMAL,
                            CLIENT_PSTR_to_SERVER_BLOB2,
                            CLIENT_PSTR_to_SERVER_DATETIMEUS,
                            CLIENT_PSTR_to_SERVER_INTVDSUS},
    /* CLIENT_BYTEARRAY */ {NULL, CLIENT_BYTEARRAY_to_SERVER_UINT,
                            CLIENT_BYTEARRAY_to_SERVER_BINT,
                            CLIENT_BYTEARRAY_to_SERVER_BREAL,
                            CLIENT_BYTEARRAY_to_SERVER_BCSTR,
                            CLIENT_BYTEARRAY_to_SERVER_BYTEARRAY,
                            CLIENT_BYTEARRAY_to_SERVER_BLOB,
                            CLIENT_BYTEARRAY_to_SERVER_DATETIME,
                            CLIENT_BYTEARRAY_to_SERVER_INTVYM,
                            CLIENT_BYTEARRAY_to_SERVER_INTVDS,
                            CLIENT_BYTEARRAY_to_SERVER_VUTF8,
                            CLIENT_BYTEARRAY_to_SERVER_DECIMAL,
                            CLIENT_BYTEARRAY_to_SERVER_BLOB2,
                            CLIENT_BYTEARRAY_to_SERVER_DATETIMEUS,
                            CLIENT_BYTEARRAY_to_SERVER_INTVDSUS},
    /* CLIENT_PSTR2     */ {NULL, CLIENT_PSTR2_to_SERVER_UINT,
                            CLIENT_PSTR2_to_SERVER_BINT,
                            CLIENT_PSTR2_to_SERVER_BREAL,
                            CLIENT_PSTR2_to_SERVER_BCSTR,
                            CLIENT_PSTR2_to_SERVER_BYTEARRAY,
                            CLIENT_PSTR2_to_SERVER_BLOB,
                            CLIENT_PSTR2_to_SERVER_DATETIME,
                            CLIENT_PSTR2_to_SERVER_INTVYM,
                            CLIENT_PSTR2_to_SERVER_INTVDS,
                            CLIENT_PSTR2_to_SERVER_VUTF8,
                            CLIENT_PSTR2_to_SERVER_DECIMAL,
                            CLIENT_PSTR2_to_SERVER_BLOB2,
                            CLIENT_PSTR2_to_SERVER_DATETIMEUS,
                            CLIENT_PSTR2_to_SERVER_INTVDSUS},
    /* CLIENT_BLOB      */ {NULL, CLIENT_BLOB_to_SERVER_UINT,
                            CLIENT_BLOB_to_SERVER_BINT,
                            CLIENT_BLOB_to_SERVER_BREAL,
                            CLIENT_BLOB_to_SERVER_BCSTR,
                            CLIENT_BLOB_to_SERVER_BYTEARRAY,
                            CLIENT_BLOB_to_SERVER_BLOB,
                            CLIENT_BLOB_to_SERVER_DATETIME,
                            CLIENT_BLOB_to_SERVER_INTVYM,
                            CLIENT_BLOB_to_SERVER_INTVDS,
                            CLIENT_BLOB_to_SERVER_VUTF8,
                            CLIENT_BLOB_to_SERVER_DECIMAL,
                            CLIENT_BLOB_to_SERVER_BLOB2,
                            CLIENT_BLOB_to_SERVER_DATETIMEUS,
                            CLIENT_BLOB_to_SERVER_INTVDSUS},
    /* CLIENT_DATETIME  */ {NULL, CLIENT_DATETIME_to_SERVER_UINT,
                            CLIENT_DATETIME_to_SERVER_BINT,
                            CLIENT_DATETIME_to_SERVER_BREAL,
                            CLIENT_DATETIME_to_SERVER_BCSTR,
                            CLIENT_DATETIME_to_SERVER_BYTEARRAY,
                            CLIENT_DATETIME_to_SERVER_BLOB,
                            CLIENT_DATETIME_to_SERVER_DATETIME,
                            CLIENT_DATETIME_to_SERVER_INTVYM,
                            CLIENT_DATETIME_to_SERVER_INTVDS,
                            CLIENT_DATETIME_to_SERVER_VUTF8,
                            CLIENT_DATETIME_to_SERVER_DECIMAL,
                            CLIENT_DATETIME_to_SERVER_BLOB2,
                            CLIENT_DATETIME_to_SERVER_DATETIMEUS,
                            CLIENT_DATETIME_to_SERVER_INTVDSUS},
    /* CLIENT_INTVYM    */ {NULL, CLIENT_INTVYM_to_SERVER_UINT,
                            CLIENT_INTVYM_to_SERVER_BINT,
                            CLIENT_INTVYM_to_SERVER_BREAL,
                            CLIENT_INTVYM_to_SERVER_BCSTR,
                            CLIENT_INTVYM_to_SERVER_BYTEARRAY,
                            CLIENT_INTVYM_to_SERVER_BLOB,
                            CLIENT_INTVYM_to_SERVER_DATETIME,
                            CLIENT_INTVYM_to_SERVER_INTVYM,
                            CLIENT_INTVYM_to_SERVER_INTVDS,
                            CLIENT_INTVYM_to_SERVER_VUTF8,
                            CLIENT_INTVYM_to_SERVER_DECIMAL,
                            CLIENT_INTVYM_to_SERVER_BLOB2,
                            CLIENT_INTVYM_to_SERVER_DATETIMEUS,
                            CLIENT_INTVYM_to_SERVER_INTVDSUS},
    /* CLIENT_INTVDS    */ {NULL, CLIENT_INTVDS_to_SERVER_UINT,
                            CLIENT_INTVDS_to_SERVER_BINT,
                            CLIENT_INTVDS_to_SERVER_BREAL,
                            CLIENT_INTVDS_to_SERVER_BCSTR,
                            CLIENT_INTVDS_to_SERVER_BYTEARRAY,
                            CLIENT_INTVDS_to_SERVER_BLOB,
                            CLIENT_INTVDS_to_SERVER_DATETIME,
                            CLIENT_INTVDS_to_SERVER_INTVYM,
                            CLIENT_INTVDS_to_SERVER_INTVDS,
                            CLIENT_INTVDS_to_SERVER_VUTF8,
                            CLIENT_INTVDS_to_SERVER_DECIMAL,
                            CLIENT_INTVDS_to_SERVER_BLOB2,
                            CLIENT_INTVDS_to_SERVER_DATETIMEUS,
                            CLIENT_INTVDS_to_SERVER_INTVDSUS},
    /* CLIENT_VUTF8     */ {NULL, CLIENT_VUTF8_to_SERVER_UINT,
                            CLIENT_VUTF8_to_SERVER_BINT,
                            CLIENT_VUTF8_to_SERVER_BREAL,
                            CLIENT_VUTF8_to_SERVER_BCSTR,
                            CLIENT_VUTF8_to_SERVER_BYTEARRAY,
                            CLIENT_VUTF8_to_SERVER_BLOB,
                            CLIENT_VUTF8_to_SERVER_DATETIME,
                            CLIENT_VUTF8_to_SERVER_INTVYM,
                            CLIENT_VUTF8_to_SERVER_INTVDS,
                            CLIENT_VUTF8_to_SERVER_VUTF8,
                            CLIENT_VUTF8_to_SERVER_DECIMAL,
                            CLIENT_VUTF8_to_SERVER_BLOB2,
                            CLIENT_VUTF8_to_SERVER_DATETIMEUS,
                            CLIENT_VUTF8_to_SERVER_INTVDSUS},
    /* CLIENT_BLOB2   */ {NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                          NULL, NULL, NULL, NULL, NULL},
    /* CLIENT_DATETIMEUS */ {NULL, CLIENT_DATETIMEUS_to_SERVER_UINT,
                             CLIENT_DATETIMEUS_to_SERVER_BINT,
                             CLIENT_DATETIMEUS_to_SERVER_BREAL,
                             CLIENT_DATETIMEUS_to_SERVER_BCSTR,
                             CLIENT_DATETIMEUS_to_SERVER_BYTEARRAY,
                             CLIENT_DATETIMEUS_to_SERVER_BLOB,
                             CLIENT_DATETIMEUS_to_SERVER_DATETIME,
                             CLIENT_DATETIMEUS_to_SERVER_INTVYM,
                             CLIENT_DATETIMEUS_to_SERVER_INTVDS,
                             CLIENT_DATETIMEUS_to_SERVER_VUTF8,
                             CLIENT_DATETIMEUS_to_SERVER_DECIMAL,
                             CLIENT_DATETIMEUS_to_SERVER_BLOB2,
                             CLIENT_DATETIMEUS_to_SERVER_DATETIMEUS,
                             CLIENT_DATETIMEUS_to_SERVER_INTVDSUS},
    /* CLIENT_INTVDSUS  */ {NULL, CLIENT_INTVDSUS_to_SERVER_UINT,
                            CLIENT_INTVDSUS_to_SERVER_BINT,
                            CLIENT_INTVDSUS_to_SERVER_BREAL,
                            CLIENT_INTVDSUS_to_SERVER_BCSTR,
                            CLIENT_INTVDSUS_to_SERVER_BYTEARRAY,
                            CLIENT_INTVDSUS_to_SERVER_BLOB,
                            CLIENT_INTVDSUS_to_SERVER_DATETIME,
                            CLIENT_INTVDSUS_to_SERVER_INTVYM,
                            CLIENT_INTVDSUS_to_SERVER_INTVDS,
                            CLIENT_INTVDSUS_to_SERVER_VUTF8,
                            CLIENT_INTVDSUS_to_SERVER_DECIMAL,
                            CLIENT_INTVDSUS_to_SERVER_BLOB2,
                            CLIENT_INTVDSUS_to_SERVER_DATETIMEUS,
                            CLIENT_INTVDSUS_to_SERVER_INTVDSUS},
};
TYPES_INLINE int
CLIENT_to_SERVER(const void *in, int inlen, int intype, int isnull,
                 const struct field_conv_opts *inopts, blob_buffer_t *inblob,
                 void *out, int outlen, int outtype, int flags, int *outdtsz,
                 const struct field_conv_opts *outopts, blob_buffer_t *outblob)
{
    int ridx;
    int rc;
    if (intype < CLIENT_MINTYPE || intype > CLIENT_MAXTYPE)
        return -1;

    if (outtype < SERVER_MINTYPE || outtype > SERVER_MAXTYPE)
        return -1;

    if (isnull) {
        if (outblob) {
            /* This is critical for updates to work correctly.  We must mark
             * the blob as collected even if it doesn't exist (is null) so
             * that the update code will know to delete the old blob. */
            outblob->collected = 1;
        }
        set_null(out, outlen);
        return 0;
    }

    ridx = outtype - SERVER_MINTYPE;
    rc = client_to_server_convert_map[intype][ridx](in, inlen, isnull, inopts,
                                                    inblob, out, outlen,
                                                    outdtsz, outopts, outblob);
    return rc;
}

const int server_to_server_convert_tbl[SERVER_MAXTYPE][SERVER_MAXTYPE] = {
    /* this table will be used in constraint checks to see whether
       a field in a key is convertible to a field in referring table key, or
       not.  The actual data convertibility is not so important as it will fail
       to convert in later record conversion anyway, if the data isn't right.
       USING SAME TYPES AS BELOW HERE.  MUST UPDATE, IF UPDATING TABLE BELOW AS
       WELL */
    {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
    {0, 1, 1, 1, 1, 0, 0, 1, 1, 1, 0, 0, 0, 1, 1},
    {0, 1, 1, 1, 1, 0, 0, 1, 1, 1, 0, 0, 0, 1, 1},
    {0, 1, 1, 1, 1, 0, 0, 1, 1, 1, 0, 0, 0, 1, 1},
    {0, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
    {0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0},
    {0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0},
    {0, 1, 1, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0},
    {0, 1, 1, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0},
    {0, 1, 1, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1},
    {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0},
    {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
    {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
    {0, 1, 1, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0},
    {0, 1, 1, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1},
};

int (*server_to_server_convert_map[SERVER_MAXTYPE][SERVER_MAXTYPE])(
    const void *in, int inlen, const struct field_conv_opts *inopts,
    blob_buffer_t *inblob, void *out, int outlen, int *outdtsz,
    const struct field_conv_opts *outopts, blob_buffer_t *outblob) = {
    /* FROM */ /* TO */
               /*                       SERVER_MINTYPE    SERVER_UINT
                  SERVER_BINT                     SERVER_BREAL
                  SERVER_BCSTR                      SERVER_BYTEARRAY
                  SERVER_BLOB                             SERVER_DATETIME
                  SERVER_INVYM                       SERVER_INTVDS
                  SERVER_VUTF8                        SERVER_DECIMAL
                  SERVER_BLOB2            SERVER_INTVDSUS*/
    /* SERVER_MINTYPE   */ {NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                            NULL, NULL, NULL, NULL, NULL, NULL},
    /* SERVER_UINT      */ {NULL, SERVER_UINT_to_SERVER_UINT,
                            SERVER_UINT_to_SERVER_BINT,
                            SERVER_UINT_to_SERVER_BREAL,
                            SERVER_UINT_to_SERVER_BCSTR,
                            SERVER_UINT_to_SERVER_BYTEARRAY,
                            SERVER_UINT_to_SERVER_BLOB,
                            SERVER_UINT_to_SERVER_DATETIME,
                            SERVER_UINT_to_SERVER_INTVYM,
                            SERVER_UINT_to_SERVER_INTVDS,
                            SERVER_UINT_to_SERVER_VUTF8,
                            SERVER_UINT_to_SERVER_DECIMAL,
                            SERVER_UINT_to_SERVER_BLOB2,
                            SERVER_UINT_to_SERVER_DATETIMEUS,
                            SERVER_UINT_to_SERVER_INTVDSUS},
    /* SERVER_BINT      */ {NULL, SERVER_BINT_to_SERVER_UINT,
                            SERVER_BINT_to_SERVER_BINT,
                            SERVER_BINT_to_SERVER_BREAL,
                            SERVER_BINT_to_SERVER_BCSTR,
                            SERVER_BINT_to_SERVER_BYTEARRAY,
                            SERVER_BINT_to_SERVER_BLOB,
                            SERVER_BINT_to_SERVER_DATETIME,
                            SERVER_BINT_to_SERVER_INTVYM,
                            SERVER_BINT_to_SERVER_INTVDS,
                            SERVER_BINT_to_SERVER_VUTF8,
                            SERVER_BINT_to_SERVER_DECIMAL,
                            SERVER_BINT_to_SERVER_BLOB2,
                            SERVER_BINT_to_SERVER_DATETIMEUS,
                            SERVER_BINT_to_SERVER_INTVDSUS},
    /* SERVER_BREAL     */ {NULL, SERVER_BREAL_to_SERVER_UINT,
                            SERVER_BREAL_to_SERVER_BINT,
                            SERVER_BREAL_to_SERVER_BREAL,
                            SERVER_BREAL_to_SERVER_BCSTR,
                            SERVER_BREAL_to_SERVER_BYTEARRAY,
                            SERVER_BREAL_to_SERVER_BLOB,
                            SERVER_BREAL_to_SERVER_DATETIME,
                            SERVER_BREAL_to_SERVER_INTVYM,
                            SERVER_BREAL_to_SERVER_INTVDS,
                            SERVER_BREAL_to_SERVER_VUTF8,
                            SERVER_BREAL_to_SERVER_DECIMAL,
                            SERVER_BREAL_to_SERVER_BLOB2,
                            SERVER_BREAL_to_SERVER_DATETIMEUS,
                            SERVER_BREAL_to_SERVER_INTVDSUS},
    /* SERVER_BCSTR     */ {NULL, SERVER_BCSTR_to_SERVER_UINT,
                            SERVER_BCSTR_to_SERVER_BINT,
                            SERVER_BCSTR_to_SERVER_BREAL,
                            SERVER_BCSTR_to_SERVER_BCSTR,
                            SERVER_BCSTR_to_SERVER_BYTEARRAY,
                            SERVER_BCSTR_to_SERVER_BLOB,
                            SERVER_BCSTR_to_SERVER_DATETIME,
                            SERVER_BCSTR_to_SERVER_INTVYM,
                            SERVER_BCSTR_to_SERVER_INTVDS,
                            SERVER_BCSTR_to_SERVER_VUTF8,
                            SERVER_BCSTR_to_SERVER_DECIMAL,
                            SERVER_BCSTR_to_SERVER_BLOB2,
                            SERVER_BCSTR_to_SERVER_DATETIMEUS,
                            SERVER_BCSTR_to_SERVER_INTVDSUS},
    /* SERVER_BYTEARRAY */ {NULL, SERVER_BYTEARRAY_to_SERVER_UINT,
                            SERVER_BYTEARRAY_to_SERVER_BINT,
                            SERVER_BYTEARRAY_to_SERVER_BREAL,
                            SERVER_BYTEARRAY_to_SERVER_BCSTR,
                            SERVER_BYTEARRAY_to_SERVER_BYTEARRAY,
                            SERVER_BYTEARRAY_to_SERVER_BLOB,
                            SERVER_BYTEARRAY_to_SERVER_DATETIME,
                            SERVER_BYTEARRAY_to_SERVER_INTVYM,
                            SERVER_BYTEARRAY_to_SERVER_INTVDS,
                            SERVER_BYTEARRAY_to_SERVER_VUTF8,
                            SERVER_BYTEARRAY_to_SERVER_DECIMAL,
                            SERVER_BYTEARRAY_to_SERVER_BLOB2,
                            SERVER_BYTEARRAY_to_SERVER_DATETIMEUS,
                            SERVER_BYTEARRAY_to_SERVER_INTVDSUS},
    /* SERVER_BLOB      */ {NULL, SERVER_BLOB_to_SERVER_UINT,
                            SERVER_BLOB_to_SERVER_BINT,
                            SERVER_BLOB_to_SERVER_BREAL,
                            SERVER_BLOB_to_SERVER_BCSTR,
                            SERVER_BLOB_to_SERVER_BYTEARRAY,
                            SERVER_BLOB_to_SERVER_BLOB,
                            SERVER_BLOB_to_SERVER_DATETIME,
                            SERVER_BLOB_to_SERVER_INTVYM,
                            SERVER_BLOB_to_SERVER_INTVDS,
                            SERVER_BLOB_to_SERVER_VUTF8,
                            SERVER_BLOB_to_SERVER_DECIMAL,
                            SERVER_BLOB_to_SERVER_BLOB2,
                            SERVER_BLOB_to_SERVER_DATETIMEUS,
                            SERVER_BLOB_to_SERVER_INTVDSUS},
    /* SERVER_DATETIME  */ {NULL, SERVER_DATETIME_to_SERVER_UINT,
                            SERVER_DATETIME_to_SERVER_BINT,
                            SERVER_DATETIME_to_SERVER_BREAL,
                            SERVER_DATETIME_to_SERVER_BCSTR,
                            SERVER_DATETIME_to_SERVER_BYTEARRAY,
                            SERVER_DATETIME_to_SERVER_BLOB,
                            SERVER_DATETIME_to_SERVER_DATETIME,
                            SERVER_DATETIME_to_SERVER_INTVYM,
                            SERVER_DATETIME_to_SERVER_INTVDS,
                            SERVER_DATETIME_to_SERVER_VUTF8,
                            SERVER_DATETIME_to_SERVER_DECIMAL,
                            SERVER_DATETIME_to_SERVER_BLOB2,
                            SERVER_DATETIME_to_SERVER_DATETIMEUS,
                            SERVER_DATETIME_to_SERVER_INTVDSUS},
    /* SERVER_INTVYM    */ {NULL, SERVER_INTVYM_to_SERVER_UINT,
                            SERVER_INTVYM_to_SERVER_BINT,
                            SERVER_INTVYM_to_SERVER_BREAL,
                            SERVER_INTVYM_to_SERVER_BCSTR,
                            SERVER_INTVYM_to_SERVER_BYTEARRAY,
                            SERVER_INTVYM_to_SERVER_BLOB,
                            SERVER_INTVYM_to_SERVER_DATETIME,
                            SERVER_INTVYM_to_SERVER_INTVYM,
                            SERVER_INTVYM_to_SERVER_INTVDS,
                            SERVER_INTVYM_to_SERVER_VUTF8,
                            SERVER_INTVYM_to_SERVER_DECIMAL,
                            SERVER_INTVYM_to_SERVER_BLOB2,
                            SERVER_INTVYM_to_SERVER_DATETIMEUS,
                            SERVER_INTVYM_to_SERVER_INTVDSUS},
    /* SERVER_INTVDS    */ {NULL, SERVER_INTVDS_to_SERVER_UINT,
                            SERVER_INTVDS_to_SERVER_BINT,
                            SERVER_INTVDS_to_SERVER_BREAL,
                            SERVER_INTVDS_to_SERVER_BCSTR,
                            SERVER_INTVDS_to_SERVER_BYTEARRAY,
                            SERVER_INTVDS_to_SERVER_BLOB,
                            SERVER_INTVDS_to_SERVER_DATETIME,
                            SERVER_INTVDS_to_SERVER_INTVYM,
                            SERVER_INTVDS_to_SERVER_INTVDS,
                            SERVER_INTVDS_to_SERVER_VUTF8,
                            SERVER_INTVDS_to_SERVER_DECIMAL,
                            SERVER_INTVDS_to_SERVER_BLOB2,
                            SERVER_INTVDS_to_SERVER_DATETIMEUS,
                            SERVER_INTVDS_to_SERVER_INTVDSUS},
    /* SERVER_VUTF8     */ {NULL, SERVER_VUTF8_to_SERVER_UINT,
                            SERVER_VUTF8_to_SERVER_BINT,
                            SERVER_VUTF8_to_SERVER_BREAL,
                            SERVER_VUTF8_to_SERVER_BCSTR,
                            SERVER_VUTF8_to_SERVER_BYTEARRAY,
                            SERVER_VUTF8_to_SERVER_BLOB,
                            SERVER_VUTF8_to_SERVER_DATETIME,
                            SERVER_VUTF8_to_SERVER_INTVYM,
                            SERVER_VUTF8_to_SERVER_INTVDS,
                            SERVER_VUTF8_to_SERVER_VUTF8,
                            SERVER_VUTF8_to_SERVER_DECIMAL,
                            SERVER_VUTF8_to_SERVER_BLOB2,
                            SERVER_VUTF8_to_SERVER_DATETIMEUS,
                            SERVER_VUTF8_to_SERVER_INTVDSUS},
    /* SERVER_DECIMAL       */ {NULL, SERVER_DECIMAL_to_SERVER_UINT,
                                SERVER_DECIMAL_to_SERVER_BINT,
                                SERVER_DECIMAL_to_SERVER_BREAL,
                                SERVER_DECIMAL_to_SERVER_BCSTR,
                                SERVER_DECIMAL_to_SERVER_BYTEARRAY,
                                SERVER_DECIMAL_to_SERVER_BLOB,
                                SERVER_DECIMAL_to_SERVER_DATETIME,
                                SERVER_DECIMAL_to_SERVER_INTVYM,
                                SERVER_DECIMAL_to_SERVER_INTVDS,
                                SERVER_DECIMAL_to_SERVER_VUTF8,
                                SERVER_DECIMAL_to_SERVER_DECIMAL,
                                SERVER_DECIMAL_to_SERVER_BLOB2,
                                SERVER_DECIMAL_to_SERVER_DATETIMEUS,
                                SERVER_DECIMAL_to_SERVER_INTVDSUS},
    /* SERVER_BLOB2       */ {NULL, SERVER_BLOB2_to_SERVER_UINT,
                              SERVER_BLOB2_to_SERVER_BINT,
                              SERVER_BLOB2_to_SERVER_BREAL,
                              SERVER_BLOB2_to_SERVER_BCSTR,
                              SERVER_BLOB2_to_SERVER_BYTEARRAY,
                              SERVER_BLOB2_to_SERVER_BLOB,
                              SERVER_BLOB2_to_SERVER_DATETIME,
                              SERVER_BLOB2_to_SERVER_INTVYM,
                              SERVER_BLOB2_to_SERVER_INTVDS,
                              SERVER_BLOB2_to_SERVER_VUTF8,
                              SERVER_BLOB2_to_SERVER_DECIMAL,
                              SERVER_BLOB2_to_SERVER_BLOB2,
                              SERVER_BLOB2_to_SERVER_DATETIMEUS,
                              SERVER_BLOB_to_SERVER_INTVDSUS},
    /* SERVER_DATETIMEUS  */ {NULL, SERVER_DATETIMEUS_to_SERVER_UINT,
                              SERVER_DATETIMEUS_to_SERVER_BINT,
                              SERVER_DATETIMEUS_to_SERVER_BREAL,
                              SERVER_DATETIMEUS_to_SERVER_BCSTR,
                              SERVER_DATETIMEUS_to_SERVER_BYTEARRAY,
                              SERVER_DATETIMEUS_to_SERVER_BLOB,
                              SERVER_DATETIMEUS_to_SERVER_DATETIME,
                              SERVER_DATETIMEUS_to_SERVER_INTVYM,
                              SERVER_DATETIMEUS_to_SERVER_INTVDS,
                              SERVER_DATETIMEUS_to_SERVER_VUTF8,
                              SERVER_DATETIMEUS_to_SERVER_DECIMAL,
                              SERVER_DATETIMEUS_to_SERVER_BLOB2,
                              SERVER_DATETIMEUS_to_SERVER_DATETIMEUS,
                              SERVER_DATETIMEUS_to_SERVER_INTVDSUS},
    /* SERVER_INTVDSUS  */ {NULL, SERVER_INTVDSUS_to_SERVER_UINT,
                            SERVER_INTVDSUS_to_SERVER_BINT,
                            SERVER_INTVDSUS_to_SERVER_BREAL,
                            SERVER_INTVDSUS_to_SERVER_BCSTR,
                            SERVER_INTVDSUS_to_SERVER_BYTEARRAY,
                            SERVER_INTVDSUS_to_SERVER_BLOB,
                            SERVER_INTVDSUS_to_SERVER_DATETIME,
                            SERVER_INTVDSUS_to_SERVER_INTVYM,
                            SERVER_INTVDSUS_to_SERVER_INTVDS,
                            SERVER_INTVDSUS_to_SERVER_VUTF8,
                            SERVER_INTVDSUS_to_SERVER_DECIMAL,
                            SERVER_INTVDSUS_to_SERVER_BLOB2,
                            SERVER_INTVDSUS_to_SERVER_DATETIMEUS,
                            SERVER_INTVDSUS_to_SERVER_INTVDSUS},
};
TYPES_INLINE int SERVER_to_SERVER(const void *in, int inlen, int intype,
                                  const struct field_conv_opts *inopts,
                                  blob_buffer_t *inblob, int iflags, void *out,
                                  int outlen, int outtype, int oflags,
                                  int *outdtsz,
                                  const struct field_conv_opts *outopts,
                                  blob_buffer_t *outblob)
{
    int rc;
    if (intype < SERVER_MINTYPE || intype > SERVER_MAXTYPE)
        return -1;

    if (outtype < SERVER_MINTYPE || outtype > SERVER_MAXTYPE)
        return -1;

    rc = server_to_server_convert_map[intype - SERVER_MINTYPE]
                                     [outtype - SERVER_MINTYPE](
                                         in, inlen, inopts, inblob, out, outlen,
                                         outdtsz, outopts, outblob);
    return rc;
}

TYPES_INLINE int NULL_to_SERVER(void *out, int outlen, int outtype)
{
    set_null(out, outlen);
    return 0;
}

TYPES_INLINE int convertible_types(int intype, int outtype)
{
    int rc;
    if (intype < SERVER_MINTYPE || intype > SERVER_MAXTYPE)
        return -1;

    if (outtype < SERVER_MINTYPE || outtype > SERVER_MAXTYPE)
        return -1;

    return server_to_server_convert_tbl[intype - SERVER_MINTYPE]
                                       [outtype - SERVER_MINTYPE];
}

int dec_ctx_init(void *pctx, int type, int rounding)
{
    decContext *ctx = (decContext *)pctx;

    decContextDefault(ctx, type);
    if (rounding != DEC_ROUND_NONE)
        decContextSetRounding(ctx, rounding);

    return 0;
}

int dec_parse_rounding(char *str, int len)
{
    int i;
    const char *tok[] = {
        "DEC_ROUND_NONE",
        "DEC_ROUND_CEILING",   /* round towards +infinity */
        "DEC_ROUND_UP",        /* round away from 0               */
        "DEC_ROUND_HALF_UP",   /* 0.5 rounds up                   */
        "DEC_ROUND_HALF_EVEN", /* 0.5 rounds to nearest even      */
        "DEC_ROUND_HALF_DOWN", /* 0.5 rounds down                 */
        "DEC_ROUND_DOWN",      /* round towards 0 (truncate)      */
        "DEC_ROUND_FLOOR",     /* round towards -infinity         */
        "DEC_ROUND_05UP"       /* round for reround               */
    };
    int tokl[] = {14, 17, 12, 17, 19, 19, 14, 15, 14};
    int ret[] = {DEC_ROUND_NONE,    DEC_ROUND_CEILING,   DEC_ROUND_UP,
                 DEC_ROUND_HALF_UP, DEC_ROUND_HALF_EVEN, DEC_ROUND_HALF_DOWN,
                 DEC_ROUND_DOWN,    DEC_ROUND_FLOOR,     DEC_ROUND_05UP};

    for (i = 0; i < sizeof(tokl) / sizeof(tokl[0]); i++) {
        if (len == tokl[i] && !strncmp(str, tok[i], tokl[i]))
            return ret[i];
    }

    return ret[0];
}

const char *dec_print_mode(int mode)
{
    char *tok[] = {
        "DEC_ROUND_NONE",
        "DEC_ROUND_CEILING",   /* round towards +infinity */
        "DEC_ROUND_UP",        /* round away from 0               */
        "DEC_ROUND_HALF_UP",   /* 0.5 rounds up                   */
        "DEC_ROUND_HALF_EVEN", /* 0.5 rounds to nearest even      */
        "DEC_ROUND_HALF_DOWN", /* 0.5 rounds down                 */
        "DEC_ROUND_DOWN",      /* round towards 0 (truncate)      */
        "DEC_ROUND_FLOOR",     /* round towards -infinity         */
        "DEC_ROUND_05UP"       /* round for reround               */
    };

    if (mode < sizeof(tok) / sizeof(tok[0]))
        return tok[mode];

    return tok[0];
}

/* server default datetime precision */
int gbl_datetime_precision = DTTZ_PREC_MSEC;
/*
** Correctly set a interval days-seconds
** it receives positive values for sec and msec
*/
void _setIntervalDS(intv_ds_t *ds, long long sec, int msec)
{
    bzero(ds, sizeof(*ds));
    ds->conv = 1;
    ds->frac = msec % 1000;
    ds->sec = msec / 1000 + sec % 60;
    sec /= 60; /*in min*/
    ds->mins = ds->sec / 60 + sec % 60;
    ds->sec = ds->sec % 60;
    sec /= 60; /*in hours*/
    ds->hours = ds->mins / 60 + sec % 24;
    ds->mins = ds->mins % 60;
    sec /= 24; /*in days*/
    ds->days = ds->hours / 24 + sec;
    ds->hours = ds->hours % 24;
    ds->prec = DTTZ_PREC_MSEC;
}

/*
** Correctly set a interval days-seconds
** it receives positive values for sec and usec
*/
void _setIntervalDSUS(intv_ds_t *ds, long long sec, int usec)
{
    bzero(ds, sizeof(*ds));
    ds->conv = 1;
    ds->frac = usec % 1000000;
    ds->sec = usec / 1000000 + sec % 60;
    sec /= 60; /*in min*/
    ds->mins = ds->sec / 60 + sec % 60;
    ds->sec = ds->sec % 60;
    sec /= 60; /*in hours*/
    ds->hours = ds->mins / 60 + sec % 24;
    ds->mins = ds->mins % 60;
    sec /= 24; /*in days*/
    ds->days = ds->hours / 24 + sec;
    ds->hours = ds->hours % 24;
    ds->prec = DTTZ_PREC_USEC;
}

/* convert dttz object to server datetime */
#define dttz_to_server_dt_func_body(dt, UDT, prec, frac, fracdt)               \
    long long sec;                                                             \
    fracdt frac;                                                               \
    bzero(opts, sizeof(*opts));                                                \
    opts->flags |= 2 /*FLD_CONV_TZONE*/;                                       \
    strncpy(opts->tzname, tz, sizeof(opts->tzname));                           \
    if (datetime_check_range(in->dttz_sec, in->dttz_frac))                     \
        return -1;                                                             \
    /* brr, ugly */                                                            \
    tmpin[0] = 8; /* data_bit */                                               \
    sec = flibc_htonll(in->dttz_sec);                                          \
    if (in->dttz_prec == prec)                                                 \
        frac = in->dttz_frac;                                                  \
    else                                                                       \
        frac = in->dttz_frac * POW10(prec) /                                   \
               ((in->dttz_prec == DTTZ_PREC_MSEC) ? 1000 : 1000000);           \
    frac = (sizeof(fracdt) == sizeof(uint16_t)) ? htons(frac) : htonl(frac);   \
    memcpy(&tmpin[1], &sec, sizeof(long long));                                \
    tmpin[1] ^= 0x80; /* switch to int8b format */                             \
    memcpy(&tmpin[1 + sizeof(long long)], &frac, sizeof(fracdt));              \
    return 0;
/* END OF dttz_to_server_dt_func_body */

static int dttz_to_server_datetime(const dttz_t *in, const char *tz,
                                   uint8_t *tmpin,
                                   struct field_conv_opts_tz *opts)
{
    dttz_to_server_dt_func_body(datetime, DATETIME, 3, msec, unsigned short);
}

static int dttz_to_server_datetimeus(const dttz_t *in, const char *tz,
                                     uint8_t *tmpin,
                                     struct field_conv_opts_tz *opts)
{
    dttz_to_server_dt_func_body(datetimeus, DATETIMEUS, 6, usec, unsigned int);
}

int dttz_to_str(const dttz_t *in, char *out, int outlen, int *outdtsz,
                const char *tz)
{
    char *tmpIn;
    struct field_conv_opts_tz tzopts;
    int outnull;

    switch (in->dttz_prec) {
    case DTTZ_PREC_MSEC:
        tmpIn = alloca(SERVER_DATETIME_LEN);
        if (dttz_to_server_datetime(in, tz, (uint8_t *)tmpIn, &tzopts))
            return 1;
        if (SERVER_DATETIME_to_CLIENT_CSTR(
                tmpIn, SERVER_DATETIME_LEN, NULL, NULL, out, outlen, &outnull,
                outdtsz, (const struct field_conv_opts *)&tzopts, NULL)) {
            return 1;
        }
        break;
    case DTTZ_PREC_USEC:
        tmpIn = alloca(SERVER_DATETIMEUS_LEN);
        if (dttz_to_server_datetimeus(in, tz, (uint8_t *)tmpIn, &tzopts))
            return 1;
        if (SERVER_DATETIMEUS_to_CLIENT_CSTR(
                tmpIn, SERVER_DATETIMEUS_LEN, NULL, NULL, out, outlen, &outnull,
                outdtsz, (const struct field_conv_opts *)&tzopts, NULL)) {
            return 1;
        }
        break;
    default:
        return 1;
    }

    return 0;
}

int real_to_dttz(double d, dttz_t *dt, int precision)
{
    /* a real is alway convertible to any precision */
    dt->dttz_conv = 1;
    dt->dttz_sec = d;
    if ((long long)(d * 1000) == d * 1000) {
        dt->dttz_frac = (d - (long long)d) * 1E3 + 0.5;
        dt->dttz_prec = DTTZ_PREC_MSEC;
    } else {
        dt->dttz_frac = (d - (long long)d) * 1E6 + 0.5;
        dt->dttz_prec = DTTZ_PREC_USEC;
    }

    if (precision > dt->dttz_prec) {
        /* If user asked for high precision, promote it now. */
        dt->dttz_frac *= 1000;
        dt->dttz_prec = DTTZ_PREC_USEC;
    }

    if (datetime_check_range(dt->dttz_sec, dt->dttz_frac))
        return -1;

    return 0;
}

int int_to_dttz(int64_t i, dttz_t *dt, int precision)
{
    dt->dttz_prec = (precision == 0) ? gbl_datetime_precision : precision;
    /* an integer is alway convertible to any precision */
    dt->dttz_conv = 1;
    dt->dttz_sec = i;

    if (datetime_check_range(dt->dttz_sec, 0))
        return -1;

    /* converting an int to a datetime will have 0 msec */
    dt->dttz_frac = 0;
    return 0;
}

int str_to_dttz(const char *z, int n, const char *tz, dttz_t *dt, int precision)
{
    /* if the fraction has more than 6 digits, make a usec datetime.
       otherwise make a msec datetime. */
    if (!tz || !z)
        return 1;
    char *tmp;
    struct field_conv_opts_tz tzopts;
    int outdtsz;
    /* provide the timezone to the conversion routines */
    bzero(&tzopts, sizeof(tzopts));
    tzopts.flags |= 2 /*FLD_CONV_TZONE*/;
    strncpy(tzopts.tzname, tz, sizeof(tzopts.tzname));

    if (is_usec_dt(z, n)) {
        dt->dttz_prec = DTTZ_PREC_USEC;
        tmp = alloca(SERVER_DATETIMEUS_LEN);
        if (CLIENT_CSTR_to_SERVER_DATETIMEUS(
                z, n, 0, (struct field_conv_opts *)&tzopts, NULL, &tmp[0],
                SERVER_DATETIMEUS_LEN, &outdtsz, NULL, NULL)) {
            return 1;
        }
        tmp[1] ^= 0x80;
        /* server to native */
        dt->dttz_sec = flibc_ntohll(*(unsigned long long *)&tmp[1]);
        dt->dttz_frac = ntohl(*(unsigned int *)&tmp[9]);

        if (datetime_check_range(dt->dttz_sec, dt->dttz_frac))
            return -1;
    } else {
        dt->dttz_prec = DTTZ_PREC_MSEC;
        tmp = alloca(SERVER_DATETIME_LEN);
        if (CLIENT_CSTR_to_SERVER_DATETIME(
                z, n, 0, (struct field_conv_opts *)&tzopts, NULL, &tmp[0],
                SERVER_DATETIME_LEN, &outdtsz, NULL, NULL)) {
            return 1;
        }
        tmp[1] ^= 0x80;
        /* server to native */
        dt->dttz_sec = flibc_ntohll(*(unsigned long long *)&tmp[1]);
        dt->dttz_frac = ntohs(*(unsigned short *)&tmp[9]);

        if (datetime_check_range(dt->dttz_sec, dt->dttz_frac))
            return -1;
    }
    /* We have to make a compromise here by make strings always convertible -
       We have seen people do "insert into datetime values(cast(now() as text)".
       */
    dt->dttz_conv = 1;
    if (precision > dt->dttz_prec) {
        /* If user asked for high precision, promote it now. */
        dt->dttz_frac *= 1000;
        dt->dttz_prec = DTTZ_PREC_USEC;
    }
    return 0;
}

int dttz_to_client_datetime(const dttz_t *in, const char *tz,
                            cdb2_client_datetime_t *out)
{
    char tmpIn[SERVER_DATETIME_LEN];
    struct field_conv_opts_tz tzopts;
    int outnull;
    int outdtsz;
    /* conv input to network byte order */
    if (dttz_to_server_datetime(in, tz, (uint8_t *)&tmpIn, &tzopts))
        return 1;
    cdb2_client_datetime_t tmp;
    if (SERVER_DATETIME_to_CLIENT_DATETIME(
            &tmpIn, sizeof(tmpIn), NULL, NULL, &tmp, sizeof(tmp), &outnull,
            &outdtsz, (const struct field_conv_opts *)&tzopts, NULL)) {
        return 1;
    }
    /* unpack into 'out' buffer in host byte order */
    if (!client_datetime_get(out, (uint8_t *)&tmp, (uint8_t *)(&tmp + 1)))
        return 1;
    if (out->tm.tm_year + 1900 > 9999 || out->tm.tm_year + 1900 < -9999)
        return -1;
    return 0;
}

int dttz_to_client_datetimeus(const dttz_t *in, const char *tz,
                              cdb2_client_datetimeus_t *out)
{
    char tmpIn[SERVER_DATETIMEUS_LEN];
    struct field_conv_opts_tz tzopts;
    int outnull;
    int outdtsz;
    /* conv input to network byte order */
    if (dttz_to_server_datetimeus(in, tz, (uint8_t *)&tmpIn, &tzopts))
        return 1;
    cdb2_client_datetimeus_t tmp;
    if (SERVER_DATETIMEUS_to_CLIENT_DATETIMEUS(
            &tmpIn, sizeof(tmpIn), NULL, NULL, &tmp, sizeof(tmp), &outnull,
            &outdtsz, (const struct field_conv_opts *)&tzopts, NULL)) {
        return 1;
    }
    /* unpack into 'out' buffer in host byte order */
    if (!client_datetimeus_get(out, (uint8_t *)&tmp, (uint8_t *)(&tmp + 1)))
        return 1;
    if (out->tm.tm_year + 1900 > 9999 || out->tm.tm_year + 1900 < -9999)
        return -1;
    return 0;
}

int timespec_to_dttz(const struct timespec *ts, dttz_t *dt, int precision)
{
    /* it is the caller's responsibility to validate precision */
    int range, denominator, addend;
    range = pow(10, precision);
    denominator = 1000000000 / range;
    addend = (denominator >> 1);

    /* now() is alway convertible */
    dt->dttz_conv = DTTZ_CONV_NOW;
    dt->dttz_sec = ts->tv_sec;
    dt->dttz_prec = precision;
    dt->dttz_frac = (ts->tv_nsec + addend) / denominator;
    if (dt->dttz_frac >= range) {
        dt->dttz_frac = 0;
        dt->dttz_sec++;
    }
    return 0;
}

#define client_dt_to_dttz_func_body(dt, UDT, prec, frac, fracdt)               \
    int rc, outsz;                                                             \
    SRV_TP(dt) sdt;                                                            \
    struct field_conv_opts_tz optstz;                                          \
    struct field_conv_opts *opts;                                              \
                                                                               \
    opts = (struct field_conv_opts *)&optstz;                                  \
    optstz.flags = FLD_CONV_TZONE;                                             \
    if (little_endian)                                                         \
        optstz.flags |= FLD_CONV_LENDIAN;                                      \
                                                                               \
    strncpy(optstz.tzname, outtz, sizeof(optstz.tzname));                      \
                                                                               \
    /* convert to a server datetime first */                                   \
    rc = CLIENT_##UDT##_to_SERVER_##UDT(in, sizeof(*in), 0, opts, NULL, &sdt,  \
                                        sizeof(sdt), &outsz, NULL, NULL);      \
    if (rc)                                                                    \
        return rc;                                                             \
                                                                               \
    /* epoch is in server format */                                            \
    out->dttz_sec = flibc_ntohll(sdt.sec);                                     \
    int8b_to_int8(out->dttz_sec, (comdb2_int8 *)&out->dttz_sec);               \
    out->dttz_frac = (sizeof(fracdt) == sizeof(uint16_t)) ? htons(sdt.frac)    \
                                                          : htonl(sdt.frac);   \
    out->dttz_prec = prec;                                                     \
    out->dttz_conv = 0;                                                        \
    return rc;                                                                 \
/* END OF client_dt_to_dttz_func_body */

int client_datetime_to_dttz(const cdb2_client_datetime_t *in, const char *outtz,
                            dttz_t *out, int little_endian)
{
    client_dt_to_dttz_func_body(datetime, DATETIME, 3, msec, unsigned short);
}

int client_datetimeus_to_dttz(const cdb2_client_datetimeus_t *in,
                              const char *outtz, dttz_t *out, int little_endian)
{
    client_dt_to_dttz_func_body(datetimeus, DATETIMEUS, 6, usec, unsigned int);
}

static int get_int_field(int64_t *out, void *in, size_t len, int flip)
{
    int16_t i16;
    int32_t i32;
    int64_t i64;
    switch (len) {
    case sizeof(int16_t):
        memcpy(&i16, in, len);
        if (flip) i16 = flibc_shortflip(i16);
        *out = i16;
        break;
    case sizeof(int32_t):
        memcpy(&i32, in, len);
        if (flip) i32 = flibc_intflip(i32);
        *out = i32;
        break;
    case sizeof(int64_t):
        memcpy(&i64, in, len);
        if (flip) i64 = flibc_llflip(i64);
        *out = i64;
        break;
    default:
        return -1;
    }
    return 0;
}

static int get_uint_field(uint64_t *out, void *in, size_t len, int flip)
{
    uint16_t u16;
    uint32_t u32;
    uint64_t u64;
    switch (len) {
    case sizeof(uint16_t):
        memcpy(&u16, in, len);
        if (flip) u16 = flibc_shortflip(u16);
        *out = u16;
        break;
    case sizeof(uint32_t):
        memcpy(&u32, in, len);
        if (flip) u32 = flibc_intflip(u32);
        *out = u32;
        break;
    case sizeof(uint64_t):
        memcpy(&u64, in, len);
        if (flip) u64 = flibc_llflip(u64);
        *out = u64;
        break;
    default:
        return -1;
    }
    return *out <= LLONG_MAX ? 0 : -1;
}

static int get_real_field(double *out, void *in, size_t len, int flip)
{
    float f;
    double d;
    switch (len) {
    case sizeof(float):
        memcpy(&f, in, len);
        if (flip) f = flibc_floatflip(f);
        *out = f;
        break;
    case sizeof(double):
        memcpy(&d, in, len);
        if (flip) d = flibc_dblflip(d);
        *out = d;
        break;
    default:
        return -1;
    }
    return 0;
}

/**
 * Zero don't properly memcmp in the format (sign,exp,coef)
 * This function normalize the zeros, which makes all zeros
 * positives and drops the exponent
 *
 * Exponent is returned by the function, and signed is passed
 * in the optional argument.
 *
 */
static int _scrub_zero_decimal32(server_decimal32_t *pdec32, int *sign)
{
    int ret;
    char exp;
    int i;

    /* handle zeros */
    if (pdec32->sign == 0) {
        /* take quantum from exponent */
        exp = pdec32->exp;
        exp ^= 0x80;
        exp ^= 0x0FF;

        ret = -exp + 1;

        if (sign) {
            *sign = 0; /* all zeros part of the key are positive */
            pdec32->sign = 1;
            for (i = 0; i < sizeof(pdec32->coef); i++)
                pdec32->coef[i] ^= 0x0FF;
            pdec32->exp = 0x0;
        } else {
            pdec32->exp = 0x0FF;
        }
    } else {
        /* take quantum from exponent */
        exp = pdec32->exp;
        exp ^= 0x80;

        ret = -exp + 1;

        pdec32->exp = 0x0;

        if (sign) {
            *sign = 1;
        }
    }

    return ret;
}

static int _scrub_zero_decimal64(server_decimal64_t *pdec, int *sign)
{
    int ret;
    short exp, tmp;
    int i;

    /* handle zeros */
    if (pdec->sign == 0) {
        /* take quantum from exponent */
        tmp = pdec->exp ^ 0x0FFFF;

        tmp = ntohs(tmp);

        int2b_to_int2(tmp, &exp);

        ret = -exp + 1;

        if (sign) {
            *sign = 0; /* all zeros part of the key are positive */
            pdec->sign = 1;
            for (i = 0; i < sizeof(pdec->coef); i++)
                pdec->coef[i] ^= 0x0FF;
            pdec->exp = 0x0;
        } else {
            pdec->exp = 0x0FFFF;
        }
    } else {
        /* take quantum from exponent */
        tmp = htons(pdec->exp);
        int2b_to_int2(tmp, &exp);

        ret = -exp + 1;

        pdec->exp = 0x0;

        if (sign) {
            *sign = 1;
        }
    }

    return ret;
}

static int _scrub_zero_decimal128(server_decimal128_t *pdec, int *sign)
{
    int ret;
    short exp, tmp;
    int i;

    /* handle zeros */
    if (pdec->sign == 0) {
        /* take quantum from exponent */
        tmp = pdec->exp ^ 0x0FFFF;

        tmp = ntohs(tmp);

        int2b_to_int2(tmp, &exp);

        ret = -exp + 1;

        if (sign) {
            *sign = 0; /* all zeros part of the key are positive */
            pdec->sign = 1;
            for (i = 0; i < sizeof(pdec->coef); i++)
                pdec->coef[i] ^= 0x0FF;
            pdec->exp = 0x0;
        } else {
            pdec->exp = 0x0FFFF;
        }
    } else {
        /* take quantum from exponent */
        tmp = htons(pdec->exp);
        int2b_to_int2(tmp, &exp);

        ret = -exp + 1;

        pdec->exp = 0x0;

        if (sign) {
            *sign = 1;
        }
    }

    return ret;
}

/**
 * Quantum-s break sorting since they insert random numbers in the middle of the
 * key.  This functions zero out quantums and return them to caller, in case
 * they need to be preserved out-of-band
 *
 * Special handling for zero numbers: the exponent for a zero number in whatever
 * radix
 * does not matter.  To properly order tupples (sign, exponent, coefficient),
 * zero numbers
 * need to have the exponent set to MINIMUM value for positive numbers, and to
 * MAXIMUM value
 * for negative numbers.
 *
 */
short decimal_quantum_get(char *pdec, int len, int *sign)
{
    short sret = 0;
    int i;
    switch (len) {
    case sizeof(server_decimal32_t): {
        server_decimal32_t *pdec32 = (server_decimal32_t *)pdec;
        int is_zero = 0;
        char ret = 0;

        if (pdec32->sign) {
            for (i = 0; i < DECSINGLE_PACKED_COEF - 1 && pdec32->coef[i] == 0;
                 i++)
                ;
            if ((i == DECSINGLE_PACKED_COEF - 1) &&
                /* zeros so far, check last dib */
                !(pdec32->coef[DECSINGLE_PACKED_COEF - 1] & 0x0F0))
                is_zero = 1;
        } else {
            for (i = 0;
                 i < DECSINGLE_PACKED_COEF - 1 && pdec32->coef[i] == 0x0FF; i++)
                ;
            if ((i == DECSINGLE_PACKED_COEF - 1) &&
                /* zeros so far, check last dib */
                (pdec32->coef[DECSINGLE_PACKED_COEF - 1] & 0x0F0) == 0x0F0)
                is_zero = 1;
        }

        if (!is_zero) {
            ret = pdec32->coef[DECSINGLE_PACKED_COEF - 1] & 0x0F;
            if (pdec32->sign == 0) {
                ret = ret ^ 0x0F;
            }
            pdec32->coef[DECSINGLE_PACKED_COEF - 1] =
                (pdec32->sign) ? ((((server_decimal32_t *)pdec)
                                       ->coef[DECSINGLE_PACKED_COEF - 1] &
                                   0x0F0) |
                                  0x01)
                               : ((((server_decimal32_t *)pdec)
                                       ->coef[DECSINGLE_PACKED_COEF - 1] &
                                   0x0F0) |
                                  0x0E);

            if (sign)
                *sign = (pdec32->sign) ? 1 : 0;
        } else {
            ret = _scrub_zero_decimal32(pdec32, sign);
        }
        sret = ret;
        break;
    }
    case sizeof(server_decimal64_t): {
        server_decimal64_t *pdec64 = (server_decimal64_t *)pdec;
        int is_zero = 0;
        short ret;

        if (pdec64->sign) {
            for (i = 0; i < DECDOUBLE_PACKED_COEF - 1 && pdec64->coef[i] == 0;
                 i++)
                ;
        } else {
            for (i = 0;
                 i < DECDOUBLE_PACKED_COEF - 1 && pdec64->coef[i] == 0x0FF; i++)
                ;
        }
        if (i >= DECDOUBLE_PACKED_COEF - 1)
            is_zero = 1;

        if (!is_zero) {
            ret = pdec64->coef[DECDOUBLE_PACKED_COEF - 1];
            if (pdec64->sign == 0) {
                ret = ret ^ 0x0FFFF;
            }
            pdec64->coef[DECDOUBLE_PACKED_COEF - 1] =
                (pdec64->sign) ? 1 : 0x0FE;

            if (sign)
                *sign = (pdec64->sign) ? 1 : 0;
        } else {
            ret = _scrub_zero_decimal64(pdec64, sign);
#if 0
           if(pdec64->sign == 0)
           {
              pdec64->exp=0x0FFFF;
           }
           else
           {
              pdec64->exp=0x0;
           }
#endif
        }
        sret = ret;
        break;
    }
    case sizeof(server_decimal128_t): {
        server_decimal128_t *pdec128 = (server_decimal128_t *)pdec;
        int is_zero = 0;
        short ret;

        if (pdec128->sign) {
            for (i = 0; i < DECQUAD_PACKED_COEF - 1 && pdec128->coef[i] == 0;
                 i++)
                ;
        } else {
            for (i = 0;
                 i < DECQUAD_PACKED_COEF - 1 && pdec128->coef[i] == 0x0FF; i++)
                ;
        }
        if (i >= DECQUAD_PACKED_COEF - 1)
            is_zero = 1;

        if (!is_zero) {
            ret = pdec128->coef[DECQUAD_PACKED_COEF - 1];
            if (pdec128->sign == 0) {
                ret = ret ^ 0x0FFFF;
            }
            pdec128->coef[DECQUAD_PACKED_COEF - 1] =
                (pdec128->sign) ? 1 : 0x0FE;

            if (sign)
                *sign = (pdec128->sign) ? 1 : 0;
        } else {
            ret = _scrub_zero_decimal128(pdec128, sign);
#if 0
            if(pdec128->sign == 0)
            {
                pdec128->exp=0x0FFFF;
            }
            else
            {
                pdec128->exp=0x0;
            }
#endif
        }
        sret = ret;
        break;
    }
    }
    return sret;
}

void decimal_quantum_set(char *pdec, int len, short *pquantum, int *sign)
{
    int mark;
    int i;
    short squantum = 0;

    squantum = (pquantum) ? *pquantum : 0;

    switch (len) {
    case sizeof(server_decimal32_t): {
        server_decimal32_t *pdec32 = (server_decimal32_t *)pdec;
        char exp;
        char quantum = squantum;

        if (pdec32->sign == 0) {
            mark = (pdec32->coef[DECSINGLE_PACKED_COEF - 1] & 0x0F) ^ 0x0F;
            if (quantum)
                quantum ^= 0x0F;
        } else {
            mark = pdec32->coef[DECSINGLE_PACKED_COEF - 1] & 0x0F;
        }
        /* if this is new, don't set it to 0; if this is old; don't set it to
         * non-zero*/
        if (pquantum == NULL && mark)
            break;
        else if (pquantum != NULL && mark == 0)
            break;

        pdec32->coef[DECSINGLE_PACKED_COEF - 1] &= 0x0F0;

        /*special handling of 0-es, exp has quantum*/
        for (i = 0; i < DECSINGLE_PACKED_COEF - 1 && pdec32->coef[i] == 0; i++)
            ;

        if ((i == DECSINGLE_PACKED_COEF - 1) &&
            /* zeros so far, check last dib */
            !(pdec32->coef[DECSINGLE_PACKED_COEF - 1] & 0x0F0)) {
            /* zero */
            exp = -(quantum - 1);
            if (sign) {
                pdec32->exp = exp ^ 0x080;
                if (!*sign) {
                    pdec32->sign = 0;
                    for (i = 0; i < DECSINGLE_PACKED_COEF - 1; i++)
                        pdec32->coef[i] ^= 0x0FF;
                    pdec32->coef[DECSINGLE_PACKED_COEF - 1] = 0x0FE;
                    pdec32->exp ^= 0x0FF;
                } else {
                    pdec32->sign = 1;
                    pdec32->coef[DECSINGLE_PACKED_COEF - 1] = 1;
                }
            } else {
                pdec32->exp = exp ^ 0x080;
                pdec32->coef[DECSINGLE_PACKED_COEF - 1] |= 1;
            }
        } else {
            /* nonzero */
            pdec32->coef[DECSINGLE_PACKED_COEF - 1] |= quantum;
        }

        break;
    }
    case sizeof(server_decimal64_t): {
        server_decimal64_t *pdec64 = (server_decimal64_t *)pdec;
        short exp;
        short tmp;
        short quantum = squantum;

        if (pdec64->sign == 0) {
            mark = pdec64->coef[DECDOUBLE_PACKED_COEF - 1] ^ 0x0FF;
            quantum = quantum ^ 0x0FFFF;
        } else {
            mark = pdec64->coef[DECDOUBLE_PACKED_COEF - 1];
        }
        /* if this is new, don't set it to 0; if this is old; don't set it to
         * non-zero*/
        if (pquantum == NULL && mark)
            break;
        else if (pquantum != NULL && mark == 0)
            break;

        /*special handling of 0-es, exp has quantum*/
        for (i = 0; i < DECDOUBLE_PACKED_COEF - 1 && pdec64->coef[i] == 0; i++)
            ;

        if (i >= DECDOUBLE_PACKED_COEF - 1) {
            /* zero */
            exp = -(quantum - 1);
            if (sign) {
                int2b_to_int2(exp, &tmp);
                pdec64->exp = htons(tmp);
                if (!*sign) {
                    pdec64->sign = 0;
                    for (i = 0; i < DECDOUBLE_PACKED_COEF - 1; i++)
                        pdec64->coef[i] ^= 0x0FF;
                    pdec64->coef[DECDOUBLE_PACKED_COEF - 1] = 0x0FE;
                    pdec64->exp ^= 0x0FFFF;
                } else {
                    pdec64->sign = 1;
                    pdec64->coef[DECDOUBLE_PACKED_COEF - 1] = 1;
                }
            } else {
                int2b_to_int2(exp, (comdb2_int2 *)&pdec64->exp);
                pdec64->coef[DECDOUBLE_PACKED_COEF - 1] = 1;
            }
        } else {
            /* nonzero */
            pdec64->coef[DECDOUBLE_PACKED_COEF - 1] = quantum;
        }

        break;
    }
    case sizeof(server_decimal128_t): {
        server_decimal128_t *pdec128 = (server_decimal128_t *)pdec;
        short exp;
        short tmp;
        short quantum = squantum;

        if (pdec128->sign == 0) {
            mark = pdec128->coef[DECQUAD_PACKED_COEF - 1] ^ 0x0FF;
            quantum = quantum ^ 0x0FFFF;
        } else {
            mark = pdec128->coef[DECQUAD_PACKED_COEF - 1];
        }
        /* if this is new, don't set it to 0; if this is old; don't set it to
         * non-zero*/
        if (pquantum == NULL && mark)
            break;
        else if (pquantum != NULL && mark == 0)
            break;

        /*special handling of 0-es, exp has quantum*/
        for (i = 0; i < DECQUAD_PACKED_COEF - 1 && pdec128->coef[i] == 0; i++)
            ;

        if (i >= DECQUAD_PACKED_COEF - 1) {
            /* zero */
            exp = -(quantum - 1);
            if (sign) {
                int2b_to_int2(exp, &tmp);
                pdec128->exp = htons(tmp);
                if (!*sign) {
                    pdec128->sign = 0;
                    for (i = 0; i < DECQUAD_PACKED_COEF - 1; i++)
                        pdec128->coef[i] ^= 0x0FF;
                    pdec128->coef[DECQUAD_PACKED_COEF - 1] = 0x0FE;
                    pdec128->exp ^= 0x0FFFF;
                } else {
                    pdec128->sign = 1;
                    pdec128->coef[DECQUAD_PACKED_COEF - 1] = 1;
                }
            } else {
                int2b_to_int2(exp, (comdb2_int2 *)&pdec128->exp);
                pdec128->coef[DECQUAD_PACKED_COEF - 1] = 1;
            }
        } else {
            /* nonzero */
            pdec128->coef[DECQUAD_PACKED_COEF - 1] = quantum;
        }

        break;
    }
    }
}

int dttz_cmp(const dttz_t *d1, const dttz_t *d2)
{
    if (d1->dttz_sec != d2->dttz_sec)
        return d1->dttz_sec < d2->dttz_sec ? -1 : 1;
    if (d1->dttz_prec == d2->dttz_prec)
        return d1->dttz_frac - d2->dttz_frac;
    return (d1->dttz_prec == DTTZ_PREC_MSEC ? d1->dttz_frac * 1000
                                            : d1->dttz_frac) -
           (d2->dttz_prec == DTTZ_PREC_MSEC ? d2->dttz_frac * 1000
                                            : d2->dttz_frac);
}

int interval_cmp(const intv_t *i1, const intv_t *i2)
{
    int rc;
    if (i1->type != i2->type &&
        ((i1->type == INTV_DS_TYPE && i2->type != INTV_DSUS_TYPE) ||
         (i1->type == INTV_DSUS_TYPE && i2->type != INTV_DS_TYPE)))
    bad:
    return i1->sign * memcmp(&i1->u, &i2->u, sizeof(i1->u));
    if ((rc = i1->sign - i2->sign) != 0)
        return rc < 0 ? -1 : 1;
    switch (i1->type) {
    case INTV_YM_TYPE:
        if (i1->u.ym.years != i2->u.ym.years)
            return i1->sign * (i1->u.ym.years - i2->u.ym.years);
        if (i1->u.ym.months != i2->u.ym.months)
            return i1->sign * (i1->u.ym.months - i2->u.ym.months);
        return 0;
    case INTV_DS_TYPE:
        if (i1->u.ds.days != i2->u.ds.days)
            return i1->sign * (i1->u.ds.days - i2->u.ds.days);
        if (i1->u.ds.hours != i2->u.ds.hours)
            return i1->sign * (i1->u.ds.hours - i2->u.ds.hours);
        if (i1->u.ds.mins != i2->u.ds.mins)
            return i1->sign * (i1->u.ds.mins - i2->u.ds.mins);
        if (i1->u.ds.sec != i2->u.ds.sec)
            return i1->sign * (i1->u.ds.sec - i2->u.ds.sec);
        if (i1->type == i2->type && i1->u.ds.frac != i2->u.ds.frac)
            return i1->sign * (i1->u.ds.frac - i2->u.ds.frac);
        if (i1->type != i2->type && i1->u.ds.frac * 1000 != i2->u.ds.frac)
            return i1->sign * (i1->u.ds.frac * 1000 - i2->u.ds.frac);
        return 0;
    case INTV_DSUS_TYPE:
        if (i1->u.ds.days != i2->u.ds.days)
            return i1->sign * (i1->u.ds.days - i2->u.ds.days);
        if (i1->u.ds.hours != i2->u.ds.hours)
            return i1->sign * (i1->u.ds.hours - i2->u.ds.hours);
        if (i1->u.ds.mins != i2->u.ds.mins)
            return i1->sign * (i1->u.ds.mins - i2->u.ds.mins);
        if (i1->u.ds.sec != i2->u.ds.sec)
            return i1->sign * (i1->u.ds.sec - i2->u.ds.sec);
        if (i1->type == i2->type && i1->u.ds.frac != i2->u.ds.frac)
            return i1->sign * (i1->u.ds.frac - i2->u.ds.frac);
        if (i1->type != i2->type && i1->u.ds.frac != i2->u.ds.frac * 1000)
            return i1->sign * (i1->u.ds.frac - i2->u.ds.frac * 1000);
        return 0;
    default:
        goto bad;
    }
}

int int_to_interval(int64_t i, uint64_t *n0, uint64_t *n1, int *sign)
{
    *sign = 1;
    if (i < 0) {
        *sign = -1;
        i = -i;
    }
    *n0 = i;
    *n1 = 0;
    return 0;
}

int double_to_interval(double d, uint64_t *n0, uint64_t *n1, int *sign)
{
    *sign = 1;
    if (d < 0) {
        *sign = -1;
        d = -d;
    }
    *n0 = d;
    *n1 = ((d - (*n0)) + 0.0000005) * 1000000;
    return 0;
}

/*
** 'type' is both input & output param
** input: INTV_YM_TYPE or INTV_DS_TYPE
** output: result in year-month(0) or ds(1) or n0,n1(2)
*/
int str_to_interval(const char *c, int sz, int *type, uint64_t *n0,
                    uint64_t *n1, intv_ds_t *ds, int *sign)
{
    if (c[sz] != '\0') {
        char *tmp = alloca(sz + 1);
        memcpy(tmp, c, sz);
        tmp[sz] = '\0';
        c = tmp;
    }
    int intv_type = *type;
    while (isspace(*c))
        ++c;
    *sign = 1;
    if (*c == '-') {
        *sign = -1;
        ++c;
    }
    char *end;
    errno = 0; // strto* will set this on under/over flow
    while (isspace(*c))
        ++c;
    if (strchr(c, '-')) { /* PARSE year-month */
        if (intv_type != INTV_YM_TYPE)
            return 1;
        *type = 0;

        *n0 = strtoull(c, &end, 10);
        if (errno == ERANGE)
            return 1;
        if (*end != '-')
            return 1;
        c = end + 1;

        *n1 = strtoull(c, &end, 10);
        if (errno == ERANGE)
            return 1;
        if (*end != '\0')
            return 1;
        return 0;
    } else if (strchr(c, ' ')) { /* PARSE days hr:min:sec.fraction */
        if (intv_type != INTV_DS_TYPE && intv_type != INTV_DSUS_TYPE)
            return 1;
        bzero(ds, sizeof(intv_ds_t));
        *type = 1;

        ds->days = strtoul(c, &end, 10);
        if (errno == ERANGE)
            return 1;
        if (*end != ' ')
            return 1;
        c = end + 1;

        /* hr:min:sec.fraction is optional */
        if (*c == '\0') {
            ds->prec = DTTZ_PREC_MSEC;
            ds->conv = 1;
            return 0;
        }

        ds->hours = strtoul(c, &end, 10);
        if (errno == ERANGE)
            return 1;
        if (*end != ':')
            return 1;
        c = end + 1;

        ds->mins = strtoul(c, &end, 10);
        if (errno == ERANGE)
            return 1;
        if (*end != ':')
            return 1;
        c = end + 1;

        ds->sec = strtoul(c, &end, 10);
        if (errno == ERANGE)
            return 1;
        if (*end == '.') {
            c = end + 1;
            ds->frac = strtoul(c, &end, 10);
            if (errno == ERANGE)
                return 1;
            if (*end != '\0')
                return 1;
            ds->prec =
                ((end - c) >= DTTZ_PREC_USEC) ? DTTZ_PREC_USEC : DTTZ_PREC_MSEC;
            ds->conv = 0;
        } else { /* missing ms */
            ds->frac = 0;
            ds->prec = DTTZ_PREC_MSEC;
            ds->conv = 1;
        }
        return 0;
    } else {           /* just a number */
        int dummysign; // Already parsed sign
        *type = 2;
        int64_t i = strtoll(c, &end, 10);
        if (errno == ERANGE)
            return 1;
        if (*end == '\0') {
            return int_to_interval(i, n0, n1, &dummysign);
        }
        double d = strtod(c, &end);
        if (errno == ERANGE)
            return 1;
        if (*end == '\0')
            return double_to_interval(d, n0, n1, &dummysign);
    }
    return 1;
}

/*
** Normalize an interval year-month
*/
void _normalizeIntervalYM(intv_ym_t *ym)
{
    ym->years += ym->months / 12;
    ym->months = ym->months % 12;
}

/* doesn't check ds's sign */
static void ds_to_secs_and_msecs(const intv_ds_t *ds, uint64_t *sec,
                                 uint64_t *msec)
{
    *sec = ds->days * 24 * 3600 + ds->hours * 3600 + ds->mins * 60 + ds->sec;
    *msec = (ds->prec == DTTZ_PREC_MSEC) ? ds->frac : ds->frac / 1000;
}

static void ds_to_secs_and_usecs(const intv_ds_t *ds, uint64_t *sec,
                                 uint64_t *usec)
{
    *sec = ds->days * 24 * 3600 + ds->hours * 3600 + ds->mins * 60 + ds->sec;
    *usec = (ds->prec == DTTZ_PREC_USEC) ? ds->frac : ds->frac * 1000;
}

double interval_to_double(const intv_t *i)
{
    uint64_t s, m;
    switch (i->type) {
    case INTV_YM_TYPE:
        return i->sign * (double)(i->u.ym.years * 12 + i->u.ym.months);
    case INTV_DS_TYPE:
        ds_to_secs_and_msecs(&i->u.ds, &s, &m);
        return i->sign * ((double)s + ((m % 1000) / 1000.0));
    case INTV_DSUS_TYPE:
        ds_to_secs_and_usecs(&i->u.ds, &s, &m);
        return i->sign * ((double)s + ((m % 1000000) / 1000000.0));
    default:
        return 0;
    }
}

/* datetime - datetime = intv_ds */
void sub_dttz_dttz(const dttz_t *d1, const dttz_t *d2, intv_t *intv)
{
    long long tmp = 0;
    intv->sign = 1;
    intv_ds_t *ds = &intv->u.ds;

    unsigned int d1_frac, d2_frac;
    d1_frac = d1->dttz_frac;
    d2_frac = d2->dttz_frac;

    if (d1->dttz_prec > d2->dttz_prec) {
        d2_frac *= 1000;
        intv->type = INTV_DSUS_TYPE;
        ds->prec = d1->dttz_prec;
    } else if (d1->dttz_prec < d2->dttz_prec) {
        d1_frac *= 1000;
        intv->type = INTV_DSUS_TYPE;
        ds->prec = d2->dttz_prec;
    } else if (d1->dttz_prec == DTTZ_PREC_MSEC) {
        intv->type = INTV_DS_TYPE;
        ds->prec = DTTZ_PREC_MSEC;
    } else if (d1->dttz_prec == DTTZ_PREC_USEC) {
        intv->type = INTV_DSUS_TYPE;
        ds->prec = DTTZ_PREC_USEC;
    }

    if ((d1->dttz_sec < d2->dttz_sec) ||
        ((d1->dttz_sec == d2->dttz_sec) && (d1_frac < d2_frac))) {
        if (d2_frac < d1_frac) {
            if (intv->type == INTV_DS_TYPE)
                _setIntervalDS(ds, d2->dttz_sec - d1->dttz_sec - 1,
                               1000 + d2_frac - d1_frac);
            else
                _setIntervalDSUS(ds, d2->dttz_sec - d1->dttz_sec - 1,
                                 1000000 + d2_frac - d1_frac);
        } else {
            if (intv->type == INTV_DS_TYPE)
                _setIntervalDS(ds, d2->dttz_sec - d1->dttz_sec,
                               d2_frac - d1_frac);
            else
                _setIntervalDSUS(ds, d2->dttz_sec - d1->dttz_sec,
                                 d2_frac - d1_frac);
        }
        intv->sign = -1;
    } else {
        if (d1_frac < d2_frac) {
            if (intv->type == INTV_DS_TYPE)
                _setIntervalDS(ds, d1->dttz_sec - d2->dttz_sec - 1,
                               1000 + d1_frac - d2_frac);
            else
                _setIntervalDSUS(ds, d1->dttz_sec - d2->dttz_sec - 1,
                                 1000000 + d1_frac - d2_frac);
        } else {
            if (intv->type == INTV_DS_TYPE)
                _setIntervalDS(ds, d1->dttz_sec - d2->dttz_sec,
                               d1_frac - d2_frac);
            else
                _setIntervalDSUS(ds, d1->dttz_sec - d2->dttz_sec,
                                 d1_frac - d2_frac);
        }
    }

    ds->conv = (d1->dttz_conv + d2->dttz_conv >= DTTZ_CONV_NOW) ? 1 : 0;
}

void sub_dttz_intvds(const dttz_t *dt, const intv_t *ds, dttz_t *rs)
{
    if (ds->sign < 0) {
        intv_t tmp = *ds;
        tmp.sign = 1;
        add_dttz_intvds(dt, &tmp, rs);
        return;
    }
    uint64_t s, m;
    unsigned int dt_frac, ds_frac, pow10;

    ds_to_secs_and_msecs(&ds->u.ds, &s, &m);
    rs->dttz_sec = dt->dttz_sec - s;

    dt_frac = dt->dttz_frac;
    ds_frac = ds->u.ds.frac;

    if (dt->dttz_prec > ds->u.ds.prec) {
        ds_frac *= 1000;
        rs->dttz_prec = dt->dttz_prec;
        pow10 = 1000000;
    } else if (dt->dttz_prec < ds->u.ds.prec) {
        dt_frac *= 1000;
        rs->dttz_prec = ds->u.ds.prec;
        pow10 = 1000000;
    } else if (dt->dttz_prec == DTTZ_PREC_MSEC) {
        rs->dttz_prec = DTTZ_PREC_MSEC;
        pow10 = 1000;
    } else {
        rs->dttz_prec = DTTZ_PREC_USEC;
        pow10 = 1000000;
    }

    if (dt_frac < ds_frac) {
        --rs->dttz_sec;
        rs->dttz_frac = pow10 + dt_frac - ds_frac;
    } else {
        rs->dttz_frac = dt_frac - ds_frac;
    }
    rs->dttz_sec += rs->dttz_frac / pow10;
    rs->dttz_frac = rs->dttz_frac % pow10;
    rs->dttz_conv = (dt->dttz_conv + ds->u.ds.conv >= DTTZ_CONV_NOW) ? 1 : 0;
}

void add_dttz_intvds(const dttz_t *dt, const intv_t *ds, dttz_t *rs)
{
    if (ds->sign < 0) {
        intv_t tmp = *ds;
        tmp.sign = 1;
        sub_dttz_intvds(dt, &tmp, rs);
        return;
    }

    uint64_t s, m;
    unsigned int pow10;
    if (dt->dttz_prec > ds->u.ds.prec) {
        // datetimeus + intervalds
        rs->dttz_prec = DTTZ_PREC_USEC;
        ds_to_secs_and_usecs(&ds->u.ds, &s, &m);
        rs->dttz_frac = dt->dttz_frac + m;
        pow10 = 1000000;
    } else if (dt->dttz_prec < ds->u.ds.prec) {
        // datetime + intervaldsus
        rs->dttz_prec = DTTZ_PREC_USEC;
        ds_to_secs_and_usecs(&ds->u.ds, &s, &m);
        rs->dttz_frac = dt->dttz_frac * 1000 + m;
        pow10 = 1000000;
    } else if (dt->dttz_prec == DTTZ_PREC_MSEC) {
        // datetime + intervalds
        rs->dttz_prec = DTTZ_PREC_MSEC;
        ds_to_secs_and_msecs(&ds->u.ds, &s, &m);
        rs->dttz_frac = dt->dttz_frac + m;
        pow10 = 1000;
    } else {
        // datetimeus + intervaldsus
        rs->dttz_prec = DTTZ_PREC_USEC;
        ds_to_secs_and_usecs(&ds->u.ds, &s, &m);
        rs->dttz_frac = dt->dttz_frac + m;
        pow10 = 1000000;
    }

    rs->dttz_sec = dt->dttz_sec + s;
    rs->dttz_sec += rs->dttz_frac / pow10;
    rs->dttz_frac = rs->dttz_frac % pow10;
    rs->dttz_conv = (dt->dttz_conv + ds->u.ds.conv >= DTTZ_CONV_NOW) ? 1 : 0;
}

void set_null_func(void *to, int sz) { set_null(to, sz); }

void set_data_func(void *to, const void *from, int sz)
{
    set_data(to, from, sz);
}

static void client_intv_ym_to_intv_t(const cdb2_client_intv_ym_t *in,
                                     intv_t *out, int flip)
{
    memset(out, 0, sizeof(intv_t));
    out->type = INTV_YM_TYPE;
    intv_ym_t *ym = &out->u.ym;
    if (flip) {
        out->sign = flibc_intflip(in->sign);
        ym->years = flibc_intflip(in->years);
        ym->months = flibc_intflip(in->months);
    } else {
        out->sign = in->sign;
        ym->years = in->years;
        ym->months = in->months;
    }
}

static void client_intv_ds_to_intv_t(const cdb2_client_intv_ds_t *in,
                                     intv_t *out, int flip)
{
    memset(out, 0, sizeof(intv_t));
    out->type = INTV_DS_TYPE;
    intv_ds_t *ds = &out->u.ds;
    ds->prec = DTTZ_PREC_MSEC;
    ds->conv = 1;
    if (flip) {
        out->sign = flibc_intflip(in->sign);
        ds->days =  flibc_intflip(in->days);
        ds->hours = flibc_intflip(in->hours);
        ds->mins = flibc_intflip(in->mins);
        ds->sec = flibc_intflip(in->sec);
        ds->frac = flibc_intflip(in->msec);
    } else {
        out->sign = in->sign;
        ds->days =  in->days;
        ds->hours = in->hours;
        ds->mins = in->mins;
        ds->sec = in->sec;
        ds->frac = in->msec;
    }
}

static void client_intv_dsus_to_intv_t(const cdb2_client_intv_dsus_t *in,
                                       intv_t *out, int flip)
{
    client_intv_ds_to_intv_t((cdb2_client_intv_ds_t *)in, out, flip);
    out->type = INTV_DSUS_TYPE;
    out->u.ds.prec = DTTZ_PREC_USEC;
}

int get_type(struct param_data *param, void *p, int len, int type,
             const char *tzname, int little)
{
    int flip = 0;
#   if BYTE_ORDER == BIG_ENDIAN
    if (little)
#   elif BYTE_ORDER == LITTLE_ENDIAN
    if (!little)
#   endif
        flip = 1;
    param->null = 0;
    switch (type) {
    case CLIENT_INT:
        param->len = sizeof(param->u.i);
        return get_int_field(&param->u.i, p, len, flip);
    case CLIENT_UINT:
        param->len = sizeof(param->u.i);
        return get_uint_field(&param->u.i, p, len, flip);
    case CLIENT_REAL:
        param->len = sizeof(param->u.r);
        return get_real_field(&param->u.r, p, len, flip);
    case CLIENT_CSTR:
    case CLIENT_PSTR:
    case CLIENT_PSTR2:
    case CLIENT_VUTF8:
        param->u.p = p;
        param->len = len;
        return 0;
    case CLIENT_BLOB:
    case CLIENT_BYTEARRAY:
        param->u.p = p;
        param->len = len;
        return 0;
    case CLIENT_DATETIME:
        param->len = sizeof(param->u.dt);
        return client_datetime_to_dttz(p, tzname, &param->u.dt, little);
    case CLIENT_DATETIMEUS:
        param->len = sizeof(param->u.dt);
        return client_datetimeus_to_dttz(p, tzname, &param->u.dt, little);
    case CLIENT_INTVYM:
        param->len = sizeof(param->u.tv);
        client_intv_ym_to_intv_t(p, &param->u.tv, flip);
        return 0;
    case CLIENT_INTVDS:
        param->len = sizeof(param->u.tv);
        client_intv_ds_to_intv_t(p, &param->u.tv, flip);
        return 0;
    case CLIENT_INTVDSUS:
        param->len = sizeof(param->u.tv);
        client_intv_dsus_to_intv_t(p, &param->u.tv, flip);
        return 0;
    }
    return -1;
}

#include <str0.h>
int intv_to_str(const intv_t *tv, char *out, int len, int *used)
{
    switch (tv->type) {
    case INTV_YM_TYPE:
        *used = snprintf0(out, len, "%s%u-%2.2u", tv->sign == -1 ? "- " : "",
                          tv->u.ym.years, tv->u.ym.months);
        break;
    case INTV_DS_TYPE:
        *used =
            snprintf0(out, len, "%s%u %2.2u:%2.2u:%2.2u.%3.3u",
                      tv->sign == -1 ? "- " : "", tv->u.ds.days, tv->u.ds.hours,
                      tv->u.ds.mins, tv->u.ds.sec, tv->u.ds.frac);
        break;
    case INTV_DSUS_TYPE:
        *used =
            snprintf0(out, len, "%s%u %2.2u:%2.2u:%2.2u.%6.6u",
                      tv->sign == -1 ? "- " : "", tv->u.ds.days, tv->u.ds.hours,
                      tv->u.ds.mins, tv->u.ds.sec, tv->u.ds.frac);
        break;
    default:
        return -1;
    }
    if (*used < len)
        return 0;
    return -1;
}
