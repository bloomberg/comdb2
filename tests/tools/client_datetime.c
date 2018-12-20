#define DB_MAX_TZNAMEDB 36

#include <sys/types.h>
#include <compile_time_assert.h>
#include "datetime.h"
#include "endian_core.h"
#include <stddef.h>
#include "client_datetime.h"


enum
{
    SERVER_DATETIME_LEN = 1 + 8 + 2,
    TM_LEN = 4 + 4 + 4 + 4 + 4 + 4 + 4 + 4 + 4,
    CLIENT_DATETIME_LEN = TM_LEN + 4 + (1 * DB_MAX_TZNAMEDB),
    CLIENT_INTV_YM_LEN = 4 + 4 + 4,
    CLIENT_INTV_DS_LEN = 4 + 4 + 4 + 4 + 4 + 4 
};


BB_COMPILE_TIME_ASSERT(tm_size, offsetof(struct tm, tm_isdst) + sizeof(int) == TM_LEN);

uint8_t *tm_put(const struct tm *p_tm, uint8_t *p_buf,
        const uint8_t *p_buf_end)
{
    if(p_buf_end < p_buf || TM_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_tm->tm_sec), sizeof(p_tm->tm_sec), p_buf, p_buf_end);
    p_buf = buf_put(&(p_tm->tm_min), sizeof(p_tm->tm_min), p_buf, p_buf_end);
    p_buf = buf_put(&(p_tm->tm_hour), sizeof(p_tm->tm_hour), p_buf, p_buf_end);
    p_buf = buf_put(&(p_tm->tm_mday), sizeof(p_tm->tm_mday), p_buf, p_buf_end);
    p_buf = buf_put(&(p_tm->tm_mon), sizeof(p_tm->tm_mon), p_buf, p_buf_end);
    p_buf = buf_put(&(p_tm->tm_year), sizeof(p_tm->tm_year), p_buf, p_buf_end);
    p_buf = buf_put(&(p_tm->tm_wday), sizeof(p_tm->tm_wday), p_buf, p_buf_end);
    p_buf = buf_put(&(p_tm->tm_yday), sizeof(p_tm->tm_yday), p_buf, p_buf_end);
    p_buf = buf_put(&(p_tm->tm_isdst), sizeof(p_tm->tm_isdst), 
            p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *tm_get(struct tm *p_tm, const uint8_t *p_buf,
        const uint8_t *p_buf_end)
{
    if(p_buf_end < p_buf || TM_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_tm->tm_sec), sizeof(p_tm->tm_sec), p_buf, p_buf_end);
    p_buf = buf_get(&(p_tm->tm_min), sizeof(p_tm->tm_min), p_buf, p_buf_end);
    p_buf = buf_get(&(p_tm->tm_hour), sizeof(p_tm->tm_hour), p_buf, p_buf_end);
    p_buf = buf_get(&(p_tm->tm_mday), sizeof(p_tm->tm_mday), p_buf, p_buf_end);
    p_buf = buf_get(&(p_tm->tm_mon), sizeof(p_tm->tm_mon), p_buf, p_buf_end);
    p_buf = buf_get(&(p_tm->tm_year), sizeof(p_tm->tm_year), p_buf, p_buf_end);
    p_buf = buf_get(&(p_tm->tm_wday), sizeof(p_tm->tm_wday), p_buf, p_buf_end);
    p_buf = buf_get(&(p_tm->tm_yday), sizeof(p_tm->tm_yday), p_buf, p_buf_end);
    p_buf = buf_get(&(p_tm->tm_isdst), sizeof(p_tm->tm_isdst), 
            p_buf, p_buf_end);

    return p_buf;
}





BB_COMPILE_TIME_ASSERT(client_datetime_size,
        (sizeof(struct client_datetime) + TM_LEN - sizeof(struct tm))
        == CLIENT_DATETIME_LEN);

uint8_t *client_datetime_put(const client_datetime_t *p_client_datetime,
        uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if(p_buf_end < p_buf || CLIENT_DATETIME_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = tm_put(&(p_client_datetime->tm), p_buf, p_buf_end);
    p_buf = buf_put(&(p_client_datetime->msec), sizeof(p_client_datetime->msec),
            p_buf, p_buf_end);
    p_buf = buf_no_net_put(&(p_client_datetime->tzname), 
            sizeof(p_client_datetime->tzname), p_buf, p_buf_end);

    return p_buf;
}



const uint8_t *client_datetime_get(client_datetime_t *p_client_datetime,
        const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if(p_buf_end < p_buf || CLIENT_DATETIME_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = tm_get(&(p_client_datetime->tm), p_buf, p_buf_end);
    p_buf = buf_get(&(p_client_datetime->msec), sizeof(p_client_datetime->msec),
            p_buf, p_buf_end);
    p_buf = buf_no_net_get(&(p_client_datetime->tzname), 
            sizeof(p_client_datetime->tzname), p_buf, p_buf_end);

    return p_buf;
}




