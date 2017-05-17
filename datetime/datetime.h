#ifndef _DATE_TIME_H_
#define _DATE_TIME_H_

#include <time.h>

/* tzname max length, based on the current list of valid tzones*/
#undef TZNAME_MAX
#define TZNAME_MAX      33

/* represent number of seconds since 1970 */
typedef long long db_time_t;

/*
   convert a db_time_t to a struct tm, where db_time_t
   is UTC time, and struct tm is with respect to timezone "name"
*/
int     db_time2struct( const char * const name,
                const db_time_t * const timeval, struct tm *outtm);
/*
   converts a struct tm in the timezone "name" to db_time_t
   as UTC time
*/
db_time_t       db_struct2time( const char * const name,
                struct tm * const tm);


#endif

