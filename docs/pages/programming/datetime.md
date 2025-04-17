---
title: Datetime types
keywords: code
sidebar: mydoc_sidebar
permalink: datetime.html
---

## Datetimes

### Datetime overview

Datetime is a comdb2 datatype that represents an absolute position in time, or timestamp (for example, 
```2008-02-01T10:01:00 GMT```).

Timestamps are preferred to packing dates into integers (eg: storing ```time()``` output, or packed date values
like 20080201)

There are many advantages by doing this, like:

* 64 bit time values; the range of datetime values is unlimited for practical purposes - finally you can store your 
  Methuselah bonds expiration dates and wonder if they are going to happen on a Monday or Friday 13
* Timezone support, which allows applications to provide timestamps as local time values; the time values are easily 
  convertible from one timezone to another and are fully comparable with one another (for example, it would be easy 
  to determine the New York local time corresponding to an event happening in Tokyo, on 04-01-2006, at 01:00am Asia/Tokyo).
* Daylight saving support, which eliminates the need to change the data or code when a daylight 
  saving occurs, and again, permits comparing local time values from all over the world by obeying the local 
  daylight saving rules
* Time arithmetic support - one can subtract, add or multiply time intervals and datetime values 
* Time decomposition support - one can extract the hour(s), the minutes, seconds, year, month value, and so on. For 
  example, can easily retrieve all the events happening in March, 2008; or all the events happening at 3am New York 
  time. 
* Fraction support: the smallest time increment is one millisecond for the ```datetime``` type or one microsecond
  for the ```datetimeus``` type.

The application communicates with the database using local time values.  The locale is determine by the timezone name.  
If the application needs to use absolute time values, it can set its timezone name to ```GMT```.  A list of valid 
timezone names can be found at [Timezone Names](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones). The database receives the local time values and 
the associated timezone name and stores them as absolute values in a compact form (11 bytes for ```datetime``` values,
or 13 bytes for ```datetimeus``` values).

### Datetime values

The application can specify a datetime value as:

* A string, following the ISO 8601 format (extended to fully support the timezone naming scheme)
* C structure ```cdb2_client_datetime_t``` or ```cdb2_client_datetimeus_t``` bound to an SQL parameter, which 
  contains ```struct tm```-like fields and the timezone name
* Numeric types: If the number is an integer, is interpreted as "epoch" seconds.  If the number is a floating 
  point value, the integer part is the "epoch" seconds and the fractional part is the number of milliseconds 
  (rounded to a range of 0.001-0.999). Epoch times supplied this way don't have an associated timezone component,
  and are treated as GMT values.

Clients can use a couple of different formats to store and retrieve datetime values.

The simplest is a string, in "extended" ISO8601 format.  The format is ``` "YYYY-MM-DDThh:mm:ss.mmm <timezone string>" ```

It differs from ISO8601 in the usage of a timezone string rather than an offset from GMT. The minimum amount of 
information required is a date ```YYYY-MM-DD```.  The tail can be omitted, so ```YYYY-MM-DDThh```, 
```YYYY-MM-DDThh:mm```, and so on. The colons ":" can be omitted as well, so 
```YYYY-MM-DDThhmmss.mmm <timezone string>``` is valid as well. 

The second application side datatype is a "packed C structure" representation of a datetime.
The structure definition is:


```c

typedef struct cdb2_tm 
{
  int tm_sec;
  int tm_min;
  int tm_hour;
  int tm_mday;
  int tm_mon;
  int tm_year;
  int tm_wday;
  int tm_yday;
  int tm_isdst;
}
cdb2_tm_t;

/* datetime type definition */
typedef struct cdb2_client_datetime {
        cdb2_tm_t           tm;
        unsigned int        msec;
        char                tzname[CDB2_MAX_TZNAME];
} cdb2_client_datetime_t;

typedef struct cdb2_client_datetimeus {
        cdb2_tm_t           tm;
        unsigned int        usec;
        char                tzname[CDB2_MAX_TZNAME];
} cdb2_client_datetimeus_t;

/* interval types definition */
typedef struct cdb2_client_intv_ym {
    int                 sign;       /* sign of the interval, +/-1 */
    unsigned int        years;      /* interval year */
    unsigned int        months;     /* interval months [0-11] */
} cdb2_client_intv_ym_t;

typedef struct cdb2_client_intv_ds {
    int                 sign;       /* sign of the interval, +/-1 */
    unsigned int        days;       /* interval days    */
    unsigned int        hours;      /* interval hours   */
    unsigned int        mins;       /* interval minutes */
    unsigned int        sec;        /* interval sec     */
    unsigned int        msec;       /* msec             */
} cdb2_client_intv_ds_t;

typedef struct cdb2_client_intv_dsus {
    int                 sign;       /* sign of the interval, +/-1 */
    unsigned int        days;       /* interval days    */
    unsigned int        hours;      /* interval hours   */
    unsigned int        mins;       /* interval minutes */
    unsigned int        sec;        /* interval sec     */
    unsigned int        usec;       /* usec             */
} cdb2_client_intv_dsus_t;

```

where ```DB_MAX_TZNAME=36```. 


## Intervals

### Interval overview

An interval type represents an amount of time (for example, 3 hours and 20 minutes). 

Intervals come in two flavors: 

 * Year-to-month 
 * Day-to-seconds.
 
The first type stores years and months (like 1 year and 10 months), while the latter stores days, hours, 
minutes, seconds and milliseconds/microseconds.  The interval types are mutually incompatible (can't tell 
how many seconds are in a month).

Interval values are more or less indistinguishable from a quantity value. For example, it would be easy to 
store a year-to-month interval value as an integer, representing 12*years+months.  The real advantage of storing 
time intervals in their own type comes from the SQL support for time arithmetic and decomposition.

### Interval values

The datatype names for interval types are:

* ```intervalym``` for year-to-month interval
* ```intervalds``` for day-to-second interval

The intervals, both year-to-month and day-to-second, can be expressed in a several ways:

* A string
   * year-to-month format is:  ```"(-) YEARS-MONTHS"```
   * day-to-second format is:  ```"(-) DAYS HH:MM:SS.MSS"```
* C struct bound to an SQL parameter:
   * ```cdb2_client_intv_ym``` for Year-Month intervals
   * ```cdb2_client_intv_ds``` for Day-Second intervals
   * ```cdb2_client_intv_dsus``` for Day-Second intervals with millisecond precision
* Numeric values 
   * For year-to-month intervals the value represents the number of months computed using the formula: 
     ```sign*[years*12+months]```
   * For day-to-second intervals the value represents:
       * If an integer, the number of seconds.
       * If a floating point, the number of seconds as integer part and milliseconds as the fractional part.

The "-" prefix followed by a space denotes a negative time interval (like "3hours ahead").  The day-to-second 
format allows shorter forms by omitting the tail. You are required to have at least the days and the following 
space to have the interval value parsed correctly).  In the C structures used to bind values, the quantities 
are unsigned (i.e. positive) values, the sign being determined by the sign field.  Example: -1msec will be 
expressed by ```sign=-1, days=hours=mins=sec=0, msec=1```.

## Local Time Values

The datetime field permits introducing local time values (i.e. wall-clock) into the database.  The client 
specifies implicitly or explicitly the timezone location, and Comdb2 determines the "absolute"
time value and stores.  The same time value can be retrieved afterwards using the same timezone (in which 
case the same value will be returned) or a different timezone (in which case the value will be adjusted for the 
new timezone).

Example:

Let say somebody in NYC enters the time 01:00PM (as Eastern Standard Time). One second later someone in Seattle retrieves the same time (using Pacific Standard Time). The
value retrieved is 10:01AM. This shows how the db handles automatically the timezone differences.
Another client in Seattle could retrieve the data using NYC time (by specifying the EST timezone for that specific record) or any other zone.
Two clients sharing same longitude coordinates, but obeying different daylight saving rules or different GMT offsets, are able to exchange time values in the same fashion (support for daylight savings and GMT offset anomalies).

## Timezone specification

Each time the application stores or retrieves a datetime field, it does so using a local time value corresponding 
to a certain timezone.  There are a couple of ways to set the timezone:

* Implicit mode: the simplest thing is to do nothing; the client will use the machine timezone to determine the 
  locale in this order:
    * Use ```COMDB2TZ``` environment variable if exists
    * Use ```TZ``` environment variable if exists
    * Use ```America/New_York``` as a final fallback
* Specify per session:
   * Running ["```SET TIMEZONE```](sql.html#set-timezone) *yourtimezone*" establishes a default for that SQL
     connection.
* Explicit mode: while inserting records, include a timezone string that can be used to specify the locale for that 
  value specifically (example: "2008-01-01T120101.000 US/Eastern").  If binding ```datetime```/```datetimeus``` values,
  and the timezone is left empty, the session/environment value will be used.

The order of precedence for determining the effective timezone name is:

* Explicit mode (for updates only, no way to supply this for a ```SELECT```)
* Session mode
* Implicit mode

While retrieving datetime values, you need to use either the implicit or the session based timezone to get the 
values in the expected timezone.  If you use the implicit local timezone, the same code running on machines 
with different timezone values will end up inserting/fetching different values.  For setting the timezone,
keep in mind that [```SET``` statements are deferred](transaction_model.html#immediate-and-deferred-statements). If 
you supply an invalid timezone, you won't get an error until the next statement that uses it.

## Using datetimes and intervals in SQL

Examples in this section will use this table definition:

```
schema { 
        datetime timestamp                                                                                     
}

keys { 
        "UUID"      = uuid                                                                                         
        "TIMESTAMP" = timestamp                                                                            
        "UUID_TM"   = uuid+timestamp                                                              
} 
```

### Retrieving a datetime value

* ```SELECT timestamp FROM t ```: returns a value as a cdb2_client_datetime_t structure (```CDB2_DATETIME```)
* ```SELECT utimestamp FROM t ```: returns a value as a cdb2_client_datetimeus_t structure (```CDB2_DATETIMEUS```)
* ```SELECT CAST(timestamp AS text) FROM t```: returns the datetime in string format (ISO 8601). (```CDB2_CSTRING```)
* ```SELECT CAST(timestamp AS int) FROM t```: returns the epoch seconds as int. (```CDB2_INTEGER```)
* ```SELECT CAST(timestamp AS real) FROM t```: returns the epoch seconds and msec as fractional part. (```CDB2_REAL```)

### Inserting a datetime value

* Implicit conversion:
    * ```INSERT INTO t (timestamp) VALUES ("2007-10-01") ```
* Explicit conversion:
    * ```INSERT INTO t (timestamp) VALUES (CAST("2007-10-01" as datetime)) ```
* now():
    * ```INSERT INTO t (timestamp) VALUES (now()) ```: insert the current time as datetime record.

### Intervals

Intervals can be specified in sql using string or numerical format described above (the numerical format being 
number of months for year-to-month format and number of seconds.milliseconds  for day-to-second format).

In addition, the following casting can be used to specify intervals (mostly used with datetime arithmetic):

* ```CAST(number AS year)``` interprets "number" as number of years, generating a year-to-month interval
* ```CAST(number AS month)``` interprets "number" as number of months, generating a year-to-month interval
* ```CAST(number AS days)``` interprets "number" as number of days, generating a day-to-second interval
* ```CAST(number AS hours)``` interprets "number" as number of hours, generating a day-to-second interval
* ```CAST(number AS minutes)``` interprets "number" as number of minutes, generating a day-to-second interval
* ```CAST(number AS seconds)``` interprets "number" as number of seconds, generating a day-to-second interval

### SQL Arithmetic

The following operations (or a combinations of) are valid:

* interval values can be added and subtracted to/from a datetime value, generating a new datetime value
* intervals values can be multiplied and divided by a numeric value, generating a new interval value (example, ```cast(1 as day) * 3  == cast(3 as day)```)
* intervals of the same type can be added, generating a new interval value

Examples:

* ```SELECT CAST("2001-01-02" AS datetime) + CAST("10" AS year) ```: generates "2011-01-02" datetime.
* ```SELECT CAST("2001-01-02" AS datetime) + CAST("10" AS day) ```: generates "2001-01-12" datetime.
* ```SELECT CAST("2001-12-31T23:59:59.999" AS datetime) + CAST(0.001 AS seconds)```: generates "2002-01-01" datetime.

### SQL Functions

* ```now()``` - provides a datetime value equal with the timestamp value for the moment when this function is evaluated
    *   `now()` or `now('ms')` produces millisecond-precision datetime
    *   `now('us')` produces microsecond-precision datetime
* ```year(timestamp)``` - retrieves the year from a datetime value
* ```month(timestamp)``` -retrieves the month from a datetime value
* ```day(timestamp)``` -retrieves the day from a datetime value
* ```hour(timestamp)``` -retrieves the hour from a datetime value
* ```minute(timestamp)``` -retrieves the minutes from a datetime value
* ```second(timestamp)``` -retrieves the seconds from a datetime value
* ```days(timestamp, 'type')``` -retrieves the number of days as a function of type:
    *   if ```type```=='w',  since the beginning of the week
    *   if ```type```=='m',  since the beginning of the month
    *   if ```type```=='y',  since the beginning of the year
    *  Example: ```select days(cast("2010-01-02" as datetime), x)``` returns 6 for x='w', and 1 for x=='m' or x=='y'. 


## Microsecond-precision Datetimes and Intervals

Comdb2 supports microsecond-precision datetime and interval types. The type names for these 2 types are 
```datetimeus``` and ```intervaldsus```, respectively.

```datetimeus``` and ```intervaldsus``` can be considered as an extension of their corresponding 
millisecond-precision type, with larger fractional second precision.

### Example

```shell
cdb2sql> create csc2 table t { schema { datetimeus v } }
[create csc2 table t { schema { datetimeus v } }] rc 0
cdb2sql> insert into t values(now('us'))
(rows inserted=1)
[insert into t values(now())] rc 0
cdb2sql> insert into t values('2222-2-2T2:2:2.222222')
(rows inserted=1)
[insert into t values('2222-2-2T2:2:2.222222')] rc 0
cdb2sql> select * from t
(v="2016-06-09T180734.045056 America/New_York")
(v="2222-02-02T020202.222222 America/New_York")
[select * from t] rc 0
```

### Expressing Datetimeus and Intervaldsus in String Format

| | String Format | Precision | Fraction Range |
|----|---------------|-----------|----------------|
|datetimeus|```YYYY-MM-DDThh:mm:ss[.ssssss] <timezone string>```|6 digits. Excess precision is silently discarded | 000000 - 999999 |
|intervaldsus|```[- ]DAYS HH:MM:SS[.ssssss] <timezone string>```|6 digits. Excess precision is silently discarded | 000000 - 999999 |
