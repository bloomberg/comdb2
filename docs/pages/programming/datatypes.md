---
title: Database types
keywords:
tags:
sidebar: mydoc_sidebar
permalink: datatypes.html
---

Comdb2 supports 6 basic types:

  1. INTEGER
  1. REAL
  1. TEXT
  1. BLOB
  1. DATETIME
  1. INTERVAL

For purposes of storage and permissible values, these types have multiple subtypes.  The following is a list of 
all supported types, their value ranges, and other related information.  Note that in addition to a valid value 
of a given type, any column can hold an additional value of ```NULL```.

## Integer types

These types are basic fixed size integer.  When queried via the [C api](c_api.html), they are returned as a 
```CDB2_INTEGER``` value, which corresponds to a C type of ```int64_t```.

|Type|SQL Datatype|Description|Range|
|---|---|---|---|
|```short```|```CDB2_INTEGER```| 2 byte signed integer|-32768 to 32767 | |
|```u_short```|```CDB2_INTEGER```|2 byte unsigned integer|0 to 65535 | |
|```int```|```CDB2_INTEGER```|4 byte signed integer|-2147483648 to 2147483647 | |
|```u_int```|```CDB2_INTEGER```|4 byte unsigned integer|0 to 4294967295 | |
|longlong|```CDB2_INTEGER```|8 byte signed integer|-9223372036854775808 to 9223372036854775807 | |

## Floating point types

These are floating point types.  They return ```CDB2_REAL``` values (```double```).

|Type|SQL Datatype|Description|Range|
|---|---|---|---|
|float|```CDB2_REAL```|4 byte signed IEEE Floating Point|-3.402823466E+38 to -1.175494351E-38 and 1.175494351E-38 to 3.402823466E+38 and NaN and +Inf and -Inf | |
|double|```CDB2_REAL```|8 byte signed IEEE Floating Point| -1.7976931348623157E+308 to -2.2250738585072014E-308 and 2.2250738585072014E-308 to 1.7976931348623157E+308 and and NaN and +Inf and -Inf |  |

## Decimal types

Decimal types are decimal representations for real numbers. They provide support for arithmetic on decimal numbers without loss of 
precision as long as the required number of digits can be accommodated by the format limitations of exponent and 
significant. See the [decimal types](decimals.html) section for more details.  When queried, they return 
```CDB2_CSTRING``` (string) values to avoid losing precision.  

|Type|SQL Datatype|Description|Range|
|---|---|---|---|
|decimal32| CDB2_CSTRING |  Single precision decimal number | Exponent in in the range -95 to +96, and the significant has 7 digits.  It supports numbers in range +-0.000000x10^-95 to +-9.999999x10^96.  | 
|decimal64| CDB2_CSTRING |   Double precision decimal number | Exponent in in the range -383 to +384, and the significant has 16 digits.  It supports numbers in range +-0.000000000000000x10^-383 to +-9.999999999999999x10^384  | 
|decimal128| CDB2_CSTRING |  Quad precision decimal number | Exponent in in the range -6143 to +6144, and the significant has 34 digits.  It supports numbers in range +-0.000000000000000000000000000000000x10^-6143 to +-9.999999999999999999999999999999999x10^6144  |


## Blob types

Both ```BLOB``` types in Comdb2 are treated by the database engine as uninterpreted byte arrays. They are not 
convertible to any other type.  ```BYTE``` fields must be declared with a size.  When specifying field values, the value 
must match the size defined in the schema.  Blobs are not declared with a size.  Instead they can specify a size hint.
Values of this size or smaller are stored inline with the rest of the record's fields.  Larger values are stored in
a separate entity (and are thus slower to access). Another restriction on ```BLOB``` fields is that they may not
be used as a part of any key. ```BYTE``` fields can be used in keys. 

|Type|SQL Datatype|Description|Range|
|---|---|---|---|
|byte|```CDB2_BLOB```|Arbitrary binary data |Blobs may be 0 bytes to the maximum record size (16k)|
|blob|```CDB2_BLOB```|Arbitrary binary data | 0 bytes to 255MB

## Text types

There are 2 text types. ```CSTRING``` is a fixed-size field.  The size must be declared in the table schema 
definition.  Values smaller than the declared size are allowed. Larger values are not.  No validation is done on the
string value, except that it may not contain 0x00 bytes.

```VUTF8``` is a variable length field.  Like ```BLOB```, the field can be optionally declared with a size hint.
Values smaller than that size are stored inline in the record.  Values larger (up to 255MB) are stored in a separate
entity, with the associated access cost.  ```VUTF8``` fields may not be a part of a key.  ```VUTF8``` values must
be valid UTF-8.  Strings that contain invalid characters are rejected.  ```VUTF8``` values may not contain 0x00 bytes.

Both ```CSTRING``` and ```VUTF8``` types are returned to applications as CDB2_CSTRING types.
They both return the size of the value which includes the null-terminator. On input, the value
gets truncated at the first 0x00 (if any) and anything beyond that gets discarded.

|Type|SQL Datatype|Description|Range|
|---|---|---|---|
|cstring|```CDB2_CSTRING```|A C style string with NULL termination| 0 or more characters terminated with '\0' up to the record size (16k) |

## Datetime types

Comdb2 datetime types store the amount of time that has passed since the start of the time epoch 
(```1970-01-01T00:00:00.000 GMT```).  On the way into the database, datetime values specify the local timezone.  Time 
is converted from that timezone to UTC and stored.  At query time, the application will pass its local timezone.
Times are converted to this timezone before being sent to the application.  Timezone information is not stored
with the time value.  Datetime can be stored with either millisecond or microsecond precision, depending on the
type specified in the table schema.

The return type from the database for these types is a structure that contains fields for datetime components
(day, month, etc.):

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
```
Datetime fields are represented into a compacted form on disk. Applications use either a ```cdb2_client_datetime_t``` structure, a ```cdb2_client_datetimeus_t```
structure, or a string using a ISO 8601 modified format.  An integer value converted to/from a datetime represents the number of seconds since the start of 
the epoch.  A real value is seconds (integer portion) and milliseconds (fraction portion).  

If the client specified a timezone, the datetime information is relative to this point local time.  The client can specify a timezone inline with the 
value (eg: ```2016-10-09T19:22:23 America/New_York```), or set it per connection with [```SET TIMEZONE```](sql.html#set-timezone).  In the absence of 
both, the client will send the value of the ```$COMDB2_TZ``` environment variable,
then the ```$TZ``` environment variable.  The final fallback value is ```America/New_York```.
  
Learn more about [datetime datatypes here](datetime.html).

|Type|SQL Datatype|Client datatype|Range|
|----|------------|---------------|-----|
|datetime|```CDB2_DATETIME```|```cdb2_client_datetime_t```|-9999-01-01T235959.000 GMT, 9999-12-31T000000.000 GMT
|datetimeus|```CDB2_DATETIMEUS```|```cdb2_client_datetimeus_t```|-9999-01-01T235959.000000 GMT, 9999-12-31T000000.000000 GMT

## Interval types

Interval datatypes represent the amount of time that passed between two absolute points in time.  Subtracting datetime values results in an interval.
There are 2 classes of intervals - years to months and days to seconds. This split is necessary since a "month" is a variable amount of time.

Learn more about [interval datatypes here](datetime.html).

|Type      | SQL datatype | Client datatype | Example         |
|----------|--------------|-----------------|-----------------|
|intervalym|```CDB2_INTERVALYM```|```client_intv_ym_t```|"YEARS-MONTHS", cast(number as year), cast(number as month)|
|intervalds|```CDB2_INTERVALDS```|```client_intv_ds_t```| "DAYS HH:MM:SS.MSS", cast(number as days), cast(number as hours), cast(number as minutes), cast(number as seconds) |
|intervaldsus|```CDB2_INTERVALDSUS```|```client_intv_dsus_t```|"DAYS HH:MM:SS.USSSSS", cast(number as days), cast(number as hours), cast(number as minutes), cast(number as seconds) |
