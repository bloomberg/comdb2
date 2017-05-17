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

/* EPOCHLIB.H - RETURNS DIFFERENT STYLES OF TIMESTAMP BASED ON EPOCH */
/* paul x1552 */
#ifndef INCLUDED_EPOCHLIB_H
#define INCLUDED_EPOCHLIB_H

#include <inttypes.h>

#ifdef __cplusplus
extern "C" {
#endif

/* RETURNS # SECONDS SINCE 1/1/1970 */
void time_epoch_(int32_t *epoch);
int time_epoch(void);

/* RETURNS # MILLISECONDS SINCE TIM.TSK STARTED */
void time_epochms_(int32_t *epochms);
int time_epochms(void);

/* REPLACES ICLOCK ROUTINE
   *parm 0, arr[]= hour,min,sec
   *parm 1  arr[]= "hh:mm:ss\0"
   *parm 2  arr[]= hr*3600+min*60+sec
*/
void eclock_(int *parm, int arr[3]);

/* RETURN LOCAL TIME/DATE/DAY GIVEN EPOCH, OR NOW IF EPOCH IS 0 */
void elcltmdt_(int *epoch, int time[3], int date[3], int *day);

/* USES SAME TIME FORMAT STRING AS STRFTIME */
void time_string_(char *output, char *format, int olen, int flen);

/* USES SAME TIME FORMAT STRING AS STRFTIME */
void time_stringt_(int *nowp, char *output, char *format, int olen, int flen);

/* REPLACES DATE ROUTINE */
void edate_(int arr[]); /* return date as 3i4 yy dd mm */
void hdate_(int arr[]); /* return date as 3i4 yyyy dd mm */

#ifdef __cplusplus
}
#endif

#endif
