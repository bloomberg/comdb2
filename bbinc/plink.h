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

#ifndef INCLUDED_PLINK_H
#define INCLUDED_PLINK_H

/* this plink timestamp module is generated at plink time.
   you can query the values at runtime with plink_constant() func.
   07/19/02

'"@(#)plink TIME: $(PLINK_DATE)",'\
'"@(#)plink LOGNAME: $(PLINK_LOGNAME)", '\
'"@(#)plink HOSTNAME: $(HOSTNAME)", '\
'"@(#)plink TASK: $(TASK)",'\
'"@(#)plink SNAME: $(SNAME)",'\
'"@(#)plink MAKEFILE: $(PLINK_MAKEFILE)", 0 };'

*/

#if defined __cplusplus
extern "C" {
#endif

/* returns char string for plink constant or 0 */
const char *plink_constant(int which);
/* fortran routine for the above. string returned in outs */
void plink_constant_(const int *which, char *outs, int *rcode, int len);
/* routine to print the given plink constant */
void dump_plink_constant(int which);
void dump_plink_constant_(int *which);
/* routine to print all plink constants */
void dump_plink_constants(void);
void dump_plink_constants_(void);
/* routine to allow iterating over all the plink constants */
/* YOU MUST UPDATE THE 'next' ARG YOURSELF (WITH THE FIRST */
/* CALL SETTING IT TO 0) AND YOU MUST STOP */
/* WHEN API RETURNS 'null': */
const char *plink_constant_iterate_next(int nxt);

enum PLINK_TIMESTAMP_CONSTS {
    PLINK_TIME = 0,
    PLINK_LOGNAME = 1,
    PLINK_HOSTNAME = 2,
    PLINK_TASK = 3,
    PLINK_SNAME = 4,
    PLINK_MAKEFILE = 5,
    PLINK_____END
};

#if defined __cplusplus
}
#endif

#endif
