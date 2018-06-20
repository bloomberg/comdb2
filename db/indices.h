/*
   Copyright 2015, 2018, Bloomberg Finance L.P.

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

#ifndef INCLUDED_INDICES_H
#define INCLUDED_INDICES_H


#include <inttypes.h>
#include "cdb2_constants.h"

//
//del needs to sort before adds because dels used to happen online
enum ctktype {CTK_DEL, CTK_UPD, CTK_ADD};
typedef struct {
    struct dbtable *usedb;
    short ixnum;
    char ixkey[MAXKEYLEN];
    uint8_t type;
    unsigned long long genid;
    unsigned long long newgenid; // new genid used for update
} ctkey;

#endif
