/*
   Copyright 2017, Bloomberg Finance L.P.

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

#ifndef _INCLUDED_PORT_POSIX_H_
#define _INCLUDED_PORT_POSIX_H_

#include "berksockets.h"

/* Unix-style Paths */
static char CDB2DBCONFIG_NOBBENV[512] = "/opt/bb/etc/cdb2/config/comdb2db.cfg";
/* The real path is COMDB2_ROOT + CDB2DBCONFIG_NOBBENV_PATH */
static char CDB2DBCONFIG_NOBBENV_PATH[] = "/etc/cdb2/config.d/";
static char CDB2DBCONFIG_TEMP_BB_BIN[512] = "/bb/bin/comdb2db.cfg";

#endif
