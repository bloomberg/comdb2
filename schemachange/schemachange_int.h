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

#ifndef INCLUDE_SCHEMACHANGE_INT_H
#define INCLUDE_SCHEMACHANGE_INT_H

/**  OLD IMPORT FROM schemachange.c
one day these will be moved to the respective .c and .h files, hopefully */

//#include <largefile.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <alloca.h>
#include <stdarg.h>
#include <string.h>
#include <strings.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/statvfs.h>
#include <time.h>
#include <limits.h> /* for UINT_MAX */
#include <poll.h>
#include <ctrace.h>
#include <bdb_api.h>
#include <bdb_fetch.h>
#include <sbuf2.h>
#include <str0.h>
#include <dynschematypes.h>
#include <dynschemaload.h>
#include <memory_sync.h>
#include <endian_core.h>
#include "comdb2.h"

#include "tag.h"
#include "util.h"
#include "osqlrepository.h"

#include <genid.h>
#include <bdb_schemachange.h>
#include <stdbool.h>

#include "mem_schemachange.h"
#include "mem_override.h"

/***************************************************/

/* This is done for simplicity so that one does not
have include each header */
#include "sc_add_table.h"
#include "sc_alter_table.h"
#include "sc_callbacks.h"
#include "sc_csc2.h"
#include "sc_delete_table.h"
#include "sc_global.h"
#include "sc_logic.h"
#include "sc_lua.h"
#include "sc_net.h"
#include "sc_queues.h"
#include "sc_records.h"
#include "sc_schema.h"
#include "sc_stripes.h"
#include "sc_struct.h"
#include "sc_util.h"
#include "views.h"

#endif
