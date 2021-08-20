/*
   Copyright 2021 Bloomberg Finance L.P.

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
#ifndef _OSQL_CHAIN_H_
#define _OSQL_CHAIN_H_

#include "comdb2.h"
#include "sbuf2.h"
#include "osqlsession.h"
#include "sqloffload.h"
#include "block_internal.h"
#include "comdb2uuid.h"

/* The idea is here you can do an initial populate. However, you can
 also add schema changes to the queues while they are being prepared
 in the schema change folder. This is critical for dynamically choosing
 schema changes based on results of previous schema changes. I think
 we still need to keep this initial populate though, as sometimes you
 need to flip schema changes so that the original schema change isn't executed
 first. */

struct schema_change_type *populate_sc_chain(struct schema_change_type *sc, int *failed);
#endif
