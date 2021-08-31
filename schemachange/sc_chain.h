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

#ifndef _SC_CHAIN_H_
#define _SC_CHAIN_H_

#include "schemachange.h"

void append_to_chain(struct schema_change_type *sc, struct schema_change_type *sc_chain_next);
void add_next_to_chain(struct schema_change_type *sc, struct schema_change_type *sc_chain_next);
#endif