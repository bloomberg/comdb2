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

#include "sc_chain.h"
#include "schemachange.h"
#include "logmsg.h"
#include <math.h>

void append_to_chain(struct schema_change_type *sc, struct schema_change_type *sc_chain_next){
    if (sc->sc_chain_next){
        append_to_chain(sc->sc_chain_next, sc_chain_next);
    } else {
        sc->sc_chain_next = sc_chain_next;
    }
}

void add_next_to_chain(struct schema_change_type *sc, struct schema_change_type *sc_chain_next){
    if (sc_chain_next->sc_chain_next){
        add_next_to_chain(sc, sc_chain_next->sc_chain_next);
    }
    sc_chain_next->sc_chain_next = sc->sc_chain_next;
    sc->sc_chain_next = sc_chain_next;
}