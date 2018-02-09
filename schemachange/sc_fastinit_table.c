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

#include <memory_sync.h>
#include <autoanalyze.h>
#include <translistener.h>

#include "schemachange.h"
#include "sc_fastinit_table.h"
#include "sc_schema.h"
#include "sc_struct.h"
#include "sc_csc2.h"
#include "sc_global.h"
#include "sc_logic.h"
#include "sc_callbacks.h"
#include "sc_records.h"
#include "sc_drop_table.h"
#include "sc_add_table.h"

int do_fastinit(struct ireq *iq, struct schema_change_type *s, tran_type *tran)
{
    return do_drop_table(iq, s, tran);
}

int finalize_fastinit_table(struct ireq *iq, struct schema_change_type *s,
                            tran_type *tran)
{
    int rc = 0;
    extern int gbl_broken_max_rec_sz;
    int saved_broken_max_rec_sz = gbl_broken_max_rec_sz;

    if (s->db->lrl > COMDB2_MAX_RECORD_SIZE) {
        // we want to allow fastiniting this tbl
        gbl_broken_max_rec_sz = s->db->lrl - COMDB2_MAX_RECORD_SIZE;
    }

    rc = finalize_drop_table(iq, s, tran) || do_add_table(iq, s, tran) ||
         finalize_add_table(iq, s, tran);

    gbl_broken_max_rec_sz = saved_broken_max_rec_sz;

    return rc;
}
