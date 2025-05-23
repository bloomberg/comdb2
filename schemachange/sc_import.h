/*
   Copyright 2025 Bloomberg Finance L.P.

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

#ifndef INCLUDE_SC_IMPORT_H
#define INCLUDE_SC_IMPORT_H

int do_import(struct ireq *iq, struct schema_change_type *s,
                   tran_type *tran);
int finalize_import(struct ireq *iq, struct schema_change_type *s,
                         tran_type *transac);

enum bulk_import_validation_rc {
    BULK_IMPORT_VALIDATION_OK = 0,
    BULK_IMPORT_VALIDATION_WARN,
    BULK_IMPORT_VALIDATION_FATAL,
};

// validates bulk import inputs
// returns
//      VALIDATION_OK if all inputs are valid
//      VALIDATION_WARN if table names or database names have invalid characters
//      VALIDATION_FATAL if machine names have invalid characters
enum bulk_import_validation_rc validate_bulk_import_inputs(const char * const tablename, const char * const bulk_import_src_mach,
    const char * const bulk_import_src_dbname, const char * const bulk_import_src_tablename);

#endif
