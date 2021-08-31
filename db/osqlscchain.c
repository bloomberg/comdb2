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


#include "osqlscchain.h"
#include <schemachange.h>
#include "logmsg.h"
#include <sc_chain.h>
#include <math.h>
#include <sc_queues.h>
#include <mem_override.h>

// Gets the table name from the trigger csc2
static char *get_trigger_table_name(char *newcsc2){
    newcsc2 += strlen("table ");
    int len = 0;
    while(newcsc2[len] != '\n'){len++;}
    char *table_name = malloc((len + 1) * sizeof(char));
    strncpy(table_name, newcsc2, len);
    table_name[len] = '\0';
    return table_name;
}

extern int gbl_audit_trigger_debug;

static struct schema_change_type *create_audit_table_sc(char *name){
	struct schema_change_type *sc = new_schemachange_type();
    sc->sc_chain_next = NULL;

    // It's totally possible any of the immediatly below section is wrong
    sc->onstack = 1;
	sc->type = DBTYPE_TAGGED_TABLE;
	sc->scanmode = gbl_default_sc_scanmode;
    sc->live = 1;

	sc->addonly = 1;

	char *prefix = "$audit_";
	strcpy(sc->tablename, prefix);
	strcat(sc->tablename, name);
	struct dbtable *db = get_dbtable_by_name(name);
	sc->newcsc2 = get_audit_schema(db->schema);

    if (db->instant_schema_change) sc->instant_sc = 1;

	// What is ODH? This is just copied from timepart
	if (db->odh) sc->headers = 1;

    make_name_available(sc->tablename);

	return sc;
}

static struct schema_change_type *gen_audit_lua(char *table_name, char *spname){
	char *code_start =
    "local function main(event)\n"
    "	local audit = db:table('";
    char * code_end =
        "')\n"
    "    local chg = {}\n"
    "    if event.new ~= nil then\n"
    "        for k, v in pairs(event.new) do\n"
    "            chg['new_'..k] = v\n"
    "        end\n"
    "    end\n"
    "    if event.old ~= nil then\n"
    "        for k, v in pairs(event.old) do\n"
    "            chg['old_'..k] = v\n"
    "        end\n"
    "    end\n"
    "    chg.type = event.type\n"
    "    chg.tbl = event.name\n"
    "    chg.logtime = db:now()\n"
    "    return audit:insert(chg)\n"
    "end\n";
    char *code = malloc((strlen(code_start) + strlen(code_end) + strlen(table_name) + 1) * sizeof(char));
    strcpy(code, code_start);
    strcat(code, table_name);
    strcat(code, code_end);

    struct schema_change_type *sc = new_schemachange_type();
    strcpy(sc->tablename, spname);
    sc->addsp = 1;
    sc->newcsc2 = code;
    strcpy(sc->fname, "built-in audit");
	return sc;
}

static struct schema_change_type *populate_audit_trigger_chain(struct schema_change_type *sc){
    char *tablename = get_trigger_table_name(sc->newcsc2);
    struct schema_change_type *sc_full = create_audit_table_sc(tablename);
    struct schema_change_type *sc_proc = gen_audit_lua(sc_full->tablename, sc->tablename + 3);
    append_to_chain(sc_full, sc_proc);
    append_to_chain(sc_full, sc);
    sc->dont_expand = 1;
    sc->audit_table = sc_full->tablename;
    sc->trigger_table = tablename;
    return sc_full;
}

extern int gbl_carry_alters_to_audits;

static struct schema_change_type *make_audit_alters_nothrevent(struct schema_change_type *sc, int *failed){

    char **audits;
    int num_audits;

    if (bdb_get_audit_sp_tran(sc->tran, sc->tablename, &audits, &num_audits, TABLE_TO_AUDITS)){
        *failed = 1;
    } else if(num_audits > 0) {
        // If the alters are being carried through, you need to do them synchronously
        sc->nothrevent = 1;
    }
    return sc;

}

struct schema_change_type *populate_sc_chain(struct schema_change_type *sc, int *failed){
    *failed = 0;
    if (sc->dont_expand){
        return sc;
    } if (sc->is_trigger == AUDIT_TRIGGER) {
        return populate_audit_trigger_chain(sc);
    } else if (sc->alteronly && sc->newcsc2 && gbl_carry_alters_to_audits) {
        return make_audit_alters_nothrevent(sc, failed);
    } else {
        return sc;
    }
}
