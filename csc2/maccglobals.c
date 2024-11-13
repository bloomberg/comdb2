/* all macc's globals */
#include "macc.h"
#include "maccparse.h"
#include <string.h>
#include "cheapstack.h"

char *blankchar = "";
char sync_names[4][8] = {"none", "full", "normal", "source"};

macc_globals_t *macc_globals;
int any_errors;
int current_line;
struct constant constants[MAX];

void dyns_init_globals()
{
    if (macc_globals) { // We want to make sure that previous call cleaned up
        cheap_stack_trace();
        abort(); // Maybe turn into a WARNING once certain that we dont leak
                 // and then free(macc_globals);
    }
    any_errors = 0;
    current_line = 0;
    memset(constants, 0, sizeof(constants));
    macc_globals = calloc(1, sizeof(*macc_globals));

    for (int i = 0; i < MAXINDEX; i++) {
        macc_globals->ixblocksize[i] = 4096;
        macc_globals->lv0size[i] = 4096;
        macc_globals->spltpercnt[i] = 50;
    }
    macc_globals->opt_dbname = blankchar;
    macc_globals->opt_incldir = blankchar;
    macc_globals->opt_dtadir = blankchar;
    macc_globals->opt_lrldir = blankchar;
    macc_globals->opt_prefix = blankchar;
    macc_globals->current_case = -1;
    macc_globals->union_index = -1;
    macc_globals->union_level = -1;
    /* macc_globals->opt_cachesz = 300; */
    /* macc_globals->opt_noprefix = 2;     Don't generate prefixes for variables
     */
    /* macc_globals->opt_threadsafe = 1; */
    macc_globals->opt_filesfx =
        ""; /* Include file suffix..By default disabled */
    macc_globals->opt_schematype = 2;
    macc_globals->nconstraints = -1;
    macc_globals->n_check_constraints = -1;
    memset(macc_globals->periods, 0, sizeof(macc_globals->periods));
    macc_globals->nperiods = 0;
}

void dyns_cleanup_globals()
{
    free(macc_globals);
    macc_globals = NULL;
}
