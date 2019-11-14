/* all macc's globals */
#include "macc.h"
#include "maccparse.h"
#include <string.h>


char *blankchar = "";
char sync_names[4][8] = {"none", "full", "normal", "source"};

macc_globals_t *macc_globals;
int any_errors;
int current_line;
struct constant constants[MAX];


void init_globals()
{
    printf("AZ: %s entering\n", __func__);
    if (macc_globals) {
        abort();
        free(macc_globals);
    }
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
    macc_globals->opt_cachesz = 300;
    macc_globals->opt_noprefix = 2;    /* Don't generate prefixes for variables */
    macc_globals->opt_threadsafe = 1;  /* By default, access routine is thread-safe */
    macc_globals->opt_filesfx = ""; /* Include file suffix..By default disabled */
    macc_globals->opt_schematype = 2;
    macc_globals->nconstraints = -1;
    macc_globals->n_check_constraints = -1;
}
