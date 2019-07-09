#include "comdb2.h"
#include "sql.h"

static int msys_write_response(struct sqlclntstate *a, int type, void *data, int n) {
    return 0;
}
static int msys_read_response(struct sqlclntstate *a, int b, void *c, int d)
{
    return -1;
}
static void *msys_save_stmt(struct sqlclntstate *clnt, void *arg)
{
    return strdup(clnt->sql);
}
static void *msys_restore_stmt(struct sqlclntstate *clnt, void *arg)
{
    clnt->sql = arg;
    return NULL;
}
static void *msys_destroy_stmt(struct sqlclntstate *clnt, void *arg)
{
    free(arg);
    return NULL;
}
static void *msys_print_stmt(struct sqlclntstate *clnt, void *arg)
{
    return arg;
}
static int msys_param_count(struct sqlclntstate *a)
{
    return 0;
}
static int msys_param_index(struct sqlclntstate *a, const char *b, int64_t *c)
{
    return -1;
}
static int msys_param_value(struct sqlclntstate *a, struct param_data *b, int c)
{
    return -1;
}
static int msys_override_count(struct sqlclntstate *a)
{
    return 0;
}
static int msys_override_type(struct sqlclntstate *a, int b)
{
    return 0;
}
static int msys_clr_cnonce(struct sqlclntstate *a)
{
    return -1;
}
static int msys_has_cnonce(struct sqlclntstate *a)
{
    return 0;
}
static int msys_set_cnonce(struct sqlclntstate *a)
{
    return -1;
}
static int msys_get_cnonce(struct sqlclntstate *a, snap_uid_t *b)
{
    return -1;
}
static int msys_get_snapshot(struct sqlclntstate *a, int *b, int *c)
{
    return -1;
}
static int msys_upd_snapshot(struct sqlclntstate *a)
{
    return -1;
}
static int msys_clr_snapshot(struct sqlclntstate *a)
{
    return -1;
}
static int msys_has_high_availability(struct sqlclntstate *a)
{
    return 0;
}
static int msys_set_high_availability(struct sqlclntstate *a)
{
    return -1;
}
static int msys_clr_high_availability(struct sqlclntstate *a)
{
    return -1;
}
static int msys_get_high_availability(struct sqlclntstate *a)
{
    return 0;
}
static int msys_has_parallel_sql(struct sqlclntstate *a)
{
    return 0;
}
static void msys_add_steps(struct sqlclntstate *a, double b)
{
}
static void msys_setup_client_info(struct sqlclntstate *a, struct sqlthdstate *b, char *c)
{
}
static int msys_skip_row(struct sqlclntstate *a, uint64_t b)
{
    return 0;
}
static int msys_log_context(struct sqlclntstate *a, struct reqlogger *b)
{
    return 0;
}
static uint64_t msys_get_client_starttime(struct sqlclntstate *a)
{
    return 0;
}
static int msys_get_client_retries(struct sqlclntstate *a)
{
    return 0;
}
static int msys_send_intrans_response(struct sqlclntstate *a)
{
    return 1;
}

void msys_init_default_callbacks(struct plugin_callbacks *c) {
    c->write_response = msys_write_response;
    c->read_response = msys_read_response;
    c->save_stmt = msys_save_stmt;
    c->restore_stmt = msys_restore_stmt;
    c->destroy_stmt = msys_destroy_stmt;
    c->print_stmt = msys_print_stmt;
    c->param_count = msys_param_count;
    c->param_index = msys_param_index;
    c->param_value = msys_param_value;
    c->override_count = msys_override_count;
    c->override_type = msys_override_type;
    c->has_cnonce = msys_has_cnonce;
    c->set_cnonce = msys_set_cnonce;
    c->clr_cnonce = msys_clr_cnonce;
    c->get_cnonce = msys_get_cnonce;
    c->get_snapshot = msys_get_snapshot;
    c->upd_snapshot = msys_upd_snapshot;
    c->clr_snapshot = msys_clr_snapshot;
    c->has_high_availability = msys_has_high_availability;
    c->set_high_availability = msys_set_high_availability;
    c->clr_high_availability = msys_clr_high_availability;
    c->get_high_availability = msys_get_high_availability;
    c->has_parallel_sql = msys_has_parallel_sql;
    c->add_steps = msys_add_steps;
    c->setup_client_info = msys_setup_client_info;
    c->skip_row = msys_skip_row;
    c->log_context = msys_log_context;
    c->get_client_starttime = msys_get_client_starttime;
    c->get_client_retries = msys_get_client_retries;
    c->send_intrans_response = msys_send_intrans_response;
    c->column_count = NULL;
    c->column_type = NULL;
    c->column_int64 = NULL;
    c->column_double = NULL;
    c->column_text = NULL;
    c->column_bytes = NULL;
    c->column_blob = NULL;
    c->column_datetime = NULL;
    c->column_interval = NULL;
    c->state = NULL;
    c->next_row = NULL;
}
