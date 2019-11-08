#include <stdio.h>
#include <cdb2api.h>

static void *my_simple_hook(cdb2_hndl_tp *hndl, void *user_arg, int argc, void **argv)
{
    puts("The event is registered by the dynamically loaded library");
    return NULL;
}

void cdb2_lib_init(void)
{
    puts("Registering BEFORE_SEND_QUERY event");
    cdb2_register_event(NULL, CDB2_BEFORE_SEND_QUERY, 0, my_simple_hook, NULL, 0);
}
