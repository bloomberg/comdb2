#if defined _IBM_SOURCE
#include "db_config_ibm.h"
#elif defined _LINUX_SOURCE
#include "db_config_linux.h"
#elif defined _SUN_SOURCE
#include "db_config_sun.h"
#elif defined _HP_SOURCE
#include "db_config_hp.h"
#else
#error "Unknown architecture"
#endif
