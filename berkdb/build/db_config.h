#if defined _LINUX_SOURCE
#include "db_config_linux.h"
#elif defined _SUN_SOURCE
#include "db_config_sun.h"
#else
#error "Unknown architecture"
#endif
