#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stddef.h>

#include <fcntl.h>
#include <unistd.h>
#include <limits.h>

#include "logmsg.h"

extern "C" {

#define TOOL(x) #x,

#define TOOLS           \
   TOOL(cdb2_dump)      \
   TOOL(cdb2_printlog)  \
   TOOL(cdb2_sqlreplay) \
   TOOL(cdb2_stat)      \
   TOOL(cdb2_verify)    \
   TOOL(cdb2sockpool)   \
   TOOL(cdb2sql)        \
   TOOL(comdb2ar)       \

#undef TOOL
#define TOOL(x) int tool_ ##x ##_main(int argc, char *argv[]);

TOOLS

#undef TOOL
#define TOOL(x) { #x, tool_ ##x ##_main },

struct tool {
   const char *tool;
   int (*main_func)(int argc, char *argv[]);
};

struct tool tool_callbacks[] = {
   TOOLS
   NULL
};

static char* last_path_component(char *argv0) {
   char *s;
   s = strrchr(argv0, '/');
   if (s)
      return strdup(s+1);
   else
      return strdup(argv0);
}

int comdb2_main(int argc, char *argv[]);
int comdb2ma_init(int, int);

}

/* multicall wrapper */
int main(int argc, char *argv[]) {
    int rc;
    char *exe;

    /* allocate initializer first */
    comdb2ma_init(0, 0);

    /* more reliable */
#ifdef _LINUX_SOURCE
    char fname[PATH_MAX];
    rc = readlink("/proc/self/exe", fname, sizeof(fname));
    if (rc > 0)
       exe = last_path_component(fname);
#endif
    if (exe == NULL) {
       /* more portable */
       exe = last_path_component(argv[0]);
    }

    for (int i = 0; tool_callbacks[i].tool; i++) {
       if (strcmp(tool_callbacks[i].tool, exe) == 0)
          return tool_callbacks[i].main_func(argc, argv);
    }

    comdb2_main(argc, argv);
}
