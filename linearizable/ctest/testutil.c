#include <unistd.h>
#include <ctype.h>
#include <string.h>
#include <strings.h>
#include <pthread.h>
#include <testutil.h>
#include <stdint.h>
#include <stdarg.h>

#define FMTSZ 512
void tdprintf(FILE *f, cdb2_hndl_tp *db, const char *func, int line, const char *format, ...)
{
    va_list ap;
    char fmt[FMTSZ];
    char buf[128];

    fmt[0] = '\0';

    if (strncasecmp(format, "XXX ", 3) == 0) {
        strcat(fmt, "XXX ");
    }

    snprintf(buf, sizeof(buf), "td %u ", (uint32_t)pthread_self());
    strcat(fmt, buf);

    snprintf(buf, sizeof(buf), "handle %p ", db);
    strcat(fmt, buf);

    snprintf(buf, sizeof(buf), "%s:%d ", func, line);
    strcat(fmt, buf);
    
    snprintf(buf, sizeof(buf), "cnonce '%s' ", db ? cdb2_cnonce(db) : "(null)");
    strcat(fmt, buf);

    int file = -1, offset = -1;
    if (db) 
        cdb2_snapshot_file(db, &file, &offset);
    snprintf(buf, sizeof(buf), "snapshot_lsn [%d][%d] ", file, offset);
    strcat(fmt, buf);

    strcat(fmt, format);
    va_start(ap, format);
    vfprintf(f, fmt, ap);
    va_end(ap);
}


