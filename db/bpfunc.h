#ifndef INCLUDE_BPLOG_FUNC_H
#define INCLUDE_BPLOG_FUNC_H

#include <comdb2.h>
#include <osqlsession.h>
#include <sql.h>
#include <bpfunc.pb-c.h>

enum { MSGERR_MAXLEN = 256 };

enum {
    BPFUNC_SC = 0,
    BPFUNC_CREATE_TIMEPART = 1,
    BPFUNC_DROP_TIMEPART = 2,
    BPFUNC_GRANT = 3,
    BPFUNC_PASSWORD = 4,
    BPFUNC_AUTHENTICATION = 5,
    BPFUNC_ALIAS = 6,
    BPFUNC_ANALYZE_THRESHOLD = 7,
    BPFUNC_ANALYZE_COVERAGE = 8,
    BPFUNC_TIMEPART_RETENTION = 9,
    BPFUNC_ROWLOCKS_ENABLE = 10,
    BPFUNC_GENID48_ENABLE = 11,
    BPFUNC_SET_SKIPSCAN = 12
};

typedef struct bpfunc bpfunc_t;
typedef int (*bpfunc_prot)(void *tran, bpfunc_t *arg, char *err);
typedef struct bpfunc_user_info { void *iq; } bpfunc_info;

int bpfunc_init(void *tran, int32_t function_id, int32_t data_len, bpfunc_info *info);
void free_bpfunc(bpfunc_t *func);
void free_bpfunc_arg(BpfuncArg *arg);

int bpfunc_prepare(bpfunc_t **func, void *tran, int32_t data_len,
                   uint8_t *data, bpfunc_info *info);

struct bpfunc {
    BpfuncArg *arg;
    bpfunc_prot exec;
    bpfunc_prot success;
    bpfunc_prot fail;
    bpfunc_info *info;
};

#endif
