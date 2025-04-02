#ifndef COMDB2VDBE_H
#define COMDB2VDBE_H

#include "sqliteInt.h"
#include "vdbe.h"

typedef struct OpFuncSetup OpFuncSetup;

void freeOpFunc(OpFunc* o);
int moreRecords(OpFunc* opf);
char* nextString(OpFunc* opf, Mem* mem);
int nextInteger(OpFunc* opf);
double nextReal(OpFunc* opf);
int opFuncPrintf(OpFunc* arg, char* format, ...);
int opFuncWriteInteger(OpFunc* opf, int val);
int opFuncWriteReal(OpFunc* opf, double val);
int comdb2VdbeAddExecFunc(Vdbe *p, Parse *pParse, void* arg, 
    vdbeFunc func, vdbeFuncArgFree dest);
int comdb2prepareOpFunc(Vdbe* v, Parse* pParse, int int_arg, void *arg, 
    vdbeFunc func, vdbeFuncArgFree freeFunc, OpFuncSetup *settings);
int comdb2prepareRstMsg(Vdbe* v, Parse* pParse, int int_arg, void *arg,
    vdbeFunc func, vdbeFuncArgFree freeFunc);
int comdb2prepareSString(Vdbe* v, Parse* pParse, int int_arg, void *arg,
    vdbeFunc func, vdbeFuncArgFree freeFunc);
int comdb2prepareNoRows(Vdbe* v, Parse* pParse, int int_arg, void *arg,
    vdbeFunc func, vdbeFuncArgFree freeFunc);
enum
{
    OPFUNC_STRING_TYPE,
    OPFUNC_INT_TYPE,
    OPFUNC_REAL_TYPE
};

struct OpFuncSetup
{
    int     n_cols;
    const char**  cols_names;
    const int*    cols_types;
    int     buf_size;
};

struct rstMsg
{
    int rc;
    int staticMsg;
    char* msg;
};

int comdb2GenerateRstMsg(OpFunc *f);
void free_rstMsg(struct rstMsg* rec);

int resolveTableName(sqlite3 *db, struct SrcList_item *p, const char *zDb,
                     char *tablename, size_t len);

#endif // COMDB2VDBE_H
