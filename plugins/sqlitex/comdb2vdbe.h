#ifndef COMDB2VDBE_H
#define COMDB2VDBE_H

#include "sqliteInt.h"
#include "vdbe.h"

typedef struct OpFuncSetup OpFuncSetup;

int initOpFuncX(OpFunc* o, size_t len);
int initOpFuncXDefault(OpFunc* o);
void freeOpFuncX(OpFunc* o);
int moreRecordsX(OpFunc* opf);
char* nextStringX(OpFunc* opf, Mem* mem);
int nextIntegerX(OpFunc* opf);
double nextRealX(OpFunc* opf);
int opFuncPrintfX(OpFunc* arg, char* format, ...);
int opFuncWriteIntegerX(OpFunc* opf, int val);
int opFuncWriteRealX(OpFunc* opf, double val);
int comdb2VdbeAddExecFuncX(Vdbe *p, Parse *pParse, void* arg, 
    vdbeFunc func, vdbeFuncArgFree dest);
int comdb2prepareOpFuncX(Vdbe* v, Parse* pParse, int int_arg, void *arg, 
    vdbeFunc func, vdbeFuncArgFree freeFunc, OpFuncSetup *settings);
int comdb2prepareRstMsgX(Vdbe* v, Parse* pParse, int int_arg, void *arg,
    vdbeFunc func, vdbeFuncArgFree freeFunc);
int comdb2prepareSStringX(Vdbe* v, Parse* pParse, int int_arg, void *arg,
    vdbeFunc func, vdbeFuncArgFree freeFunc);
int comdb2prepareNoRowsX(Vdbe* v, Parse* pParse, int int_arg, void *arg,
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
    char**  cols_names;
    int*    cols_types;
    int     buf_size;
};

struct rstMsg
{
    int rc;
    int staticMsg;
    char* msg;
};

int comdb2GenerateRstMsgX(OpFunc *f);
void free_rstMsgX(struct rstMsg* rec);

void resolveTableNameX(struct SrcList_item *p, char *zDb, char *tablename);

#endif // COMDB2VDBE_H
