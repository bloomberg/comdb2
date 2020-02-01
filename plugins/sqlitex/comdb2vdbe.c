#include "comdb2vdbe.h"
#include "vdbeInt.h"
#include "vdbe.h"

/* Unfortunately needed for now */
extern pthread_key_t query_info_key;


inline int initOpFuncX(OpFunc* o, size_t len)
{
    assert(o != NULL);
    
    if (len > 0)
        o->buf = o->readNext = o->writeNext = malloc(len);
    else
        o->buf = o->readNext = o->writeNext = NULL;
    o->len = len;
    o->end = o->buf + o->len;
    return o->buf == NULL;
}

inline int initOpFuncXDefault(OpFunc* o)
{
    return initOpFuncX(o, DEFAULT_OPFUNC_BUFLEN);
}

inline void freeOpFuncX(OpFunc* func)
{
    if (!func)
        return;

    if (func->arg && func->destructor)
    {
        if (func->destructor)
            func->destructor(func->arg);
        if (func->buf)
            free(func->buf);
        sqlitex_free(func);
    }

}

static inline void shallowFreeOpFuncSetup(OpFuncSetup* stp)
{
    free(stp);
}

const int   OPFUNC_NOROWS_NCOLS = 0;
const char* OPFUNC_NOROWS_COLNAMES[] = {};
const int   OPFUNC_NOROWS_COLTYPES[] = {};
const size_t   OPFUNC_NOROWS_BUFSIZE = 0;

static inline OpFuncSetup* getNoRowsSetup()
{
    OpFuncSetup* stp = malloc(sizeof(OpFuncSetup));

    if (stp != NULL)
    {
        stp->n_cols = OPFUNC_NOROWS_NCOLS;
        stp->cols_names = (char **) OPFUNC_NOROWS_COLNAMES;
        stp->cols_types = (int *) OPFUNC_NOROWS_COLTYPES;
        stp->buf_size = OPFUNC_NOROWS_BUFSIZE;
    }
    
    return stp;
}


const int   OPFUNC_RSTMSG_NCOLS = 2;
const char* OPFUNC_RSTMSG_COLNAMES[] = {"Return Code", "Message"};
const int   OPFUNC_RSTMSG_COLTYPES[] = {OPFUNC_INT_TYPE, OPFUNC_STRING_TYPE};
const size_t   OPFUNC_RSTMSG_BUFSIZE = 255;

static inline OpFuncSetup* getRstMsgSetup()
{
    OpFuncSetup* stp = malloc(sizeof(OpFuncSetup));

    if (stp != NULL)
    {
        stp->n_cols =  OPFUNC_RSTMSG_NCOLS;
        stp->cols_names = (char **) OPFUNC_RSTMSG_COLNAMES;
        stp->cols_types = (int *) OPFUNC_RSTMSG_COLTYPES;
        stp->buf_size = OPFUNC_RSTMSG_BUFSIZE;
    }
    
    return stp;
}

const int   OPFUNC_SSTRING_NCOLS = 1;
const char* OPFUNC_SSTRING_COLNAMES[] = {"Message"};
const int   OPFUNC_SSTRING_COLTYPES[] = {OPFUNC_STRING_TYPE};
const size_t   OPFUNC_SSTRING_BUFSIZE = 255;

static inline OpFuncSetup* getSStringSetup()
{
    OpFuncSetup* stp = malloc(sizeof(OpFuncSetup));

    if (stp != NULL)
    {
        stp->n_cols = OPFUNC_SSTRING_NCOLS;
        stp->cols_names = (char **) OPFUNC_SSTRING_COLNAMES;
        stp->cols_types = (int *) OPFUNC_SSTRING_COLTYPES;
        stp->buf_size = OPFUNC_SSTRING_BUFSIZE;
    }
    
    return stp;
}

int moreRecordsX(OpFunc* opf)
{
    return opf->readNext < opf->writeNext;
}

static inline int moreIntegerRecords(OpFunc* opf)
{
    return opf->readNext + sizeof(int) < opf->writeNext;
}

static inline int moreRealRecords(OpFunc* opf)
{
    return opf->readNext + sizeof(double) < opf->writeNext;
}

int opFuncPrintfX(OpFunc* opf, char* format, ...)
{
    if (opf->end == NULL || opf->writeNext == NULL)
        return 0;

    va_list args;
    va_start(args,format);
    
    if (opf->writeNext >= opf->end)
        return 0;

    int free_space = opf->len - (opf->writeNext - opf->buf);
    int used = vsnprintf(opf->writeNext, free_space, format, args);
    opf->writeNext += used + 1;
    
    va_end(args);
    
    return used;
}

int opFuncWriteIntegerX(OpFunc* opf, int val)
{
    if (opf->end == NULL || opf->writeNext == NULL)
        return 0;   

    if (opf->writeNext + sizeof(int) >= opf->end)
        return 0;

    *((int*)opf->writeNext) = val;
    opf->writeNext += sizeof(int);

    return sizeof(int);
}

int opFuncWriteRealX(OpFunc* opf, double val)
{
     if (opf->end == NULL || opf->writeNext == NULL)
        return 0;    
    
    if (opf->writeNext + sizeof(double) >= opf->end)
        return 0;

    *((double*)opf->writeNext) = val;
    opf->writeNext += sizeof(double);

    return sizeof(double);
}

static inline char* advanceNext(OpFunc* opf, size_t len)
{
    char *rst = opf->readNext; 

    if (opf->readNext + len >= opf->writeNext)
        opf->readNext = opf->writeNext;
    else
        opf->readNext = opf->readNext + len;    
    
    return rst;
 
}

char* nextStringX(OpFunc* opf, Mem *mem)
{


    mem->z = opf->readNext;

    if (moreRecordsX(opf))
    {
        mem->n = strnlen(opf->readNext,opf->writeNext - opf->readNext);
        if (mem->n)
            advanceNext(opf, mem->n + 1);
    }

    return mem->z;
}

int nextIntegerX(OpFunc* opf)
{
    
    if (moreIntegerRecords(opf))
    {
        int rst = *((int*) opf->readNext); 
        advanceNext(opf, sizeof(int));
        return rst;
    }
    return 0;
}

double nextRealX(OpFunc* opf)
{
    if (moreRealRecords(opf))
    {
        double rst = *((double*) opf->readNext);
        advanceNext(opf, sizeof(double));
        return rst;
    }
    return 0;
}


/* This function is For testing purposes only */
int comdb2VdbeAddExecFuncX(Vdbe *v, Parse *pParse, 
    void *arg, vdbeFunc func, vdbeFuncArgFree dest)
{
    OpFunc *f = sqlitexMalloc(sizeof(OpFunc)); 
    initOpFuncXDefault(f);
    f->arg = arg;
    f->func = func;
    f->destructor = dest;
    int mem = ++pParse->nMem;
    int reg1 = ++pParse->nMem;
    int reg2 =  ++pParse->nMem;
    int reg3 = ++pParse->nMem;
    sqlitexVdbeAddOp4(v, OP_OpFuncLoad, 0,mem,0,(char*) f, P4_OPFUNC);
    sqlitexVdbeAddOp4(v, OP_OpFuncExec, mem,0, 0, (char*) NULL, P4_NOTUSED);
    sqlitexVdbeAddOp4(v, OP_OpFuncString,mem,reg1,0, (char*) NULL,P4_NOTUSED);
    sqlitexVdbeAddOp4(v, OP_OpFuncInteger,mem,reg2,0, (char*) NULL,P4_NOTUSED);
    sqlitexVdbeAddOp4(v, OP_OpFuncReal,mem,reg3,0, (char*) NULL,P4_NOTUSED);
    sqlitexVdbeAddOp2(v,OP_ResultRow,reg1,3);
    sqlitexVdbeAddOp2(v,OP_OpFuncNext,mem,3);
    sqlitexVdbeSetNumCols(v,3);
    sqlitexVdbeSetColName(v,0,COLNAME_NAME, "result",SQLITEX_STATIC);
    sqlitexVdbeSetColName(v,1,COLNAME_NAME, "result1",SQLITEX_STATIC);
    sqlitexVdbeSetColName(v,2,COLNAME_NAME, "result2",SQLITEX_STATIC);
    return 0;
}



static inline void addReadValue(Vdbe* v, int type, int opfunc_reg, int output_reg )
{
    switch(type)
    {
        case OPFUNC_STRING_TYPE:
            sqlitexVdbeAddOp4(v, OP_OpFuncString, 
                opfunc_reg, output_reg,0, (char*) NULL,P4_NOTUSED);
            break;
        case OPFUNC_INT_TYPE:
            sqlitexVdbeAddOp4(v, OP_OpFuncInteger, 
                opfunc_reg, output_reg,0, (char*) NULL,P4_NOTUSED);
            break;
        case OPFUNC_REAL_TYPE:
            sqlitexVdbeAddOp4(v, OP_OpFuncReal, 
                opfunc_reg, output_reg,0, (char*) NULL,P4_NOTUSED);
           break;
    }
}

static inline void setColumnNames(Vdbe* v, OpFuncSetup* settings)
{
    int i;

    sqlitexVdbeSetNumCols(v,settings->n_cols);

    for (i = 0; i < settings->n_cols; i++)
        sqlitexVdbeSetColName(v,i,COLNAME_NAME, settings->cols_names[i],SQLITEX_TRANSIENT);
}

int comdb2prepareOpFuncX(Vdbe* v, Parse* pParse, int int_arg, void *arg, 
    vdbeFunc func, vdbeFuncArgFree freeFunc, OpFuncSetup *settings)
{
    OpFunc *f = sqlitexMalloc(sizeof(OpFunc)); 
    initOpFuncX(f, settings->buf_size);
    f->int_arg = int_arg;
    f->arg = arg;
    f->func = func;
    f->destructor = freeFunc;
    
    int funcreg = ++pParse->nMem;
    

    sqlitexVdbeAddOp4(v, OP_OpFuncLoad, 0,funcreg,0,(char*) f, P4_OPFUNC); 
    sqlitexVdbeAddOp4(v, OP_OpFuncExec, funcreg,0, 0, (char*) NULL, P4_NOTUSED); 
    
    //This is where the loop starts
    int i;
    int basereg = ++pParse->nMem;
    for (i=0; i < settings->n_cols; i++)
    {
        addReadValue(v, settings->cols_types[i], funcreg, basereg + i);
    }
    pParse->nMem += settings->n_cols - 1;
    
    if (settings->n_cols > 0)
    {
        sqlitexVdbeAddOp2(v,OP_ResultRow,basereg,settings->n_cols);
        // the constant must refer to the index of the first reading instruction
        sqlitexVdbeAddOp2(v,OP_OpFuncNext,funcreg, 3);
        setColumnNames(v, settings);
    }
    return 0;
}

int comdb2prepareNoRowsX(Vdbe* v, Parse* pParse, int int_arg,  void *arg,
    vdbeFunc func, vdbeFuncArgFree freeFunc)
{
   OpFuncSetup* stp =  getNoRowsSetup();

    if (stp)
    {
        comdb2prepareOpFuncX(v, pParse, int_arg, arg, func, freeFunc, stp);
        shallowFreeOpFuncSetup(stp);
        return SQLITE_OK;
    }

    return SQLITE_NOMEM;
}

int comdb2prepareRstMsgX(Vdbe* v, Parse* pParse, int int_arg, void *arg,
    vdbeFunc func, vdbeFuncArgFree freeFunc)
{
   OpFuncSetup* stp =  getRstMsgSetup();

    if (stp)
    {
        comdb2prepareOpFuncX(v, pParse, int_arg, arg, func, freeFunc, stp);
        shallowFreeOpFuncSetup(stp);
        return SQLITE_OK;
    }

    return SQLITE_NOMEM;
}

int comdb2prepareSStringX(Vdbe* v, Parse* pParse, int int_arg,  void *arg,
    vdbeFunc func, vdbeFuncArgFree freeFunc)
{
    OpFuncSetup* stp =  getSStringSetup();

    if (stp)
    {    
        comdb2prepareOpFuncX(v, pParse, int_arg, arg, func, freeFunc, stp);
        shallowFreeOpFuncSetup(stp);
        return SQLITE_OK;
    }

    return SQLITE_NOMEM;

}

int comdb2GenerateRstMsgX(OpFunc *f)
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);

    struct rstMsg *s = (struct rstMsg*)f->arg;
    
    opFuncWriteIntegerX(f, s->rc);
    opFuncPrintfX(f, "%s", s->msg);

    return SQLITE_OK;
}


