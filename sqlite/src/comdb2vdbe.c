#include "comdb2vdbe.h"
#include "vdbeInt.h"
#include "vdbe.h"

static int initOpFunc(OpFunc* o, size_t len)
{
    assert( o != NULL );
    
    if( len > 0 ){
        o->buf = o->readNext = o->writeNext = calloc(1, len);
    }else{
        o->buf = o->readNext = o->writeNext = NULL;
    }
    o->len = len;
    o->end = o->buf + o->len;
    return o->buf == NULL;
}

static int initOpFuncDefault(OpFunc* o)
{
    return initOpFunc(o, DEFAULT_OPFUNC_BUFLEN);
}

static int resize_buf(OpFunc *o, int size)
{
    char *old_buf = o->buf;
    ptrdiff_t write_offset = o->writeNext - old_buf;
    ptrdiff_t read_offset = o->readNext - old_buf;
    ptrdiff_t newsize = o->end - old_buf;
    while (newsize < size) newsize *= 2;
    char *newbuf = realloc(old_buf, newsize);
    if (!newbuf) return -1;
    if (o->buf != newbuf) {
        o->writeNext = newbuf + write_offset;
        o->readNext = newbuf + read_offset;
        o->buf = newbuf;
    }
    o->end = o->buf + newsize;
    o->len = newsize;
    return 0;
}

void freeOpFunc(OpFunc* func)
{
    if (!func)
        return;

    if (func->arg && func->destructor)
    {
        if (func->destructor)
            func->destructor(func->arg);
        if (func->buf)
            free(func->buf);
        sqlite3_free(func);
    }

}

static void shallowFreeOpFuncSetup(OpFuncSetup* stp)
{
    free(stp);
}

static int   OPFUNC_NOROWS_NCOLS = 0;
static const char* OPFUNC_NOROWS_COLNAMES[] = {0};
static int   OPFUNC_NOROWS_COLTYPES[] = {0};
static size_t   OPFUNC_NOROWS_BUFSIZE = 0;

static OpFuncSetup* getNoRowsSetup()
{
    OpFuncSetup* stp = malloc(sizeof(OpFuncSetup));

    if (stp != NULL)
    {
        stp->n_cols = OPFUNC_NOROWS_NCOLS;
        stp->cols_names = OPFUNC_NOROWS_COLNAMES;
        stp->cols_types = OPFUNC_NOROWS_COLTYPES;
        stp->buf_size = OPFUNC_NOROWS_BUFSIZE;
    }
    
    return stp;
}


static int   OPFUNC_RSTMSG_NCOLS = 2;
static const char* OPFUNC_RSTMSG_COLNAMES[] = {"Return Code", "Message"};
static int   OPFUNC_RSTMSG_COLTYPES[] = {OPFUNC_INT_TYPE, OPFUNC_STRING_TYPE};
static size_t   OPFUNC_RSTMSG_BUFSIZE = 255;

static inline OpFuncSetup* getRstMsgSetup()
{
    OpFuncSetup* stp = malloc(sizeof(OpFuncSetup));

    if (stp != NULL)
    {
        stp->n_cols =  OPFUNC_RSTMSG_NCOLS;
        stp->cols_names = OPFUNC_RSTMSG_COLNAMES;
        stp->cols_types = OPFUNC_RSTMSG_COLTYPES;
        stp->buf_size = OPFUNC_RSTMSG_BUFSIZE;
    }
    
    return stp;
}

static int   OPFUNC_SSTRING_NCOLS = 1;
static const char* OPFUNC_SSTRING_COLNAMES[] = {"Message"};
static int   OPFUNC_SSTRING_COLTYPES[] = {OPFUNC_STRING_TYPE};
static size_t   OPFUNC_SSTRING_BUFSIZE = 256;

static inline OpFuncSetup* getSStringSetup()
{
    OpFuncSetup* stp = malloc(sizeof(OpFuncSetup));

    if (stp != NULL)
    {
        stp->n_cols = OPFUNC_SSTRING_NCOLS;
        stp->cols_names = OPFUNC_SSTRING_COLNAMES;
        stp->cols_types = OPFUNC_SSTRING_COLTYPES;
        stp->buf_size = OPFUNC_SSTRING_BUFSIZE;
    }
    
    return stp;
}

int moreRecords(OpFunc* opf)
{
    return opf->readNext < opf->writeNext;
}

static inline int moreIntegerRecords(OpFunc* opf)
{
    return opf->readNext + sizeof(int) <= opf->writeNext;
}

static inline int moreRealRecords(OpFunc* opf)
{
    return opf->readNext + sizeof(double) <= opf->writeNext;
}

int opFuncPrintf(OpFunc* opf, char* format, ...)
{
    if (opf->end == NULL || opf->writeNext == NULL)
        return 0;

    va_list args;
    va_start(args,format);
    int size = vsnprintf(NULL, 0, format, args);
    va_end(args);

    va_start(args,format);
    if (opf->writeNext + size >= opf->end && resize_buf(opf, 2*opf->len))
       return 0;

    int free_space = opf->len - (opf->writeNext - opf->buf);
    int used = vsnprintf(opf->writeNext, free_space, format, args);
    opf->writeNext += used + 1;
    
    va_end(args);
    
    return used;
}

int opFuncWriteInteger(OpFunc* opf, int val)
{
    if (opf->end == NULL || opf->writeNext == NULL)
        return 0;   

    if (opf->writeNext + sizeof(int) >= opf->end &&  resize_buf(opf, sizeof(int)))
       return 0;

    *((int*)opf->writeNext) = val;
    opf->writeNext += sizeof(int);

    return sizeof(int);
}

int opFuncWriteReal(OpFunc* opf, double val)
{
     if (opf->end == NULL || opf->writeNext == NULL)
        return 0;    
    
    if (opf->writeNext + sizeof(double) >= opf->end && resize_buf(opf, sizeof(double)))
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

char* nextString(OpFunc* opf, Mem *mem)
{
    mem->z = opf->readNext;

    if (moreRecords(opf))
    {
        mem->n = strnlen(opf->readNext,opf->writeNext - opf->readNext);
        if (mem->n)
            advanceNext(opf, mem->n + 1);
    }

    return mem->z;
}

int nextInteger(OpFunc* opf)
{
    
    if (moreIntegerRecords(opf))
    {
        int rst = *((int*) opf->readNext); 
        advanceNext(opf, sizeof(int));
        return rst;
    }
    return 0;
}

double nextReal(OpFunc* opf)
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
int comdb2VdbeAddExecFunc(Vdbe *v, Parse *pParse, 
    void *arg, vdbeFunc func, vdbeFuncArgFree dest)
{
    OpFunc *f = sqlite3Malloc(sizeof(OpFunc)); 
    initOpFuncDefault(f);
    f->arg = arg;
    f->func = func;
    f->destructor = dest;
    int mem = ++pParse->nMem;
    int reg1 = ++pParse->nMem;
    int reg2 =  ++pParse->nMem;
    int reg3 = ++pParse->nMem;
    sqlite3VdbeAddOp4(v, OP_OpFuncLoad, 0,mem,0,(char*) f, P4_OPFUNC);
    sqlite3VdbeAddOp4(v, OP_OpFuncExec, mem,0, 0, (char*) NULL, P4_NOTUSED);
    sqlite3VdbeAddOp4(v, OP_OpFuncString,mem,reg1,0, (char*) NULL,P4_NOTUSED);
    sqlite3VdbeAddOp4(v, OP_OpFuncInteger,mem,reg2,0, (char*) NULL,P4_NOTUSED);
    sqlite3VdbeAddOp4(v, OP_OpFuncReal,mem,reg3,0, (char*) NULL,P4_NOTUSED);
    sqlite3VdbeAddOp2(v,OP_ResultRow,reg1,3);
    sqlite3VdbeAddOp2(v,OP_OpFuncNext,mem,3);
    sqlite3VdbeSetNumCols(v,3);
    sqlite3VdbeSetColName(v,0,COLNAME_NAME, "result",SQLITE_STATIC);
    sqlite3VdbeSetColName(v,1,COLNAME_NAME, "result1",SQLITE_STATIC);
    sqlite3VdbeSetColName(v,2,COLNAME_NAME, "result2",SQLITE_STATIC);
    return 0;
}



static inline void addReadValue(Vdbe* v, int type, int opfunc_reg, int output_reg )
{
    switch(type)
    {
        case OPFUNC_STRING_TYPE:
            sqlite3VdbeAddOp4(v, OP_OpFuncString, 
                opfunc_reg, output_reg,0, (char*) NULL,P4_NOTUSED);
            break;
        case OPFUNC_INT_TYPE:
            sqlite3VdbeAddOp4(v, OP_OpFuncInteger, 
                opfunc_reg, output_reg,0, (char*) NULL,P4_NOTUSED);
            break;
        case OPFUNC_REAL_TYPE:
            sqlite3VdbeAddOp4(v, OP_OpFuncReal, 
                opfunc_reg, output_reg,0, (char*) NULL,P4_NOTUSED);
           break;
    }
}

static inline void setColumnNames(Vdbe* v, OpFuncSetup* settings)
{
    int i;

    sqlite3VdbeSetNumCols(v,settings->n_cols);

    for (i = 0; i < settings->n_cols; i++)
        sqlite3VdbeSetColName(v,i,COLNAME_NAME, settings->cols_names[i],SQLITE_TRANSIENT);
}

int comdb2prepareOpFunc(Vdbe* v, Parse* pParse, int int_arg, void *arg, 
    vdbeFunc func, vdbeFuncArgFree freeFunc, OpFuncSetup *settings)
{
    OpFunc *f = sqlite3Malloc(sizeof(OpFunc)); 
    initOpFunc(f, settings->buf_size);
    f->int_arg = int_arg;
    f->arg = arg;
    f->func = func;
    f->destructor = freeFunc;
    
    int funcreg = ++pParse->nMem;
    

    sqlite3VdbeAddOp4(v, OP_OpFuncLoad, 0,funcreg,0,(char*) f, P4_OPFUNC); 
    sqlite3VdbeAddOp4(v, OP_OpFuncExec, funcreg,0, 0, (char*) NULL, P4_NOTUSED); 

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
        sqlite3VdbeAddOp2(v,OP_ResultRow,basereg,settings->n_cols);
        // the constant must refer to the index of the first reading instruction
        sqlite3VdbeAddOp2(v,OP_OpFuncNext,funcreg, 3);
        setColumnNames(v, settings);
    }
    return 0;
}

int comdb2prepareNoRows(Vdbe* v, Parse* pParse, int int_arg,  void *arg,
    vdbeFunc func, vdbeFuncArgFree freeFunc)
{
   OpFuncSetup* stp =  getNoRowsSetup();

    if (stp)
    {
        comdb2prepareOpFunc(v, pParse, int_arg, arg, func, freeFunc, stp);
        shallowFreeOpFuncSetup(stp);
        return SQLITE_OK;
    }

    return SQLITE_NOMEM;
}

int comdb2prepareRstMsg(Vdbe* v, Parse* pParse, int int_arg, void *arg,
    vdbeFunc func, vdbeFuncArgFree freeFunc)
{
   OpFuncSetup* stp =  getRstMsgSetup();

    if (stp)
    {
        comdb2prepareOpFunc(v, pParse, int_arg, arg, func, freeFunc, stp);
        shallowFreeOpFuncSetup(stp);
        return SQLITE_OK;
    }

    return SQLITE_NOMEM;
}

int comdb2prepareSString(Vdbe* v, Parse* pParse, int int_arg,  void *arg,
    vdbeFunc func, vdbeFuncArgFree freeFunc)
{
    OpFuncSetup* stp =  getSStringSetup();

    if (stp)
    {    
        comdb2prepareOpFunc(v, pParse, int_arg, arg, func, freeFunc, stp);
        shallowFreeOpFuncSetup(stp);
        return SQLITE_OK;
    }

    return SQLITE_NOMEM;

}

int comdb2GenerateRstMsg(OpFunc *f)
{
    struct rstMsg *s = (struct rstMsg*)f->arg;
    
    opFuncWriteInteger(f, s->rc);
    opFuncPrintf(f, "%s", s->msg);

    return SQLITE_OK;
}


