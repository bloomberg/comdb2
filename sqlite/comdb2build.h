#ifndef COMDB2BUILD_H
#define COMDB2BUILD_H

#include <sqliteInt.h>
#include <schemachange.h>

#define SQLITE_OPEN_READWRITE        0x00000002  /* Ok for sqlite3_open_v2() */
#define SQLITE_OPEN_CREATE           0x00000004  /* Ok for sqlite3_open_v2() */
#define SQLITE_OPEN_DELETEONCLOSE    0x00000008  /* VFS only */
#define SQLITE_OPEN_EXCLUSIVE        0x00000010

#define ODH_OFF 0x0001
#define IPU_OFF 0x0002
#define ISC_OFF 0x0004

#define BLOB_NONE     0x0008
#define BLOB_RLE      0x0010
#define BLOB_CRLE     0x0020
#define BLOB_ZLIB     0x0040
#define BLOB_LZ4      0x0080

#define REC_NONE      0x0100
#define REC_RLE       0x0200
#define REC_CRLE      0x0400
#define REC_ZLIB      0x0800
#define REC_LZ4       0x1000

#define FORCE_REBUILD 0x2000

#define REBUILD_ALL     1
#define REBUILD_DATA    2
#define REBUILD_BLOB    4

#define OPT_ON(opt, val) (val & opt)

#define SET_ANALYZE_SUMTHREAD(opt, val) opt += ((val & 0xFFFF) << 16)
#define GET_ANALYZE_SUMTHREAD(opt) ((opt & (0xFFFF << 16)) >> 16)

#define SET_ANALYZE_THREAD(opt, val) opt += (val & 0xFFFF)
#define GET_ANALYZE_THREAD(opt) (opt & 0xFFFF)

/* Enum for sequence options (type member)*/
enum {
    SEQ_MIN_VAL = 1, // Minimum Value
    SEQ_MAX_VAL = 2, // Maximum Value
    SEQ_INC = 4, // Increment by Value
    SEQ_CYCLE = 8, // Flag for cyclic sequence
    SEQ_START_VAL = 16, // Start Value
    SEQ_CHUNK_SIZE = 32, // Size of chunk to dispense
    SEQ_RESTART_VAL = 64, // Value to restart a sequence to
    SEQ_RESTART_TO_START_VAL = 128 // Flag to restart sequence to start val
};

int  readIntFromToken(Token* t, int *rst);
int  comdb2SqlSchemaChange_tran(OpFunc *arg);
void comdb2CreateTableCSC2(Parse *, Token *, Token *, int, Token *, int, int);
void comdb2AlterTableCSC2(Parse *, Token *, Token *, int, Token *, int dryrun);
void comdb2DropTable(Parse *pParse, SrcList *pName);
void comdb2AlterTableStart(Parse *, Token *, Token *, int);
void comdb2AlterTableEnd(Parse *);
void comdb2CreateTableStart(Parse *, Token *, Token *, int, int, int, int);
void comdb2CreateTableEnd(Parse *, Token *, Token *, u8, int);
void comdb2AddColumn(Parse *, Token *, Token *);
void comdb2AddDefaultValue(Parse *, ExprSpan *);
void comdb2AddNull(Parse *);
void comdb2AddNotNull(Parse *, int);
void comdb2AddPrimaryKey(Parse *, ExprList *, int, int, int);
void comdb2AddIndex(Parse *, ExprList *, int, u8);
void comdb2AddDbpad(Parse *, int);
void comdb2CreateIndex(Parse *, Token *, Token *, SrcList *, ExprList *, int,
                       Token *, ExprSpan *, int, int, u8, int);
void comdb2CreateForeignKey(Parse *, ExprList *, Token *, ExprList *, int);
void comdb2DeferForeignKey(Parse *, int);
void comdb2DropColumn(Parse *, Token *);
void comdb2DropIndex(Parse *, SrcList *, int);
void comdb2DropIndexExtn(Parse *, Token *, Token *, int);

void comdb2enableGenid48(Parse*, int);
void comdb2enableRowlocks(Parse*, int);
void comdb2analyzeCoverage(Parse*, Token*, Token*, int val);
void comdb2getAnalyzeCoverage(Parse* pParse, Token *nm, Token *lnm);
void comdb2analyzeThreshold(Parse*, Token*, Token*, int th);
void comdb2getAnalyzeThreshold(Parse* pParse, Token *nm, Token *lnm);
void comdb2setSkipscan(Parse* pParse, Token* nm, Token* lnm, int enable);


void comdb2setAlias(Parse*, Token*, Token*);
void comdb2getAlias(Parse*, Token*);

void comdb2RebuildFull(Parse*,Token*,Token*);
void comdb2RebuildIndex(Parse*, Token*, Token*, Token*);
void comdb2RebuildData(Parse*, Token*, Token*);
void comdb2RebuildDataBlob(Parse*,Token*, Token*);
void comdb2Truncate(Parse*, Token*, Token*);

void comdb2bulkimport(Parse*, Token*, Token*, Token*, Token*);

void comdb2CreateProcedure(Parse*, Token*, Token*, Token*);
void comdb2DefaultProcedure(Parse*, Token*, Token*, int);
void comdb2DropProcedure(Parse*, Token*, Token*, int);

void comdb2CreateTimePartition(Parse* p, Token* table, Token* name, 
                           Token* period,Token* retention, Token* start);

void comdb2DropTimePartition(Parse* p, Token* name);

void comdb2CreateTimePartition(Parse* p, Token* table, Token* name, 
                               Token* period, Token* retention, Token* start);

void comdb2analyze(Parse*, int opt, Token*, Token*, int);

void comdb2grant(Parse* pParse, int revoke, int permission, Token* nm,
        Token* lnm, Token* u);

void comdb2timepartRetention(Parse*, Token*, Token*, int val);

void comdb2enableAuth(Parse* pParse, int on);
void comdb2setPassword(Parse* pParse, Token* password, Token* nm);
void comdb2deletePassword(Parse* pParse, Token* nm);

void comdb2CreateSequence(Parse *pParse, char *name, long long min_val,
                          long long max_val, long long inc, bool cycle,
                          long long start_val, long long chunk_size, bool err);
void comdb2AlterSequence(Parse *pParse, char *name, long long min_val,
                         long long max_val, long long inc, bool cycle,
                         long long start_val, long long chunk_size, int flags, int dryrun);
void comdb2DropSequence(Parse *pParse, char *name);

int  comdb2genidcontainstime(void);
void comdb2schemachangeCommitsleep(Parse* pParse, int num);
void comdb2schemachangeConvertsleep(Parse* pParse, int num);
void comdb2putTunable(Parse *pParse, Token *name, Token *value);

enum
{
    KW_ALL = 0,
    KW_RES = 1,
    KW_FB  = 2
};

void comdb2getkw(Parse* pParse, int reserved);

#define TokenStr(out, in)                                                      \
    char out[in->n + 1];                                                       \
    memcpy(out, in->z, in->n);                                                 \
    out[in->n] = '\0';                                                         \
    sqlite3Dequote(out)

#endif // COMDB2BUILD_H
