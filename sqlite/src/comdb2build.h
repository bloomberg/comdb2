#ifndef COMDB2BUILD_H
#define COMDB2BUILD_H

#include <sqliteInt.h>
#include <schemachange.h>
#include <db_access.h>

#define SQLITE_OPEN_READWRITE        0x00000002  /* Ok for sqlite3_open_v2() */
#define SQLITE_OPEN_CREATE           0x00000004  /* Ok for sqlite3_open_v2() */
#define SQLITE_OPEN_DELETEONCLOSE    0x00000008  /* VFS only */
#define SQLITE_OPEN_EXCLUSIVE        0x00000010

#define ODH_OFF 0x00000010
#define ODH_ON  0x00000020
#define IPU_OFF 0x00000040
#define IPU_ON  0x00000080
#define ISC_OFF 0x00000100
#define ISC_ON  0x00000200

#define BLOB_NONE     0x00000400
#define BLOB_RLE      0x00000800
#define BLOB_CRLE     0x00001000
#define BLOB_ZLIB     0x00002000
#define BLOB_LZ4      0x00004000

#define REC_NONE      0x00008000
#define REC_RLE       0x00010000
#define REC_CRLE      0x00020000
#define REC_ZLIB      0x00040000
#define REC_LZ4       0x00080000

#define PAGE_ORDER    0x00100000
#define READ_ONLY     0x00200000

#define REBUILD_ALL   0x00400000
#define REBUILD_DATA  0x00800000
#define REBUILD_BLOB  0x01000000
#define FORCE_SC      0x02000000

#define OPT_ON(opt, val) (val & opt)

#define SET_ANALYZE_SUMTHREAD(opt, val) opt += ((val & 0xFFFF) << 16)
#define GET_ANALYZE_SUMTHREAD(opt) ((opt & (0xFFFF << 16)) >> 16)

#define SET_ANALYZE_THREAD(opt, val) opt += (val & 0xFFFF)
#define GET_ANALYZE_THREAD(opt) (opt & 0xFFFF)

#define COMDB2_NOT_AUTHORIZED_ERRMSG "comdb2: not authorized"

int  readIntFromToken(Token* t, int *rst);
int  comdb2SqlSchemaChange(OpFunc *);
int  comdb2SqlSchemaChange_tran(OpFunc *arg);
void comdb2CreateTableCSC2(Parse *, Token *, Token *, int, Token *, int, int);
void comdb2AlterTableCSC2(Parse *, Token *, Token *, int, Token *);
void comdb2DropTable(Parse *, SrcList *);
void comdb2AlterTableStart(Parse *, Token *, Token *);
void comdb2AlterTableEnd(Parse *);
void comdb2AlterColumnStart(Parse *, Token *);
void comdb2AlterColumnEnd(Parse *);
void comdb2AlterColumnType(Parse *, Token *);
void comdb2AlterColumnSetDefault(Parse *, Expr *, const char *, const char *);
void comdb2AlterColumnDropDefault(Parse *);
void comdb2AlterColumnDropAutoIncrement(Parse *);
void comdb2AlterColumnSetNotNull(Parse *);
void comdb2AlterColumnDropNotNull(Parse *);
void comdb2AlterTableOptions(Parse *pParse, uint32_t);
void comdb2CreateTableStart(Parse *, Token *, Token *, int, int, int, int);
void comdb2CreateTableEnd(Parse *, Token *, Token *, u8, int);
void comdb2CreateTableLikeEnd(Parse *, Token *, Token *);
void comdb2AddColumn(Parse *, Token *, Token *);
void comdb2AddDefaultValue(Parse *, Expr *, const char *, const char *);
void comdb2AddNull(Parse *);
void comdb2SetAutoIncrement(Parse *);
void comdb2AddNotNull(Parse *, int);
void comdb2AddPrimaryKey(Parse *, ExprList *, int, int, int);
void comdb2DropPrimaryKey(Parse *);
void comdb2AddIndex(Parse *, Token *, ExprList *, int, Expr *, const char *,
                    const char *, int, u8, int, ExprList *);
void comdb2AddDbpad(Parse *, int);
void comdb2AddCheckConstraint(Parse *, Expr *, const char *, const char *);
void comdb2CreateIndex(Parse *, Token *, Token *, SrcList *, ExprList *, int,
                       Token *, Expr *, const char *, const char *, int, int,
                       u8, int, ExprList *, int);
void comdb2CreateForeignKey(Parse *, ExprList *, Token *, ExprList *, int);
void comdb2DeferForeignKey(Parse *, int);
void comdb2DropForeignKey(Parse *, Token *);
void comdb2DropConstraint(Parse *, Token *);
void comdb2DropColumn(Parse *, Token *);
void comdb2DropIndex(Parse *, Token *, Token *, int);
void comdb2AlterDropIndex(Parse *, Token *);
void comdb2AlterCommitPending(Parse *);

void comdb2enableGenid48(Parse*, int);
void comdb2enableRowlocks(Parse*, int);
void comdb2analyzeCoverage(Parse*, Token*, Token*, int val);
void comdb2CreateRangePartition(Parse *pParse, Token*, Token*, ExprList*);
void comdb2getAnalyzeCoverage(Parse* pParse, Token *nm, Token *lnm);
void comdb2analyzeThreshold(Parse*, Token*, Token*, int th);
void comdb2getAnalyzeThreshold(Parse* pParse, Token *nm, Token *lnm);
void comdb2setSkipscan(Parse* pParse, Token* nm, Token* lnm, int enable);


void comdb2setAlias(Parse*, Token*, Token*);
void comdb2getAlias(Parse*, Token*);

void comdb2RebuildFull(Parse*,Token*,Token*,int opt);
void comdb2RebuildIndex(Parse*, Token*, Token*, Token*,int opt);
void comdb2RebuildData(Parse*, Token*, Token*,int opt);
void comdb2RebuildDataBlob(Parse*,Token*, Token*,int opt);
void comdb2Truncate(Parse*, Token*, Token*);

void comdb2SchemachangeControl(Parse*, int, Token*, Token *);

void comdb2bulkimport(Parse*, Token*, Token*, Token*, Token*);

void comdb2CreateProcedure(Parse*, Token*, Token*, Token*);
void comdb2DefaultProcedure(Parse*, Token*, Token*, int);
void comdb2DropProcedure(Parse*, Token*, Token*, int);


void comdb2CreatePartition(Parse* p, Token* table, Token* name, 
                           Token* period, Token* retention, Token* start);
void comdb2DropPartition(Parse* p, Token* name);
void comdb2CreateTimePartition(Parse* p, Token* period, Token* retention,
                               Token* start);
void comdb2CreateManualPartition(Parse* p, Token* retention, Token* start);
void comdb2SaveMergeTable(Parse* p, Token* name, Token* database, int alter);

void comdb2analyze(Parse*, int opt, Token*, Token*, int);

void comdb2grant(Parse* pParse, int revoke, int permission, Token* nm,
        Token* lnm, Token* u);

void comdb2timepartRetention(Parse*, Token*, Token*, int val);
void comdb2CounterIncr(Parse*, Token*, Token*);
void comdb2CounterSet(Parse*, Token*, Token*, long long val);

void comdb2enableAuth(Parse* pParse, int on);
void comdb2setPassword(Parse* pParse, Token* password, Token* nm);
void comdb2deletePassword(Parse* pParse, Token* nm);
int  comdb2genidcontainstime(void);
void comdb2schemachangeCommitsleep(Parse* pParse, int num);
void comdb2schemachangeConvertsleep(Parse* pParse, int num);
void comdb2putTunable(Parse*, Token*, Token*, Token*);

enum
{
    KW_ALL = 0,
    KW_RES = 1,
    KW_FB  = 2
};

void comdb2getkw(Parse* pParse, int reserved);
int comdb2TokenToStr(Token *nm, char *buf, size_t len);

int comdb2IsPrepareOnly(Parse* pParse);
int comdb2AuthenticateUserOp(Parse* pParse);

void create_default_consumer_sp(Parse *, char *);

#endif // COMDB2BUILD_H
