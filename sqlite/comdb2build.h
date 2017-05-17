#ifndef COMDB2BUILD_H
#define COMDB2BUILD_H

#include <sqliteInt.h>
#include <schemachange.h>

#define ODH_OFF 1
#define IPU_OFF 2
#define ISC_OFF 4

#define BLOB_RLE  8
#define BLOB_CRLE 16
#define BLOB_ZLIB 32
#define BLOB_LZ4  64

#define REC_RLE   128
#define REC_CRLE  256
#define REC_ZLIB  512
#define REC_LZ4   1024

#define FORCE_REBUILD 2048

#define REBUILD_ALL     1
#define REBUILD_DATA    2
#define REBUILD_BLOB    4


#define SET_OPT_ON(opt,val) val |= opt;
#define SET_OPT_OFF(opt,val) val = (val | opt) ^ opt; 
#define OPT_ON(opt, val) (val & opt)

#define SET_ANALYZE_SUMTHREAD(opt, val) opt += ((val & 0xFFFF) << 16)
#define GET_ANALYZE_SUMTHREAD(opt) ((opt & (0xFFFF << 16)) >> 16)

#define SET_ANALYZE_THREAD(opt, val) opt += (val & 0xFFFF)
#define GET_ANALYZE_THREAD(opt) (opt & 0xFFFF)



int  readIntFromToken(Token* t, int *rst);


void fillTableOption(struct schema_change_type*, int);

int  comdb2SqlSchemaChange(OpFunc *arg);
void comdb2CreateTable(Parse*, Token*, Token*, int, Token*, int, int);
void comdb2AlterTable(Parse*, Token*, Token*, int, Token*, int dryrun);
void comdb2DropTable(Parse *pParse, SrcList *pName);

void comdb2enableGenid48(Parse*, int);
void comdb2enableRowlocks(Parse*, int);
void comdb2analyzeCoverage(Parse*, Token*, Token*, int val);
void comdb2getAnalyzeCoverage(Parse* pParse, Token *nm, Token *lnm);
void comdb2analyzeThreshold(Parse*, Token*, Token*, int th);
void comdb2getAnalyzeThreshold(Parse* pParse, Token *nm, Token *lnm);


void comdb2setAlias(Parse*, Token*, Token*);
void comdb2getAlias(Parse*, Token*);

void comdb2rebuildFull(Parse*,Token*,Token*);

void comdb2rebuildIndex(Parse*, Token*, Token*, Token*);

void comdb2rebuildData(Parse*, Token*, Token*);

void comdb2rebuildDataBlob(Parse*,Token*, Token*);

void comdb2truncate(Parse*, Token*, Token*);

void comdb2bulkimport(Parse*, Token*, Token*, Token*, Token*);

void comdb2CreateProcedure(Parse*, Token*, Token*, Token*);
void comdb2DefaultProcedure(Parse*, Token*, Token*, int);
void comdb2DropProcedure(Parse*, Token*, Token*, int);

void comdb2CreateTimePartition(Parse* p, Token* table, Token* name, 
                           Token* period,Token* retention, Token* start);

void comdb2DropTimePartition(Parse* p, Token* name);

void comdb2bulkimport(Parse*, Token*,Token*, Token*, Token*);

void comdb2analyze(Parse*, int opt, Token*, Token*, int);

void comdb2grant(Parse* pParse, int revoke, int permission, Token* nm,
        Token* lnm, Token* u);

void comdb2enableAuth(Parse* pParse, int on);
void comdb2setPassword(Parse* pParse, Token* password, Token* nm);
void comdb2deletePassword(Parse* pParse, Token* nm);
int  comdb2genidcontainstime(void);
void comdb2schemachangeCommitsleep(Parse* pParse, int num);
void comdb2schemachangeConvertsleep(Parse* pParse, int num);

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
