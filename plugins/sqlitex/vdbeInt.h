/*
** 2003 September 6
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This is the header file for information that is private to the
** VDBE.  This information used to all be at the top of the single
** source code file "vdbe.c".  When that file became too big (over
** 6000 lines long) it was split up into several smaller files and
** this header information was factored out.
*/
#ifndef _VDBEINT_H_
#define _VDBEINT_H_

/* COMDB2 MODIFICATION */
#include <time.h>
#include <pthread.h>
#include <strings.h>

/*
** The maximum number of times that a statement will try to reparse
** itself before giving up and returning SQLITE_SCHEMA.
*/
#ifndef SQLITE_MAX_SCHEMA_RETRY
# define SQLITE_MAX_SCHEMA_RETRY 50
#endif

/*
** VDBE_DISPLAY_P4 is true or false depending on whether or not the
** "explain" P4 display logic is enabled.
*/
#if !defined(SQLITE_OMIT_EXPLAIN) || !defined(NDEBUG) \
     || defined(VDBE_PROFILE) || defined(SQLITE_DEBUG)
# define VDBE_DISPLAY_P4 1
#else
# define VDBE_DISPLAY_P4 0
#endif

/*
** SQL is translated into a sequence of instructions to be
** executed by a virtual machine.  Each instruction is an instance
** of the following structure.
*/
typedef struct VdbeOp Op;

/*
** Boolean values
*/
typedef unsigned Bool;



#ifdef  SQLITE_BUILDING_FOR_COMDB2
enum { VDBESORTER_FIND, VDBESORTER_MOVE, VDBESORTER_WRITE };
/*TODO: COMDB2 MODIFICATION */
/* moved vdbesorter here because is needed in sqlglue.c */
/* Opaque type used by code in vdbesort.c */
typedef struct PmaReader PmaReader;
typedef struct MergeEngine MergeEngine;
typedef struct SorterRecord SorterRecord;
/* Opaque type used by code in vdbesort.c */
typedef struct VdbeSorter VdbeSorter;
typedef struct SorterList SorterList;
typedef struct SortSubtask SortSubtask;
typedef struct SorterFile SorterFile;

struct SorterFile {
  sqlitex_file *pFd;              /* File handle */
  i64 iEof;                       /* Bytes of data stored in pFd */
};

/*
** An in-memory list of objects to be sorted.
**
** If aMemory==0 then each object is allocated separately and the objects
** are connected using SorterRecord.u.pNext.  If aMemory!=0 then all objects
** are stored in the aMemory[] bulk memory, one right after the other, and
** are connected using SorterRecord.u.iNext.
*/
struct SorterList {
  SorterRecord *pList;            /* Linked list of records */
  u8 *aMemory;                    /* If non-NULL, bulk memory to hold pList */
  int szPMA;                      /* Size of pList as PMA in bytes */
};

struct SortSubtask {
  SQLiteThread *pThread;          /* Background thread, if any */
  int bDone;                      /* Set if thread is finished but not joined */
  VdbeSorter *pSorter;            /* Sorter that owns this sub-task */
  UnpackedRecord *pUnpacked;      /* Space to unpack a record */
  SorterList list;                /* List for thread to write to a PMA */
  int nPMA;                       /* Number of PMAs currently in file */
  SorterFile file;                /* Temp file for level-0 PMAs */
  SorterFile file2;               /* Space for other PMAs */
};


struct VdbeSorter {
  int mnPmaSize;                  /* Minimum PMA size, in bytes */
  int mxPmaSize;                  /* Maximum PMA size, in bytes.  0==no limit */
  int mxKeysize;                  /* Largest serialized key seen so far */
  int pgsz;                       /* Main database page size */
  PmaReader *pReader;             /* Readr data from here after Rewind() */
  MergeEngine *pMerger;           /* Or here, if bUseThreads==0 */
  sqlitex *db;                    /* Database connection */
  KeyInfo *pKeyInfo;              /* How to compare records */
  UnpackedRecord *pUnpacked;      /* Used by VdbeSorterCompare() */
  SorterList list;                /* List of in-memory records */
  int iMemory;                    /* Offset of free space in list.aMemory */
  int nMemory;                    /* Size of list.aMemory allocation in bytes */
  u8 bUsePMA;                     /* True if one or more PMAs created */
  u8 bUseThreads;                 /* True to use background threads */
  u8 iPrev;                       /* Previous thread used to flush PMA */
  u8 nTask;                       /* Size of aTask[] array */
  SortSubtask aTask[1];           /* One or more subtasks */

  /* COMDB2 MODIFICATION */
  int nfind;
  int nmove;
  int nwrite;
};

#endif



/* Opaque type used by the explainer */
typedef struct Explain Explain;

/* Elements of the linked list at Vdbe.pAuxData */
typedef struct AuxData AuxData;

/*
** A cursor is a pointer into a single BTree within a database file.
** The cursor can seek to a BTree entry with a particular key, or
** loop over all entries of the Btree.  You can also insert new BTree
** entries or retrieve the key or data from the entry that the cursor
** is currently pointing to.
**
** Cursors can also point to virtual tables, sorters, or "pseudo-tables".
** A pseudo-table is a single-row table implemented by registers.
** 
** Every cursor that the virtual machine has open is represented by an
** instance of the following structure.
*/
typedef struct VdbeCursor VdbeCursor;
struct VdbeCursor {
  BtCursor *pCursor;    /* The cursor structure of the backend */
  Btree *pBt;           /* Separate file holding temporary table */
  KeyInfo *pKeyInfo;    /* Info about index keys needed by index cursors */
  int seekResult;       /* Result of previous sqlite3BtreeMoveto() */
  int pseudoTableReg;   /* Register holding pseudotable content. */
  i16 nField;           /* Number of fields in the header */
  u16 nHdrParsed;       /* Number of header fields parsed so far */
#ifdef SQLITE_DEBUG
  u8 seekOp;            /* Most recent seek operation on this cursor */
  u8 wrFlag;            /* The wrFlag argument to sqlite3BtreeCursor() */
#endif
  i8 iDb;               /* Index of cursor database in db->aDb[] (or -1) */
  u8 nullRow;           /* True if pointing to a row with no data */
  u8 deferredMoveto;    /* A call to sqlite3BtreeMoveto() is needed */
  Bool isEphemeral:1;   /* True for an ephemeral table */
  Bool useRandomRowid:1;/* Generate new record numbers semi-randomly */
  Bool isTable:1;       /* True if a table requiring integer keys */
  Bool isOrdered:1;     /* True if the underlying table is BTREE_UNORDERED */
  Pgno pgnoRoot;        /* Root page of the open btree cursor */
  sqlitex_vtab_cursor *pVtabCursor;  /* The cursor for a virtual table */
  i64 seqCount;         /* Sequence counter */
  i64 movetoTarget;     /* Argument to the deferred sqlite3BtreeMoveto() */
  VdbeSorter *pSorter;  /* Sorter object for OP_SorterOpen cursors */
  VdbeCursor *pAltCursor; /* Associated index cursor from which to read */
  int *aAltMap;           /* Mapping from table to index column numbers */
#ifdef SQLITE_ENABLE_COLUMN_USED_MASK
  u64 maskUsed;         /* Mask of columns used by this cursor */
#endif

  /* Cached information about the header for the data record that the
  ** cursor is currently pointing to.  Only valid if cacheStatus matches
  ** Vdbe.cacheCtr.  Vdbe.cacheCtr will never take on the value of
  ** CACHE_STALE and so setting cacheStatus=CACHE_STALE guarantees that
  ** the cache is out of date.
  **
  ** aRow might point to (ephemeral) data for the current row, or it might
  ** be NULL.
  */
  u32 cacheStatus;      /* Cache is valid if this matches Vdbe.cacheCtr */
  u32 payloadSize;      /* Total number of bytes in the record */
  u32 szRow;            /* Byte available in aRow */
  u32 iHdrOffset;       /* Offset to next unparsed byte of the header */
  const u8 *aRow;       /* Data for the current row, if all on one page */
  u32 *aOffset;         /* Pointer to aType[nField] */
  int nCookFields;
  u32 aType[1];         /* Type values for all entries in the record */
  /* 2*nField extra array elements allocated for aType[], beyond the one
  ** static element declared in the structure.  nField total array slots for
  ** aType[] and nField+1 array slots for aOffset[] */
};
typedef struct VdbeCursor VdbeCursor;

/*
** When a sub-program is executed (OP_Program), a structure of this type
** is allocated to store the current value of the program counter, as
** well as the current memory cell array and various other frame specific
** values stored in the Vdbe struct. When the sub-program is finished, 
** these values are copied back to the Vdbe from the VdbeFrame structure,
** restoring the state of the VM to as it was before the sub-program
** began executing.
**
** The memory for a VdbeFrame object is allocated and managed by a memory
** cell in the parent (calling) frame. When the memory cell is deleted or
** overwritten, the VdbeFrame object is not freed immediately. Instead, it
** is linked into the Vdbe.pDelFrame list. The contents of the Vdbe.pDelFrame
** list is deleted when the VM is reset in VdbeHalt(). The reason for doing
** this instead of deleting the VdbeFrame immediately is to avoid recursive
** calls to sqlitexVdbeMemRelease() when the memory cells belonging to the
** child frame are released.
**
** The currently executing frame is stored in Vdbe.pFrame. Vdbe.pFrame is
** set to NULL if the currently executing frame is the main program.
*/
typedef struct VdbeFrame VdbeFrame;
struct VdbeFrame {
  Vdbe *v;                /* VM this frame belongs to */
  VdbeFrame *pParent;     /* Parent of this frame, or NULL if parent is main */
  Op *aOp;                /* Program instructions for parent frame */
  i64 *anExec;            /* Event counters from parent frame */
  Mem *aMem;              /* Array of memory cells for parent frame */
  u8 *aOnceFlag;          /* Array of OP_Once flags for parent frame */
  VdbeCursor **apCsr;     /* Array of Vdbe cursors for parent frame */
  void *token;            /* Copy of SubProgram.token */
  i64 lastRowid;          /* Last insert rowid (sqlitex.lastRowid) */
  int nCursor;            /* Number of entries in apCsr */
  int pc;                 /* Program Counter in parent (calling) frame */
  int nOp;                /* Size of aOp array */
  int nMem;               /* Number of entries in aMem */
  int nOnceFlag;          /* Number of entries in aOnceFlag */
  int nChildMem;          /* Number of memory cells for child frame */
  int nChildCsr;          /* Number of cursors for child frame */
  int nChange;            /* Statement changes (Vdbe.nChange)     */
  int nDbChange;          /* Value of db->nChange */
};

#define VdbeFrameMem(p) ((Mem *)&((u8 *)p)[ROUND8(sizeof(VdbeFrame))])

/*
** A value for VdbeCursor.cacheValid that means the cache is always invalid.
*/
#define CACHE_STALE 0

/*
** Internally, the vdbe manipulates nearly all SQL values as Mem
** structures. Each Mem struct may cache multiple representations (string,
** integer etc.) of the same value.
** (!) If you change the order of fileds in Mem struct, please be sure to
**     change columnNullValue() in vdbeapi.c accordingly.
*/
struct Mem {
  union MemValue {
    double r;           /* Real value used when MEM_Real is set in flags */
    i64 i;              /* Integer value used when MEM_Int is set in flags */
    int nZero;          /* Used when bit MEM_Zero is set in flags */
    FuncDef *pDef;      /* Used only when flags==MEM_Agg */
    RowSet *pRowSet;    /* Used only when flags==MEM_RowSet */
    VdbeFrame *pFrame;  /* Used when flags==MEM_Frame */
    OpFunc *pOpFunc;    /* Used when flags==MEM_OpFunc */
  } u;
  /* COMDB2 MODIFICATION */
  union {
    dttz_t     dt;    /* Datetime support */
    intv_t     tv;    /* Interval and Decimal (hack) support */
  } du;
  const char *tz;     /* timezone pointer */
  int dtprec;         /* Preferred datetime precision upon conversion.
                         If the Mem is already a DATEIME object, no conversion
                         is needed. The precision is ignored (eg, data is from
                         ondisk datetime or from api datetime parameter). */

  /* COMDB2 MODIFICATION */
  u32 flags;          /* Some combination of MEM_Null, MEM_Str, MEM_Dyn, etc. */
  u8  enc;            /* SQLITE_UTF8, SQLITE_UTF16BE, SQLITE_UTF16LE */
  int n;              /* Number of characters in string value, excluding '\0' */
  char *z;            /* String or BLOB value */
  sqlitex *db;        /* The associated database connection */
  /* ShallowCopy only needs to copy the information above */
  char *zMalloc;      /* Space to hold MEM_Str or MEM_Blob if szMalloc>0 */
  int szMalloc;       /* Size of the zMalloc allocation */
  u32 uTemp;          /* Transient storage for serial_type in OP_MakeRecord */
  void (*xDel)(void*);/* Destructor for Mem.z - only valid if MEM_Dyn */
#ifdef SQLITE_DEBUG
  Mem *pScopyFrom;    /* This Mem is a shallow copy of pScopyFrom */
  void *pFiller;      /* So that sizeof(Mem) is a multiple of 8 */
#endif

};

/* One or more of the following flags are set to indicate the validOK
** representations of the value stored in the Mem struct.
**
** If the MEM_Null flag is set, then the value is an SQL NULL value.
** No other flags may be set in this case.
**
** If the MEM_Str flag is set then Mem.z points at a string representation.
** Usually this is encoded in the same unicode encoding as the main
** database (see below for exceptions). If the MEM_Term flag is also
** set, then the string is nul terminated. The MEM_Int and MEM_Real 
** flags may coexist with the MEM_Str flag.
*/
#define MEM_Null      0x00001   /* Value is NULL */
#define MEM_Str       0x00002   /* Value is a string */
#define MEM_Int       0x00004   /* Value is an integer */
#define MEM_Real      0x00008   /* Value is a real number */
#define MEM_Blob      0x00010   /* Value is a BLOB */
#define MEM_Datetime  0x00020   /* Value is a datetime */
#define MEM_Interval  0x00040   /* Value is an interval/decimal */
#define MEM_Small     0x00080   /* Value is a small float */
#define MEM_AffMask   0x000ff   /* Mask of affinity bits */
#define MEM_RowSet    0x00100   /* Value is a RowSet object */
#define MEM_Frame     0x00200   /* Value is a VdbeFrame object */
#define MEM_Undefined 0x00400   /* Value is undefined */
#define MEM_Cleared   0x00800   /* NULL set by OP_Null, not from data */
#define MEM_TypeMask  0x00fff   /* Mask of type bits */

/* Whenever Mem contains a valid string or blob representation, one of
** the following flags must be set to determine the memory management
** policy for Mem.z.  The MEM_Term flag tells us whether or not the
** string is \000 or \u0000 terminated
*/
/* COMDB2 MODIFICATION */
#define MEM_Term      0x01000   /* String rep is nul terminated */
#define MEM_Dyn       0x02000   /* Need to call Mem.xDel() on Mem.z */
#define MEM_Static    0x04000   /* Mem.z points to a static string */
#define MEM_Ephem     0x08000   /* Mem.z points to an ephemeral string */
#define MEM_Agg       0x10000   /* Mem.z points to an agg function context */
#define MEM_Zero      0x20000   /* Mem.i contains count of 0s appended to blob */
#define MEM_Xor       0x40000   /* Mem.z needs XOR; <DESCEND> keys */ 
#define MEM_OpFunc    0x80000   /* Mem.u is a custom function */ 

#ifdef SQLITE_OMIT_INCRBLOB
  #undef MEM_Zero
  #define MEM_Zero 0x0000
#endif

/*
** Clear any existing type flags from a Mem and replace them with f
*/
#define MemSetTypeFlag(p, f) \
   ((p)->flags = ((p)->flags&~(MEM_TypeMask|MEM_Zero))|f)

/*
** Return true if a memory cell is not marked as invalid.  This macro
** is for use inside assert() statements only.
*/
#ifdef SQLITE_DEBUG
#define memIsValid(M)  ((M)->flags & MEM_Undefined)==0
#endif

/*
** Each auxiliary data pointer stored by a user defined function 
** implementation calling sqlitex_set_auxdata() is stored in an instance
** of this structure. All such structures associated with a single VM
** are stored in a linked list headed at Vdbe.pAuxData. All are destroyed
** when the VM is halted (if not before).
*/
struct AuxData {
  int iOp;                        /* Instruction number of OP_Function opcode */
  int iArg;                       /* Index of function argument. */
  void *pAux;                     /* Aux data pointer */
  void (*xDelete)(void *);        /* Destructor for the aux data */
  AuxData *pNext;                 /* Next element in list */
};

/*
** The "context" argument for an installable function.  A pointer to an
** instance of this structure is the first argument to the routines used
** implement the SQL functions.
**
** There is a typedef for this structure in sqlite.h.  So all routines,
** even the public interface to SQLite, can use a pointer to this structure.
** But this file is the only place where the internal details of this
** structure are known.
**
** This structure is defined inside of vdbeInt.h because it uses substructures
** (Mem) which are only defined there.
*/
struct sqlitex_context {
  Mem *pOut;            /* The return value is stored here */
  FuncDef *pFunc;       /* Pointer to function information */
  Mem *pMem;            /* Memory cell used to store aggregate context */
  Vdbe *pVdbe;          /* The VM that owns this context */
  int iOp;              /* Instruction number of OP_Function */
  int isError;          /* Error code returned by the function. */
  u8 skipFlag;          /* Skip accumulator loading if true */
  u8 fErrorOrAux;       /* isError!=0 or pVdbe->pAuxData modified */
};

/* The DblquoteStr object holds the text of a double-quoted
** string for a prepared statement.  A linked list of these objects
** is constructed during statement parsing and is held on Vdbe.pDblStr.
** When computing a normalized SQL statement for an SQL statement, that
** list is consulted for each double-quoted identifier to see if the
** identifier should really be a string literal.
*/
typedef struct DblquoteStr DblquoteStr;
struct DblquoteStr {
  DblquoteStr *pNextStr;   /* Next string literal in the list */
  char z[8];               /* Dequoted value for the string */
};

/*
** An Explain object accumulates indented output which is helpful
** in describing recursive data structures.
*/
struct Explain {
  Vdbe *pVdbe;       /* Attach the explanation to this Vdbe */
  StrAccum str;      /* The string being accumulated */
  int nIndent;       /* Number of elements in aIndent */
  u16 aIndent[100];  /* Levels of indentation */
  char zBase[100];   /* Initial space */
};

/* A bitfield type for use inside of structures.  Always follow with :N where
** N is the number of bits.
*/
typedef unsigned bft;  /* Bit Field Type */

typedef struct ScanStatus ScanStatus;
struct ScanStatus {
  int addrExplain;                /* OP_Explain for loop */
  int addrLoop;                   /* Address of "loops" counter */
  int addrVisit;                  /* Address of "rows visited" counter */
  int iSelectID;                  /* The "Select-ID" for this loop */
  LogEst nEst;                    /* Estimated output rows per loop */
  char *zName;                    /* Name of table or index */
};

/*
** An instance of the virtual machine.  This structure contains the complete
** state of the virtual machine.
**
** The "sqlitex_stmt" structure pointer that is returned by sqlitex_prepare()
** is really a pointer to an instance of this structure.
**
** The Vdbe.inVtabMethod variable is set to non-zero for the duration of
** any virtual table method invocations made by the vdbe program. It is
** set to 2 for xDestroy method calls and 1 for all other methods. This
** variable is used for two purposes: to allow xDestroy methods to execute
** "DROP TABLE" statements and to prevent some nasty side effects of
** malloc failure when SQLite is invoked recursively by a virtual table 
** method function.
*/
struct Vdbe {
  sqlitex *db;            /* The database connection that owns this statement */
  Op *aOp;                /* Space to hold the virtual machine's program */
  Mem *aMem;              /* The memory locations */
  Mem **apArg;            /* Arguments to currently executing user function */
  Mem *aColName;          /* Column names to return */
  Mem *pResultSet;        /* Pointer to an array of results */
  Parse *pParse;          /* Parsing context used to create this Vdbe */
  int nMem;               /* Number of memory locations currently allocated */
  int nOp;                /* Number of instructions in the program */
  int nCursor;            /* Number of slots in apCsr[] */
  u32 magic;              /* Magic number for sanity checking */
  char *zErrMsg;          /* Error message written here */
  Vdbe *pPrev,*pNext;     /* Linked list of VDBEs with the same Vdbe.db */
  VdbeCursor **apCsr;     /* One element of this array for each open cursor */
  Mem *aVar;              /* Values for the OP_Variable opcode. */
  char **azVar;           /* Name of variables */
  ynVar nVar;             /* Number of entries in aVar[] */
  ynVar nzVar;            /* Number of entries in azVar[] */
  u32 cacheCtr;           /* VdbeCursor row cache generation counter */
  int pc;                 /* The program counter */
  int rc;                 /* Value to return */
#ifdef SQLITE_DEBUG
  int rcApp;              /* errcode set by sqlitex_result_error_code() */
#endif
  u16 nResColumn;         /* Number of columns in one row of the result set */
  u8 errorAction;         /* Recovery action to do in case of an error */
  u8 minWriteFileFormat;  /* Minimum file format for writable database files */
  bft explain:2;          /* True if EXPLAIN present on SQL command */
  bft inVtabMethod:2;     /* See comments above */
  bft changeCntOn:1;      /* True to update the change-counter */
  bft expired:1;          /* True if the VM needs to be recompiled */
  bft runOnlyOnce:1;      /* Automatically expire on reset */
  bft usesStmtJournal:1;  /* True if uses a statement journal */
  bft readOnly:1;         /* True for statements that do not write */
  bft bIsReader:1;        /* True for statements that read */
  bft isPrepareV2:1;      /* True if prepared with prepare_v2() */
  bft doingRerun:1;       /* True if rerunning after an auto-reprepare */
  int nChange;            /* Number of db changes made since last reset */
  yDbMask btreeMask;      /* Bitmask of db->aDb[] entries referenced */
  yDbMask lockMask;       /* Subset of btreeMask that requires a lock */
  int iStatement;         /* Statement number (or 0 if has not opened stmt) */
  u32 aCounter[5];        /* Counters used by sqlitex_stmt_status() */
#ifndef SQLITE_OMIT_TRACE
  i64 startTime;          /* Time when query started - used for profiling */
#endif
  i64 iCurrentTime;       /* Value of julianday('now') for this statement */
  i64 nFkConstraint;      /* Number of imm. FK constraints this VM */
  i64 nStmtDefCons;       /* Number of def. constraints when stmt started */
  i64 nStmtDefImmCons;    /* Number of def. imm constraints when stmt started */
  char *zSql;             /* Text of the SQL statement that generated this */
#ifdef SQLITE_ENABLE_NORMALIZE
  char *zNormSql;         /* Normalization of the associated SQL statement */
  DblquoteStr *pDblStr;   /* List of double-quoted string literals */
#endif
  void *pFree;            /* Free this when deleting the vdbe */
  VdbeFrame *pFrame;      /* Parent frame */
  VdbeFrame *pDelFrame;   /* List of frame objects to free on VM reset */
  int nFrame;             /* Number of frames in pFrame list */
  u32 expmask;            /* Binding to these vars invalidates VM */
  SubProgram *pProgram;   /* Linked list of all sub-programs used by VM */
  int nOnceFlag;          /* Size of array aOnceFlag[] */
  u8 *aOnceFlag;          /* Flags for OP_Once */
  AuxData *pAuxData;      /* Linked list of auxdata allocations */
#ifdef SQLITE_ENABLE_STMT_SCANSTATUS
  i64 *anExec;            /* Number of times each op has been executed */
  int nScan;              /* Entries in aScan[] */
  ScanStatus *aScan;      /* Scan definitions for sqlitex_stmt_scanstatus() */
#endif
  /* COMDB2 MODIFICATION */
  int *updCols;           /* list of columns modified in this update */
  Table **tbls;           /* list of tables to be open. */ 
  void *lockInfo;         /* Information on locks. */
  u16 numTables;
  char tzname[TZNAME_MAX];/* timezone info for datetime support */
  u8 doExplainTrace;
  char *explainTrace;
  int explainTraceLen;
  int explainTraceAlloced;
  int dtprec;             /* datetime precision - make it u32 to silence compiler */
  struct timespec tspec;  /* time of prepare, used for stable now() */
  int trackIdentifiers;
  Hash identifiers;
  char fingerprint[16];
};

/*
** The following are allowed values for Vdbe.magic
*/
#define VDBE_MAGIC_INIT     0x26bceaa5    /* Building a VDBE program */
#define VDBE_MAGIC_RUN      0xbdf20da3    /* VDBE is ready to execute */
#define VDBE_MAGIC_HALT     0x519c2973    /* VDBE has completed execution */
#define VDBE_MAGIC_DEAD     0xb606c3c8    /* The VDBE has been deallocated */

/*
** Function prototypes
*/
void sqlitexVdbeFreeCursor(Vdbe *, VdbeCursor*);
void sqliteVdbePopStack(Vdbe*,int);
int sqlitexVdbeCursorMoveto(VdbeCursor**, int*);
int sqlitexVdbeCursorRestore(VdbeCursor*);
#if defined(SQLITE_DEBUG) || defined(VDBE_PROFILE)
void sqlitexVdbePrintOp(FILE*, int, Op*);
#endif
u32 sqlitexVdbeSerialTypeLen(u32);
u32 sqlitexVdbeSerialType(Mem*, int);
u32 sqlitexVdbeSerialPut(unsigned char*, Mem*, u32);
static inline u32 sqlitexVdbeSerialGet(const unsigned char*, u32, Mem*);
void sqlitexVdbeDeleteAuxData(Vdbe*, int, int);

int sqlite2BtreeKeyCompare(BtCursor *, const void *, int, int, int *);
static inline int sqlitexVdbeIdxKeyCompare(sqlitex*,VdbeCursor*,UnpackedRecord*,int*);
int sqlitexVdbeIdxRowid(sqlitex*, BtCursor*, i64*);
int sqlitexVdbeExec(Vdbe*);
int sqlitexVdbeList(Vdbe*);
int sqlitexVdbeHalt(Vdbe*);
int sqlitexVdbeChangeEncoding(Mem *, int);
int sqlitexVdbeMemTooBig(Mem*);
int sqlitexVdbeMemCopy(Mem*, const Mem*);
void sqlitexVdbeMemShallowCopy(Mem*, const Mem*, int);
void sqlitexVdbeMemMove(Mem*, Mem*);
int sqlitexVdbeMemNulTerminate(Mem*);
int sqlitexVdbeMemSetStr(Mem*, const char*, int, u8, void(*)(void*));
void sqlitexVdbeMemSetInt64(Mem*, i64);
/* COMDB2 MODIFICATION */
int sqlitexVdbeMemSetDatetime(Mem*, dttz_t*, const char *tz);
int sqlitexVdbeMemSetInterval(Mem *pMem, intv_t *tv);
int sqlitexVdbeMemSetDecimal(Mem*, decQuad*);


#ifdef SQLITE_OMIT_FLOATING_POINT
# define sqlitexVdbeMemSetDouble sqlitexVdbeMemSetInt64
#else
  void sqlitexVdbeMemSetDouble(Mem*, double);
#endif
void sqlitexVdbeMemInit(Mem*,sqlitex*,u16);
void sqlitexVdbeMemSetNull(Mem*);
void sqlitexVdbeMemSetZeroBlob(Mem*,int);
void sqlitexVdbeMemSetRowSet(Mem*);
int sqlitexVdbeMemMakeWriteable(Mem*);
int sqlitexVdbeMemStringify(Mem*, u8, u8);
i64 sqlitexVdbeIntValue(Mem*);
int sqlitexVdbeMemIntegerify(Mem*);
double sqlitexVdbeRealValue(Mem*);
void sqlitexVdbeIntegerAffinity(Mem*);
int sqlitexVdbeMemRealify(Mem*);
int sqlitexVdbeMemNumerify(Mem*);
int sqlitexVdbeMemCast(Vdbe *, Mem*,u8,u8);
int sqlitexVdbeMemFromBtree(BtCursor*,u32,u32,int,Mem*);
/* COMDB2 MODIFICATION */
int sqlitexVdbeMemDatetimefy(Mem*);
int sqlitexVdbeMemDatetimefyTz(Mem*, const char*);
int sqlitexVdbeMemDecimalfy(Mem*);
int sqlitexVdbeMemIntervalfy(Mem *pMem, int type);
int sqlitexVdbeMemIntervalAndInterval(const Mem *first, const Mem *secnd, int op, Mem * res);
int sqlitexVdbeMemIntervalAndInt(const Mem *a, const Mem *b, int op, Mem * res);
int sqlitexVdbeMemIntAndInterval(const Mem *a, const Mem *b, int op, Mem * res);
int sqlitexVdbeMemDatetimeAndDatetime(const Mem *first, const Mem *secnd, int op, Mem * res);
int sqlitexVdbeMemDatetimeAndInterval(const Mem *a, const Mem *b, int op, Mem * res);
int sqlitexVdbeMemIntervalAndDatetime(const Mem *a, const Mem *b, int op, Mem * res);

void sqlitexVdbeMemRelease(Mem *p);
#define VdbeMemDynamic(X)  \
  (((X)->flags&(MEM_Agg|MEM_Dyn|MEM_RowSet|MEM_Frame))!=0)
int sqlitexVdbeMemFinalize(Mem*, FuncDef*);
const char *sqlitexOpcodeName(int);
int sqlitexVdbeMemGrow(Mem *pMem, int n, int preserve);
int sqlitexVdbeMemClearAndResize(Mem *pMem, int n);
int sqlitexVdbeCloseStatement(Vdbe *, int);
void sqlitexVdbeFrameDelete(VdbeFrame*);
int sqlitexVdbeFrameRestore(VdbeFrame *);
int sqlitexVdbeTransferError(Vdbe *p);

int sqlitexVdbeSorterInit(sqlitex *, int, VdbeCursor *);
void sqlitexVdbeSorterReset(sqlitex *, VdbeSorter *);
void sqlitexVdbeSorterClose(sqlitex *, VdbeCursor *);
int sqlitexVdbeSorterRowkey(const VdbeCursor *, Mem *);
int sqlitexVdbeSorterNext(sqlitex *, const VdbeCursor *, int *);
int sqlitexVdbeSorterRewind(const VdbeCursor *, int *);
int sqlitexVdbeSorterWrite(const VdbeCursor *, Mem *);
int sqlitexVdbeSorterCompare(const VdbeCursor *, Mem *, int, int *);

#if !defined(SQLITE_OMIT_SHARED_CACHE) && SQLITE_THREADSAFE>0
  void sqlitexVdbeEnter(Vdbe*);
  void sqlitexVdbeLeave(Vdbe*);
#else
# define sqlitexVdbeEnter(X)
# define sqlitexVdbeLeave(X)
#endif

#ifdef SQLITE_DEBUG
void sqlitexVdbeMemAboutToChange(Vdbe*,Mem*);
int sqlitexVdbeCheckMemInvariants(Mem*);
#endif

#ifndef SQLITE_OMIT_FOREIGN_KEY
int sqlitexVdbeCheckFk(Vdbe *, int);
#else
# define sqlitexVdbeCheckFk(p,i) 0
#endif

int sqlitexVdbeMemTranslate(Mem*, u8);
#ifdef SQLITE_DEBUG
  void sqlitexVdbePrintSql(Vdbe*);
  void sqlitexVdbeMemPrettyPrint(Mem *pMem, char *zBuf);
#endif
int sqlitexVdbeMemHandleBom(Mem *pMem);

#ifndef SQLITE_OMIT_INCRBLOB
  int sqlitexVdbeMemExpandBlob(Mem *);
  #define ExpandBlob(P) (((P)->flags&MEM_Zero)?sqlitexVdbeMemExpandBlob(P):0)
#else
  #define sqlitexVdbeMemExpandBlob(x) SQLITE_OK
  #define ExpandBlob(P) SQLITE_OK
#endif

/* COMDB2 MODIFICATION */
#include <pthread.h>
pthread_key_t sqlitexVDBEkey;
int convMem2ClientDatetimeX(Mem *pMem, void *out);
int convMem2ClientDatetimeStrX(Mem *pMem, void *out, int outlen, int *outdtsz);
int convDttz2ClientDatetimeX(dttz_t *dttz, const char *tzname, void *out, int);

int sqliteVdbeMemDecimalBasicArithmeticsXX(Mem *a, Mem *b, int opcode, Mem * res, int flipped);

extern int gbl_decimal_rounding;
extern int dec_ctx_init( void * crt, int type, int rounding);
extern int dfp_conv_check_status( void *pctx, char *from, char *to);

int sqlitexLockStmtTables(sqlitex_stmt *);

Mem* sqlitexGetCachedResultRow(sqlitex_stmt *pStmt, int *nColumns);

#define sqlitexIsFixedLengthSerialType(t) ( (t)<12 || (t)==SQLITE_MAX_U32 || (t)==(SQLITE_MAX_U32-1) )

#endif /* !defined(_VDBEINT_H_) */
