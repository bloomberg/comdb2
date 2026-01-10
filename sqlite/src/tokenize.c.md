# SQLite tokenize.c Modifications for Comdb2

This document explains the modifications made to SQLite's tokenizer (`tokenize.c`) for the Comdb2 project. All modifications are wrapped in `#if defined(SQLITE_BUILDING_FOR_COMDB2)` preprocessor directives.

## Summary of Modifications

The Comdb2 modifications to SQLite's tokenizer add support for:

1. **Curly brace tokens** for NoSQL-style queries
2. **Special numeric-suffix keywords** (`GENID48`, `LZ4`)
3. **Mandatory semicolon checking** for SQL statements
4. **Source list extraction mode** for parsing table/view references
5. **Enhanced SQL normalization** with alternate identifier handling
6. **Flexible database connection handling** (optional Vdbe parameter)

## Detailed Modifications

### 1. Curly Brace Character Classes (Lines 7-10, 58-61)

```c
#define CC_LB        29    /* '{' */
#define CC_RB        30    /* '}' */
```

**What it does:**
- Adds two new character classes for left brace `{` and right brace `}`
- These are assigned class codes 29 and 30 respectively

**Why:**
- Enables the tokenizer to recognize curly braces as special tokens
- Required for NoSQL-style query syntax support in Comdb2

### 2. Character Classification Arrays (Lines 18-22, 73-77, 101-107)

**ASCII encoding modifications:**
```c
/* 7x */    1,  1,  1,  1,  1,  1,  1,  1,  0,  1,  1, 29, 10, 30, 25, 27,
```

**EBCDIC encoding modifications:**
```c
/* Cx */   29,  1,  1,  1,  1,  1,  1,  1,  1,  1, 27, 27, 27, 27, 27, 27,
/* Dx */   30,  1,  1,  1,  1,  1,  1,  1,  1,  1, 27, 27, 27, 27, 27, 27,
```

**What it does:**
- Maps the `{` character (0x7B in ASCII, 0xC0 in EBCDIC) to CC_LB (29)
- Maps the `}` character (0x7D in ASCII, 0xD0 in EBCDIC) to CC_RB (30)

**Why:**
- Ensures both ASCII and EBCDIC character encodings properly classify curly braces
- Maintains compatibility across different platforms and character sets

### 3. Special Keyword Tokens: GENID48 and LZ4 (Lines 44-55, 520-531)

```c
if( i==5 && sqlite3StrNICmp((char*)z,"genid",5)==0 && z[i]=='4'
    && z[i+1]=='8' && !IdChar(z[i+2]) ){
  *tokenType = TK_GENID48;
  return 7;
}else if( i==2 && sqlite3StrNICmp((char*)z,"lz",2)==0 && z[i]=='4'
    && !IdChar(z[i+1]) ){
  *tokenType = TK_LZ4;
  return 3;
}
```

**What it does:**
- Recognizes `GENID48` as a special token (returns TK_GENID48, length 7)
- Recognizes `LZ4` as a special token (returns TK_LZ4, length 3)
- Both are case-insensitive (uses `sqlite3StrNICmp`)

**Why:**
- `GENID48`: Comdb2-specific feature for 48-bit generation IDs used for row versioning
- `LZ4`: Support for LZ4 compression algorithm specification
- These tokens contain digits, which normally wouldn't be recognized as SQL keywords
- The special handling allows "keyword + number" combinations that SQLite's standard keyword table can't handle

### 4. NoSQL Token Handler (Lines 62-74, 538-550)

```c
case CC_LB: {
  /*
  ** NOTE: This code assumes that the curly braced block represents the
  **       last token in the string.  For backward compatibility, there
  **       is no checking for unmatched curly braces.
  */
  for(i=1; (c=z[i])!=0; i++){}
  testcase( z[i-1]=='}' );  testcase( z[i-1]!='}' );
  *tokenType = z[i-1]=='}' ? TK_NOSQL : TK_ILLEGAL;
  return i;
}
```

**What it does:**
- When a `{` is encountered, scans to the end of the string
- If the last character is `}`, returns TK_NOSQL token
- If not properly closed, returns TK_ILLEGAL
- Assumes the curly-braced block is the last token

**Why:**
- Supports Comdb2's NoSQL query syntax embedded in SQL statements
- Example: `SELECT * FROM table WHERE {json_query}`
- Backward compatibility: doesn't validate brace matching to avoid breaking existing queries
- Treats the entire braced content as a single token for special processing

### 5. Mandatory Semicolon Checking (Lines 82-84, 97-119, 638-640, 666-686, 698-704)

**Initialization:**
```c
int need_to_check_for_semi = pParse->prepFlags & SQLITE_PREPARE_REQUIRE_SEMI;
```

**End-of-input checking:**
```c
if (need_to_check_for_semi && lastTokenParsed != TK_SEMI) {
  pParse->rc = SQLITE_MISSING_SEMI;
  break;
} else
```

**Illegal token checking:**
```c
} else if (need_to_check_for_semi && !sqlite3_complete(pParse->zTail)) {
  // This check is needed for a case in which a trailing semicolon
  // is nested within an illegal token -- ex: select ';
  pParse->rc = SQLITE_MISSING_SEMI;
  break;
```

**What it does:**
- When `SQLITE_PREPARE_REQUIRE_SEMI` flag is set, enforces semicolon requirement
- Checks at end of input: if last token wasn't TK_SEMI, sets SQLITE_MISSING_SEMI error
- Additional check for incomplete statements using `sqlite3_complete()`
- Once a valid semicolon is found, disables the check

**Why:**
- Comdb2 requires explicit semicolons for statement delimiting in certain contexts
- Prevents ambiguity in multi-statement execution
- Edge case handling: catches semicolons embedded in malformed string literals
- Example of caught error: `SELECT 'incomplete string;` (semicolon inside unclosed quote)

### 6. Source List Extraction Mode (Lines 144-155, 716-727)

```c
if( pParse->prepFlags&SQLITE_PREPARE_SRCLIST_ONLY ){
  if( pParse->rc!=SQLITE_OK ){
    if( pParse->zErrMsg ){
      sqlite3DbFree(db, pParse->zErrMsg);
      pParse->zErrMsg = 0;
    }
    pParse->rc = SQLITE_OK;
    break;
  }
}
```

**What it does:**
- When `SQLITE_PREPARE_SRCLIST_ONLY` flag is set, parsing stops after extracting source tables
- Suppresses and clears any parse errors
- Resets parse status to SQLITE_OK before breaking

**Why:**
- Allows Comdb2 to extract table/view references from SQL without full validation
- Useful for dependency analysis, access control, and query routing
- Doesn't fail on syntax errors - just extracts what it can and stops
- Performance optimization: avoids full parse when only metadata is needed

### 7. Enhanced SQL Normalization Function (Lines 163-363, 814-1014)

**New function signature:**
```c
char *sqlite3Normalize_alternate(
  Vdbe *pVdbe,       /* VM being reprepared */
  const char *zSql,  /* The original SQL string */
  int iDefDqId       /* Zero if double quoted strings should always be
                      * treated as identifiers when there is no Vdbe
                      * available */
)
```

**Key features:**

#### a. Helper Function: `isLeftOperand()` (Lines 169-181, 820-832)
```c
static int isLeftOperand(int tokenType){
  switch( tokenType ){
    case TK_NULL:
    case TK_TRUEFALSE:
    case TK_STRING:
    case TK_INTEGER:
    case TK_FLOAT:
    case TK_VARIABLE:
    case TK_BLOB:
    case TK_ID:       return 1;
    default:          return 0;
  }
}
```

**What it does:**
- Determines if a token can be the left operand of a binary +/- expression

**Why:**
- Helps distinguish between binary operators (`5 + 3`) and unary operators (`+5`)
- Used in normalization to preserve operator context

#### b. Helper Function: `requiresTableId()` (Lines 186-209, 838-860)
```c
static int requiresTableId(int tokenType, int useStrId, int prevType){
  switch( tokenType ){
    case TK_FROM:
    case TK_JOIN:
    case TK_INTO:
    case TK_UPDATE:
    case TK_TABLE:    return 1;
    case TK_LP:
    case TK_SPACE:    return useStrId;
    case TK_STRING:
    case TK_ID:       return ( prevType==TK_INTO || prevType==TK_TABLE );
    default:          return 0;
  }
}
```

**What it does:**
- Determines if the next string token should be treated as an identifier rather than a literal
- Context-aware: considers current token, previous token, and current state

**Why:**
- Handles cases like: `SELECT * FROM 'tablename'` - 'tablename' should be an identifier
- Supports: `INSERT INTO 'table'('col1', 'col2')` - all quoted names are identifiers
- Important for Comdb2's flexible quoting rules

#### c. Main Normalization Logic (Lines 211-363, 862-1013)

**Key differences from standard SQLite normalization:**

1. **Flexible identifier/literal detection:**
```c
int isSingleQuotedLiteral = ( zSql[i]=='\'' );
int isDoubleQuotedLiteral = ( zSql[i]=='"'  && sqlite3VdbeUsesDoubleQuotedString(pVdbe, zId, iDefDqId) );
int isLiteral = ( isSingleQuotedLiteral || isDoubleQuotedLiteral ) && !useStrId;
```

2. **TK_NOSQL preservation:**
```c
case TK_NOSQL: {
  addSpaceSeparator(pStr);
  sqlite3_str_append(pStr, zSql+i, n);
  break;
}
```

3. **Context-aware +/- handling:**
```c
case TK_PLUS:
case TK_MINUS: {
  if( isLeftOperand(prevType) ){
    sqlite3_str_append(pStr, zSql+i, n);
  }
  break;
}
```

**What it does:**
- Normalizes SQL for query plan caching and comparison
- Preserves NoSQL tokens verbatim
- Handles Comdb2's context-dependent string literal vs identifier distinction
- Only includes +/- operators when they're binary (not unary)

**Why:**
- Standard SQLite normalization doesn't understand Comdb2's quoting rules
- NoSQL blocks must be preserved exactly for later processing
- Better cache hit rates by normalizing equivalent queries consistently

### 8. Modified Standard Normalization (Lines 373-377, 385-389, 397-413, 418-424, 1024-1028, 1041-1045, 1099-1112, 1135-1141)

**Optional Vdbe parameter:**
```c
char *sqlite3Normalize(
  Vdbe *pVdbe,       /* VM being reprepared */
  const char *zSql   /* The original SQL string */
#if defined(SQLITE_BUILDING_FOR_COMDB2)
  ,int iDefDqId      /* Zero if double quoted strings should always be
                      * treated as identifiers when there is no Vdbe
                      * available */
#endif
)
```

**Nullable database connection:**
```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
  db = pVdbe ? sqlite3VdbeDb(pVdbe) : 0;
#else
  db = sqlite3VdbeDb(pVdbe);
#endif
```

**Memory allocation change:**
```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
  char *zId = sqlite3_mprintf("%.*s", n, zSql+i);
#else
  char *zId = sqlite3DbStrNDup(db, zSql+i, n);
#endif
```

**Enhanced double-quote checking:**
```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
  if( zSql[i]=='"' && sqlite3VdbeUsesDoubleQuotedString(pVdbe, zId, iDefDqId) ){
#else
  if( zSql[i]=='"' && sqlite3VdbeUsesDoubleQuotedString(pVdbe, zId) ){
#endif
```

**NoSQL token handling:**
```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
  case TK_NOSQL: {
    addSpaceSeparator(pStr);
    sqlite3_str_append(pStr, zSql+i, n);
    break;
  }
#endif
```

**What it does:**
- Allows normalization without a valid Vdbe (pVdbe can be NULL)
- Uses general-purpose `sqlite3_mprintf` instead of db-specific allocation
- Passes additional `iDefDqId` parameter for double-quote interpretation
- Preserves NoSQL tokens in normalized output

**Why:**
- Comdb2 needs to normalize SQL in contexts where no Vdbe exists yet
- The `iDefDqId` parameter controls default behavior for double-quoted strings when database context is unavailable
- NoSQL blocks must survive normalization for later execution
- More flexible than standard SQLite which always requires a Vdbe

## Token Type Summary

New token types introduced for Comdb2:

| Token Type | Value | Description |
|------------|-------|-------------|
| `TK_GENID48` | (defined elsewhere) | 48-bit generation ID keyword |
| `TK_LZ4` | (defined elsewhere) | LZ4 compression algorithm keyword |
| `TK_NOSQL` | (defined elsewhere) | NoSQL query block enclosed in `{}` |

New error codes:

| Error Code | Description |
|------------|-------------|
| `SQLITE_MISSING_SEMI` | Mandatory semicolon is missing |

New prepare flags:

| Flag | Purpose |
|------|---------|
| `SQLITE_PREPARE_REQUIRE_SEMI` | Require statements to end with semicolon |
| `SQLITE_PREPARE_SRCLIST_ONLY` | Extract source tables only, ignore parse errors |

## Architectural Considerations

### Backward Compatibility

- All modifications are conditionally compiled with `SQLITE_BUILDING_FOR_COMDB2`
- NoSQL token handling explicitly preserves backward compatibility (no brace matching validation)
- Semicolon checking is opt-in via prepare flags
- Standard SQLite behavior is unchanged when compiled without the flag

### Performance Implications

1. **NoSQL tokens**: Single-pass scan to end of string (O(n) where n is remaining SQL length)
2. **Special keywords**: Two simple string comparisons with length checks (O(1) for typical cases)
3. **Semicolon checking**: One additional flag check per token (negligible overhead)
4. **Source list mode**: Early termination saves parsing time

### Integration Points

The tokenizer modifications integrate with:

- **Parser** (parse.y): Must recognize new token types (TK_GENID48, TK_LZ4, TK_NOSQL)
- **Vdbe** (vdbe.c): Used for context in normalization
- **Prepare** (prepare.c): Sets prepare flags for semicolon/sourcelist modes
- **Error handling**: New SQLITE_MISSING_SEMI error code

## Use Cases

### 1. NoSQL Hybrid Queries
```sql
SELECT * FROM users WHERE {find: {age: {$gt: 18}}};
```
The `{find: {age: {$gt: 18}}}` portion is tokenized as a single TK_NOSQL token.

### 2. Compression Specification
```sql
CREATE TABLE data (id INT, content TEXT) WITH LZ4 COMPRESSION;
```
`LZ4` is recognized as TK_LZ4 token, not as "LZ" identifier followed by "4" integer.

### 3. Generation ID Queries
```sql
SELECT GENID48 FROM mytable WHERE id = 5;
```
`GENID48` is recognized as TK_GENID48 token for accessing 48-bit row generation IDs.

### 4. Semicolon Enforcement
```sql
-- With SQLITE_PREPARE_REQUIRE_SEMI flag:
SELECT * FROM table   -- Error: SQLITE_MISSING_SEMI
SELECT * FROM table;  -- OK
```

### 5. Table Dependency Extraction
```sql
-- With SQLITE_PREPARE_SRCLIST_ONLY flag:
SELECT a.*, b.col FROM table1 a JOIN table2 b ON a.id = b.id WHERE invalid syntax here
-- Successfully extracts: table1, table2
-- Ignores the syntax error in WHERE clause
```

## Conclusion

The Comdb2 modifications to SQLite's tokenizer enable:

- **NoSQL integration** through curly-brace token support
- **Extended keyword vocabulary** for Comdb2-specific features
- **Stricter SQL validation** with optional semicolon enforcement
- **Metadata extraction** without full parse validation
- **Enhanced normalization** for better query plan caching

These changes maintain full backward compatibility with standard SQLite while extending its capabilities for Comdb2's distributed database requirements.
