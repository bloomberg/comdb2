/*
   Copyright 2019 Bloomberg Finance L.P.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */

#include <fcntl.h>
#include <unistd.h>
#include "sqliteInt.h"
#include "priority_queue.h"
#include "sql.h"
#include "comdb2_atomic.h"
#include "comdb2_ruleset.h"
#include "logmsg.h"
#include "sbuf2.h"
#include "tohex.h"

#define RULESET_MAX_COUNT (1000)

#define RULESET_DELIM "\t\n\r\v\f "
#define RULESET_FLAG_DELIM "\t\n\r\v\f ,{}"
#define RULESET_TEXT_DELIM ";"

static const struct compareInfo globCaseInfo = { '*', '?', '[', 0 };
static const struct compareInfo globNoCaseInfo = { '*', '?', '[', 1 };

extern int patternCompare(const u8*,const u8*,const struct compareInfo*,u32);

extern const char *re_compile(void**,const char*,int);
extern int re_match(void*,const unsigned char*,int);
extern void re_free(void*);

static uint64_t gbl_ruleset_generation = 0;

static int glob_match(
  const char *zStr1,
  const char *zStr2
){
  return patternCompare((u8*)zStr2, (u8*)zStr1, &globCaseInfo, '[');
}

static int glob_nocase_match(
  const char *zStr1,
  const char *zStr2
){
  return patternCompare((u8*)zStr2, (u8*)zStr1, &globNoCaseInfo, '[');
}

static ruleset_string_match_t do_regexp_match(
  const char *zPattern,
  const char *zStr,
  int noCase
){
  int rc;
  void *pRe = 0;
  const char *zErr;

  zErr = re_compile(&pRe, zPattern, noCase);
  if( zErr ){
    logmsg(LOGMSG_ERROR,
           "%s: cannot compile regular expression \"%s\": %s\n",
           __func__, zPattern, zErr);
    re_free(pRe);
    return RULESET_S_ERROR;
  }
  if( pRe==0 ){
    logmsg(LOGMSG_ERROR,
           "%s: out of memory for regular expression \"%s\"\n",
           __func__, zPattern);
    return RULESET_S_ERROR;
  }
  if( re_match(pRe, (const unsigned char *)zStr, -1) ){
    rc = RULESET_S_TRUE;
  }else{
    rc = RULESET_S_FALSE;
  }
  re_free(pRe);
  return rc;
}

static int regexp_match(
  const char *zStr1,
  const char *zStr2
){
  return do_regexp_match(zStr2, zStr1, 0);
}

static int regexp_nocase_match(
  const char *zStr1,
  const char *zStr2
){
  return do_regexp_match(zStr2, zStr1, 1);
}

static xStrCmp comdb2_get_xstrcmp_for_mode(
  ruleset_match_mode_t mode
){
  int noCase = mode & RULESET_MM_NOCASE;
  mode &= ~RULESET_MM_NOCASE;
  switch( mode ){
    case RULESET_MM_EXACT:  return noCase ? strcasecmp : strcmp;
    case RULESET_MM_GLOB:   return noCase ? glob_nocase_match : glob_match;
    case RULESET_MM_REGEXP: return noCase ? regexp_nocase_match : regexp_match;
  }
  return NULL;
}

static xMemCmp comdb2_get_xmemcmp_for_mode(
  ruleset_match_mode_t mode
){
  if( mode==RULESET_MM_EXACT ) return memcmp;
  return NULL;
}

static void comdb2_ruleset_str_to_action(
  enum ruleset_action *pAction,
  char *zBuf,
  char **pzBad
){
  *pAction = RULESET_A_INVALID; /* assume the worst */
  if( !zBuf ) return;
  while( isspace(zBuf[0]) ) zBuf++;
  if( sqlite3_stricmp(zBuf, "NONE")==0 ){
    *pAction = RULESET_A_NONE;
  }else if( sqlite3_stricmp(zBuf, "REJECT")==0 ){
    *pAction = RULESET_A_REJECT;
  }else if( sqlite3_stricmp(zBuf, "REJECT_ALL")==0 ){
    *pAction = RULESET_A_REJECT_ALL;
  }else if( sqlite3_stricmp(zBuf, "UNREJECT")==0 ){
    *pAction = RULESET_A_UNREJECT;
  }else if( sqlite3_stricmp(zBuf, "LOW_PRIO")==0 ){
    *pAction = RULESET_A_LOW_PRIO;
  }else if( sqlite3_stricmp(zBuf, "HIGH_PRIO")==0 ){
    *pAction = RULESET_A_HIGH_PRIO;
  }else if( pzBad!=NULL ){
    *pzBad = zBuf;
  }
}

static const char *comdb2_ruleset_action_to_str(
  enum ruleset_action action,
  char *zBuf,
  size_t nBuf,
  int bStrict
){
  switch( action ){
    case RULESET_A_NONE:       return "NONE";
    case RULESET_A_REJECT:     return "REJECT";
    case RULESET_A_REJECT_ALL: return "REJECT_ALL";
    case RULESET_A_UNREJECT:   return "UNREJECT";
    case RULESET_A_LOW_PRIO:   return "LOW_PRIO";
    case RULESET_A_HIGH_PRIO:  return "HIGH_PRIO";
    default: {
      if( bStrict ){
        return NULL;
      }else{
        snprintf(zBuf, nBuf, "0x%x", action);
        return zBuf;
      }
    }
  }
}

static void comdb2_ruleset_str_to_flags(
  enum ruleset_flags *pFlags,
  char *zBuf,
  char **pzBad
){
  enum ruleset_flags flags = RULESET_F_NONE;
  int count = 0;
  *pFlags = RULESET_F_INVALID; /* assume the worst */
  if( !zBuf ) return;
  if( !sqlite3IsCorrectlyBraced(zBuf) ) return;
  char *zTok = strtok(zBuf, RULESET_FLAG_DELIM);
  while( zTok!=NULL ){
    if( sqlite3_stricmp(zTok, "NONE")==0 ){
      flags |= RULESET_F_NONE;
      count++;
    }else if( sqlite3_stricmp(zTok, "PRINT")==0 ){
      flags |= RULESET_F_PRINT;
      count++;
    }else if( sqlite3_stricmp(zTok, "STOP")==0 ){
      flags |= RULESET_F_STOP;
      count++;
    }else{
      if( pzBad!=NULL ) *pzBad = zTok;
      return;
    }
    zTok = strtok(NULL, RULESET_FLAG_DELIM);
  }
  if( count>0 ) *pFlags = flags;
}

static void comdb2_ruleset_flags_to_str(
  enum ruleset_flags flags,
  char *zBuf,
  size_t nBuf
){
  if( nBuf>0 && flags==RULESET_F_NONE ){
    snprintf(zBuf, nBuf, "NONE");
    return;
  }
  char *zOrig = zBuf;
  int nRet;
  if( nBuf>0 && flags&RULESET_F_PRINT ){
    nRet = snprintf(zBuf, nBuf, "PRINT ");
    if( nRet>0 ){ zBuf += nRet; nBuf -= nRet; }
  }
  if( nBuf>0 && flags&RULESET_F_STOP ){
    nRet = snprintf(zBuf, nBuf, "STOP ");
    if( nRet>0 ){ zBuf += nRet; nBuf -= nRet; }
  }
  int nLen = strlen(zOrig);
  if( nLen>0 && zOrig[nLen-1]==' ' ){
    zOrig[nLen-1] = '\0';
  }
}

static void comdb2_ruleset_str_to_match_mode(
  enum ruleset_match_mode *pMode,
  char *zBuf,
  char **pzBad
){
  enum ruleset_match_mode mode = RULESET_MM_NONE;
  int count = 0;
  *pMode = RULESET_MM_INVALID; /* assume the worst */
  if( !zBuf ) return;
  if( !sqlite3IsCorrectlyBraced(zBuf) ) return;
  char *zTok = strtok(zBuf, RULESET_FLAG_DELIM);
  while( zTok!=NULL ){
    if( sqlite3_stricmp(zTok, "NONE")==0 ){
      mode |= RULESET_MM_NONE;
      count++;
    }else if( sqlite3_stricmp(zTok, "EXACT")==0 ){
      mode |= RULESET_MM_EXACT;
      count++;
    }else if( sqlite3_stricmp(zTok, "GLOB")==0 ){
      mode |= RULESET_MM_GLOB;
      count++;
    }else if( sqlite3_stricmp(zTok, "REGEXP")==0 ){
      mode |= RULESET_MM_REGEXP;
      count++;
    }else if( sqlite3_stricmp(zTok, "NOCASE")==0 ){
      mode |= RULESET_MM_NOCASE;
      count++;
    }else{
      if( pzBad!=NULL ) *pzBad = zTok;
      return;
    }
    zTok = strtok(NULL, RULESET_FLAG_DELIM);
  }
  if( count>0 ) *pMode = mode;
}

static void comdb2_ruleset_match_mode_to_str(
  enum ruleset_match_mode mode,
  char *zBuf,
  size_t nBuf
){
  if( nBuf>0 && mode==RULESET_MM_NONE ){
    snprintf(zBuf, nBuf, "NONE");
    return;
  }
  char *zOrig = zBuf;
  int nRet;
  if( nBuf>0 && mode&RULESET_MM_EXACT ){
    nRet = snprintf(zBuf, nBuf, "EXACT ");
    if( nRet>0 ){ zBuf += nRet; nBuf -= nRet; }
  }
  if( nBuf>0 && mode&RULESET_MM_GLOB ){
    nRet = snprintf(zBuf, nBuf, "GLOB ");
    if( nRet>0 ){ zBuf += nRet; nBuf -= nRet; }
  }
  if( nBuf>0 && mode&RULESET_MM_REGEXP ){
    nRet = snprintf(zBuf, nBuf, "REGEXP ");
    if( nRet>0 ){ zBuf += nRet; nBuf -= nRet; }
  }
  if( nBuf>0 && mode&RULESET_MM_NOCASE ){
    nRet = snprintf(zBuf, nBuf, "NOCASE ");
    if( nRet>0 ){ zBuf += nRet; nBuf -= nRet; }
  }
  int nLen = strlen(zOrig);
  if( nLen>0 && zOrig[nLen-1]==' ' ){
    zOrig[nLen-1] = '\0';
  }
}

const char *comdb2_priority_to_str(
  priority_t priority,
  char *zBuf,
  size_t nBuf,
  int bStrict
){
  /*
  ** WARNING: This code assumes that higher priority values have
  **          lower numerical values.
  */
  switch( priority ){
    case PRIORITY_T_INVALID: return "INVALID";
    case PRIORITY_T_HIGHEST: return "HIGHEST";
    case PRIORITY_T_LOWEST:  return "LOWEST";
    case PRIORITY_T_HEAD:    return "HEAD";
    case PRIORITY_T_TAIL:    return "TAIL";
    case PRIORITY_T_DEFAULT: return "DEFAULT";
    default: {
      if( bStrict ){
        return NULL;
      }else{
        snprintf(zBuf, nBuf, "0x%llx", priority);
        return zBuf;
      }
    }
  }
}

static priority_t comdb2_clamp_priority(
  priority_t priority
){
  /*
  ** WARNING: This code assumes that higher priority values have
  **          lower numerical values.
  */
  if( priority<PRIORITY_T_HIGHEST ){ return PRIORITY_T_HIGHEST; }
  if( priority>PRIORITY_T_LOWEST ){ return PRIORITY_T_LOWEST; }
  return priority;
}

static priority_t comdb2_adjust_priority(
  enum ruleset_action action,
  priority_t priority,
  priority_t adjustment
){
  /*
  ** WARNING: This code assumes that higher priority values have
  **          lower numerical values.
  */
  if( action==RULESET_A_HIGH_PRIO ){
    adjustment = -adjustment;
  }
  priority += adjustment;
  return comdb2_clamp_priority(priority);
}

static void comdb2_adjust_result_priority(
  enum ruleset_action action,
  priority_t adjustment,
  struct ruleset_result *result
){
  result->priority = comdb2_adjust_priority(
    action, result->priority, adjustment
  );
}

static void comdb2_dump_ruleset_item(
  loglvl level,
  char *zMessage,
  struct ruleset *rules,
  struct ruleset_item *rule,
  struct sqlclntstate *clnt
){
  const char *zAction;
  char zFlags[100]; /* TODO: When there are more flags, increase this. */
  char zMode[100];  /* TODO: When there are more modes, increase this. */
  char zFingerprint[FPSZ*2+1]; /* 0123456789ABCDEF0123456789ABCDEF\0 */

  memset(zFlags, 0, sizeof(zFlags));
  memset(zMode, 0, sizeof(zMode));
  memset(zFingerprint, 0, sizeof(zFingerprint));

  zAction = comdb2_ruleset_action_to_str(rule->action, NULL, 0, 1);
  comdb2_ruleset_flags_to_str(rule->flags, zFlags, sizeof(zFlags));
  comdb2_ruleset_match_mode_to_str(rule->mode, zMode, sizeof(zMode));

  if( rule->pFingerprint!=NULL ){
    util_tohex(zFingerprint, (char *)rule->pFingerprint, FPSZ);
  }else{
    snprintf(zFingerprint, sizeof(zFingerprint), "<null>");
  }

  logmsg(level, "%s: ruleset %p rule #%d %s seqNo %llu, action "
         "{%s} (0x%llX), adjustment %lld, flags {%s} (0x%llX), mode "
         "{%s} (0x%llX), originHost {%s}, originTask {%s}, user {%s}, "
         "sql {%s}, fingerprint {%s}\n",
         __func__, rules, rule->ruleNo,
         zMessage ? zMessage : "<null>",
         (unsigned long long int)(clnt ? clnt->seqNo : 0),
         zAction ? zAction : "<null>",
         (unsigned long long int)rule->action,
         rule->adjustment,
         zFlags,
         (unsigned long long int)rule->flags,
         zMode,
         (unsigned long long int)rule->mode,
         rule->zOriginHost ? rule->zOriginHost : "<null>",
         rule->zOriginTask ? rule->zOriginTask : "<null>",
         rule->zUser ? rule->zUser : "<null>",
         rule->zSql ? rule->zSql : "<null>",
         zFingerprint);
}

static ruleset_match_t comdb2_evaluate_ruleset_item(
  xStrCmp stringComparer,
  struct ruleset *rules,
  struct ruleset_item *rule,
  struct sqlclntstate *clnt,
  struct ruleset_result *result
){
  if( stringComparer==NULL ){
    stringComparer = comdb2_get_xstrcmp_for_mode(rule->mode);
  }
  if( stringComparer!=NULL ){
    if( rule->zOriginHost!=NULL && ((clnt->origin_host==NULL) ||
        stringComparer(clnt->origin_host, rule->zOriginHost)!=0) ){
      return RULESET_M_FALSE; /* have criteria, not matched */
    }
    if( rule->zOriginTask!=NULL && ((clnt->conninfo.pename==NULL) ||
        stringComparer(clnt->conninfo.pename, rule->zOriginTask)!=0) ){
      return RULESET_M_FALSE; /* have criteria, not matched */
    }
    if( rule->zUser!=NULL && (!clnt->have_user ||
        stringComparer(clnt->user, rule->zUser)!=0) ){
      return RULESET_M_FALSE; /* have criteria, not matched */
    }
    if( rule->zSql!=NULL && ((clnt->sql==NULL) ||
        stringComparer(clnt->sql, rule->zSql)!=0) ){
      return RULESET_M_FALSE; /* have criteria, not matched */
    }
  }else{
    if( rule->zOriginHost!=NULL ){
      return RULESET_M_NONE; /* no comparer ==> no matching */
    }
    if( rule->zOriginTask!=NULL ){
      return RULESET_M_NONE; /* no comparer ==> no matching */
    }
    if( rule->zUser!=NULL ){
      return RULESET_M_NONE; /* no comparer ==> no matching */
    }
    if( rule->zSql!=NULL ){
      return RULESET_M_NONE; /* no comparer ==> no matching */
    }
  }
  if( rule->pFingerprint!=NULL && !comdb2_ruleset_fingerprints_allowed() ){
    char zFingerprint[FPSZ*2+1]; /* 0123456789ABCDEF0123456789ABCDEF\0 */

    memset(zFingerprint, 0, sizeof(zFingerprint));
    util_tohex(zFingerprint, (char *)rule->pFingerprint, FPSZ);

    logmsg(LOGMSG_ERROR,
           "%s: rule #%d has fingerprint \"%s\" when fingerprints are "
           "disabled\n", __func__, rule->ruleNo, zFingerprint);

    return RULESET_M_ERROR; /* have forbidden criteria */
  }
  if( rule->pFingerprint!=NULL && memcmp(
          clnt->work.aFingerprint, rule->pFingerprint, FPSZ)!=0 ){
    return RULESET_M_FALSE; /* have criteria, not matched */
  }
  switch( rule->action ){
    case RULESET_A_NONE: {
      /* do nothing (i.e. caller wants to test for match only) */
      if( (result->action&RULESET_A_REJECT_MASK)==0 ){
        result->ruleNo = rule->ruleNo;
      }
      break;
    }
    case RULESET_A_REJECT: {
      if( (result->action&RULESET_A_REJECT_MASK)==0 ){
        result->ruleNo = rule->ruleNo;
        result->action |= RULESET_A_REJECT;
      }
      break;
    }
    case RULESET_A_REJECT_ALL: {
      result->ruleNo = rule->ruleNo;
      result->action &= ~RULESET_A_REJECT;
      result->action |= RULESET_A_REJECT_ALL;
      break;
    }
    case RULESET_A_UNREJECT: {
      result->ruleNo = rule->ruleNo;
      result->action &= ~RULESET_A_REJECT_MASK;
      break;
    }
    case RULESET_A_LOW_PRIO:
    case RULESET_A_HIGH_PRIO: {
      if( (result->action&RULESET_A_REJECT_MASK)==0 ){
        result->ruleNo = rule->ruleNo;
      }
      result->action |= rule->action;
      comdb2_adjust_result_priority(rule->action, rule->adjustment, result);
      break;
    }
    default: {
      logmsg(LOGMSG_ERROR,
             "%s: unsupported action 0x%x for rule #%d\n", __func__,
             rule->action, rule->ruleNo);
      break;
    }
  }
  /*
  ** NOTE: If we get to this point, it is for one of the following reasons:
  **
  **       1. There are no criteria specified for this rule; therefore, it
  **          is always considered to "match".
  **
  **       2. This rule matched using the specified mode and all criteria.
  */
  loglvl level = (rule->flags & RULESET_F_PRINT) ? LOGMSG_USER : LOGMSG_DEBUG;
  if( logmsg_level_ok(level) ){
    comdb2_dump_ruleset_item(level, "MATCHED", rules, rule, clnt);
  }
  return (rule->flags&RULESET_F_STOP) ? RULESET_M_STOP : RULESET_M_TRUE;
}

int comdb2_ruleset_fingerprints_allowed(void){
  /*
  ** NOTE: When strict fingerprints are enabled double-quoted strings within
  **       SQL queries will be assumed to refer to database identifiers, not
  **       string literals.  In that mode, fingerprint based matching rules
  **       make sense because SQL queries would be normalized consistently,
  **       even if the query is not prepared first.  This is necessary, in
  **       part, because preparing SQL queries on non-SQL engine threads is
  **       seen as too expensive.
  */
  return gbl_strict_dbl_quotes;
}

size_t comdb2_evaluate_ruleset(
  xStrCmp stringComparer,
  struct ruleset *rules,
  struct sqlclntstate *clnt,
  struct ruleset_result *result
){
  size_t count = 0;
  if( rules!=NULL ){
    for(int i=0; i<rules->nRule; i++){
      struct ruleset_item *rule = &rules->aRule[i];
      if( rule->ruleNo==0 ){ continue; }
      ruleset_match_t match = comdb2_evaluate_ruleset_item(
        stringComparer, rules, rule, clnt, result
      );
      if( match==RULESET_M_ERROR ){
        /* HACK: Invalidate current ruleset result if error. */
        memset(result, 0, sizeof(struct ruleset_result));
        break;
      }
      if( match==RULESET_M_STOP ){ count++; break; }
      if( match==RULESET_M_TRUE ){ count++; }
    }
  }
  return count;
}

size_t comdb2_ruleset_result_to_str(
  struct ruleset_result *result,
  char *zBuf,
  size_t nBuf
){
  char zBuf2[100] = {0};
  return (size_t)snprintf(zBuf, nBuf, "ruleNo=%d, action=%s, priority=%s",
      result->ruleNo, comdb2_ruleset_action_to_str(result->action, NULL, 0, 1),
      comdb2_priority_to_str(result->priority, zBuf2, sizeof(zBuf2), 0)
  );
}

static int blob_string_to_fingerprint(
  char *zIn, /* format must be: X'0123456789ABCDEF0123456789ABCDEF' */
  unsigned char *zOut /* must be a block of at least FPSZ */
){
  size_t nIn = strlen(zIn);
  if( nIn!=35 ) return 1;
  if( zIn[0]!='X' && zIn[0]!='x' ) return 2;
  if( zIn[1]!='\'' ) return 3;
  if( zIn[nIn-1]!='\'' ) return 4;
  int i = 0;
  for(i=2; i<nIn-1; i+=2){
    int j = i - 2;
    if( !sqlite3Isxdigit(zIn[i]) ) return 5;
    zOut[j/2] = (sqlite3HexToInt(zIn[i])<<4) | sqlite3HexToInt(zIn[i+1]);
  }
  return 0;
}

void comdb2_dump_ruleset(struct ruleset *rules){
  if( rules==NULL ) return;
  logmsg(LOGMSG_USER,
         "%s: ruleset %p, generation %llu, rule count %zu, "
         "rule fingerprint count %zu\n", __func__, rules,
         (unsigned long long int)rules->generation, rules->nRule,
         rules->nFingerprint);
  if( rules->aRule==NULL ){
    logmsg(LOGMSG_USER,
           "%s: rules for ruleset %p are missing!\n", __func__, rules);
    return;
  }
  for(int i=0; i<rules->nRule; i++){
    struct ruleset_item *rule = &rules->aRule[i];
    if( rule->ruleNo==0 ){ continue; }
    comdb2_dump_ruleset_item(LOGMSG_USER, NULL, rules, rule, NULL);
  }
}

void comdb2_free_ruleset(struct ruleset *rules){
  ATOMIC_ADD64(gbl_ruleset_generation, 1);
  if( rules!=NULL ){
    if( rules->aRule!=NULL ){
      for(int i=0; i<rules->nRule; i++){
        struct ruleset_item *rule = &rules->aRule[i];
        if( rule->zOriginHost!=NULL ){
          free(rule->zOriginHost);
          rule->zOriginHost = NULL;
        }
        if( rule->zOriginTask!=NULL ){
          free(rule->zOriginTask);
          rule->zOriginTask = NULL;
        }
        if( rule->zUser!=NULL ){
          free(rule->zUser);
          rule->zUser = NULL;
        }
        if( rule->zSql!=NULL ){
          free(rule->zSql);
          rule->zSql = NULL;
        }
        if( rule->pFingerprint!=NULL ){
          free(rule->pFingerprint);
          rule->pFingerprint = NULL;
        }
      }
      free(rules->aRule);
      rules->aRule = NULL;
    }
    free(rules);
  }
}

int comdb2_load_ruleset(
  const char *zFileName,
  struct ruleset **pRules
){
  int rc;
  char zError[8192];
  char zLine[8192];
  const char *zField = NULL;
  size_t nLine;
  int lineNo = 0;
  int fd = -1;
  SBUF2 *sb = NULL;
  i64 version = 0;
  struct ruleset *rules = calloc(1, sizeof(struct ruleset));

  if( rules==NULL ){
    snprintf(zError, sizeof(zError),
             "%s:%d, cannot allocate ruleset",
             zFileName, lineNo);
    goto failure;
  }
  fd = open(zFileName, O_RDONLY);
  if( fd==-1 ){
    snprintf(zError, sizeof(zError), "%s:%d, open failed errno=%d",
             zFileName, lineNo, errno);
    goto failure;
  }
  sb = sbuf2open(fd, 0);
  if( sb==NULL ){
    snprintf(zError, sizeof(zError), "%s:%d, sbuf2open failed errno=%d",
             zFileName, lineNo, errno);
    goto failure;
  }
  while( 1 ){
    memset(zLine, 0, sizeof(zLine));
    if( sbuf2gets(zLine, sizeof(zLine), sb)<=0 ) break;
    nLine = strlen(zLine);
    if( zLine[nLine-1]=='\n' || zLine[nLine-1]=='\r' ){
      zLine[nLine-1] = '\0';
    }
    lineNo++;
    if( !zLine[0] ) continue; /* blank line */
    char *zBuf = zLine;
    char *zBad = NULL;
    char *zTok = NULL;
    while( isspace(zBuf[0]) ) zBuf++; /* skip leading spaces */
    if( zBuf[0]=='\0' ) continue; /* blank or space-only line */
    if( zBuf[0]=='#' ) continue; /* comment line */
    if( version!=0 ){
      zTok = strtok(zBuf, RULESET_DELIM);
      if( zTok==NULL ){
        snprintf(zError, sizeof(zError),
                 "%s:%d, expected start-of-rule",
                 zFileName, lineNo);
        goto failure;
      }
      if( sqlite3_stricmp(zTok, "rule")!=0 ){
        snprintf(zError, sizeof(zError),
                 "%s:%d, expected literal string 'rule'",
                 zFileName, lineNo);
        goto failure;
      }
      zTok = strtok(NULL, RULESET_DELIM);
      if( zTok==NULL ){
        snprintf(zError, sizeof(zError),
                 "%s:%d, expected rule number after 'rule'",
                 zFileName, lineNo);
        goto failure;
      }
      i64 ruleNo = 0;
      if( sqlite3Atoi64(zTok, &ruleNo, strlen(zTok), SQLITE_UTF8)!=0 ){
        snprintf(zError, sizeof(zError),
                 "%s:%d, bad rule number '%s', not an integer",
                 zFileName, lineNo, zTok);
        goto failure;
      }
      if( ruleNo<1 || ruleNo>RULESET_MAX_COUNT ){
        snprintf(zError, sizeof(zError),
                 "%s:%d, rule number %lld is out-of-bounds, "
                 "must be between 1 and %d",
                 zFileName, lineNo, ruleNo, RULESET_MAX_COUNT);
        goto failure;
      }
      if( ruleNo>rules->nRule ){
        size_t nNewRule = ruleNo - rules->nRule;
        struct ruleset_item *aNewRule = realloc(
          rules->aRule, (size_t)ruleNo*sizeof(struct ruleset_item)
        );
        if( aNewRule==NULL ){
          snprintf(zError, sizeof(zError),
                   "%s:%d, could not reallocate %lld rules",
                   zFileName, lineNo, ruleNo);
          goto failure;
        }
        memset(&aNewRule[rules->nRule],0,nNewRule*sizeof(struct ruleset_item));
        rules->aRule = aNewRule;
        rules->nRule = ruleNo;
      }
      assert( ruleNo>0 );
      assert( ruleNo<=rules->nRule );
      struct ruleset_item *rule = &rules->aRule[ruleNo-1];
      rule->ruleNo = ruleNo; /* NOTE: Rule now present. */
      if( rule->mode==RULESET_MM_NONE ){
        rule->mode = RULESET_MM_DEFAULT; /* NOTE: System default match mode. */
      }
      zTok = strtok(NULL, RULESET_DELIM);
      while( zTok!=NULL ){
        zField = "action";
        if( sqlite3_stricmp(zTok, zField)==0 ){
          zTok = strtok(NULL, RULESET_DELIM);
          if( zTok==NULL ){
            snprintf(zError, sizeof(zError),
                     "%s:%d, expected %s value after '%s'",
                     zFileName, lineNo, zField, zField);
            goto failure;
          }
          comdb2_ruleset_str_to_action(&rule->action, zTok, &zBad);
          if( rule->action==RULESET_A_INVALID ){
            snprintf(zError, sizeof(zError),
                     "%s:%d, bad %s field value '%s'",
                     zFileName, lineNo, zField, zBad);
            goto failure;
          }
          zTok = strtok(NULL, RULESET_DELIM);
          continue;
        }
        zField = "adjustment";
        if( sqlite3_stricmp(zTok, zField)==0 ){
          zTok = strtok(NULL, RULESET_DELIM);
          if( zTok==NULL ){
            snprintf(zError, sizeof(zError),
                     "%s:%d, expected %s value after '%s'",
                     zFileName, lineNo, zField, zField);
            goto failure;
          }
          if( sqlite3Atoi64(zTok, &rule->adjustment, strlen(zTok),
                            SQLITE_UTF8)!=0 ){
            snprintf(zError, sizeof(zError),
                     "%s:%d, bad %s value '%s', not an integer",
                     zFileName, lineNo, zField, zTok);
            goto failure;
          }
          if( rule->adjustment<0 ){
            snprintf(zError, sizeof(zError),
                     "%s:%d, bad %s value '%s', cannot be negative",
                     zFileName, lineNo, zField, zTok);
            goto failure;
          }
          if( rule->adjustment>PRIORITY_T_ADJUSTMENT_MAXIMUM ){
            snprintf(zError, sizeof(zError),
                     "%s:%d, bad %s value '%s', cannot exceed %lld",
                     zFileName, lineNo, zField, zTok,
                     PRIORITY_T_ADJUSTMENT_MAXIMUM);
            goto failure;
          }
          zTok = strtok(NULL, RULESET_DELIM);
          continue;
        }
        zField = "flags";
        if( sqlite3_stricmp(zTok, zField)==0 ){
          zTok = strtok(NULL, RULESET_TEXT_DELIM);
          if( zTok==NULL ){
            snprintf(zError, sizeof(zError),
                     "%s:%d, expected %s value after '%s'",
                     zFileName, lineNo, zField, zField);
            goto failure;
          }
          comdb2_ruleset_str_to_flags(&rule->flags, zTok, &zBad);
          if( rule->flags==RULESET_F_INVALID ){
            snprintf(zError, sizeof(zError),
                     "%s:%d, bad %s value '%s'",
                     zFileName, lineNo, zField, zBad);
            goto failure;
          }
          zTok = strtok(NULL, RULESET_DELIM);
          continue;
        }
        zField = "mode";
        if( sqlite3_stricmp(zTok, zField)==0 ){
          zTok = strtok(NULL, RULESET_TEXT_DELIM);
          if( zTok==NULL ){
            snprintf(zError, sizeof(zError),
                     "%s:%d, expected %s value after '%s'",
                     zFileName, lineNo, zField, zField);
            goto failure;
          }
          comdb2_ruleset_str_to_match_mode(&rule->mode, zTok, &zBad);
          if( rule->mode==RULESET_MM_INVALID ){
            snprintf(zError, sizeof(zError),
                     "%s:%d, bad %s value '%s'",
                     zFileName, lineNo, zField, zBad);
            goto failure;
          }
          zTok = strtok(NULL, RULESET_DELIM);
          continue;
        }
        zField = "originHost";
        if( sqlite3_stricmp(zTok, zField)==0 ){
          zTok = strtok(NULL, RULESET_DELIM);
          if( zTok==NULL ){
            snprintf(zError, sizeof(zError),
                     "%s:%d, expected %s value after '%s'",
                     zFileName, lineNo, zField, zField);
            goto failure;
          }
          if( rule->zOriginHost ){
            free(rule->zOriginHost);
            rule->zOriginHost = NULL;
          }
          rule->zOriginHost = strdup(zTok);
          if( rule->zOriginHost==NULL ){
            snprintf(zError, sizeof(zError),
                     "%s:%d, could not duplicate %s value",
                     zFileName, lineNo, zField);
            goto failure;
          }
          zTok = strtok(NULL, RULESET_DELIM);
          continue;
        }
        zField = "originTask";
        if( sqlite3_stricmp(zTok, zField)==0 ){
          zTok = strtok(NULL, RULESET_DELIM);
          if( zTok==NULL ){
            snprintf(zError, sizeof(zError),
                     "%s:%d, expected %s value after '%s'",
                     zFileName, lineNo, zField, zField);
            goto failure;
          }
          if( rule->zOriginTask ){
            free(rule->zOriginTask);
            rule->zOriginTask = NULL;
          }
          rule->zOriginTask = strdup(zTok);
          if( rule->zOriginTask==NULL ){
            snprintf(zError, sizeof(zError),
                     "%s:%d, could not duplicate %s value",
                     zFileName, lineNo, zField);
            goto failure;
          }
          zTok = strtok(NULL, RULESET_DELIM);
          continue;
        }
        zField = "user";
        if( sqlite3_stricmp(zTok, zField)==0 ){
          zTok = strtok(NULL, RULESET_DELIM);
          if( zTok==NULL ){
            snprintf(zError, sizeof(zError),
                     "%s:%d, expected %s value after '%s'",
                     zFileName, lineNo, zField, zField);
            goto failure;
          }
          if( rule->zUser ){
            free(rule->zUser);
            rule->zUser = NULL;
          }
          rule->zUser = strdup(zTok);
          if( rule->zUser==NULL ){
            snprintf(zError, sizeof(zError),
                     "%s:%d, could not duplicate %s value",
                     zFileName, lineNo, zField);
            goto failure;
          }
          zTok = strtok(NULL, RULESET_DELIM);
          continue;
        }
        zField = "sql";
        if( sqlite3_stricmp(zTok, zField)==0 ){
          zTok = strtok(NULL, RULESET_TEXT_DELIM);
          if( zTok==NULL ){
            snprintf(zError, sizeof(zError),
                     "%s:%d, expected %s value after '%s'",
                     zFileName, lineNo, zField, zField);
            goto failure;
          }
          if( rule->zSql ){
            free(rule->zSql);
            rule->zUser = NULL;
          }
          rule->zSql = strdup(zTok);
          if( rule->zSql==NULL ){
            snprintf(zError, sizeof(zError),
                     "%s:%d, could not duplicate %s value",
                     zFileName, lineNo, zField);
            goto failure;
          }
          zTok = strtok(NULL, RULESET_DELIM);
          continue;
        }
        zField = "fingerprint";
        if( sqlite3_stricmp(zTok, zField)==0 ){
          if( !comdb2_ruleset_fingerprints_allowed() ){
            snprintf(zError, sizeof(zError),
                     "%s:%d, field '%s' forbidden by configuration",
                     zFileName, lineNo, zField);
            goto failure;
          }
          rules->nFingerprint++;
          zTok = strtok(NULL, RULESET_DELIM);
          if( zTok==NULL ){
            snprintf(zError, sizeof(zError),
                     "%s:%d, expected %s value after '%s'",
                     zFileName, lineNo, zField, zField);
            goto failure;
          }
          if( rule->pFingerprint ){
            free(rule->pFingerprint);
            rule->pFingerprint = NULL;
          }
          rule->pFingerprint = calloc(FPSZ, sizeof(unsigned char));
          if( rule->pFingerprint==NULL ){
            snprintf(zError, sizeof(zError),
                     "%s:%d, could not allocate %s value",
                     zFileName, lineNo, zField);
            goto failure;
          }
          if( blob_string_to_fingerprint(zTok, rule->pFingerprint) ){
            snprintf(zError, sizeof(zError),
                     "%s:%d, could not parse %s value from '%s'",
                     zFileName, lineNo, zField, zTok);
            goto failure;
          }
          zTok = strtok(NULL, RULESET_DELIM);
          continue;
        }
        snprintf(zError, sizeof(zError),
                 "%s:%d, unknown field '%s' for rule #%lld",
                 zFileName, lineNo, zTok, ruleNo);
        goto failure;
      }
    }else{
      zTok = strtok(zBuf, RULESET_DELIM);
      if( zTok==NULL ){
        snprintf(zError, sizeof(zError),
                 "%s:%d, expected version-of-rules",
                 zFileName, lineNo);
        goto failure;
      }
      if( sqlite3_stricmp(zTok, "version")!=0 ){
        snprintf(zError, sizeof(zError),
                 "%s:%d, expected literal string 'version'",
                 zFileName, lineNo);
        goto failure;
      }
      zTok = strtok(NULL, RULESET_DELIM);
      if( zTok==NULL ){
        snprintf(zError, sizeof(zError),
                 "%s:%d, expected rule version after 'version'",
                 zFileName, lineNo);
        goto failure;
      }
      if( sqlite3Atoi64(zTok, &version, strlen(zTok), SQLITE_UTF8)!=0 ){
        snprintf(zError, sizeof(zError),
                 "%s:%d, bad rule version '%s', not an integer",
                 zFileName, lineNo, zTok);
        goto failure;
      }
      if( version!=1 ){
        snprintf(zError, sizeof(zError),
                 "%s:%d, unsupported rule version %lld",
                 zFileName, lineNo, version);
        goto failure;
      }
    }
  }

  comdb2_free_ruleset(*pRules);
  rules->generation = ATOMIC_ADD64(gbl_ruleset_generation, 1);
  *pRules = rules;

  rc = 0;
  goto done;

failure:
  logmsg(LOGMSG_ERROR, "%s", zError);
  comdb2_free_ruleset(rules);
  rc = 1;

done:
  if( sb!=NULL ) sbuf2close(sb);
  if( fd!=-1 ) close(fd);
  return rc;
}

int comdb2_save_ruleset(
  const char *zFileName,
  struct ruleset *rules
){
  int rc;
  char zError[8192];
  char zBuf[100];
  const char *zStr;
  int fd = -1;
  SBUF2 *sb = NULL;

  if( rules==NULL ){
    snprintf(zError, sizeof(zError), "%s, cannot save invalid ruleset",
             zFileName);
    goto failure;
  }
  fd = open(zFileName, O_WRONLY | O_CREAT | O_TRUNC, 0666);
  if( fd==-1 ){
    snprintf(zError, sizeof(zError), "%s, open failed errno=%d",
             zFileName, errno);
    goto failure;
  }
  sb = sbuf2open(fd, 0);
  if( sb==NULL ){
    snprintf(zError, sizeof(zError), "%s, sbuf2open failed errno=%d",
             zFileName, errno);
    goto failure;
  }
  sbuf2printf(sb, "version %d\n\n", 1);
  for(int i=0; i<rules->nRule; i++){
    struct ruleset_item *rule = &rules->aRule[i];
    int ruleNo = rule->ruleNo;
    if( ruleNo==0 ){ continue; }
    int mayNeedLf = 1;
    if( rule->action!=RULESET_A_INVALID ){
      zStr = comdb2_ruleset_action_to_str(rule->action, NULL, 0, 1);
      if( zStr!=NULL ){
        if( i>0 && mayNeedLf ){ sbuf2printf(sb, "\n"); mayNeedLf = 0; }
        sbuf2printf(sb, "rule %d action %s\n", ruleNo, zStr);
      }
    }
    if( rule->adjustment!=0 ){
      if( i>0 && mayNeedLf ){ sbuf2printf(sb, "\n"); mayNeedLf = 0; }
      sbuf2printf(sb, "rule %d adjustment %lld\n", ruleNo, rule->adjustment);
    }
    if( rule->flags!=RULESET_F_NONE ){
      memset(zBuf, 0, sizeof(zBuf));
      comdb2_ruleset_flags_to_str(rule->flags, zBuf, sizeof(zBuf));
      if( zBuf[0]!='\0' ){
        if( i>0 && mayNeedLf ){ sbuf2printf(sb, "\n"); mayNeedLf = 0; }
        sbuf2printf(sb, "rule %d flags {%s}\n", ruleNo, zBuf);
      }
    }
    if( rule->mode!=RULESET_MM_NONE ){
      memset(zBuf, 0, sizeof(zBuf));
      comdb2_ruleset_match_mode_to_str(rule->mode, zBuf, sizeof(zBuf));
      if( zBuf[0]!='\0' ){
        if( i>0 && mayNeedLf ){ sbuf2printf(sb, "\n"); mayNeedLf = 0; }
        sbuf2printf(sb, "rule %d mode {%s}\n", ruleNo, zBuf);
      }
    }
    if( rule->zOriginHost!=NULL ){
      if( i>0 && mayNeedLf ){ sbuf2printf(sb, "\n"); mayNeedLf = 0; }
      sbuf2printf(sb, "rule %d originHost %s\n", ruleNo, rule->zOriginHost);
    }
    if( rule->zOriginTask!=NULL ){
      if( i>0 && mayNeedLf ){ sbuf2printf(sb, "\n"); mayNeedLf = 0; }
      sbuf2printf(sb, "rule %d originTask %s\n", ruleNo, rule->zOriginTask);
    }
    if( rule->zUser!=NULL ){
      if( i>0 && mayNeedLf ){ sbuf2printf(sb, "\n"); mayNeedLf = 0; }
      sbuf2printf(sb, "rule %d user %s\n", ruleNo, rule->zUser);
    }
    if( rule->zSql!=NULL ){
      if( i>0 && mayNeedLf ){ sbuf2printf(sb, "\n"); mayNeedLf = 0; }
      sbuf2printf(sb, "rule %d sql %s\n", ruleNo, rule->zSql);
    }
    if( rule->pFingerprint!=NULL ){
      memset(zBuf, 0, sizeof(zBuf));
      util_tohex(zBuf, (char *)rule->pFingerprint, FPSZ);
      if( i>0 && mayNeedLf ){ sbuf2printf(sb, "\n"); mayNeedLf = 0; }
      sbuf2printf(sb, "rule %d fingerprint X'%s'\n", ruleNo, zBuf);
    }
  }
  rc = 0;
  goto done;

failure:
  logmsg(LOGMSG_ERROR, "%s", zError);
  rc = 1;

done:
  if( sb!=NULL ) sbuf2close(sb);
  if( fd!=-1 ) close(fd);
  return rc;
}
