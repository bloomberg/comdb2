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

#include <errno.h>
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

#define RULESET_MIN_BUF   (100)
#define RULESET_MAX_BUF   (8192)
#define RULESET_MAX_COUNT (1000)

#define RULESET_DELIM "\t\n\r\v\f "
#define RULESET_FLAG_DELIM "\t\n\r\v\f ,{}"
#define RULESET_TEXT_DELIM ";"

/* =============== BEGIN CODE STOLEN FROM "sqlite/src/func.c" =============== */
static const struct compareInfo globCaseInfo = { '*', '?', '[', 0 };
static const struct compareInfo globNoCaseInfo = { '*', '?', '[', 1 };

#define SQLITE_MATCH             0
extern int patternCompare(const u8*,const u8*,const struct compareInfo*,u32);
/* ================ END CODE STOLEN FROM "sqlite/src/func.c" ================ */

/* =========== BEGIN CODE STOLEN FROM "sqlite/ext/misc/regexp.c" ============ */
extern const char *re_compile(void**,const char*,int);
extern int re_match(void*,const unsigned char*,int);
extern void re_free(void*);
/* ============ END CODE STOLEN FROM "sqlite/ext/misc/regexp.c" ============= */

static uint64_t gbl_ruleset_generation = 0;

static int glob_match(
  const char *zStr1,
  const char *zStr2
){
  if( patternCompare(
          (u8*)zStr2, (u8*)zStr1, &globCaseInfo, '[')==SQLITE_MATCH ){
    return RULESET_S_TRUE;
  }else{
    return RULESET_S_FALSE;
  }
}

static int glob_nocase_match(
  const char *zStr1,
  const char *zStr2
){
  if( patternCompare(
          (u8*)zStr2, (u8*)zStr1, &globNoCaseInfo, '[')==SQLITE_MATCH ){
    return RULESET_S_TRUE;
  }else{
    return RULESET_S_FALSE;
  }
}

static int regexp_match(
  const char *zStr1,
  const char *zStr2
){
  if( re_match((void *)zStr2, (const unsigned char *)zStr1, -1) ){
    return RULESET_S_TRUE;
  }else{
    return RULESET_S_FALSE;
  }
}

static xStrCmp comdb2_get_xstrcmp_for_mode(
  ruleset_match_mode_t mode
){
  int noCase = mode&RULESET_MM_NOCASE;
  mode &= ~RULESET_MM_NOCASE;
  switch( mode ){
    case RULESET_MM_EXACT:  return noCase ? strcasecmp : strcmp;
    case RULESET_MM_GLOB:   return noCase ? glob_nocase_match : glob_match;
    case RULESET_MM_REGEXP: return regexp_match; /* NOTE: noCase option was
                                                  *       already handled at
                                                  *       compile-time. */
  }
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
  char **pzEnd,
  char **pzBad,
  char **pzSav
){
  enum ruleset_flags flags = RULESET_F_NONE;
  int count = 0;
  *pFlags = RULESET_F_INVALID; /* assume the worst */
  if( !zBuf ) return;
  if( !sqlite3IsCorrectlyBraced(zBuf) ) return;
  char *zTok = strtok_r(zBuf, RULESET_FLAG_DELIM, pzSav);
  while( zTok!=NULL ){
    if( sqlite3_stricmp(zTok, "NONE")==0 ){
      flags |= RULESET_F_NONE;
      count++;
    }else if( sqlite3_stricmp(zTok, "DISABLE")==0 ){
      flags |= RULESET_F_DISABLE;
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
    if( pzEnd!=NULL ) *pzEnd = zTok+strlen(zTok);
    zTok = strtok_r(NULL, RULESET_FLAG_DELIM, pzSav);
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
  if( nBuf>0 && flags&RULESET_F_DISABLE ){
    nRet = snprintf(zBuf, nBuf, "DISABLE ");
    if( nRet>0 ){ zBuf += nRet; nBuf -= nRet; }
  }
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
  char **pzEnd,
  char **pzBad,
  char **pzSav
){
  enum ruleset_match_mode mode = RULESET_MM_NONE;
  int count = 0;
  *pMode = RULESET_MM_INVALID; /* assume the worst */
  if( !zBuf ) return;
  if( !sqlite3IsCorrectlyBraced(zBuf) ) return;
  char *zTok = strtok_r(zBuf, RULESET_FLAG_DELIM, pzSav);
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
    if( pzEnd!=NULL ) *pzEnd = zTok+strlen(zTok);
    zTok = strtok_r(NULL, RULESET_FLAG_DELIM, pzSav);
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
  uint64_t seqNo
){
  const char *zAction;
  char zFlags[RULESET_MIN_BUF]; /* TODO: When more flags, increase this. */
  char zMode[RULESET_MIN_BUF];  /* TODO: When more modes, increase this. */
  char zFingerprint[FPSZ*2+1]; /* 0123456789ABCDEF0123456789ABCDEF\0 */

  memset(zFlags, 0, sizeof(zFlags));
  memset(zMode, 0, sizeof(zMode));
  memset(zFingerprint, 0, sizeof(zFingerprint));

  zAction = comdb2_ruleset_action_to_str(rule->action, NULL, 0, 1);
  comdb2_ruleset_flags_to_str(rule->flags, zFlags, sizeof(zFlags));
  comdb2_ruleset_match_mode_to_str(rule->mode, zMode, sizeof(zMode));

  struct ruleset_item_criteria *criteria = &rule->criteria;

  if( criteria->pFingerprint!=NULL ){
    util_tohex(zFingerprint, (char *)criteria->pFingerprint, FPSZ);
  }else{
    snprintf(zFingerprint, sizeof(zFingerprint), "<null>");
  }

  logmsg(level, "%s: ruleset %p rule #%d %s seqNo %llu, action "
         "{%s} (0x%llX), adjustment %lld, flags {%s} (0x%llX), mode "
         "{%s} (0x%llX), originHost {%s}, originTask {%s}, user {%s}, "
         "sql {%s}, fingerprint {%s}, evalCount %d, matchCount %d\n",
         __func__, rules, rule->ruleNo,
         zMessage ? zMessage : "<null>",
         (unsigned long long int)seqNo,
         zAction ? zAction : "<null>",
         (unsigned long long int)rule->action, rule->adjustment,
         zFlags, (unsigned long long int)rule->flags,
         zMode, (unsigned long long int)rule->mode,
         criteria->zOriginHost ? criteria->zOriginHost : "<null>",
         criteria->zOriginTask ? criteria->zOriginTask : "<null>",
         criteria->zUser ? criteria->zUser : "<null>",
         criteria->zSql ? criteria->zSql : "<null>",
         zFingerprint, rule->evalCount, rule->matchCount);
}

static ruleset_match_t comdb2_evaluate_ruleset_item(
  xStrCmp stringComparer,
  struct ruleset *rules,
  struct ruleset_item *rule,
  struct ruleset_item_criteria *context,
  struct ruleset_result *result
){
  rule->evalCount++;
  if( stringComparer==NULL ){
    stringComparer = comdb2_get_xstrcmp_for_mode(rule->mode);
  }
  struct ruleset_item_criteria *criteria = &rule->criteria;
  struct ruleset_item_criteria_cache *cache = &rule->cache;
  const char *zOriginHost;
  const char *zOriginTask;
  const char *zUser;
  const char *zSql;
  if( stringComparer==regexp_match ){
    zOriginHost = (const char *)cache->pOriginHostRe;
    zOriginTask = (const char *)cache->pOriginTaskRe;
    zUser = (const char *)cache->pUserRe;
    zSql = (const char *)cache->pSqlRe;
  }else{
    zOriginHost = criteria->zOriginHost;
    zOriginTask = criteria->zOriginTask;
    zUser = criteria->zUser;
    zSql = criteria->zSql;
  }
  if( stringComparer!=NULL ){
    if( zOriginHost!=NULL && ((context->zOriginHost==NULL) ||
        stringComparer(context->zOriginHost, zOriginHost)!=0) ){
      return RULESET_M_FALSE; /* have criteria, not matched */
    }
    if( zOriginTask!=NULL && ((context->zOriginTask==NULL) ||
        stringComparer(context->zOriginTask, zOriginTask)!=0) ){
      return RULESET_M_FALSE; /* have criteria, not matched */
    }
    if( zUser!=NULL && ((context->zUser==NULL) ||
        stringComparer(context->zUser, zUser)!=0) ){
      return RULESET_M_FALSE; /* have criteria, not matched */
    }
    if( zSql!=NULL && ((context->zSql==NULL) ||
        stringComparer(context->zSql, zSql)!=0) ){
      return RULESET_M_FALSE; /* have criteria, not matched */
    }
  }else{
    if( zOriginHost!=NULL ){
      return RULESET_M_NONE; /* no comparer ==> no matching */
    }
    if( zOriginTask!=NULL ){
      return RULESET_M_NONE; /* no comparer ==> no matching */
    }
    if( zUser!=NULL ){
      return RULESET_M_NONE; /* no comparer ==> no matching */
    }
    if( zSql!=NULL ){
      return RULESET_M_NONE; /* no comparer ==> no matching */
    }
  }
  if( criteria->pFingerprint!=NULL ){
    if( !comdb2_ruleset_fingerprints_allowed() ){
      char zFingerprint[FPSZ*2+1]; /* 0123456789ABCDEF0123456789ABCDEF\0 */

      memset(zFingerprint, 0, sizeof(zFingerprint));
      util_tohex(zFingerprint, (char *)criteria->pFingerprint, FPSZ);

      logmsg(LOGMSG_ERROR,
             "%s: rule #%d has fingerprint \"%s\" when fingerprints are "
             "disabled\n", __func__, rule->ruleNo, zFingerprint);

      return RULESET_M_ERROR; /* have forbidden criteria */
    }
    if( (context->pFingerprint==NULL) ||
        memcmp(context->pFingerprint, criteria->pFingerprint, FPSZ)!=0 ){
      return RULESET_M_FALSE; /* have criteria, not matched */
    }
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
        result->action = RULESET_A_REJECT;
      }
      break;
    }
    case RULESET_A_REJECT_ALL: {
      result->ruleNo = rule->ruleNo;
      result->action = RULESET_A_REJECT_ALL;
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
      result->action = rule->action;
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
  rule->matchCount++;
  loglvl level = (rule->flags&RULESET_F_PRINT) ? LOGMSG_USER : LOGMSG_DEBUG;
  if( logmsg_level_ok(level) ){
    comdb2_dump_ruleset_item(level,"MATCHED",rules,rule,get_sql_clnt_seqno());
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
  struct ruleset_item_criteria *context,
  struct ruleset_result *result
){
  size_t count = 0;
  if( rules!=NULL ){
    for(int i=0; i<rules->nRule; i++){
      struct ruleset_item *rule = &rules->aRule[i];
      if( rule->ruleNo==0 ){ continue; }
      if( rule->flags&RULESET_F_DISABLE ){ continue; }
      ruleset_match_t match = comdb2_evaluate_ruleset_item(
        stringComparer, rules, rule, context, result
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
  char zBuf2[RULESET_MIN_BUF] = {0};
  return (size_t)snprintf(zBuf, nBuf, "ruleNo=%d, action=%s, priority=%s",
      result->ruleNo, comdb2_ruleset_action_to_str(result->action, NULL, 0, 1),
      comdb2_priority_to_str(result->priority, zBuf2, sizeof(zBuf2), 0)
  );
}

static int blob_string_to_fingerprint(
  char *zIn, /* BLOB as hexadecimal string, maybe with decorations */
  unsigned char *zOut, /* must be a block of at least FPSZ */
  int bStrict /* format must be (?): X'0123456789ABCDEF0123456789ABCDEF' */
){
  size_t nIn = strlen(zIn);
  int k;
  if( bStrict ){
    if( nIn!=35 ) return 1;
    if( zIn[0]!='X' && zIn[0]!='x' ) return 2;
    if( zIn[1]!='\'' ) return 3;
    if( zIn[nIn-1]!='\'' ) return 4;
    k = 2;
  }else{
    if( nIn!=32 ) return 1;
    k = 0;
  }
  for(int i=k; i<nIn-1; i+=2){
    int j = i - k;
    if( !sqlite3Isxdigit(zIn[i]) ) return 5;
    zOut[j/2] = (sqlite3HexToInt(zIn[i])<<4) | sqlite3HexToInt(zIn[i+1]);
  }
  return 0;
}

int comdb2_enable_ruleset_item(
  struct ruleset *rules,
  int ruleNo,
  int bEnable
){
  if( rules==NULL ) return EINVAL;
  if( ruleNo<1 || ruleNo>rules->nRule ) return ERANGE;
  struct ruleset_item *rule = &rules->aRule[ruleNo-1];
  if( rule->ruleNo==0 ) return ENOENT;
  if( bEnable ){
    rule->flags &= ~RULESET_F_DISABLE;
  }else{
    rule->flags |= RULESET_F_DISABLE;
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
    comdb2_dump_ruleset_item(LOGMSG_USER, NULL, rules, rule, 0);
  }
}

static int recompile_regexp(
  const char *zPattern,
  int noCase,
  void **ppRe,
  const char **pzErr
){
  const char *zErr;

  if( *ppRe!=NULL ){
    re_free(*ppRe);
    *ppRe = NULL;
  }
  zErr = re_compile(ppRe, zPattern, noCase);
  if( zErr!=NULL ){
    if( pzErr!=NULL ) *pzErr = zErr;
    return EINVAL;
  }
  if( *ppRe==NULL ){
    if( pzErr!=NULL ) *pzErr = "out of memory";
    return ENOMEM;
  }
  return 0;
}

static void comdb2_free_ruleset_item_criteria_cache(
  struct ruleset_item_criteria_cache *cache
){
  if( cache==NULL ) return;
  if( cache->pOriginHostRe!=NULL ){
    re_free(cache->pOriginHostRe);
    cache->pOriginHostRe = NULL;
  }
  if( cache->pOriginTaskRe!=NULL ){
    re_free(cache->pOriginTaskRe);
    cache->pOriginTaskRe = NULL;
  }
  if( cache->pUserRe!=NULL ){
    re_free(cache->pUserRe);
    cache->pUserRe = NULL;
  }
  if( cache->pSqlRe!=NULL ){
    re_free(cache->pSqlRe);
    cache->pSqlRe = NULL;
  }
}

int comdb2_load_ruleset_item_criteria(
  const char *zFileName,
  int lineNo,
  char *zBuf,
  size_t nBuf, /* NOT USED */
  int noCase,
  int bAllowFingerprint,
  int bStrictFingerprint,
  struct ruleset_item_criteria *criteria,
  struct ruleset_item_criteria_cache *cache,
  char **pzField,
  char **pzSav,
  size_t *pnFingerprint,
  char *zError,
  size_t nError
){
  int rc = 0;
  if( zBuf==NULL || criteria==NULL || zError==NULL ) return EINVAL;
  const char *zReErr;
  char *zTok = pzField ? *pzField : strtok_r(zBuf, RULESET_DELIM, pzSav);
  while( zTok!=NULL ){
    const char *zField = "originHost";
    if( sqlite3_stricmp(zTok, zField)==0 ){
      zTok = strtok_r(NULL, RULESET_DELIM, pzSav);
      if( zTok==NULL ){
        snprintf(zError, nError,
                 "%s:%d, expected %s value after '%s'",
                 zFileName, lineNo, zField, zField);
        rc = EINVAL;
        goto done;
      }
      if( criteria->zOriginHost ){
        free(criteria->zOriginHost);
        criteria->zOriginHost = NULL;
      }
      criteria->zOriginHost = strdup(zTok);
      if( criteria->zOriginHost==NULL ){
        snprintf(zError, nError,
                 "%s:%d, could not duplicate %s value (%zu bytes)",
                 zFileName, lineNo, zField, strlen(zTok)+1);
        rc = ENOMEM;
        goto done;
      }
      if( cache!=NULL ){
        zReErr = NULL;
        if( recompile_regexp(zTok, noCase, &cache->pOriginHostRe, &zReErr)!=0 ){
          snprintf(zError, nError,
                   "%s:%d, bad %s regular expression '%s': %s",
                   zFileName, lineNo, zField, zTok, zReErr);
          re_free(cache->pOriginHostRe);
          cache->pOriginHostRe = NULL;
          rc = EINVAL;
          goto done;
        }
      }
      zTok = strtok_r(NULL, RULESET_DELIM, pzSav);
      continue;
    }
    zField = "originTask";
    if( sqlite3_stricmp(zTok, zField)==0 ){
      zTok = strtok_r(NULL, RULESET_DELIM, pzSav);
      if( zTok==NULL ){
        snprintf(zError, nError,
                 "%s:%d, expected %s value after '%s'",
                 zFileName, lineNo, zField, zField);
        rc = EINVAL;
        goto done;
      }
      if( criteria->zOriginTask ){
        free(criteria->zOriginTask);
        criteria->zOriginTask = NULL;
      }
      criteria->zOriginTask = strdup(zTok);
      if( criteria->zOriginTask==NULL ){
        snprintf(zError, nError,
                 "%s:%d, could not duplicate %s value (%zu bytes)",
                 zFileName, lineNo, zField, strlen(zTok)+1);
        rc = ENOMEM;
        goto done;
      }
      if( cache!=NULL ){
        zReErr = NULL;
        if( recompile_regexp(zTok, noCase, &cache->pOriginTaskRe, &zReErr)!=0 ){
          snprintf(zError, nError,
                   "%s:%d, bad %s regular expression '%s': %s",
                   zFileName, lineNo, zField, zTok, zReErr);
          re_free(cache->pOriginTaskRe);
          cache->pOriginTaskRe = NULL;
          rc = EINVAL;
          goto done;
        }
      }
      zTok = strtok_r(NULL, RULESET_DELIM, pzSav);
      continue;
    }
    zField = "user";
    if( sqlite3_stricmp(zTok, zField)==0 ){
      zTok = strtok_r(NULL, RULESET_DELIM, pzSav);
      if( zTok==NULL ){
        snprintf(zError, nError,
                 "%s:%d, expected %s value after '%s'",
                 zFileName, lineNo, zField, zField);
        rc = EINVAL;
        goto done;
      }
      if( criteria->zUser ){
        free(criteria->zUser);
        criteria->zUser = NULL;
      }
      criteria->zUser = strdup(zTok);
      if( criteria->zUser==NULL ){
        snprintf(zError, nError,
                 "%s:%d, could not duplicate %s value (%zu bytes)",
                 zFileName, lineNo, zField, strlen(zTok)+1);
        rc = ENOMEM;
        goto done;
      }
      if( cache!=NULL ){
        zReErr = NULL;
        if( recompile_regexp(zTok, noCase, &cache->pUserRe, &zReErr)!=0 ){
          snprintf(zError, nError,
                   "%s:%d, bad %s regular expression '%s': %s",
                   zFileName, lineNo, zField, zTok, zReErr);
          re_free(cache->pUserRe);
          cache->pUserRe = NULL;
          rc = EINVAL;
          goto done;
        }
      }
      zTok = strtok_r(NULL, RULESET_DELIM, pzSav);
      continue;
    }
    zField = "sql";
    if( sqlite3_stricmp(zTok, zField)==0 ){
      zTok = strtok_r(NULL, RULESET_TEXT_DELIM, pzSav);
      if( zTok==NULL ){
        snprintf(zError, nError,
                 "%s:%d, expected %s value after '%s'",
                 zFileName, lineNo, zField, zField);
        rc = EINVAL;
        goto done;
      }
      if( criteria->zSql ){
        free(criteria->zSql);
        criteria->zUser = NULL;
      }
      criteria->zSql = strdup(zTok);
      if( criteria->zSql==NULL ){
        snprintf(zError, nError,
                 "%s:%d, could not duplicate %s value (%zu bytes)",
                 zFileName, lineNo, zField, strlen(zTok)+1);
        rc = ENOMEM;
        goto done;
      }
      if( cache!=NULL ){
        zReErr = NULL;
        if( recompile_regexp(zTok, noCase, &cache->pSqlRe, &zReErr)!=0 ){
          snprintf(zError, nError,
                   "%s:%d, bad %s regular expression '%s': %s",
                   zFileName, lineNo, zField, zTok, zReErr);
          re_free(cache->pSqlRe);
          cache->pSqlRe = NULL;
          rc = EINVAL;
          goto done;
        }
      }
      zTok = strtok_r(NULL, RULESET_DELIM, pzSav);
      continue;
    }
    zField = "fingerprint";
    if( sqlite3_stricmp(zTok, zField)==0 ){
      if( !bAllowFingerprint ){
        snprintf(zError, nError,
                 "%s:%d, field '%s' forbidden by configuration",
                 zFileName, lineNo, zField);
        rc = EACCES;
        goto done;
      }
      if( pnFingerprint!=NULL ) (*pnFingerprint)++;
      zTok = strtok_r(NULL, RULESET_DELIM, pzSav);
      if( zTok==NULL ){
        snprintf(zError, nError,
                 "%s:%d, expected %s value after '%s'",
                 zFileName, lineNo, zField, zField);
        rc = EINVAL;
        goto done;
      }
      if( criteria->pFingerprint ){
        free(criteria->pFingerprint);
        criteria->pFingerprint = NULL;
      }
      criteria->pFingerprint = calloc(FPSZ, sizeof(unsigned char));
      if( criteria->pFingerprint==NULL ){
        snprintf(zError, nError,
                 "%s:%d, could not allocate %s value (%zu bytes)",
                 zFileName, lineNo, zField, (size_t)FPSZ);
        rc = ENOMEM;
        goto done;
      }
      int rc2 = blob_string_to_fingerprint(
        zTok, criteria->pFingerprint, bStrictFingerprint
      );
      if( rc2!=0 ){
        snprintf(zError, nError,
                 "%s:%d, could not parse %s value from '%s': %d",
                 zFileName, lineNo, zField, zTok, rc2);
        rc = EINVAL;
        goto done;
      }
      zTok = strtok_r(NULL, RULESET_DELIM, pzSav);
      continue;
    }
    snprintf(zError, nError,
             "%s:%d, unknown criteria field '%s'",
             zFileName, lineNo, zTok);
    rc = ENOENT;
    goto done;
  }

done:
  if( pzField ) *pzField = zTok;
  return rc;
}

void comdb2_free_ruleset_item_criteria(
  struct ruleset_item_criteria *criteria
){
  if( criteria==NULL ) return;
  if( criteria->zOriginHost!=NULL ){
    free(criteria->zOriginHost);
    criteria->zOriginHost = NULL;
  }
  if( criteria->zOriginTask!=NULL ){
    free(criteria->zOriginTask);
    criteria->zOriginTask = NULL;
  }
  if( criteria->zUser!=NULL ){
    free(criteria->zUser);
    criteria->zUser = NULL;
  }
  if( criteria->zSql!=NULL ){
    free(criteria->zSql);
    criteria->zSql = NULL;
  }
  if( criteria->pFingerprint!=NULL ){
    free(criteria->pFingerprint);
    criteria->pFingerprint = NULL;
  }
}

static void comdb2_free_ruleset_item(
  struct ruleset_item *rule
){
  if( rule==NULL ) return;
  comdb2_free_ruleset_item_criteria_cache(&rule->cache);
  comdb2_free_ruleset_item_criteria(&rule->criteria);
  memset(rule, 0, sizeof(struct ruleset_item));
}

static void comdb2_free_ruleset_int(
  struct ruleset *rules
){
  if( rules==NULL ) return;
  if( rules->aRule!=NULL ){
    for(int i=0; i<rules->nRule; i++){
      struct ruleset_item *rule = &rules->aRule[i];
      comdb2_free_ruleset_item(rule);
    }
    free(rules->aRule);
    rules->aRule = NULL;
  }
  free(rules);
}

void comdb2_free_ruleset(struct ruleset *rules){
  ATOMIC_ADD64(gbl_ruleset_generation, 1);
  comdb2_free_ruleset_int(rules);
}

static int comdb2_more_ruleset_items(
  struct ruleset *rules,
  size_t nRule,
  char *zError,
  size_t nError,
  const char *zFileName,
  int lineNo
){
  if( nRule>rules->nRule ){
    size_t nSize = nRule * sizeof(struct ruleset_item);
    size_t nNewRule = nRule - rules->nRule;
    struct ruleset_item *aNewRule = realloc(rules->aRule, nSize);
    if( aNewRule==NULL ){
      snprintf(zError, nError,
               "%s:%d, could not reallocate %zu rules for %zu new rules "
               "(%zu bytes)", zFileName, lineNo, nRule, nNewRule, nSize);
      return ENOMEM;
    }
    memset(&aNewRule[rules->nRule], 0, nNewRule * sizeof(struct ruleset_item));
    rules->aRule = aNewRule;
    rules->nRule = nRule;
  }
  return 0;
}

static int comdb2_merge_ruleset_items(
  struct ruleset *dst,
  struct ruleset *src,
  char *zError,
  size_t nError,
  const char *zFileName,
  int lineNo
){
  int rc;
  assert( dst!=src );
  rc = comdb2_more_ruleset_items(
    dst, src->nRule, zError, nError, zFileName, lineNo
  );
  if( rc!=0 ) return rc;
  assert( dst->nRule>=src->nRule );
  for(int i=0; i<src->nRule; i++){
    struct ruleset_item *srcRule = &src->aRule[i];
    if( srcRule->ruleNo==0 ){ continue; }
    struct ruleset_item *dstRule = &dst->aRule[i];
    comdb2_free_ruleset_item(dstRule);
    memcpy(dstRule, srcRule, sizeof(struct ruleset_item));
    memset(srcRule, 0, sizeof(struct ruleset_item));
  }
  return 0;
}

int comdb2_load_ruleset(
  const char *zFileName,
  struct ruleset **pRules
){
  int rc = 0;
  char zError[RULESET_MAX_BUF];
  char zLine[RULESET_MAX_BUF];
  const char *zField = NULL;
  size_t nLine;
  int lineNo = 0;
  int fd = -1;
  SBUF2 *sb = NULL;
  i64 version = 0;
  struct ruleset *rules = calloc(1, sizeof(struct ruleset));

  if( rules==NULL ){
    snprintf(zError, sizeof(zError),
             "%s:%d, cannot allocate ruleset (%zu bytes)",
             zFileName, lineNo, sizeof(struct ruleset));
    goto failure;
  }
  fd = open(zFileName, O_RDONLY);
  if( fd==-1 ){
    snprintf(zError, sizeof(zError), "%s:%d, open (read) failed errno=%d",
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
    char *zEnd = NULL;
    char *zBad = NULL;
    char *zTok = NULL;
    char *zSav = NULL;
    const char *zReErr;
    while( isspace(zBuf[0]) ) zBuf++; /* skip leading spaces */
    if( zBuf[0]=='\0' ) continue; /* blank or space-only line */
    if( zBuf[0]=='#' ) continue; /* comment line */
    if( version!=0 ){
      zTok = strtok_r(zBuf, RULESET_DELIM, &zSav);
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
      zTok = strtok_r(NULL, RULESET_DELIM, &zSav);
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
      if( comdb2_more_ruleset_items(rules,
              (size_t)ruleNo, zError, sizeof(zError), zFileName, lineNo) ){
        goto failure;
      }
      assert( ruleNo>0 );
      assert( ruleNo<=rules->nRule );
      struct ruleset_item *rule = &rules->aRule[ruleNo-1];
      if( rule->ruleNo!=ruleNo ){
        if( rule->mode==RULESET_MM_NONE ){
          rule->mode = RULESET_MM_DEFAULT; /* NOTE: System default mode. */
        }
        rule->ruleNo = ruleNo; /* NOTE: Rule is now present. */
      }
      struct ruleset_item_criteria *criteria = &rule->criteria;
      struct ruleset_item_criteria_cache *cache = &rule->cache;
      zTok = strtok_r(NULL, RULESET_DELIM, &zSav);
      while( zTok!=NULL ){
        int rc2 = comdb2_load_ruleset_item_criteria(
          zFileName, lineNo, zTok, -1, rule->mode&RULESET_MM_NOCASE,
          comdb2_ruleset_fingerprints_allowed(), 1, criteria, cache,
          &zTok, &zSav, &rules->nFingerprint, zError, sizeof(zError)
        );
        if( rc2==0 ){
          if( zTok==NULL ) break;
          continue;
        }
        if( rc2!=ENOENT ){
          rc = rc2;
          goto failure; 
        }
        memset(zError, 0, sizeof(zError));
        zField = "action";
        if( sqlite3_stricmp(zTok, zField)==0 ){
          zTok = strtok_r(NULL, RULESET_DELIM, &zSav);
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
          zTok = strtok_r(NULL, RULESET_DELIM, &zSav);
          continue;
        }
        zField = "adjustment";
        if( sqlite3_stricmp(zTok, zField)==0 ){
          zTok = strtok_r(NULL, RULESET_DELIM, &zSav);
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
          zTok = strtok_r(NULL, RULESET_DELIM, &zSav);
          continue;
        }
        zField = "flags";
        if( sqlite3_stricmp(zTok, zField)==0 ){
          zTok = strtok_r(NULL, RULESET_TEXT_DELIM, &zSav);
          if( zTok==NULL ){
            snprintf(zError, sizeof(zError),
                     "%s:%d, expected %s value after '%s'",
                     zFileName, lineNo, zField, zField);
            goto failure;
          }
          comdb2_ruleset_str_to_flags(&rule->flags, zTok, &zEnd, &zBad, &zSav);
          if( rule->flags==RULESET_F_INVALID ){
            snprintf(zError, sizeof(zError),
                     "%s:%d, bad %s value '%s'",
                     zFileName, lineNo, zField, zBad);
            goto failure;
          }
          assert( zEnd!=NULL );
          while( zEnd && zEnd[0]=='\0' && zEnd-zLine<nLine ){ zEnd++; }
          zTok = strtok_r(zEnd, RULESET_DELIM, &zSav);
          continue;
        }
        zField = "mode";
        if( sqlite3_stricmp(zTok, zField)==0 ){
          zTok = strtok_r(NULL, RULESET_TEXT_DELIM, &zSav);
          if( zTok==NULL ){
            snprintf(zError, sizeof(zError),
                     "%s:%d, expected %s value after '%s'",
                     zFileName, lineNo, zField, zField);
            goto failure;
          }
          comdb2_ruleset_str_to_match_mode(
            &rule->mode, zTok, &zEnd, &zBad, &zSav
          );
          if( rule->mode==RULESET_MM_INVALID ){
            snprintf(zError, sizeof(zError),
                     "%s:%d, bad %s value '%s'",
                     zFileName, lineNo, zField, zBad);
            goto failure;
          }
          int noCase = (rule->mode&RULESET_MM_NOCASE);
          if( rule->mode&RULESET_MM_REGEXP ){
            zReErr = NULL;
            if( criteria->zOriginHost!=NULL && recompile_regexp(
                    criteria->zOriginHost, noCase, &cache->pOriginHostRe,
                    &zReErr)!=0 ){
              snprintf(zError, sizeof(zError),
                       "%s:%d, bad %s regular expression '%s': %s",
                       zFileName, lineNo, "originHost", criteria->zOriginHost,
                       zReErr);
              re_free(cache->pOriginHostRe);
              cache->pOriginHostRe = NULL;
              goto failure;
            }
            zReErr = NULL;
            if( criteria->zOriginTask!=NULL && recompile_regexp(
                    criteria->zOriginTask, noCase, &cache->pOriginTaskRe,
                    &zReErr)!=0 ){
              snprintf(zError, sizeof(zError),
                       "%s:%d, bad %s regular expression '%s': %s",
                       zFileName, lineNo, "originTask", criteria->zOriginTask,
                       zReErr);
              re_free(cache->pOriginTaskRe);
              cache->pOriginTaskRe = NULL;
              goto failure;
            }
            zReErr = NULL;
            if( criteria->zUser!=NULL && recompile_regexp(
                    criteria->zUser, noCase, &cache->pUserRe, &zReErr)!=0 ){
              snprintf(zError, sizeof(zError),
                       "%s:%d, bad %s regular expression '%s': %s",
                       zFileName, lineNo, "user", criteria->zUser, zReErr);
              re_free(cache->pUserRe);
              cache->pUserRe = NULL;
              goto failure;
            }
            zReErr = NULL;
            if( criteria->zSql!=NULL && recompile_regexp(
                    criteria->zSql, noCase, &cache->pSqlRe, &zReErr)!=0 ){
              snprintf(zError, sizeof(zError),
                       "%s:%d, bad %s regular expression '%s': %s",
                       zFileName, lineNo, "sql", criteria->zSql, zReErr);
              re_free(cache->pSqlRe);
              cache->pSqlRe = NULL;
              goto failure;
            }
          }else{
            comdb2_free_ruleset_item_criteria_cache(cache);
          }
          assert( zEnd!=NULL );
          while( zEnd && zEnd[0]=='\0' && zEnd-zLine<nLine ){ zEnd++; }
          zTok = strtok_r(zEnd, RULESET_DELIM, &zSav);
          continue;
        }
        snprintf(zError, sizeof(zError),
                 "%s:%d, unknown criteria field '%s'",
                 zFileName, lineNo, zTok);
        goto failure;
      }
    }else{
      zTok = strtok_r(zBuf, RULESET_DELIM, &zSav);
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
      zTok = strtok_r(NULL, RULESET_DELIM, &zSav);
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

  if( *pRules!=NULL ){
    if( comdb2_merge_ruleset_items(
            *pRules, rules, zError, sizeof(zError), zFileName, lineNo)!=0 ){
      goto failure;
    }
    comdb2_free_ruleset_int(rules);
  }else{
    *pRules = rules;
  }

  (*pRules)->generation = ATOMIC_ADD64(gbl_ruleset_generation, 1);
  assert( rc==0 );
  goto done;

failure:
  logmsg(LOGMSG_ERROR, "%s", zError);
  comdb2_free_ruleset_int(rules);
  if( rc==0 ) rc = EINVAL;

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
  char zError[RULESET_MAX_BUF];
  char zBuf[RULESET_MIN_BUF];
  const char *zStr;
  int fd = -1;
  SBUF2 *sb = NULL;

  if( rules==NULL ){
    snprintf(zError, sizeof(zError), "%s, cannot save an invalid ruleset",
             zFileName);
    goto failure;
  }
  fd = open(zFileName, O_WRONLY | O_CREAT | O_TRUNC, 0666);
  if( fd==-1 ){
    snprintf(zError, sizeof(zError), "%s, open (write) failed errno=%d",
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
    struct ruleset_item_criteria *criteria = &rule->criteria;
    if( criteria->zOriginHost!=NULL ){
      if( i>0 && mayNeedLf ){ sbuf2printf(sb, "\n"); mayNeedLf = 0; }
      sbuf2printf(
        sb, "rule %d originHost %s\n", ruleNo, criteria->zOriginHost
      );
    }
    if( criteria->zOriginTask!=NULL ){
      if( i>0 && mayNeedLf ){ sbuf2printf(sb, "\n"); mayNeedLf = 0; }
      sbuf2printf(
        sb, "rule %d originTask %s\n", ruleNo, criteria->zOriginTask
      );
    }
    if( criteria->zUser!=NULL ){
      if( i>0 && mayNeedLf ){ sbuf2printf(sb, "\n"); mayNeedLf = 0; }
      sbuf2printf(sb, "rule %d user %s\n", ruleNo, criteria->zUser);
    }
    if( criteria->zSql!=NULL ){
      if( i>0 && mayNeedLf ){ sbuf2printf(sb, "\n"); mayNeedLf = 0; }
      sbuf2printf(sb, "rule %d sql %s\n", ruleNo, criteria->zSql);
    }
    if( criteria->pFingerprint!=NULL ){
      memset(zBuf, 0, sizeof(zBuf));
      util_tohex(zBuf, (char *)criteria->pFingerprint, FPSZ);
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
