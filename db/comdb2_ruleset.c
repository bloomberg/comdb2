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

#ifndef MAX
# define MAX(A,B) ((A)>(B)?(A):(B))
#endif

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
    if( pzEnd!=NULL ) *pzEnd = zTok+strlen(zTok);
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

  struct ruleset_item_criteria *criteria = rule->criteria;

  if( criteria.pFingerprint!=NULL ){
    util_tohex(zFingerprint, (char *)criteria.pFingerprint, FPSZ);
  }else{
    snprintf(zFingerprint, sizeof(zFingerprint), "<null>");
  }

  logmsg(level, "%s: ruleset %p rule #%d %s seqNo %llu, action "
         "{%s} (0x%llX), adjustment %lld, flags {%s} (0x%llX), mode "
         "{%s} (0x%llX), originHost {%s}, originTask {%s}, user {%s}, "
         "sql {%s}, fingerprint {%s}, evalCount %d, matchCount %d\n",
         __func__, rules, rule->ruleNo,
         zMessage ? zMessage : "<null>",
         (unsigned long long int)(clnt ? clnt->seqNo : 0),
         zAction ? zAction : "<null>",
         (unsigned long long int)rule->action, rule->adjustment,
         zFlags, (unsigned long long int)rule->flags,
         zMode, (unsigned long long int)rule->mode,
         criteria.zOriginHost ? criteria.zOriginHost : "<null>",
         criteria.zOriginTask ? criteria.zOriginTask : "<null>",
         criteria.zUser ? criteria.zUser : "<null>",
         criteria.zSql ? criteria.zSql : "<null>",
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
  struct ruleset_item_criteria *criteria = rule->criteria;
  const char *zOriginHost;
  const char *zOriginTask;
  const char *zUser;
  const char *zSql;
  if( stringComparer==regexp_match ){
    zOriginHost = (const char *)rule->pOriginHostRe;
    zOriginTask = (const char *)rule->pOriginTaskRe;
    zUser = (const char *)rule->pUserRe;
    zSql = (const char *)rule->pSqlRe;
  }else{
    zOriginHost = criteria.zOriginHost;
    zOriginTask = criteria.zOriginTask;
    zUser = criteria.zUser;
    zSql = criteria.zSql;
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
  if( criteria.pFingerprint!=NULL ){
    if( !comdb2_ruleset_fingerprints_allowed() ){
      char zFingerprint[FPSZ*2+1]; /* 0123456789ABCDEF0123456789ABCDEF\0 */

      memset(zFingerprint, 0, sizeof(zFingerprint));
      util_tohex(zFingerprint, (char *)criteria.pFingerprint, FPSZ);

      logmsg(LOGMSG_ERROR,
             "%s: rule #%d has fingerprint \"%s\" when fingerprints are "
             "disabled\n", __func__, rule->ruleNo, zFingerprint);

      return RULESET_M_ERROR; /* have forbidden criteria */
    }
    if( memcmp(context->pFingerprint, criteria.pFingerprint, FPSZ)!=0 ){
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
    comdb2_dump_ruleset_item(level, "MATCHED", rules, rule, get_sql_clnt());
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
    comdb2_dump_ruleset_item(LOGMSG_USER, NULL, rules, rule, NULL);
  }
}

static void comdb2_free_ruleset_item_regexps(
  struct ruleset_item *rule
){
  if( rule==NULL ) return;
  if( rule->pOriginHostRe!=NULL ){
    re_free(rule->pOriginHostRe);
    rule->pOriginHostRe = NULL;
  }
  if( rule->pOriginTaskRe!=NULL ){
    re_free(rule->pOriginTaskRe);
    rule->pOriginTaskRe = NULL;
  }
  if( rule->pUserRe!=NULL ){
    re_free(rule->pUserRe);
    rule->pUserRe = NULL;
  }
  if( rule->pSqlRe!=NULL ){
    re_free(rule->pSqlRe);
    rule->pSqlRe = NULL;
  }
}

int comdb2_load_ruleset_item_criteria(
  char *zBuf,
  size_t nBuf,
  struct ruleset_item_criteria *criteria,
  char *zError,
  size_t nError,
  int bNew
){
  if( zBuf==NULL || criteria==NULL || zError==NULL ) return EINVAL;
  char *zTok = strtok(zBuf, RULESET_DELIM);
  while( zTok!=NULL ){
    const char *zField = "originHost";
    if( sqlite3_stricmp(zTok, zField)==0 ){
      zTok = strtok(NULL, RULESET_DELIM);
      if( zTok==NULL ){
        snprintf(zError, nError,
                 "expected %s value after '%s'", zField, zField);
        return EINVAL;
      }
      if( criteria->zOriginHost ){
        free(criteria->zOriginHost);
        criteria->zOriginHost = NULL;
      }
      criteria->zOriginHost = strdup(zTok);
      if( criteria->zOriginHost==NULL ){
        snprintf(zError, nError,
                 "could not duplicate %s value", zField);
        return ENOMEM;
      }
      zTok = strtok(NULL, RULESET_DELIM);
      continue;
    }
    zField = "originTask";
    if( sqlite3_stricmp(zTok, zField)==0 ){
      zTok = strtok(NULL, RULESET_DELIM);
      if( zTok==NULL ){
        snprintf(zError, nError,
                 "expected %s value after '%s'", zField, zField);
        return EINVAL;
      }
      if( criteria->zOriginTask ){
        free(criteria->zOriginTask);
        criteria->zOriginTask = NULL;
      }
      criteria->zOriginTask = strdup(zTok);
      if( criteria->zOriginTask==NULL ){
        snprintf(zError, nError,
                 "could not duplicate %s value", zField);
        return ENOMEM;
      }
      zTok = strtok(NULL, RULESET_DELIM);
      continue;
    }
    zField = "user";
    if( sqlite3_stricmp(zTok, zField)==0 ){
      zTok = strtok(NULL, RULESET_DELIM);
      if( zTok==NULL ){
        snprintf(zError, nError,
                 "expected %s value after '%s'", zField, zField);
        return EINVAL;
      }
      if( criteria->zUser ){
        free(criteria->zUser);
        criteria->zUser = NULL;
      }
      criteria->zUser = strdup(zTok);
      if( criteria->zUser==NULL ){
        snprintf(zError, nError,
                 "could not duplicate %s value", zField);
        return ENOMEM;
      }
      zTok = strtok(NULL, RULESET_DELIM);
      continue;
    }
    zField = "sql";
    if( sqlite3_stricmp(zTok, zField)==0 ){
      zTok = strtok(NULL, RULESET_TEXT_DELIM);
      if( zTok==NULL ){
        snprintf(zError, nError,
                 "expected %s value after '%s'", zField, zField);
        return EINVAL;
      }
      if( criteria->zSql ){
        free(criteria->zSql);
        criteria->zUser = NULL;
      }
      criteria->zSql = strdup(zTok);
      if( criteria->zSql==NULL ){
        snprintf(zError, sizeof(zError),
                 "could not duplicate %s value", zField);
        return ENOMEM;
      }
      zTok = strtok(NULL, RULESET_DELIM);
      continue;
    }
    zField = "fingerprint";
    if( sqlite3_stricmp(zTok, zField)==0 ){
      zTok = strtok(NULL, RULESET_DELIM);
      if( zTok==NULL ){
        snprintf(zError, sizeof(zError),
                 "expected %s value after '%s'", zField, zField);
        return EINVAL;
      }
      if( criteria->pFingerprint ){
        free(criteria->pFingerprint);
        criteria->pFingerprint = NULL;
      }
      criteria->pFingerprint = calloc(FPSZ, sizeof(unsigned char));
      if( criteria->pFingerprint==NULL ){
        snprintf(zError, nError,
                 "could not allocate %s value", zField);
        return ENOMEM;
      }
      if( blob_string_to_fingerprint(zTok, criteria->pFingerprint) ){
        snprintf(zError, nError,
                 "could not parse %s value from '%s'", zField, zTok);
        return EINVAL;
      }
      zTok = strtok(NULL, RULESET_DELIM);
      continue;
    }
    snprintf(zError, nError,
             "unknown criteria field '%s'", zTok);
    return EINVAL;
  }
  return 0;
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
  comdb2_free_ruleset_item_regexps(rule);
  comdb2_free_ruleset_item_criteria(rule->criteria);
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

static int recompile_regexp(
  const char *zPattern,
  int noCase,
  void **ppRe
){
  const char *zErr;

  if( *ppRe!=NULL ){
    re_free(*ppRe);
    *ppRe = NULL;
  }
  zErr = re_compile(ppRe, zPattern, noCase);
  if( zErr ){
    logmsg(LOGMSG_ERROR,
           "%s: cannot compile regular expression \"%s\": %s\n",
           __func__, zPattern, zErr);
    re_free(*ppRe);
    return EINVAL;
  }
  if( *ppRe==NULL ){
    logmsg(LOGMSG_ERROR,
           "%s: out of memory for regular expression \"%s\"\n",
           __func__, zPattern);
    return ENOMEM;
  }
  return 0;
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
    size_t nNewRule = nRule - rules->nRule;
    struct ruleset_item *aNewRule = realloc(
      rules->aRule, nRule * sizeof(struct ruleset_item)
    );
    if( aNewRule==NULL ){
      snprintf(zError, nError,
               "%s:%d, could not reallocate %zu rules for %zu new rules",
               zFileName, lineNo, nRule, nNewRule);
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
  assert( dst->nRule>=src->nRule );
  if( rc!=0 ) return rc;
  for(int i=0; i<src->nRule; i++){
    struct ruleset_item *srcRule = &src->aRule[i];
    if( srcRule->ruleNo==0 ){ continue; }
    struct ruleset_item *dstRule = &dst->aRule[i];
    comdb2_free_ruleset_item(dstRule);
    memcpy(dstRule, srcRule, sizeof(struct ruleset_item));
    memset(srcRule, 0, sizeof(struct ruleset_item));
  }
  dst->nRule = MAX(dst->nRule, src->nRule);
  return 0;
}

int comdb2_load_ruleset(
  const char *zFileName,
  struct ruleset **pRules
){
  int rc;
  char zError[8192];
  char zLine[8192];
  const char *zField = NULL;
  int noCase;
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
    char *zEnd = NULL;
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
      struct ruleset_item_criteria *criteria = rule->criteria;
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
          comdb2_ruleset_str_to_flags(&rule->flags, zTok, &zEnd, &zBad);
          if( rule->flags==RULESET_F_INVALID ){
            snprintf(zError, sizeof(zError),
                     "%s:%d, bad %s value '%s'",
                     zFileName, lineNo, zField, zBad);
            goto failure;
          }
          assert( zEnd!=NULL );
          while( zEnd && zEnd[0]=='\0' && zEnd-zLine<nLine ){ zEnd++; }
          zTok = strtok(zEnd, RULESET_DELIM);
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
          comdb2_ruleset_str_to_match_mode(&rule->mode, zTok, &zEnd, &zBad);
          if( rule->mode==RULESET_MM_INVALID ){
            snprintf(zError, sizeof(zError),
                     "%s:%d, bad %s value '%s'",
                     zFileName, lineNo, zField, zBad);
            goto failure;
          }
          noCase = (rule->mode&RULESET_MM_NOCASE);
          if( rule->mode&RULESET_MM_REGEXP ){
            if( criteria.zOriginHost!=NULL && recompile_regexp(
                    criteria.zOriginHost, noCase, &rule->pOriginHostRe)!=0 ){
              snprintf(zError, sizeof(zError),
                       "%s:%d, bad %s regular expression '%s'",
                       zFileName, lineNo, "originHost", criteria.zOriginHost);
              goto failure;
            }
            if( criteria.zOriginTask!=NULL && recompile_regexp(
                    criteria.zOriginTask, noCase, &rule->pOriginTaskRe)!=0 ){
              snprintf(zError, sizeof(zError),
                       "%s:%d, bad %s regular expression '%s'",
                       zFileName, lineNo, "originTask", criteria.zOriginTask);
              goto failure;
            }
            if( criteria.zUser!=NULL && recompile_regexp(
                    criteria.zUser, noCase, &rule->pUserRe)!=0 ){
              snprintf(zError, sizeof(zError),
                       "%s:%d, bad %s regular expression '%s'",
                       zFileName, lineNo, "user", criteria.zUser);
              goto failure;
            }
            if( criteria.zSql!=NULL && recompile_regexp(
                    criteria.zSql, noCase, &rule->pSqlRe)!=0 ){
              snprintf(zError, sizeof(zError),
                       "%s:%d, bad %s regular expression '%s'",
                       zFileName, lineNo, "sql", criteria.zSql);
              goto failure;
            }
          }else{
            comdb2_free_ruleset_item_regexps(rule);
          }
          assert( zEnd!=NULL );
          while( zEnd && zEnd[0]=='\0' && zEnd-zLine<nLine ){ zEnd++; }
          zTok = strtok(zEnd, RULESET_DELIM);
          continue;
        }
        noCase = (rule->mode&RULESET_MM_NOCASE);
        zField = "originHost";
        if( sqlite3_stricmp(zTok, zField)==0 ){
          zTok = strtok(NULL, RULESET_DELIM);
          if( zTok==NULL ){
            snprintf(zError, sizeof(zError),
                     "%s:%d, expected %s value after '%s'",
                     zFileName, lineNo, zField, zField);
            goto failure;
          }
          if( criteria.zOriginHost ){
            free(criteria.zOriginHost);
            criteria.zOriginHost = NULL;
          }
          criteria.zOriginHost = strdup(zTok);
          if( criteria.zOriginHost==NULL ){
            snprintf(zError, sizeof(zError),
                     "%s:%d, could not duplicate %s value",
                     zFileName, lineNo, zField);
            goto failure;
          }
          if( rule->mode&RULESET_MM_REGEXP ){
            if( recompile_regexp(zTok, noCase, &rule->pOriginHostRe)!=0 ){
              snprintf(zError, sizeof(zError),
                       "%s:%d, bad %s regular expression '%s'",
                       zFileName, lineNo, zField, zTok);
              goto failure;
            }
          }else if( rule->pOriginHostRe!=NULL ){
            re_free(rule->pOriginHostRe);
            rule->pOriginHostRe = NULL;
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
          if( criteria.zOriginTask ){
            free(criteria.zOriginTask);
            criteria.zOriginTask = NULL;
          }
          criteria.zOriginTask = strdup(zTok);
          if( criteria.zOriginTask==NULL ){
            snprintf(zError, sizeof(zError),
                     "%s:%d, could not duplicate %s value",
                     zFileName, lineNo, zField);
            goto failure;
          }
          if( rule->mode&RULESET_MM_REGEXP ){
            if( recompile_regexp(zTok, noCase, &rule->pOriginTaskRe)!=0 ){
              snprintf(zError, sizeof(zError),
                       "%s:%d, bad %s regular expression '%s'",
                       zFileName, lineNo, zField, zTok);
              goto failure;
            }
          }else if( rule->pOriginTaskRe!=NULL ){
            re_free(rule->pOriginTaskRe);
            rule->pOriginTaskRe = NULL;
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
          if( criteria.zUser ){
            free(criteria.zUser);
            criteria.zUser = NULL;
          }
          criteria.zUser = strdup(zTok);
          if( criteria.zUser==NULL ){
            snprintf(zError, sizeof(zError),
                     "%s:%d, could not duplicate %s value",
                     zFileName, lineNo, zField);
            goto failure;
          }
          if( rule->mode&RULESET_MM_REGEXP ){
            if( recompile_regexp(zTok, noCase, &rule->pUserRe)!=0 ){
              snprintf(zError, sizeof(zError),
                       "%s:%d, bad %s regular expression '%s'",
                       zFileName, lineNo, zField, zTok);
              goto failure;
            }
          }else if( rule->pUserRe!=NULL ){
            re_free(rule->pUserRe);
            rule->pUserRe = NULL;
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
          if( criteria.zSql ){
            free(criteria.zSql);
            criteria.zUser = NULL;
          }
          criteria.zSql = strdup(zTok);
          if( criteria.zSql==NULL ){
            snprintf(zError, sizeof(zError),
                     "%s:%d, could not duplicate %s value",
                     zFileName, lineNo, zField);
            goto failure;
          }
          if( rule->mode&RULESET_MM_REGEXP ){
            if( recompile_regexp(zTok, noCase, &rule->pSqlRe)!=0 ){
              snprintf(zError, sizeof(zError),
                       "%s:%d, bad %s regular expression '%s'",
                       zFileName, lineNo, zField, zTok);
              goto failure;
            }
          }else if( rule->pSqlRe!=NULL ){
            re_free(rule->pSqlRe);
            rule->pSqlRe = NULL;
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
          if( criteria.pFingerprint ){
            free(criteria.pFingerprint);
            criteria.pFingerprint = NULL;
          }
          criteria.pFingerprint = calloc(FPSZ, sizeof(unsigned char));
          if( criteria.pFingerprint==NULL ){
            snprintf(zError, sizeof(zError),
                     "%s:%d, could not allocate %s value",
                     zFileName, lineNo, zField);
            goto failure;
          }
          if( blob_string_to_fingerprint(zTok, criteria.pFingerprint) ){
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
  rc = 0;
  goto done;

failure:
  logmsg(LOGMSG_ERROR, "%s", zError);
  comdb2_free_ruleset_int(rules);
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
    struct ruleset_item_criteria *criteria = rule->criteria;
    if( criteria.zOriginHost!=NULL ){
      if( i>0 && mayNeedLf ){ sbuf2printf(sb, "\n"); mayNeedLf = 0; }
      sbuf2printf(
        sb, "rule %d originHost %s\n", ruleNo, criteria.zOriginHost
      );
    }
    if( criteria.zOriginTask!=NULL ){
      if( i>0 && mayNeedLf ){ sbuf2printf(sb, "\n"); mayNeedLf = 0; }
      sbuf2printf(
        sb, "rule %d originTask %s\n", ruleNo, criteria.zOriginTask
      );
    }
    if( criteria.zUser!=NULL ){
      if( i>0 && mayNeedLf ){ sbuf2printf(sb, "\n"); mayNeedLf = 0; }
      sbuf2printf(sb, "rule %d user %s\n", ruleNo, criteria.zUser);
    }
    if( criteria.zSql!=NULL ){
      if( i>0 && mayNeedLf ){ sbuf2printf(sb, "\n"); mayNeedLf = 0; }
      sbuf2printf(sb, "rule %d sql %s\n", ruleNo, criteria.zSql);
    }
    if( criteria.pFingerprint!=NULL ){
      memset(zBuf, 0, sizeof(zBuf));
      util_tohex(zBuf, (char *)criteria.pFingerprint, FPSZ);
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
