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

#include "sqliteInt.h"
#include "priority_queue.h"
#include "sql.h"
#include "comdb2_ruleset.h"
#include "logmsg.h"

static const struct compareInfo globCaseInfo = { '*', '?', '[', 0 };
static const struct compareInfo globNoCaseInfo = { '*', '?', '[', 1 };

extern int patternCompare(const u8*,const u8*,const struct compareInfo*,u32);

extern const char *re_compile(void**,const char*,int);
extern int re_match(void*,const unsigned char*,int);
extern void re_free(void*);

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

static const char *comdb2_ruleset_action_to_str(
  enum ruleset_action action,
  char *zBuf,
  size_t nBuf
){
  switch( action ){
    case RULESET_A_NONE:
      return "NONE";
    case RULESET_A_REJECT:
      return "REJECT";
    case RULESET_A_UNREJECT:
      return "UNREJECT";
    case RULESET_A_LOW_PRIO:
      return "LOW_PRIO";
    case RULESET_A_HIGH_PRIO:
      return "HIGH_PRIO";
    default: {
      snprintf(zBuf, nBuf, "0x%x", action);
      return zBuf;
    }
  }
}

static const char *comdb2_priority_to_str(
  priority_t priority,
  char *zBuf,
  size_t nBuf
){
  /*
  ** WARNING: This code assumes that higher priority values have
  **          lower numerical values.
  */
  assert( priority>=PRIORITY_T_HIGHEST );
  assert( priority<=PRIORITY_T_LOWEST );
  switch( priority ){
    case PRIORITY_T_INVALID:
      return "INVALID";
    case PRIORITY_T_HIGHEST:
      return "HIGHEST";
    case PRIORITY_T_LOWEST:
      return "LOWEST";
    case PRIORITY_T_HEAD:
      return "HEAD";
    case PRIORITY_T_TAIL:
      return "TAIL";
    case PRIORITY_T_DEFAULT:
      return "DEFAULT";
    default: {
      snprintf(zBuf, nBuf, "0x%llx", priority);
      return zBuf;
    }
  }
}

priority_t comdb2_clamp_priority(
  priority_t priority
){
  /*
  ** WARNING: This code assumes that higher priority values have
  **          lower numerical values.
  */
  if( priority<PRIORITY_T_HIGHEST ){ return PRIORITY_T_HIGHEST; }
  if( priority > PRIORITY_T_LOWEST ){ return PRIORITY_T_LOWEST; }
  return priority;
}

priority_t comdb2_adjust_priority(
  enum ruleset_action action,
  priority_t priority,
  priority_t adjustment
){
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
  result->priority = comdb2_clamp_priority(
    comdb2_adjust_priority(action, result->priority, adjustment)
  );
}

ruleset_match_t comdb2_evaluate_ruleset_item(
  xStrCmp stringComparer,
  xMemCmp memoryComparer,
  struct ruleset_item *rule,
  struct sqlclntstate *clnt,
  struct ruleset_result *result
){
  if( stringComparer==NULL ){
    stringComparer = comdb2_get_xstrcmp_for_mode(rule->mode);
  }
  if( stringComparer!=NULL ){
    if( rule->zOriginHost!=NULL &&
        stringComparer(clnt->origin_host, rule->zOriginHost)!=0 ){
      return RULESET_M_FALSE; /* have criteria, not matched */
    }
    if( rule->zOriginTask!=NULL &&
        stringComparer(clnt->conninfo.pename, rule->zOriginTask)!=0 ){
      return RULESET_M_FALSE; /* have criteria, not matched */
    }
    if( rule->zUser!=NULL && (!clnt->have_user ||
        stringComparer(clnt->user, rule->zUser)!=0) ){
      return RULESET_M_FALSE; /* have criteria, not matched */
    }
    if( rule->zSql!=NULL &&
        stringComparer(clnt->sql, rule->zSql)!=0 ){
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
  if( memoryComparer!=NULL ){
    if( rule->pFingerprint!=NULL && memoryComparer(
            clnt->work.aFingerprint, rule->pFingerprint, FPSZ)!=0 ){
      return RULESET_M_FALSE; /* have criteria, not matched */
    }
  }else{
    if( rule->pFingerprint!=NULL ){
      return RULESET_M_NONE; /* no comparer ==> no matching */
    }
  }
  switch( rule->action ){
    case RULESET_A_NONE: {
      /* do nothing (i.e. caller wants to test for match only) */
      break;
    }
    case RULESET_A_REJECT: {
      result->action |= RULESET_A_REJECT;
      break;
    }
    case RULESET_A_UNREJECT: {
      result->action &= ~RULESET_A_REJECT;
      break;
    }
    case RULESET_A_LOW_PRIO:
    case RULESET_A_HIGH_PRIO: {
      comdb2_adjust_result_priority(rule->action, rule->adjustment, result);
      break;
    }
    default: {
      logmsg(LOGMSG_ERROR,
             "%s: unsupported rule action %d\n", __func__, rule->action);
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
  return (rule->flags & RULESET_F_STOP) ? RULESET_M_STOP : RULESET_M_TRUE;
}

size_t comdb2_evaluate_ruleset(
  xStrCmp stringComparer,
  xMemCmp memoryComparer,
  struct ruleset *rules,
  struct sqlclntstate *clnt,
  struct ruleset_result *result
){
  size_t count = 0;
  for( int i=0; i<rules->nRule; i++ ){
    ruleset_match_t match = comdb2_evaluate_ruleset_item(
      stringComparer, memoryComparer, &rules->aRule[i],
      clnt, result
    );
    if( match==RULESET_M_STOP ){ count++; break; }
    if( match==RULESET_M_TRUE ){ count++; }
  }
  return count;
}

size_t comdb2_ruleset_result_to_str(
  struct ruleset_result *result,
  char *zBuf,
  size_t nBuf
){
  char zActBuf[32] = {0};
  char zPriBuf[32] = {0};

  return (size_t)snprintf(zBuf, nBuf, "action=%s, priority=%s",
      comdb2_ruleset_action_to_str(result->action, zActBuf, sizeof(zActBuf)),
      comdb2_priority_to_str(result->priority, zPriBuf, sizeof(zPriBuf))
  );
}
