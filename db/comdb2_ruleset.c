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
#include "comdb2_ruleset.h"
#include "logmsg.h"
#include "sbuf2.h"

#define RULESET_DELIM "\t\n\r\v\f ,"

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

static void comdb2_ruleset_str_to_action(
  enum ruleset_action *pAction,
  char *zBuf
){
  *pAction = RULESET_A_INVALID; /* assume the worst */
  if( !zBuf ) return;
  while( isspace(zBuf[0]) ) zBuf++;
  if( sqlite3_stricmp(zBuf, "NONE")==0 ){
    *pAction = RULESET_A_NONE;
  }else if( sqlite3_stricmp(zBuf, "REJECT")==0 ){
    *pAction = RULESET_A_REJECT;
  }else if( sqlite3_stricmp(zBuf, "UNREJECT")==0 ){
    *pAction = RULESET_A_UNREJECT;
  }else if( sqlite3_stricmp(zBuf, "LOW_PRIO")==0 ){
    *pAction = RULESET_A_LOW_PRIO;
  }else if( sqlite3_stricmp(zBuf, "HIGH_PRIO")==0 ){
    *pAction = RULESET_A_HIGH_PRIO;
  }
}

static const char *comdb2_ruleset_action_to_str(
  enum ruleset_action action,
  char *zBuf,
  size_t nBuf,
  int bStrict
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
  char *zBuf
){
  enum ruleset_flags flags = RULESET_F_NONE;
  int count = 0;
  *pFlags = RULESET_F_INVALID; /* assume the worst */
  if( !zBuf ) return;
  char *zTok = strtok(zBuf, RULESET_DELIM);
  while( zTok!=NULL ){
    if( sqlite3_stricmp(zTok, "NONE")==0 ){
      count++;
    }else if( sqlite3_stricmp(zTok, "STOP")==0 ){
      flags |= RULESET_F_STOP;
      count++;
    }else{
      return; /* TODO: Bad flag, fail? */
    }
    zTok = strtok(NULL, RULESET_DELIM);
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
  if( nBuf>0 && flags&RULESET_F_STOP ){
    int nRet = snprintf(zBuf, nBuf, " STOP");
    if( nRet>0 ){ zBuf += nRet; nBuf -= nRet; }
  }
  /* more flags here... */
}

static void comdb2_ruleset_str_to_match_mode(
  enum ruleset_match_mode *pMode,
  char *zBuf
){
  enum ruleset_match_mode mode = RULESET_MM_NONE;
  int count = 0;
  *pMode = RULESET_MM_INVALID; /* assume the worst */
  if( !zBuf ) return;
  char *zTok = strtok(zBuf, RULESET_DELIM);
  while( zTok!=NULL ){
    if( sqlite3_stricmp(zTok, "NONE")==0 ){
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
      return; /* TODO: Bad flag, fail? */
    }
    zTok = strtok(NULL, RULESET_DELIM);
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
  if( nBuf>0 && mode&RULESET_MM_EXACT ){
    int nRet = snprintf(zBuf, nBuf, " EXACT");
    if( nRet>0 ){ zBuf += nRet; nBuf -= nRet; }
  }
  if( nBuf>0 && mode&RULESET_MM_GLOB ){
    int nRet = snprintf(zBuf, nBuf, " GLOB");
    if( nRet>0 ){ zBuf += nRet; nBuf -= nRet; }
  }
  if( nBuf>0 && mode&RULESET_MM_REGEXP ){
    int nRet = snprintf(zBuf, nBuf, " REGEXP");
    if( nRet>0 ){ zBuf += nRet; nBuf -= nRet; }
  }
  if( nBuf>0 && mode&RULESET_MM_NOCASE ){
    int nRet = snprintf(zBuf, nBuf, " NOCASE");
    if( nRet>0 ){ zBuf += nRet; nBuf -= nRet; }
  }
}

static const char *comdb2_priority_to_str(
  priority_t priority,
  char *zBuf,
  size_t nBuf,
  int bStrict
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
  if( priority > PRIORITY_T_LOWEST ){ return PRIORITY_T_LOWEST; }
  return priority;
}

static priority_t comdb2_adjust_priority(
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
  result->priority = comdb2_adjust_priority(
    action, result->priority, adjustment
  );
}

static ruleset_match_t comdb2_evaluate_ruleset_item(
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
      comdb2_ruleset_action_to_str(result->action, zActBuf, sizeof(zActBuf), 1),
      comdb2_priority_to_str(result->priority, zPriBuf, sizeof(zPriBuf), 1)
  );
}

static int blob_string_to_fingerprint(
  char *zIn, /* format must be: "X'0123456789ABCDEF0123456789ABCDEF'" */
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

int comdb2_load_ruleset(
  const char *zFileName,
  struct ruleset **pRules
){
  int rc;
  char zLine[8192];
  int nLine = 0;
  int fd = -1;
  SBUF2 *sb = NULL;
  i64 count = 0;
  struct ruleset_item *pRule = NULL;
  struct ruleset *rules = calloc(1, sizeof(struct ruleset));

  if( rules==NULL ){
    snprintf(zLine, sizeof(zLine),
             "%s:%d, cannot allocate ruleset",
             zFileName, nLine);
    goto failure;
  }
  fd = open(zFileName, O_RDONLY);
  if( fd==-1 ){
    snprintf(zLine, sizeof(zLine), "open failed for \"%s\" errno=%d",
             zFileName, errno);
    goto failure;
  }
  sb = sbuf2open(fd, 0);
  if( sb==NULL ){
    snprintf(zLine, sizeof(zLine), "sbuf2open failed for \"%s\" errno=%d",
             zFileName, errno);
    goto failure;
  }
  while( 1 ){
    memset(zLine, 0, sizeof(zLine));
    if( sbuf2gets(zLine, sizeof(zLine), sb)<=0 ) break;
    nLine++;
    if( !zLine[0] ) continue; /* blank line */
    char *zBuf = zLine;
    char *zTok = NULL;
    while( isspace(zBuf[0]) ) zBuf++; /* skip leading spaces */
    if( zBuf[0]=='#' ) continue; /* comment line */
    if( rules!=NULL ){
      zTok = strtok(zBuf, RULESET_DELIM);
      if( zTok==NULL ){
        snprintf(zLine, sizeof(zLine),
                 "%s:%d, expected start-of-rule",
                 zFileName, nLine);
        goto failure;
      }
      if( sqlite3_stricmp(zTok, "rule")!=0 ){
        snprintf(zLine, sizeof(zLine),
                 "%s:%d, expected literal string \"rule\"",
                 zFileName, nLine);
        goto failure;
      }
      zTok = strtok(NULL, RULESET_DELIM);
      if( zTok==NULL ){
        snprintf(zLine, sizeof(zLine),
                 "%s:%d, missing token after \"rule\"",
                 zFileName, nLine);
        goto failure;
      }
      i64 ruleNo = 0;
      if( sqlite3Atoi64(zTok, &ruleNo, strlen(zTok), SQLITE_UTF8)!=0 ){
        snprintf(zLine, sizeof(zLine),
                 "%s:%d, bad rule number \"%s\", not an integer",
                 zFileName, nLine, zTok);
        goto failure;
      }
      if( ruleNo<1 || ruleNo>count ){
        snprintf(zLine, sizeof(zLine),
                 "%s:%d, rule number %lld out-of-bounds, "
                 "must be between 1 and %lld",
                 zFileName, nLine, ruleNo, count);
        goto failure;
      }
      ruleNo--;
      pRule = &rules->aRule[ruleNo];
      zTok = strtok(NULL, RULESET_DELIM);
      while( zTok!=NULL ){
        if( sqlite3_stricmp(zTok, "action")==0 ){
          zTok = strtok(NULL, RULESET_DELIM);
          if( zTok==NULL ){
            snprintf(zLine, sizeof(zLine),
                     "%s:%d, expected value for 'action' field",
                     zFileName, nLine);
            goto failure;
          }
          comdb2_ruleset_str_to_action(&pRule->action, zTok);
          if( pRule->action==RULESET_A_INVALID ){
            snprintf(zLine, sizeof(zLine),
                     "%s:%d, bad 'action' field value \"%s\"",
                     zFileName, nLine, zTok);
            goto failure;
          }
        }else if( sqlite3_stricmp(zTok, "adjustment")==0 ){
          zTok = strtok(NULL, RULESET_DELIM);
          if( zTok==NULL ){
            snprintf(zLine, sizeof(zLine),
                     "%s:%d, expected value for 'adjustment' field",
                     zFileName, nLine);
            goto failure;
          }
          if( sqlite3Atoi64(zTok, &pRule->adjustment, strlen(zTok), SQLITE_UTF8)!=0 ){
            snprintf(zLine, sizeof(zLine),
                     "%s:%d, bad adjustment \"%s\", not an integer",
                     zFileName, nLine, zTok);
            goto failure;
          }
        }else if( sqlite3_stricmp(zTok, "flags")==0 ){
          zTok = strtok(NULL, RULESET_DELIM);
          if( zTok==NULL ){
            snprintf(zLine, sizeof(zLine),
                     "%s:%d, expected value for 'flags' field",
                     zFileName, nLine);
            goto failure;
          }
          comdb2_ruleset_str_to_flags(&pRule->flags, zTok);
          if( pRule->flags==RULESET_F_INVALID ){
            snprintf(zLine, sizeof(zLine),
                     "%s:%d, bad 'flags' field value \"%s\"",
                     zFileName, nLine, zTok);
            goto failure;
          }
        }else if( sqlite3_stricmp(zTok, "mode")==0 ){
          zTok = strtok(NULL, RULESET_DELIM);
          if( zTok==NULL ){
            snprintf(zLine, sizeof(zLine),
                     "%s:%d, expected value for 'mode' field",
                     zFileName, nLine);
            goto failure;
          }
          comdb2_ruleset_str_to_match_mode(&pRule->mode, zTok);
          if( pRule->mode==RULESET_MM_INVALID ){
            snprintf(zLine, sizeof(zLine),
                     "%s:%d, bad 'mode' field value \"%s\"",
                     zFileName, nLine, zTok);
            goto failure;
          }
        }else if( sqlite3_stricmp(zTok, "originHost")==0 ){
          zTok = strtok(NULL, RULESET_DELIM);
          if( zTok==NULL ){
            snprintf(zLine, sizeof(zLine),
                     "%s:%d, expected value for 'originHost' field",
                     zFileName, nLine);
            goto failure;
          }
          pRule->zOriginHost = strdup(zTok);
          if( pRule->zOriginHost==NULL ){
            snprintf(zLine, sizeof(zLine),
                     "%s:%d, could not allocate 'originHost' value",
                     zFileName, nLine);
            goto failure;
          }
        }else if( sqlite3_stricmp(zTok, "originTask")==0 ){
          zTok = strtok(NULL, RULESET_DELIM);
          if( zTok==NULL ){
            snprintf(zLine, sizeof(zLine),
                     "%s:%d, expected value for 'originTask' field",
                     zFileName, nLine);
            goto failure;
          }
          pRule->zOriginTask = strdup(zTok);
          if( pRule->zOriginTask==NULL ){
            snprintf(zLine, sizeof(zLine),
                     "%s:%d, could not allocate 'originTask' value",
                     zFileName, nLine);
            goto failure;
          }
        }else if( sqlite3_stricmp(zTok, "user")==0 ){
          zTok = strtok(NULL, RULESET_DELIM);
          if( zTok==NULL ){
            snprintf(zLine, sizeof(zLine),
                     "%s:%d, expected value for 'user' field",
                     zFileName, nLine);
            goto failure;
          }
          pRule->zUser = strdup(zTok);
          if( pRule->zUser==NULL ){
            snprintf(zLine, sizeof(zLine),
                     "%s:%d, could not allocate 'user' value",
                     zFileName, nLine);
            goto failure;
          }
        }else if( sqlite3_stricmp(zTok, "sql")==0 ){
          zTok = strtok(NULL, RULESET_DELIM);
          if( zTok==NULL ){
            snprintf(zLine, sizeof(zLine),
                     "%s:%d, expected value for 'sql' field",
                     zFileName, nLine);
            goto failure;
          }
          pRule->zSql = strdup(zTok);
          if( pRule->zSql==NULL ){
            snprintf(zLine, sizeof(zLine),
                     "%s:%d, could not allocate 'sql' value",
                     zFileName, nLine);
            goto failure;
          }
        }else if( sqlite3_stricmp(zTok, "fingerprint")==0 ){
          zTok = strtok(NULL, RULESET_DELIM);
          if( zTok==NULL ){
            snprintf(zLine, sizeof(zLine),
                     "%s:%d, expected value for 'fingerprint' field",
                     zFileName, nLine);
            goto failure;
          }
          pRule->pFingerprint = calloc(FPSZ, sizeof(unsigned char));
          if( pRule->pFingerprint==NULL ){
            snprintf(zLine, sizeof(zLine),
                     "%s:%d, cannot allocate fingerprint",
                     zFileName, nLine);
            goto failure;
          }
          if( blob_string_to_fingerprint(zTok, pRule->pFingerprint) ){
            snprintf(zLine, sizeof(zLine),
                     "%s:%d, cannot parse 'fingerprint' field from \"%s\"",
                     zFileName, nLine, zTok);
            goto failure;
          }
        }else{
          snprintf(zLine, sizeof(zLine),
                   "%s:%d, unknown rule %lld field \"%s\"",
                   zFileName, nLine, ruleNo, zTok);
          goto failure;
        }
        zTok = strtok(NULL, RULESET_DELIM);
      }
    }else{
      zTok = strtok(zBuf, RULESET_DELIM);
      if( zTok==NULL ){
        snprintf(zLine, sizeof(zLine),
                 "%s:%d, expected count-of-rules",
                 zFileName, nLine);
        goto failure;
      }
      if( sqlite3_stricmp(zTok, "count")!=0 ){
        snprintf(zLine, sizeof(zLine),
                 "%s:%d, expected literal string \"count\"",
                 zFileName, nLine);
        goto failure;
      }
      zTok = strtok(NULL, RULESET_DELIM);
      if( zTok==NULL ){
        snprintf(zLine, sizeof(zLine),
                 "%s:%d, missing rule count",
                 zFileName, nLine);
        goto failure;
      }
      if( sqlite3Atoi64(zTok, &count, strlen(zTok), SQLITE_UTF8)!=0 ){
        snprintf(zLine, sizeof(zLine),
                 "%s:%d, bad rule count \"%s\", not an integer",
                 zFileName, nLine, zTok);
        goto failure;
      }
      rules->nRule = count;
      rules->aRule = calloc(count, sizeof(struct ruleset_item));
      if( rules->aRule==NULL ){
        snprintf(zLine, sizeof(zLine),
                 "%s:%d, cannot allocate %lld rules",
                 zFileName, nLine, count);
        goto failure;
      }
    }
  }

  *pRules = rules;
  rc = 0;
  goto done;

failure:
  if( rules->aRule!=NULL ){
    for(int i=0; i<rules->nRule; i++){
      pRule = &rules->aRule[i];
      if( pRule->pFingerprint!=NULL ){
        free(pRule->pFingerprint);
      }
    }
    free(rules->aRule);
  }
  if( rules!=NULL ) free(rules);
  rc = 1;

done:
  if( sb!=NULL ) sbuf2close(sb);
  if( fd!=-1 ) close(fd);
  return rc;
}
