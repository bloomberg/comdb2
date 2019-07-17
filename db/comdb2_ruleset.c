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

#include "priority_queue.h"
#include "sql.h"
#include "comdb2_ruleset.h"
#include "logmsg.h"

static void comdb2_adjust_priority(
  enum ruleset_action action,
  priority_t adjustment,
  struct ruleset_result *result
){
  priority_t priority = result->priority;
  if (action == RULESET_A_HIGH_PRIO) { adjustment = -adjustment; }
  priority += adjustment;
  /*
  ** WARNING: This assumes that higher priority values have lower
  **          numerical values.
  */
  if (priority < PRIORITY_T_HIGHEST) { priority = PRIORITY_T_HIGHEST; }
  if (priority > PRIORITY_T_LOWEST) { priority = PRIORITY_T_LOWEST; }
  result->priority = priority;
}

ruleset_match_t comdb2_evaluate_ruleset_item(
  xStrCmp stringComparer,
  xMemCmp memoryComparer,
  struct ruleset_item *rule,
  struct sqlclntstate *clnt,
  unsigned char *pFingerprint,
  struct ruleset_result *result
){
  if ((rule->zOriginHost != NULL) &&
      (stringComparer(rule->zOriginHost, clnt->origin_host) != 0)) {
    return RULESET_M_FALSE;
  }
  if ((rule->zOriginTask != NULL) &&
      (stringComparer(rule->zOriginTask, clnt->conninfo.pename) != 0)) {
    return RULESET_M_FALSE;
  }
  if ((rule->zUser != NULL) && (!clnt->have_user ||
      (stringComparer(rule->zUser, clnt->user) != 0))) {
    return RULESET_M_FALSE;
  }
  if ((rule->zSql != NULL) &&
      (stringComparer(rule->zSql, clnt->work.zSql) == 0)) {
    return RULESET_M_FALSE;
  }
  if ((rule->pFingerprint != NULL) &&
      (memoryComparer(rule->pFingerprint, pFingerprint, FPSZ) != 0)) {
    return RULESET_M_FALSE;
  }
  switch (rule->action) {
    case RULESET_A_NONE: {
      /* do nothing (i.e. caller wants to test for match only) */
    }
    case RULESET_A_REJECT: {
      result->action |= RULESET_A_REJECT;
    }
    case RULESET_A_UNREJECT: {
      result->action &= ~RULESET_A_REJECT;
    }
    case RULESET_A_LOW_PRIO:
    case RULESET_A_HIGH_PRIO: {
      comdb2_adjust_priority(rule->action, rule->adjustment, result);
    }
    default: {
      logmsg(LOGMSG_ERROR,
             "%s: unsupported rule action %d\n", __func__, rule->action);
    }
  }
  return (rule->flags & RULESET_F_STOP) ? RULESET_M_STOP : RULESET_M_TRUE;
}

size_t comdb2_evaluate_ruleset(
  xStrCmp stringComparer,
  xMemCmp memoryComparer,
  struct ruleset *rules,
  struct sqlclntstate *clnt,
  unsigned char *pFingerprint,
  struct ruleset_result *result
){
  size_t count = 0;
  for (int i = 0; i < rules->nRule; i++) {
    ruleset_match_t match = comdb2_evaluate_ruleset_item(
      stringComparer, memoryComparer, rules->aRule[i], clnt,
      pFingerprint, result
    );
    if (match == RULESET_M_STOP) { count++; break; }
    if (match == RULESET_M_TRUE) { count++; }
  }
  return count;
}
