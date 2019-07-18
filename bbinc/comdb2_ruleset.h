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

#ifndef _COMDB2_RULESET_H_
#define _COMDB2_RULESET_H_

#include <stddef.h>

#ifndef FPSZ
#define FPSZ 16 /* stolen from "sql.h" FINGERPRINTSZ */
#endif

enum ruleset_action {
  RULESET_A_NONE = 0,     /* Take no action. */

  RULESET_A_REJECT = 1,   /* Reject the request.  May be combined with the
                           * 'STOP' flag to make permanent. */

  RULESET_A_UNREJECT = 2, /* Unreject the request. */

  RULESET_A_LOW_PRIO = 4, /* Lower the priority of the request by the value
                           * associated with this rule. */

  RULESET_A_HIGH_PRIO = 8 /* Raise the priority of the request by the value
                           * associated with this rule. */
};

enum ruleset_flags {
  RULESET_F_NONE = 0,     /* No special behavior. */

  RULESET_F_STOP = 1      /* Stop if the associated rule is matched.  No more
                           * rules will be processed for this request -AND-
                           * the request will NOT be retried. TODO: ? */
};

enum ruleset_match {
  RULESET_M_NONE = 0,     /* The ruleset or item was not matched, possibly
                           * because no matching was performed. */

  RULESET_M_FALSE = 1,    /* The ruleset or item was not matched. */

  RULESET_M_TRUE = 2,     /* The ruleset or item was matched. */

  RULESET_M_STOP = 4      /* The ruleset or item was matched -AND- processing
                           * of the ruleset or item should stop. */
};

struct ruleset_item {
  enum ruleset_action action;     /* If this rule is matched, what should be
                                   * done in respone? */

  priority_t adjustment;          /* For rules which adjust the priority of
                                   * of the work item, what is the (absolute)
                                   * amount the priority should be adjusted?
                                   */

  enum ruleset_flags flags;       /* The behavioral flags associated with the
                                   * rule, e.g. stop-on-match, etc. */

  char *zOriginHost;              /* Obtained via "clnt->origin_host".  If not
                                   * NULL this will be matched using exact
                                   * case-insensitive string comparisons. */

  char *zOriginTask;              /* Obtained via "clnt->conninfo.pename".  If
                                   * not NULL this be matched using exact
                                   * case-insensitive string comparisons. */

  char *zUser;                    /* Obtained via "clnt->have_user" /
                                   * "clnt->user".  If not NULL this be
                                   * matched using exact case-insensitive
                                   * string comparisons. */

  char *zSql;                     /* Obtained via "clnt->sql".  If not NULL
                                   * this be matched using exact
                                   * case-insensitive string comparisons. */

  unsigned char *pFingerprint;    /* Obtained via "reqlogger->fingerprint".
                                   * If not all zeros, this will be matched
                                   * using memcmp(). */
};

struct ruleset {
  int generation;                 /* When was this set of rules read into
                                   * memory?*/

  size_t nRule;                   /* How many rules are in this ruleset? */

  struct ruleset_item *aRule;     /* An array of rules with a minimum size of
                                   * nRule. */
};

struct ruleset_result {
  enum ruleset_action action;     /* What will the final action be for this
                                   * ruleset? */

  priority_t priority;            /* What will the final priority be for this
                                   * ruleset?  Depending on the action, this
                                   * value may be ignored. */
};

typedef int (*xStrCmp)(const char *, const char *);
typedef int (*xMemCmp)(const void *, const void *, size_t);
typedef enum ruleset_match ruleset_match_t;

priority_t comdb2_clamp_priority(
  priority_t priority
);

priority_t comdb2_adjust_priority(
  enum ruleset_action action,
  priority_t priority,
  priority_t adjustment
);

ruleset_match_t comdb2_evaluate_ruleset_item(
  xStrCmp stringComparer,
  xMemCmp memoryComparer,
  struct ruleset_item *rule,
  struct sqlclntstate *clnt,
  unsigned char *pFingerprint,
  struct ruleset_result *result
);

size_t comdb2_evaluate_ruleset(
  xStrCmp stringComparer,
  xMemCmp memoryComparer,
  struct ruleset *rules,
  struct sqlclntstate *clnt,
  unsigned char *pFingerprint,
  struct ruleset_result *result
);

size_t comdb2_ruleset_result_to_str(
  struct ruleset_result *result,
  char *zBuf,
  size_t nBuf
);

#endif /* _COMDB2_RULESET_H_ */
