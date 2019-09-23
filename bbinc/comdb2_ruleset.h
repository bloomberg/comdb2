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
  RULESET_A_INVALID = -1,   /* Invalid action, this probably means parsing a
                             * string failed to result in a valid action. */

  RULESET_A_NONE = 0,       /* Take no action. */

  RULESET_A_REJECT = 1,     /* Reject the request.  May be combined with the
                             * 'STOP' flag to make permanent. */

  RULESET_A_REJECT_ALL = 2, /* Reject the request with an error indicating
                             * that it may not be retried on another node.
                             * May be combined with the 'STOP' flag to make
                             * permanent. */

  RULESET_A_UNREJECT = 4,   /* Unreject the request.  This will undo both
                             * the 'REJECT' action and the 'REJECT_ALL'
                             * action. */

  RULESET_A_LOW_PRIO = 8,   /* Lower the priority of the request by the value
                             * associated with this rule. */

  RULESET_A_HIGH_PRIO = 16,  /* Raise the priority of the request by the value
                              * associated with this rule. */

  RULESET_A_REJECT_MASK = RULESET_A_REJECT | RULESET_A_REJECT_ALL
};

enum ruleset_flags {
  RULESET_F_INVALID = -1, /* Invalid flags, this probably means parsing a
                           * string failed to result in valid flags. */

  RULESET_F_NONE = 0,     /* No special behavior. */

  RULESET_F_DISABLE = 1,  /* Rule is currently disabled for use during the
                           * evaluation of rulesets. */

  RULESET_F_PRINT = 2,    /* Emit a trace message if the rule is matched. */

  RULESET_F_STOP = 4      /* Stop if the associated rule is matched.  No more
                           * rules will be processed for this request -AND-
                           * the request will NOT be retried. TODO: ? */
};

enum ruleset_match_mode {
  RULESET_MM_INVALID = -1, /* Invalid match mode, this probably means parsing
                            * a string failed to result in valid match mode. */

  RULESET_MM_NONE = 0,     /* No matching.  If string criteria are specified
                            * for this rule, it will not be matched and NONE
                            * will be returned as the result. */

  RULESET_MM_EXACT = 1,    /* The string criteria for the rule should be
                            * matched exactly. */

  RULESET_MM_GLOB = 2,     /* The string criteria for the rule are actually
                            * GLOB patterns, which permits metacharacters
                            * '?' and '*' as wildcards and character subsets
                            * within square braackets '[' and ']'. */

  RULESET_MM_REGEXP = 4,   /* The string criteria for the rule are actually
                            * regular expression patterns. */

  RULESET_MM_NOCASE = 8,   /* Matches should be performed in case-insensitive
                            * manner.  This flag may be combined with EXACT,
                            * GLOB, and REGEXP. */

  RULESET_MM_DEFAULT = RULESET_MM_EXACT
};

enum ruleset_string_match {
  RULESET_S_TRUE = 0,     /* The string comparison function was able to match
                           * the string value against the specified pattern. */

  RULESET_S_FALSE = 1     /* The string comparison function was did not match
                           * the string value against the specified pattern. */
};

enum ruleset_match {
  RULESET_M_NONE = 0,     /* The ruleset or item was not matched, possibly
                           * because no matching was performed. */

  RULESET_M_FALSE = 1,    /* The ruleset or item was not matched. */

  RULESET_M_TRUE = 2,     /* The ruleset or item was matched. */

  RULESET_M_STOP = 4,     /* The ruleset or item was matched -AND- processing
                           * of the ruleset or item should stop. */

  RULESET_M_ERROR = 8     /* The ruleset or item could not be matched due
                           * to one or more errors. */
};

struct ruleset_item_criteria {
  char *zOriginHost;              /* Obtained via "clnt->origin_host".  If not
                                   * NULL this pattern will be matched against
                                   * the actual value using the semantics of
                                   * the specified match mode. */

  char *zOriginTask;              /* Obtained via "clnt->conninfo.pename".  If
                                   * not NULL this pattern will be matched
                                   * against the actual value using the
                                   * semantics of the specified match mode. */

  char *zUser;                    /* Obtained via "clnt->have_user" /
                                   * "clnt->user".  If not NULL this pattern
                                   * will be matched against the actual value
                                   * using the semantics of the specified match
                                   * mode. */

  char *zSql;                     /* Obtained via "clnt->sql".  If not NULL
                                   * this pattern will be matched against the
                                   * actual value using the semantics of the
                                   * specified match mode. */

  unsigned char *pFingerprint;    /* Obtained via "clnt->work.aFingerprint".
                                   * If not NULL this will be matched using
                                   * memcmp(). */
};

struct ruleset_item_criteria_cache {
  void *pOriginHostRe;            /* This is the cached regular expression for
                                   * the origin host pattern, if needed. */

  void *pOriginTaskRe;            /* This is the cached regular expression for
                                   * the origin task pattern, if needed. */

  void *pUserRe;                  /* This is the cached regular expression for
                                   * the user pattern, if needed. */

  void *pSqlRe;                   /* This is the cached regular expression for
                                   * the SQL pattern, if needed. */
};

struct ruleset_item {
  int ruleNo;                     /* The number assigned to this rule.  If
                                   * this is zero, the rule has not been
                                   * loaded and should not be evaluated or
                                   * saved (i.e. it is an empty slot). */

  enum ruleset_action action;     /* If this rule is matched, what should be
                                   * done in respone? */

  priority_t adjustment;          /* For rules which adjust the priority of
                                   * of the work item, what is the (absolute)
                                   * amount the priority should be adjusted?
                                   */

  enum ruleset_flags flags;       /* The behavioral flags associated with the
                                   * rule, e.g. stop-on-match, etc. */

  enum ruleset_match_mode mode;   /* The matching mode used for the string
                                   * comparisons, e.g. exact, glob, regexp,
                                   * etc. */

  struct ruleset_item_criteria criteria; /* See above for field notes. */
  struct ruleset_item_criteria_cache cache; /* See above for field notes. */

  int evalCount;                  /* How many times have this rule been
                                   * evaluated? */

  int matchCount;                 /* How many times has this rule been
                                   * matched? */
};

struct ruleset {
  uint64_t generation;            /* When was this set of rules read into
                                   * memory?*/

  size_t nRule;                   /* How many rules are in this ruleset? */

  size_t nFingerprint;            /* How many rules use fingerprints? */

  struct ruleset_item *aRule;     /* An array of rules with a minimum size of
                                   * nRule. */
};

struct ruleset_result {
  enum ruleset_action action;     /* What will the final action be for this
                                   * ruleset? */

  priority_t priority;            /* What will the final priority be for this
                                   * ruleset?  Depending on the action, this
                                   * value may be ignored. */

  int ruleNo;                     /* Which rule, if any, was the primary one
                                   * responsible for the result action? */
};

typedef int (*xStrCmp)(const char *, const char *);

typedef enum ruleset_string_match ruleset_string_match_t;
typedef enum ruleset_match ruleset_match_t;
typedef enum ruleset_match_mode ruleset_match_mode_t;

const char *comdb2_priority_to_str(
  priority_t priority,
  char *zBuf,
  size_t nBuf,
  int bStrict
);

int comdb2_ruleset_fingerprints_allowed(void);

int comdb2_load_ruleset_item_criteria(
  const char *zFileName,
  int lineNo,
  char *zBuf,
  size_t nBuf,
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
);

void comdb2_free_ruleset_item_criteria(
  struct ruleset_item_criteria *criteria
);

size_t comdb2_evaluate_ruleset(
  xStrCmp stringComparer,
  struct ruleset *rules,
  struct ruleset_item_criteria *context,
  struct ruleset_result *result
);

size_t comdb2_ruleset_result_to_str(
  struct ruleset_result *result,
  char *zBuf,
  size_t nBuf
);

int comdb2_enable_ruleset_item(struct ruleset *rules, int ruleNo, int bEnable);
void comdb2_dump_ruleset(struct ruleset *rules);
void comdb2_free_ruleset(struct ruleset *rules);
int comdb2_load_ruleset(const char *zFileName, struct ruleset **pRules);
int comdb2_save_ruleset(const char *zFileName, struct ruleset *rules);

#endif /* _COMDB2_RULESET_H_ */
