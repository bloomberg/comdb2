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

#ifndef FINGERPRINTSZ
#define FINGERPRINTSZ 16 /* stolen from "sql.h" */
#endif

enum ruleset_action {
  RULESET_A_NONE = 0,     /* Take no action. */

  RULESET_A_REJECT = 1,   /* Reject the request.  May be combined with the
                           * 'STOP' flag to make permanent. */

  RULESET_A_UNREJECT = 2, /* Unreject the request. */

  RULESET_A_LOW_PRIO = 3, /* Lower the priority of the request by the value
                           * associated with this rule. */

  RULESET_A_HIGH_PRIO = 4 /* Raise the priority of the request by the value
                           * associated with this rule. */
};

enum ruleset_flags {
  RULESET_F_NONE = 0,     /* No special behavior. */

  RULESET_F_STOP = 1      /* Stop if the associated rule is matched.  No more
                           * rules will be processed for this request -AND-
                           * the request will NOT be retried. TODO: ? */
};

struct ruleset_item {
  enum ruleset_action action;       /* If this rule is matched, what should be
                                     * done in respone? */

  priority_t adjustment;            /* For rules which adjust the priority of
                                     * of the work item, what is the (absolute)
                                     * amount the priority should be adjusted?
                                     */

  enum ruleset_flags flags;         /* The behavioral flags associated with the
                                     * rule, e.g. stop-on-match, etc. */

  char *zOriginHost;                /* Obtained via "clnt->origin_host".  If not
                                     * NULL this will be matched using exact
                                     * case-insensitive string comparisons. */

  char *zOriginTask;                /* Obtained via "clnt->conninfo.pename".  If
                                     * not NULL this be matched using exact
                                     * case-insensitive string comparisons. */

  char *zUser;                      /* Obtained via "clnt->have_user" /
                                     * "clnt->user".  If not NULL this be
                                     * matched using exact case-insensitive
                                     * string comparisons. */

  char *zSql;                       /* Obtained via "clnt->sql".  If not NULL
                                     * this be matched using exact
                                     * case-insensitive string comparisons. */

  char aFingerprint[FINGERPRINTSZ]; /* Obtained via "reqlogger->fingerprint".
                                     * If not all zeros, this will be matched
                                     * using memcmp(). */
};

#endif /* _COMDB2_RULESET_H_ */
