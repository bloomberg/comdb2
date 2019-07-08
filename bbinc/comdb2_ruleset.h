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

#include "sql.h"

struct comdb2_ruleset {
  char *zOriginUser;                /* Obtained via unknown means.  If not NULL
                                     * this will be matched against using exact
                                     * case-insensitive string comparisons. */
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
