/*
   Copyright 2023 Bloomberg Finance L.P.

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
#ifndef _CLIENTHOST_H_

#include <intern_strings.h>
#include <list.h>

struct hostpolicy {
    unsigned explicit_allow : 1;
    unsigned explicit_disallow : 1;
};

struct clienthost {
    struct hostpolicy write_pol;
    struct hostpolicy brd_pol;
    struct hostpolicy cluster_pol;
    int mach_class;
    struct interned_string *s;
    int machine_dc;
    /* Interned resolved name of this host, as the cluster allow list keys on
     * it.  Resolved once by comdb2_gethostbyname(). */
    const char *resolved_host;
    /* Cached group-membership verdict for the cluster allow list.  The
     * generation goes stale when the allowed group set changes. */
    int cluster_group_gen;
    int cluster_group_time;
    int cluster_group_ok;
    /* Epoch of the last "the cluster allow list would reject this host"
     * warning.  Rate limits the warning while we still enforce the old
     * policy. */
    int cluster_warn_time;
    LINKC_T (struct clienthost) lnk;
};

void clienthost_lock(void);

void clienthost_unlock(void);

struct clienthost *retrieve_clienthost(struct interned_string *s);

struct clienthost *retrieve_clienthost_nocreate(struct interned_string *s);

#endif
