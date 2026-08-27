/*
   Copyright 2026 Bloomberg Finance L.P.

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

/* The cluster allow list names the hosts that may cluster with us.
 *
 * It holds hosts and machine groups only.  It holds no machine classes, which
 * is what makes it narrower than the "cluster with" remote policy in
 * rmtpolicy.c.
 *
 * A group is a machine cluster.  machine_cluster_machs() lists its hosts:
 * machinfo.c answers from the machine registry, and without that plugin the
 * built-in registry that the lrl "machine_cluster" directive and the
 * "machine_cluster add" message trap feed answers instead.
 *
 * The list lives in memory only.  At startup it holds this host plus the hosts
 * from the "cluster nodes" lrl directive.  "allow cluster with" adds to it,
 * "disallow cluster with" removes from it.
 *
 * Every host name that goes in or out of the list first goes through
 * comdb2_gethostbyname(), so that an operator can name a host by a small
 * integer machine identifier.  The "cluster with" remote policy in rmtpolicy.c
 * does not do this; it still keys on the name as typed.
 *
 * gbl_enforce_cluster_allow_list selects who enforces the list.  When it is
 * off we enforce the old remote policy and only warn about the hosts this list
 * would reject.  When it is on this list decides.
 */

#ifndef INCLUDED_CLUSTER_ALLOW_LIST_H
#define INCLUDED_CLUSTER_ALLOW_LIST_H

/* Seed the list with this host and the "cluster nodes" hosts.  Call this after
 * the lrl file is read and before the saved "allow" lines are replayed. */
void cluster_allow_list_init(void);

/* Return 1 if the entry was added, 0 if it was already there. */
int cluster_allow_list_add_host(const char *host);
int cluster_allow_list_add_group(const char *group);

/* Return 1 if the entry was removed, 0 if it was not there. */
int cluster_allow_list_del_host(const char *host);
int cluster_allow_list_del_group(const char *group);

/* Return 1 if host may cluster with us, 0 if it may not. */
int cluster_allow_list_permits(const char *host);

/* Print the list and the enforcement state. */
void cluster_allow_list_dump(void);

#endif /* !INCLUDED_CLUSTER_ALLOW_LIST_H */
