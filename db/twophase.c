/*
   Copyright 2015 Bloomberg Finance L.P.

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

/*
  this code is in the process of being re-purposed to work this time
  using rowlocks technology.

  a key point here is there is no fundamental difference
  between a "coordinator" and a "participant" as far as static configuration
  goes.  any comdb2 instance can become the coordinator of a distributed
  transaction when he is asked to perform a txn spanning multiple instances.
  a distributed txn can include tables in the local instance.

  the idea now is we do "almost nothing" at the "coordinator" level.
  a coordinator of a distrubuted transaction does NOT track locks,
  log undo/redo information, or keep track of what participants are invloved
  in a transaction or what their state is.

  a coordinator of a distributed transaction needs to:

  a) allocate a global gtid
  b) durably write gtid "in flight record" to gtid table.
  c) split block ops to 1 per pariticpant.
     send them as prepare blockops.
     wait for yes/no responses.
  d) if all good, durably update gtid table record to "commited" state.
     if any issue, durably update gtid record to "aborted" state.
  e) send commit(or abort) message about gtid to all participants.
  f) send back response to client

  cleanup of gtid table can happen async, some cleanup job aging them out.

  essentially the only "thing" the coordinator has to do specially is maintain
  that gtid table.  its just some record of the gtid existing and its state
  of ether "in flight" or "committed" or "aborted"
  if this becomes the central bottleneck,
  we can play games to "stripe"/parallelize this as much as possible.  it
  does need to be a properly replicated table so we cant play that many games.
  if writes to this table still are not fast enough, there is no reason in
  this scheme why multiple coordinators cannot cooexist (global gtid namespace)
  with the same foreign tables of the same participants.

  participant uses the standard rowlock code.  a gtid contains the name of
  the coordinator, so there is enough information to recover.  participant
  writes a "prepared" record rather than a "commited" record.  when he commits
  he writes a standard "committed" record.  during recovery, we unroll
  anything not prepared (instead of not commited).  we collect all txns that
  are prepared and not commited/aborted.  these end up on a
  "in flight gtid table" (see further down).  gtids with a commit/abort
  record get removed from the "in flight" table.  we dont hold up revovery
  for prepared but not committed/aborted gtids, just proceed.  they will
  commit/abort normally with the instance online.

  participant maintains an in memory table of "in flight gtids."  this table
  is constructed/reconstructed during recovery.  a prepare record gets you
  on the table, a commit/abort record gets you removed.  this table is used to
  handle the polling of the coordinator for the state of a gtid (needed to
  cover edge case in coordinator swing).  poller will lock table and ask
  about gtid state.  when message from coordinator comes in, table is locked
  and gtid removed.  under normal circumstances (no crashing) the flow of
  control is purlely "push" - ie, coordinator pushes prepare request
  to participant, then pushes a commit/abort message to the participant.
  the additional polling is needed because the coordinator does not durably
  record information like "which participants are involved in a gtid" so
  a participant master crash/swing will result in a breakdown of the "push"
  system.  this table is what also allows participant recovery to move
  forward in light of prepared but not yet commited/aborted gtids.

*/
