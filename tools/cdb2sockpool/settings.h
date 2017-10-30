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
 * This defines all the tunables we have for the sql proxy.
 * There are no include guards as it is intended for multiple inclusion.
 *
 * Includers must define the macros
 * BOOL_SETTING
 * BYTES_SETTING
 * VALUE_SETTING
 * SECS_SETTING
 * MSECS_SETTING
 *
 * All settings are variables of type unsigned
 */

BOOL_SETTING(VERBOSE, 0, "verbose trace")

BYTES_SETTING(CLIENT_STACK_SIZE, 64 * 1024, "client thread stack size")

VALUE_SETTING(POOL_MAX_FDS, 256, "max fds to pool at once (0 = no limit)")

VALUE_SETTING(POOL_MAX_FDS_PER_DB, 4,
              "max fds to pool per database at once (0 = no limit)")

SECS_SETTING(CHECK_PIPE_FREQ, 60,
             "seconds between stat'ing domain socket, 0 for off")

BOOL_SETTING(EXIT_ON_PIPE_ERROR, 1,
             "exit and turn off paul bit if our pipe is deleted")

BOOL_SETTING(UTIME_ON_PIPE, 1, "periodically update last access time on pipe")
