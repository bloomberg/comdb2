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

#ifndef INCLUDED_MEMDEBUG_H
#define INCLUDED_MEMDEBUG_H

/* Dump call callers (ie: stack traces of all points in
   the code where memory is allocated. if skip_balanced is
   set, don't dump those that returned all the blocks they
   allocated. */
void memdebug_dump_callers(FILE *f, int skip_balanced);

/* Dump all outstanding (non-free) blocks. */
void memdebug_dump_blocks(FILE *f);

/* Add a debug block */
void memdebug_add_debug_block(char *debug_msg);

#endif
