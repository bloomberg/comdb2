/*
   Copyright 2017 Bloomberg Finance L.P.

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

#ifndef __INCLUDED_COMDB2_OPCODE_H
#define __INCLUDED_COMDB2_OPCODE_H

struct comdb2_opcode {
    /* Used to lookup the opcode handler */
    int opcode;
    /* Name of the opcode */
    const char *name;
    /* The handler function */
    int (*opcode_handler)(struct ireq *);
};
typedef struct comdb2_opcode comdb2_opcode_t;

#endif /* ! __INCLUDED_COMDB2_OPCODE_H */
