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

#ifndef INCLUDED_WALKBACK_H
#define INCLUDED_WALKBACK_H

#ifdef __cplusplus
extern "C" {
#endif

/*
  Initialization makes the walkback available from signal handlers.
*/
int comdb2_walkback_initialize_module(void);

/*
  Invokes the handler for up to maxframes of run time stack frames with the
  program counter (pc) as argument.
*/

int                                           /* rcode */
comdb2_stack_pc_getlist(void **pcArray,       /* output array of program counters */
                        unsigned pcArraySize, /* number of elements in pcArray */
                        unsigned *pcOutCount  /* number of program counters returned */
);

int /* rcode */
comdb2_stack_pc_walkback(unsigned maxframes, void (*handler)(void *returnaddr, void *handlerarg), void *handlerarg);

void comdb2_stack_pc_walkback_print(void *returnaddr, void *arg);

void comdb2_walkback_strerror(int rcode, char *errormsg, unsigned maxerrormsgsize);

#ifdef __cplusplus
}
#endif

#endif
