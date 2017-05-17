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

#ifndef _LIMIT_FORTIFY_H
#define _LIMIT_FORTIFY_H

#ifdef _STDIO_H
  #error "limit_fortify.h must be included _before_ stdio.h"
#endif

/*
  On some Unix systems, programs built with optimizer levels >0, have
  _FORTIFY_SOURCE defined (-D_FORTIFY_SOURCE=2), which enables some
  additional compile-time and run-time protections, by re-defining
  function like printf, fprintf, etc.

  As a side-effect, the overridden versions of such functions defined
  locally (see db/io-override.c) never get invoked.

  As a fix, these functions have been remapped to their original names.
*/

#ifdef _FORTIFY_SOURCE
  #define __printf_chk(X, ...) printf (__VA_ARGS__)
  #define __fprintf_chk(stream, X, ...) fprintf (stream, __VA_ARGS__)
#endif /* _FORTIFY_SOURCE */

#endif /* _LIMIT_FORTIFY_H */
