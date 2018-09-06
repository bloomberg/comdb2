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

/***************************************
**                                     *
** restore malloc/calloc/realloc/free  *
**                                     *
***************************************/
#ifdef malloc
#undef malloc
#endif

#ifdef malloc0
#undef malloc0
#endif

#ifdef calloc
#undef calloc
#endif

#ifdef realloc
#undef realloc
#endif

#ifdef free
#undef free
#endif

#ifdef strdup
#undef strdup
#endif

#ifdef strndup
#undef strndup
#endif

#ifdef malloc_resize
#undef malloc_resize
#endif
