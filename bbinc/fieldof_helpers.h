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

#ifndef INCLUDED_FIELDOF_HELPERS
#define INCLUDED_FIELDOF_HELPERS

/* fieldof_type is for use within sizeof(), typeof(), and so on. Its result must
   not be read or written, as there's no "there" there. */

#define fieldof_type(type, a) (((const type *)0)->a)

#endif
