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

#ifndef INCLUDED_F2CSTR_H
#define INCLUDED_F2CSTR_H

#define F2CSTR(fstr, flen, cstr, clen)                                         \
    {                                                                          \
        int l = ((flen) > (clen)-1) ? (clen)-1 : (flen);                       \
        for (; l > 0 && (fstr)[l - 1] == ' '; l--)                             \
            ;                                                                  \
        memcpy((cstr), (fstr), l);                                             \
        (cstr)[l] = 0;                                                         \
    }

/*USES SIZEOF(CSTR) FOR CLEN */
#define F2CSTRD(fstr, flen, cstr) F2CSTR((fstr), (flen), (cstr), sizeof(cstr))

#endif
