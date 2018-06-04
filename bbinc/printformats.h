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

#ifndef INCLUDED_PRINTFORMATS_H
#define INCLUDED_PRINTFORMATS_H

#define PR_LSN "%u:%u"
#define PARM_LSN(l) l.file, l.offset
#define PARM_LSNP(l) l->file, l->offset

#define PR_TM "%04d-%02d-%02dT%02d:%02d:%02d"
#define PARM_TMP(t) t->tm_year + 1900, t->tm_mon + 1, t->tm_mday, t->tm_hour, t->tm_min, t->tm_sec

#endif
