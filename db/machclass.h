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

#ifndef INCLUDED_MACHCLASS_H
#define INCLUDED_MACHCLASS_H

enum { MAX_CPU = 65536 };

/* order is important; custom defined classes are indexed from 1 to
  < CLASS_DENIED
*/
enum mach_class {
    CLASS_UNKNOWN = 0,
    CLASS_TEST = 1,
    CLASS_ALPHA = 2,
    CLASS_UAT = 3,
    CLASS_BETA = 4,
    CLASS_PROD = 5,
    CLASS_INTEGRATION = 6,
    CLASS_DENIED = 255
};

int mach_class_init(void);
int mach_class_addclass(const char *name, int value);
int mach_class_name2class(const char *name);
const char *mach_class_class2name(int value);
int mach_class_remap_fdb_tier(const char *name, const char *tier);
const char *mach_class_class2tier(int value);

#endif
