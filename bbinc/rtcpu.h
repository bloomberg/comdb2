/*
   Copyright 2015, 2017, Bloomberg Finance L.P.

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

#ifndef INCLUDED_RTCPU_H
#define INCLUDED_RTCPU_H

void register_rtcpu_callbacks(int (*a)(const char *), int (*b)(void),
                              int (*c)(const char *), int (*d)(const char *),
                              int (*e)(const char *));
int machine_is_up(const char *host);
int machine_class(const char *host);
int machine_dc(const char *host);
int machine_num(const char *host);

#endif
