/*
   Copyright 2017, Bloomberg Finance L.P.

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

#define _WINSOCK_DEPRECATED_NO_WARNINGS
#include <winsock2.h>
#include <windows.h>
#include <unistd.h>

long gethostid(void)
{

    long ret;
    struct hostent *hp;
    DWORD sz;
    char nm[MAX_COMPUTERNAME_LENGTH + 1];

    hp = NULL;
    sz = MAX_COMPUTERNAME_LENGTH;

    if (GetComputerName(nm, &sz) == 0) {
        /* Return an arbitrary value */
        return 1;
    }

    hp = gethostbyname(nm);
    if (hp == NULL) {
        /* If this fails, return an different value */
        return 2;
    }

    ret = *(int *)hp->h_addr;
    ret = (ret << 16 | ret >> 16);
    return ret;
}
