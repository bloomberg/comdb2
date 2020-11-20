/*
   Copyright 2020, Bloomberg Finance L.P.

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

#include <sys/types.h>
#include <strings.h>

#define TOUPPER(x) (((x >= 'a') && (x <= 'z')) ? x - 32 : x)


/* case-insensitive compare */
int strcmpfunc(char **a, char **b, int len)
{
    int cmp;
    cmp = strcasecmp(*a, *b);
    return cmp;
}

u_int strhashfunc(u_char **keyp, int len)
{
    unsigned hash;
    u_char *key = *keyp;
    for (hash = 0; *key; key++)
        hash = ((hash % 8388013) << 8) + (TOUPPER(*key));
    return hash;
}

// simple hash for a ptr address
// for 4 bytes ptr (32bit arch), it's just the first 4 bytes
// for 8 bytes ptr, sum the first 4 bytes with the second 4 bytes
u_int ptrhashfunc(u_char *keyp, int len)
{
    unsigned hash = 0;
    for (int i = 0; i < sizeof(u_char *) / sizeof(int); i++)
        hash += ((int *)&keyp)[i];
    return hash;
}
