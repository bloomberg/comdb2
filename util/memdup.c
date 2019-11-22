/*
   Copyright 2019 Bloomberg Finance L.P.

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

#if !defined(NDEBUG) && defined(_LINUX_SOURCE)
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

#define MEMDUP_PAGE_SIZE 4096

static size_t memdup_sizeof(
  size_t nMem
){
  size_t nPage = nMem / MEMDUP_PAGE_SIZE;
  if( (nMem%MEMDUP_PAGE_SIZE)!=0 ) nPage++;
  return nPage * MEMDUP_PAGE_SIZE;
}

void *memdup_readonly(
  const void *pMem,
  size_t nMem
){
  void *p;
  size_t nSize;
  if( pMem==0 ) return 0;
  nSize = memdup_sizeof(nMem);
  p = mmap(0, nSize, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
  if( p==MAP_FAILED ) return 0;
  memset(p, 0, nSize);
  memcpy(p, pMem, nMem);
  if( mprotect(p, nSize, PROT_READ)!=0 ){
    memset(p, 0, nSize);
    munmap(p, nSize);
    return 0;
  }
  return p;
}

char *strdup_readonly(
  const char *zStr,
  size_t *pnStr
){
  char *z;
  size_t nStr;
  if( zStr==0 ) return 0;
  nStr = strlen(zStr);
  z = memdup_readonly(zStr, nStr + 1);
  if( z && pnStr ) *pnStr = nStr;
  return z;
}

void memdup_free(
  void *pMem,
  size_t nMem
){
  void *p = pMem;
  size_t nSize;
  if( p==0 ) return;
  nSize = memdup_sizeof(nMem);
  if( mprotect(p, nSize, PROT_READ|PROT_WRITE)!=0 ) return;
  memset(p, 0, nSize);
  munmap(p, nSize);
}
#endif /* !defined(NDEBUG) && defined(_LINUX_SOURCE) */
