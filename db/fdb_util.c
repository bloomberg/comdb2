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

#include "fdb_util.h"

/* pack an sqlite unpacked row to a packed row */
int fdb_unp_to_p(Mem *m, int ncols, int hdrsz, int datasz, char *out,
                 int outlen)
{
    char *hdrbuf, *dtabuf;
    int fnum;
    int sz;
    u32 len;

    hdrbuf = out;
    dtabuf = out + hdrsz;

    /* enough room? */
    if ((datasz + hdrsz) > outlen) {
        return -1;
    }

    /* put header size in header */
    sz = sqlite3PutVarint((unsigned char *)hdrbuf, hdrsz);
    hdrbuf += sz;

    for (fnum = 0; fnum < ncols; fnum++) {
        sz = sqlite3VdbeSerialPut(
            (unsigned char *)dtabuf, &m[fnum],
            sqlite3VdbeSerialType(&m[fnum], SQLITE_DEFAULT_FILE_FORMAT, &len));
        dtabuf += sz;
        sz = sqlite3PutVarint(
            (unsigned char *)hdrbuf,
            sqlite3VdbeSerialType(&m[fnum], SQLITE_DEFAULT_FILE_FORMAT, &len));
        hdrbuf += sz;
        assert(hdrbuf <= (out + hdrsz));
    }

    return 0;
}
