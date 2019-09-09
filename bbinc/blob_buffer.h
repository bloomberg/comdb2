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

#ifndef _INCLUDED_BLOB_BUFFER_H
#define _INCLUDED_BLOB_BUFFER_H

/* Used for collecting blob data before a keyless add/upd/del.
 * An array of these also supplements */
typedef struct blob_buffer {
    int exists; /* to differentiate 0 length from null */

    char *data;
    size_t length;

    /* collected has a double life.  on the user side, it is used to
     * track how much blob we've collected from the transaction data.
     * on the server side, it should be non-zero even for a null blob
     * so we know that it's been through the type system (helps us tell
     * which blobs to update on updates) */
    size_t collected;

    /* This is used by javasp.c to keep track of our reference to the byte
     * array object that this blob came from. */
    void *javasp_bytearray;

    /* The index of the blob.
       An ODH'd blob has OSQL_BLOB_ODH_BIT set,
       which can be tested using IS_ODH_READY(). */
    int odhind;
    /* The QBLOB msg from bplog. This is not null
       IFF the blob is received from the OSQL layer on the master */
    char *qblob;
    /* The heap memory used by bdb_unpack(). This is not null
       IFF a schema change is modifying the blob compression algorithm,
       or adding an expressional index on the blob column. */
    void *freeptr;
} blob_buffer_t;

#endif
