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

#ifndef INCLUDED_TAR_HEADER
#define INCLUDED_TAR_HEADER

#include <string>

#include <sys/stat.h>

typedef struct
{
    char filename[100];
    char mode[8];
    char uid[8];
    char gid[8];
    char size[12];
    char mtime[12];
    char checksum[8];
    char type;
    char linked_filename[100];
    char ustar[6];
    char ustar_version[2];
    char uname[32];
    char gname[32];
    char devmajor[8];
    char devminor[8];
    char prefix[155];
} tar_header;

typedef union
{
    tar_header h;
    char c[512];
} tar_block_header;



class TarHeader {
// This encapsulates a tar block header and provides methods to initialise
// all the fields.

    tar_block_header m_head;
    bool m_used_gnu;

    static const long long MAX_OCTAL_SIZE;

public:

    TarHeader();

    virtual ~TarHeader();

    void clear() throw();
    // Reset the tar header struct ready for a new file

    void set_filename(const std::string& filename);
    // Set the file name in the tar header.  If the filename given is too long
    // (more than 100 chars) then this will throw a SerialiseError

    void set_attrs(const struct stat& st);
    // Set the attributes based on the information in the stat struct

    void set_checksum();
    // Calculate and store the checksum field

    const tar_block_header& get() const { return m_head; }
    // Get a reference to the tar header data

    bool used_gnu() const { return m_used_gnu; }
    // Returns true if GNU extensions had to be used
};

#endif // INCLUDED_TAR_HEADER
