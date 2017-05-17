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

#ifndef INCLUDED_FDOSTREAM
#define INCLUDED_FDOSTREAM

#include <ostream>
#include <streambuf>

class fdoutbuf : public std::streambuf {

    int m_fd;
    // file descriptor

public:
    fdoutbuf(int _fd);
    virtual ~fdoutbuf();

    virtual std::streambuf::int_type overflow(std::streambuf::int_type c);
    virtual std::streamsize xsputn(const char* s, std::streamsize num);
    int getfd();
};

class fdostream : public std::ostream {
// This is an output stream which takes a file descriptor and takes ownership
// of that fd, which is closed in the destructor.

    fdoutbuf buf;

public:
    fdostream(int fd);
    int skip(unsigned long long size);
};

#endif // INCLUDED_FDOSTREAM
