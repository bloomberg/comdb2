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

#ifndef INCLUDED_LRLERROR
#define INCLUDED_LRLERROR

#include <exception>
#include <string>

class LRLError : public std::exception
{
    std::string m_error;

public:
    LRLError(const std::string& lrlpath, int lineno, const char *error);
    LRLError(const std::string& lrlpath, const char *error);

    virtual ~LRLError() throw();

    virtual const char* what() const throw();
};

#endif // INCLUDED_LRLERROR
