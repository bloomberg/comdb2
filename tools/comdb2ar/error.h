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

#ifndef INCLUDED_ERROR
#define INCLUDED_ERROR

#include <exception>
#include <string>
#include <sstream>

class Error : public std::exception
// This is a generic exception class which takes a string of some sort
// to represent what went wrong.
{
    std::string m_error;

public:
    Error(const char *error);
    Error(const std::string &error);
    Error(const std::stringstream &errorss);
    Error(const std::ostringstream &errorss);

    virtual ~Error() throw();

    virtual const char* what() const throw();
};

#endif // INCLUDED_ERROR
