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

#ifndef INCLUDED_APPSOCK
#define INCLUDED_APPSOCK

#include <string>
#include <memory>

struct Appsock_impl;

class Appsock {
// This encapsulates the socket connection to comdb2 which we use to send
// requests and receive responses.

    std::unique_ptr<Appsock_impl> impl;

public:

    Appsock(const std::string& dbname, const std::string& req);
    // Construct a appsock object, opening the socket to the database and
    // sending it the supplied request.  If this cannot be done
    // for some reason (ie the db is down) then we will throw an Error.

    virtual ~Appsock();
    // Destroy the object, closing our socket interface.

    void close();
    // Close our socket interface to the database.
    
    void request(const std::string& req);
    // Send a request to the database.

    bool response(const std::string& rsp);
    // Listen for a response froom the database, returns true if the expected
    // response was received.

    std::string read_response();
};

#endif // INCLUDED_APPSOCK
