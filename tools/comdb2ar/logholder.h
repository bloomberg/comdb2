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

#ifndef INCLUDED_LOGHOLDER
#define INCLUDED_LOGHOLDER

#include <string>
#include <memory>

struct LogHolder_impl;

class LogHolder {
// This encapsulates the socket connection to comdb2 which we use to instruct
// it to hold off on log file deletion.

    std::unique_ptr<LogHolder_impl> impl;
    int m_version;

public:

    LogHolder(const std::string& dbname);
    // Construct a log holder object, opening the socket to the database and
    // telling it to hold off on log file deletion.  If this cannot be done
    // for some reason then we will log this to std::clog but still construct
    // a valid (inert) object.  This is because the database might be down,
    // so failure here is not an error.

    virtual ~LogHolder();
    // Destroy the object, closing our socket interface.

    void close();
    // Close our socket interface to the database (if one was opened
    // successfully in the first place).
    
    void release_log(long long logno);
    // Inform the database that it may now delete log file #logno, and
    // any earlier log files.  This does nothing if we never connected to 
    // the database in the first place.

    std::string recovery_options();

    int version() { return m_version; };
};

#endif // INCLUDED_LOGHOLDER
