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

#ifndef INCLUDED_REPOPNEWLRL
#define INCLUDED_REPOPNEWLRL

#include <string>
#include <memory>

struct RepopNewLrl_impl;

class RepopNewLrl {
// This creates a new repopulated lrl file (ie with table definitions).

    std::unique_ptr<RepopNewLrl_impl> impl;

public:

    RepopNewLrl(const std::string& dbname, const std::string& main_lrl_file,
            const std::string& new_lrl_file, const std::string& comdb2_task);
    // Construct a repop new lrl object, opening the socket to the database and
    // telling it to repopulate an lrl.  If this cannot be done for some 
    // reason then we will log this to std::clog and try to start the supplied 
    // comdb2 with the -repopnewlrl option.

    virtual ~RepopNewLrl();
    // Destroy the object
};

#endif // INCLUDED_REPOPNEWLRL
