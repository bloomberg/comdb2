
#include "repopnewlrl.h"
#include "error.h"
#include "appsock.h"

#include <iostream>
#include <sstream>

#include <errno.h>
#include <signal.h>
#include <unistd.h>

#include <portmuxapi.h>
#include <comdb2buf.h>

#include <stdlib.h>



struct RepopNewLrl_impl {
    std::unique_ptr<Appsock> mp_appsock;
    // Connection to the database

    RepopNewLrl_impl(const std::string& dbname, const std::string& request);
};

RepopNewLrl_impl::RepopNewLrl_impl(const std::string& dbname,
        const std::string& request)
{
    try
    {
        mp_appsock.reset(new Appsock(dbname, request));
    }
    catch(Error& e)
    {
        std::clog << "Repopulate lrl appsock failed: " << e.what() << std::endl;
    }
}

RepopNewLrl::RepopNewLrl(const std::string& dbname,
        const std::string& main_lrl_file, const std::string& new_lrl_file,
        const std::string& comdb2_task) :
    impl(new RepopNewLrl_impl(dbname, std::string("repopnewlrl\n")
                + new_lrl_file + "\n"))
{
    if(impl->mp_appsock.get()) {
        bool response_successful = impl->mp_appsock->response("OK\n");

        // close either way
        impl->mp_appsock.reset();

        if(!response_successful) {
            throw Error("Repopulate lrl appsock failed: bad response from "
                    + dbname);
        }

        std::clog << "Repopulate lrl appsock succeeded on: " << dbname
            << std::endl;
        return;
    }

    // Appsock couldn't connect, assume db is down and run with -repopnewlrl
    std::ostringstream cmdss;
    cmdss << comdb2_task << " " << dbname << " -lrl " << main_lrl_file
        << " -repopnewlrl " << new_lrl_file << " >&2";
    std::clog << "Repopulating lrl file with command '" << cmdss.str() << "'"
        << std::endl;
    errno = 0;
    int rc = system(cmdss.str().c_str());
    if(rc) {
        std::ostringstream ss;
        ss << "Repopulate lrl command '" << cmdss.str() << "' failed rc "
            << rc << " errno " << errno << std::endl;
        throw Error(ss);
    }
    std::clog << "Repopulate lrl successful" << std::endl;
}

RepopNewLrl::~RepopNewLrl()
{
    // nothing to do
}
