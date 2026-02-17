#include "logholder.h"
#include "error.h"
#include "appsock.h"

#include <iostream>
#include <sstream>

#include <errno.h>
#include <signal.h>
#include <unistd.h>

#include <portmuxapi.h>
#include <comdb2buf.h>



struct LogHolder_impl {
    std::unique_ptr<Appsock> mp_appsock;
    // Connection to the database

    LogHolder_impl(const std::string& dbname, const std::string& request);
    void reset(const std::string& dbname, const std::string& request);
};

LogHolder_impl::LogHolder_impl(const std::string& dbname,
        const std::string& request)
{
    try
    {
        mp_appsock.reset(new Appsock(dbname, request));
    }
    catch(Error& e)
    {
        std::clog << "Log holder appsock failed: " << e.what() << std::endl;
    }
}

void LogHolder_impl::reset(const std::string& dbname, const std::string& request) 
{
    try {
        mp_appsock.reset(new Appsock(dbname, request));
    }
    catch(Error& e) {
        std::clog << "Log holder appsock failed: " << e.what() << std::endl;
    }
}

LogHolder::LogHolder(const std::string& dbname) : impl(new LogHolder_impl(dbname, "logdelete3\n"))
{
    
    m_version = 3;
    if(impl->mp_appsock.get()
            && !impl->mp_appsock->response("log file deletion disabled\n")) {
        close();

        std::clog << "Doesn't support logdelete3" << std::endl;

        impl->reset(dbname, "logdelete2\n");
        m_version = 2;
        if(impl->mp_appsock.get()
                && !impl->mp_appsock->response("log file deletion disabled\n")) {
            close();
            throw Error("Log holder appsock: bad response from " + dbname);
        }
    }

    std::clog << "Log deletion held for "<< dbname  << std::endl;
    std::clog << "Log deletion version"<< version() << std::endl;
}

LogHolder::~LogHolder()
{
    close();
}

void LogHolder::close()
{
    impl->mp_appsock.reset();
}

void LogHolder::release_log(long long logno)
{
    if(impl->mp_appsock.get()) {
        std::ostringstream request ;
        request << "filenum " << logno << "\n";
        impl->mp_appsock->request(request.str());
    }
}

std::string LogHolder::recovery_options()
{
    if(impl->mp_appsock.get() && m_version >= 3) {
        std::ostringstream request ;
        request << "recovery_options " << "\n";
        impl->mp_appsock->request(request.str());
        return impl->mp_appsock->read_response();
    }
    else
        return "";
}
