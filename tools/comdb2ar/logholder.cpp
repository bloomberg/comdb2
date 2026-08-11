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

// logdelete3/2 keep their original semantics: a timeout is just another bad
// response and we step down a version.  Only the v4 handshake fails hard.
static bool legacy_handshake_ok(Appsock *appsock)
{
    try {
        return appsock->response("log file deletion disabled\n");
    } catch(Error&) {
        return false;
    }
}

LogHolder::LogHolder(const std::string& dbname) : impl(new LogHolder_impl(dbname, "logdelete4\n"))
{

    m_version = 4;

    // Negotiate the highest logdelete version: a database that doesn't know a
    // version answers "-1 #unknown command" immediately and we step down.  A
    // *timeout* on logdelete4 is different -- the handler is blocked taking the
    // copy locks behind an in-flight recovery -- so response() throws and we fail
    // hard rather than copy unprotected against that recovery.
    bool v4_ok = false;
    if(impl->mp_appsock.get()) {
        try {
            v4_ok = impl->mp_appsock->response("log file deletion disabled\n");
        } catch(Error&) {
            close();
            throw Error("logdelete4 handshake timed out on " + dbname +
                        " (database busy, possibly running recovery); refusing "
                        "to fall back to an unprotected copy");
        }
    }

    if(impl->mp_appsock.get() && !v4_ok) {
        close();

        std::clog << "Doesn't support logdelete4" << std::endl;

        impl->reset(dbname, "logdelete3\n");
        m_version = 3;
        if(impl->mp_appsock.get()
                && !legacy_handshake_ok(impl->mp_appsock.get())) {
            close();

            std::clog << "Doesn't support logdelete3" << std::endl;

            impl->reset(dbname, "logdelete2\n");
            m_version = 2;
            if(impl->mp_appsock.get()
                    && !legacy_handshake_ok(impl->mp_appsock.get())) {
                close();
                throw Error("Log holder appsock: bad response from " + dbname);
            }
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

bool LogHolder::copy_ok()
{
    // Ask a v4 database whether its lock hold stayed valid for the whole copy.
    // It always answers, so anything but "ok" -- including a timeout or a
    // dropped socket -- means the copy is not trustworthy.  Pre-v4 databases
    // have no such handshake.
    if(impl->mp_appsock.get() && m_version >= 4) {
        std::ostringstream request ;
        request << "copy_complete" << "\n";
        impl->mp_appsock->request(request.str());
        try {
            return impl->mp_appsock->read_response() == "ok";
        } catch(Error& e) {
            std::clog << "copy_complete: no response from database: "
                      << e.what() << std::endl;
            return false;
        }
    }
    return true;
}
