#include <pthread.h>
#include "appsock.h"
#include "error.h"

#include <iostream>
#include <sstream>

#include <errno.h>
#include <signal.h>
#include <unistd.h>

#include <cstring>

#include <portmuxusr.h>
#include <sbuf2.h>


class SigPipeBlocker {

    sigset_t m_sset;
    sigset_t m_oset;

public:

    SigPipeBlocker();
    virtual ~SigPipeBlocker();
};

SigPipeBlocker::SigPipeBlocker()
{
    sigemptyset(&m_sset);
    sigaddset(&m_sset, SIGPIPE);
    int rc = pthread_sigmask(SIG_BLOCK, &m_sset, &m_oset);
    if(rc != 0) {
        std::ostringstream ss;
        ss << "bb_pthread_sigmask block SIGPIPE failed: " << rc;
        throw Error(ss);
    }
}

SigPipeBlocker::~SigPipeBlocker()
{
    siginfo_t info;
    struct timespec timeout = {0, 0};
    if(sigtimedwait(&m_sset, NULL, &timeout) == -1 && errno != EAGAIN) {
        std::cerr << "SigPipeBlocker::~SigPipeBlocker(): sigtimedwait error "
                 << errno << std::endl;
    }
    pthread_sigmask(SIG_SETMASK, &m_oset, NULL);
}



struct Appsock_impl {
    std::string m_dbname;
    // Name of the database we should be connected to

    int m_fd;
    // File descriptor for the socket connection, or -1 if not connected

    SBUF2 *m_sb;
    // sbuf2 object used for stream io on the connection

    Appsock_impl(const std::string& dbname);
};

Appsock_impl::Appsock_impl(const std::string& dbname) :
    m_dbname(dbname),
    m_fd(-1),
    m_sb(NULL)
{
}

Appsock::Appsock(const std::string& dbname, const std::string& req) :
    impl(new Appsock_impl(dbname))
{
    impl->m_fd = portmux_connect("localhost", "comdb2", "replication",
            impl->m_dbname.c_str());

    if(impl->m_fd < 0) {
        throw Error("portmux_connect to " + dbname + " failed");
    }

    impl->m_sb = sbuf2open(impl->m_fd, 0);
    if(!impl->m_sb) {
        ::close(impl->m_fd);
        impl->m_fd = -1;
        throw Error("sbuf2open to " + dbname + " failed");
    }

    request(req);

    std::clog << "Connected to " << dbname << " for request " << req
        << std::endl;
}

Appsock::~Appsock()
{
    close();
}

void Appsock::close()
{
    if(impl->m_sb) {
        sbuf2close(impl->m_sb);
        impl->m_sb = NULL;
        impl->m_fd = -1;
    } else if(impl->m_fd != -1) {
        ::close(impl->m_fd);
        impl->m_fd = -1;
    }
}

void Appsock::request(const std::string& req)
{
    SigPipeBlocker block_sigpipe;

    sbuf2printf(impl->m_sb, const_cast<char*>(req.c_str()));
    sbuf2flush(impl->m_sb);
}

bool Appsock::response(const std::string& rsp)
{
    SigPipeBlocker block_sigpipe;

    // Get the response - we expect "log file deletion disable"
    sbuf2settimeout(impl->m_sb, 10 * 1000, 10 * 1000);
    char line[256];
    if(sbuf2gets(line, sizeof(line), impl->m_sb) <= 0) {
        std::clog << "no response from " << impl->m_dbname << " expected " 
            << rsp << std::endl;
        return false;
    } else if(std::strcmp(line, rsp.c_str()) != 0) {
        std::clog << "bad response from " << impl->m_dbname << " expected "
            << rsp << std::endl;
        return false;
    }

    return true;
}

std::string Appsock::read_response() 
{
    SigPipeBlocker block_sigpipe;

    sbuf2settimeout(impl->m_sb, 10 * 1000, 10 * 1000);

    char line[256] = {0};
    char *eol;

    /* there's no response for unknown requests, so this becomes a 5 second timeout
     * until a new database rolls out */
    if (sbuf2gets(line, sizeof(line), impl->m_sb) < 0)
        throw Error("can't read response");
    if ((eol = strchr(line, '\n')) != NULL)
        *eol = 0;

    return std::string(line);
}
