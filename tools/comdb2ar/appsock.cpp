#include <pthread.h>
#include "appsock.h"
#include "error.h"

#include <iostream>
#include <sstream>

#include <errno.h>
#include <signal.h>
#include <unistd.h>

#include <cstring>
#include <strings.h>

#include <comdb2buf.h>

#include <netdb.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <fcntl.h>
#include <poll.h>

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
#   ifndef __APPLE__
    struct timespec timeout = {0, 0};
    if(sigtimedwait(&m_sset, NULL, &timeout) == -1 && errno != EAGAIN) {
        std::cerr << "SigPipeBlocker::~SigPipeBlocker(): sigtimedwait error "
                 << errno << std::endl;
    }
#   endif
    pthread_sigmask(SIG_SETMASK, &m_oset, NULL);
}



struct Appsock_impl {
    std::string m_dbname;
    // Name of the database we should be connected to

    int m_fd;
    // File descriptor for the socket connection, or -1 if not connected

    COMDB2BUF *m_sb;
    // sbuf2 object used for stream io on the connection

    Appsock_impl(const std::string& dbname);
};

Appsock_impl::Appsock_impl(const std::string& dbname) :
    m_dbname(dbname),
    m_fd(-1),
    m_sb(NULL)
{
}

static int lclconn(int s, const struct sockaddr *name, int namelen,
                   int timeoutms)
{
    /* connect with timeout */
    struct pollfd pfd;
    int flags, rc;
    int err;
    socklen_t len;
    if (timeoutms <= 0)
        return connect(s, name, namelen); /*no timeout specified*/
    flags = fcntl(s, F_GETFL, 0);
    if (flags < 0)
        return -1;
    if (fcntl(s, F_SETFL, flags | O_NONBLOCK) < 0) {
        return -1;
    }

    rc = connect(s, name, namelen);
    if (rc == -1 && errno == EINPROGRESS) {
        /*wait for connect event */
        pfd.fd = s;
        pfd.events = POLLOUT;
        rc = poll(&pfd, 1, timeoutms);
        if (rc == 0) {
            /*timeout*/
            /*fprintf(stderr,"connect timed out\n");*/
            return -2;
        }
        if (rc != 1) { /*poll failed?*/
            return -1;
        }
        if ((pfd.revents & POLLOUT) == 0) { /*wrong event*/
            /*fprintf(stderr,"poll event %d\n",pfd.revents);*/
            return -1;
        }
    } else if (rc == -1) {
        /*connect failed?*/
        return -1;
    }
    if (fcntl(s, F_SETFL, flags) < 0) {
        return -1;
    }
    len = sizeof(err);
    if (getsockopt(s, SOL_SOCKET, SO_ERROR, &err, &len)) {
        return -1;
    }
    errno = err;
    if (errno != 0)
        return -1;
    return 0;
}

static int cdb2_tcpresolve(const char *host, struct in_addr *in, int *port)
{
    /*RESOLVE AN ADDRESS*/
    in_addr_t inaddr;

    int len;
    char tmp[8192];
    int tmplen = 8192;
    int herr;
    struct hostent hostbuf, *hp = NULL;
    char tok[128];
    const char *cc = strchr(host, (int)':');
    if (cc == 0) {
        len = strlen(host);
        if (len >= sizeof(tok))
            return -2;
        memcpy(tok, host, len);
        tok[len] = 0;
    } else {
        *port = atoi(cc + 1);
        len = (int)(cc - host);
        if (len >= sizeof(tok))
            return -2;
        memcpy(tok, host, len);
        tok[len] = 0;
    }
    if ((inaddr = inet_addr(tok)) != (in_addr_t)-1) {
        /* it's dotted-decimal */
        memcpy(&in->s_addr, &inaddr, sizeof(inaddr));
    } else {
#ifdef __APPLE__
        hp = gethostbyname(tok);
#elif _LINUX_SOURCE
        gethostbyname_r(tok, &hostbuf, tmp, tmplen, &hp, &herr);
#elif _SUN_SOURCE
        hp = gethostbyname_r(tok, &hostbuf, tmp, tmplen, &herr);
#else
        hp = gethostbyname(tok);
#endif
        if (hp == NULL) {
            fprintf(stderr, "%s:gethostbyname(%s): errno=%d err=%s\n", __func__,
                    tok, errno, strerror(errno));
            return -1;
        }
        memcpy(&in->s_addr, hp->h_addr, hp->h_length);
    }
    return 0;
}

static int cdb2_do_tcpconnect(struct in_addr in, int port, int myport,
                              int timeoutms)
{
    int sockfd, rc;
    int sendbuff;
    if (port <= 0) {
        return -1;
    }
    if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        fprintf(stderr, "tcpconnect_to: can't create TCP socket\n");
        return -1;
    }
    sendbuff = 1; /* enable option */
    if (setsockopt(sockfd, IPPROTO_TCP, TCP_NODELAY, (char *)&sendbuff,
                   sizeof sendbuff) < 0) {
        fprintf(stderr, "tcpconnect_to: setsockopt failure\n");
        close(sockfd);
        return -1;
    }
    struct linger ling;
    ling.l_onoff = 1;
    ling.l_linger = 0;
    if (setsockopt(sockfd, SOL_SOCKET, SO_LINGER, (char *)&ling, sizeof(ling)) <
        0) {
        fprintf(stderr, "tcpconnect_to: setsockopt failure:%s",
                strerror(errno));
        close(sockfd);
        return -1;
    }

    if (myport > 0) { /* want to use specific port on local host */
        struct sockaddr_in my_addr = {0};      /* my Internet address */
        my_addr.sin_family = AF_INET;
        my_addr.sin_port = htons((u_short)myport);
        my_addr.sin_addr.s_addr = INADDR_ANY;
        if (bind(sockfd, (struct sockaddr *)&my_addr, sizeof my_addr) < 0) {
            fprintf(stderr, "tcpconnect_to: bind failed on local port %d: %s",
                    myport, strerror(errno));
            close(sockfd);
            return -1;
        }
    }

    struct sockaddr_in tcp_srv_addr = {0}; /* server's Internet socket addr */
    tcp_srv_addr.sin_family = AF_INET;
    tcp_srv_addr.sin_port = htons(port);
    memcpy(&tcp_srv_addr.sin_addr, &in.s_addr, sizeof(in.s_addr));
    /* Connect to the server.  */
    rc = lclconn(sockfd, (struct sockaddr *)&tcp_srv_addr, sizeof(tcp_srv_addr),
                 timeoutms);

    if (rc < 0) {
        close(sockfd);
        return rc;
    }
    return (sockfd); /* all OK */
}

static int cdb2_tcpconnecth_to(const char *host, int port, int myport,
                               int timeoutms)
{
    int rc;
    struct in_addr in;
    if ((rc = cdb2_tcpresolve(host, &in, &port)) != 0)
        return rc;
    return cdb2_do_tcpconnect(in, port, myport, timeoutms);
}

static int cdb2portmux_route(const char *remote_host, const char *app,
                             const char *service, const char *instance)
{
    char name[64] = {0};
    char res[32];
    COMDB2BUF *ss = NULL;
    int rc, fd;
    rc = snprintf(name, sizeof(name), "%s/%s/%s", app, service, instance);
    if (rc < 1 || rc >= sizeof(name))
        return -1;
    fd = cdb2_tcpconnecth_to(remote_host, 5105, 0, -1);
    if (fd < 0)
        return -1;
    ss = cdb2buf_open(fd, 0);
    if (ss == 0) {
        close(fd);
        return -1;
    }
    cdb2buf_printf(ss, "rte %s\n", name);
    cdb2buf_flush(ss);
    res[0] = 0;
    cdb2buf_gets(res, sizeof(res), ss);
    if (res[0] != '0') {
        cdb2buf_close(ss);
        return -1;
    }
    cdb2buf_free(ss);
    return fd;
}

Appsock::Appsock(const std::string& dbname, const std::string& req) :
    impl(new Appsock_impl(dbname))
{
    impl->m_fd = cdb2portmux_route("localhost", "comdb2", "replication",
            impl->m_dbname.c_str());

    if(impl->m_fd < 0) {
        throw Error("cdb2portmux_route to " + dbname + " failed");
    }

    impl->m_sb = cdb2buf_open(impl->m_fd, 0);
    if(!impl->m_sb) {
        ::close(impl->m_fd);
        impl->m_fd = -1;
        throw Error("cdb2buf_open to " + dbname + " failed");
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
        cdb2buf_close(impl->m_sb);
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

    cdb2buf_printf(impl->m_sb, const_cast<char*>(req.c_str()));
    cdb2buf_flush(impl->m_sb);
}

bool Appsock::response(const std::string& rsp)
{
    SigPipeBlocker block_sigpipe;

    // Get the response - we expect "log file deletion disable"
    cdb2buf_settimeout(impl->m_sb, 10 * 1000, 10 * 1000);
    char line[256];
    if(cdb2buf_gets(line, sizeof(line), impl->m_sb) <= 0) {
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

    cdb2buf_settimeout(impl->m_sb, 10 * 1000, 10 * 1000);

    char line[256] = {0};
    char *eol;

    /* there's no response for unknown requests, so this becomes a 5 second timeout
     * until a new database rolls out */
    if (cdb2buf_gets(line, sizeof(line), impl->m_sb) < 0)
        throw Error("can't read response");
    if ((eol = strchr(line, '\n')) != NULL)
        *eol = 0;

    return std::string(line);
}
