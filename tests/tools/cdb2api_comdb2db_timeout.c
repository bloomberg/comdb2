/*
 * With enforce_api_call_timeout on, cdb2_open() must return within
 * api_call_timeout even if comdb2db answers dbinfo but then never answers the
 * query for the database's hosts.
 *
 * A fake comdb2db runs in a background thread. With allow_pmux_route on, every
 * connection goes through it: it accepts "rte", answers dbinfo with itself as
 * the only node, and never responds to any sql query.
 */
#include <arpa/inet.h>
#include <inttypes.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <cdb2api.h>
#include <sqlquery.pb-c.h>
#include <sqlresponse.pb-c.h>

#define API_CALL_TIMEOUT_MS 2000
#define SLACK_MS 1000
#define GIVE_UP_SECS 30

struct newsqlheader {
    int type;
    int compression;
    int state;
    int length;
};

static int64_t epochms(void)
{
    struct timeval now;
    gettimeofday(&now, NULL);
    return (int64_t)now.tv_sec * 1000 + now.tv_usec / 1000;
}

static int read_full(int fd, void *buf, size_t len)
{
    char *p = buf;
    while (len > 0) {
        ssize_t n = read(fd, p, len);
        if (n <= 0)
            return -1;
        p += n;
        len -= n;
    }
    return 0;
}

static int write_full(int fd, const void *buf, size_t len)
{
    const char *p = buf;
    while (len > 0) {
        ssize_t n = write(fd, p, len);
        if (n <= 0)
            return -1;
        p += n;
        len -= n;
    }
    return 0;
}

static int read_line(int fd, char *buf, size_t len)
{
    size_t i = 0;
    while (i < len - 1) {
        if (read(fd, &buf[i], 1) != 1)
            return -1;
        if (buf[i] == '\n')
            break;
        ++i;
    }
    buf[i] = '\0';
    return 0;
}

static int send_dbinfo_response(int fd, int port)
{
    CDB2DBINFORESPONSE__Nodeinfo node = CDB2__DBINFORESPONSE__NODEINFO__INIT;
    node.name = "localhost";
    node.number = 1;
    node.has_port = 1;
    node.port = port;
    CDB2DBINFORESPONSE__Nodeinfo *nodes[] = {&node};

    CDB2DBINFORESPONSE response = CDB2__DBINFORESPONSE__INIT;
    response.master = &node;
    response.n_nodes = 1;
    response.nodes = nodes;

    size_t len = cdb2__dbinforesponse__get_packed_size(&response);
    unsigned char *buf = malloc(len);
    cdb2__dbinforesponse__pack(&response, buf);
    struct newsqlheader hdr = {.type = htonl(RESPONSE_HEADER__DBINFO_RESPONSE), .length = htonl(len)};
    int rc = write_full(fd, &hdr, sizeof(hdr)) || write_full(fd, buf, len);
    free(buf);
    return rc;
}

struct conn {
    int fd;
    int port;
};

static void *serve_connection(void *arg)
{
    struct conn *c = arg;
    char line[256];

    /* cdb2portmux_route(): "rte comdb2/replication/<dbname>" */
    if (read_line(c->fd, line, sizeof(line)) != 0 || strncmp(line, "rte ", 4) != 0)
        goto out;
    if (write_full(c->fd, "0\n", 2) != 0)
        goto out;
    /* "newsql" or "@newsql" */
    if (read_line(c->fd, line, sizeof(line)) != 0)
        goto out;

    while (1) {
        struct newsqlheader hdr;
        if (read_full(c->fd, &hdr, sizeof(hdr)) != 0)
            goto out;
        int type = ntohl(hdr.type);
        int len = ntohl(hdr.length);
        unsigned char *buf = NULL;
        if (len > 0) {
            buf = malloc(len);
            if (read_full(c->fd, buf, len) != 0) {
                free(buf);
                goto out;
            }
        }
        if (type != CDB2_REQUEST_TYPE__CDB2QUERY) { /* eg. RESET on a cached connection */
            free(buf);
            continue;
        }
        CDB2QUERY *query = cdb2__query__unpack(NULL, len, buf);
        free(buf);
        int is_dbinfo = query && query->dbinfo;
        if (query)
            cdb2__query__free_unpacked(query, NULL);
        if (!is_dbinfo)
            break;
        if (send_dbinfo_response(c->fd, c->port) != 0)
            goto out;
    }

    /* Hang: keep the connection open and never respond to the sql query */
    while (1)
        pause();

out:
    close(c->fd);
    free(c);
    return NULL;
}

static void *fake_comdb2db(void *arg)
{
    int lfd = *(int *)arg;
    struct sockaddr_in addr;
    socklen_t addrlen = sizeof(addr);
    getsockname(lfd, (struct sockaddr *)&addr, &addrlen);
    while (1) {
        int fd = accept(lfd, NULL, NULL);
        if (fd < 0)
            continue;
        struct conn *c = malloc(sizeof(*c));
        c->fd = fd;
        c->port = ntohs(addr.sin_port);
        pthread_t t;
        pthread_create(&t, NULL, serve_connection, c);
        pthread_detach(t);
    }
    return NULL;
}

static void give_up(int sig)
{
    static const char msg[] = "cdb2_open() did not return: comdb2db read has no timeout - fail\n";
    (void)sig;
    ssize_t n = write(2, msg, sizeof(msg) - 1);
    (void)n;
    _exit(1);
}

int main(int argc, char **argv)
{
    (void)argc;
    (void)argv;
    signal(SIGPIPE, SIG_IGN);
    signal(SIGALRM, give_up);

    int lfd = socket(AF_INET, SOCK_STREAM, 0);
    int on = 1;
    setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
    struct sockaddr_in addr = {.sin_family = AF_INET, .sin_addr.s_addr = htonl(INADDR_LOOPBACK)};
    socklen_t addrlen = sizeof(addr);
    if (bind(lfd, (struct sockaddr *)&addr, sizeof(addr)) != 0 || listen(lfd, 16) != 0 ||
        getsockname(lfd, (struct sockaddr *)&addr, &addrlen) != 0) {
        perror("fake comdb2db");
        return 1;
    }
    int port = ntohs(addr.sin_port);
    pthread_t t;
    pthread_create(&t, NULL, fake_comdb2db, &lfd);

    char cfg[] = "/tmp/cdb2api_comdb2db_timeout.XXXXXX";
    int cfd = mkstemp(cfg);
    FILE *f = fdopen(cfd, "w");
    fprintf(f,
            "comdb2db localhost\n"
            "comdb2_config:default_type=dev\n"
            "comdb2_config:portmuxport=%d\n"
            "comdb2_config:allow_pmux_route=on\n"
            "comdb2_config:api_call_timeout=%d\n"
            "comdb2_config:enforce_api_call_timeout=on\n"
            "comdb2_feature:use_bmsd=0\n",
            port, API_CALL_TIMEOUT_MS);
    fclose(f);

    cdb2_disable_sockpool();
    cdb2_set_comdb2db_config(cfg);

    alarm(GIVE_UP_SECS);
    cdb2_hndl_tp *hndl = NULL;
    int64_t start = epochms();
    int rc = cdb2_open(&hndl, "fakedb", "dev", 0);
    int64_t elapsed = epochms() - start;
    alarm(0);
    unlink(cfg);

    printf("cdb2_open rc %d in %" PRId64 "ms: %s\n", rc, elapsed, cdb2_errstr(hndl));
    cdb2_close(hndl);
    if (elapsed > API_CALL_TIMEOUT_MS + SLACK_MS) {
        fprintf(stderr, "cdb2_open took %" PRId64 "ms, expected at most %dms - fail\n", elapsed,
                API_CALL_TIMEOUT_MS + SLACK_MS);
        return 1;
    }
    return 0;
}
