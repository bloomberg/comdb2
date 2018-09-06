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

#ifndef INCLUDED_PORTMUXAPI
#define INCLUDED_PORTMUXAPI

/* register/get port through portmux

   example:
         app=comdb2
     service=replication
    instance=secdb

    For full background, FAQs and api recommendations, see
    {TEAM SYSI:PORTMUX<go>}.

*/

#include <sys/types.h>
#include <netinet/in.h>

#if defined __cplusplus
extern "C" {
#endif

typedef enum {
    PORTMUX_PORT_SUPPRESS = 1 /* Do not allocate a portmux port for this
                                 connection (in 1 port mode) */
} portmux_options_t;

/* ***
 * The following calls are wrappers to make portmux usage easier.
 * They give ability to do TCP connect//listen with a port returned
 * by portmux.
 * Underneath they use tcputil.h wrappers.
 * It is recommended to use other calls from tcplib to do read//write.
 * *** */

/**
 * @brief  Registers with portmux then starts listening for connections.
 * It will call accept_hndl() with received connection fd.
 *
 * @param[in]     app           Application to register.
 * @param[in]     service       Service to register.
 * @param[in]     instance      Instance to register.
 * @param[in]     accept_hndl   Handler to call on connection.
 *                              It is responsability of this
 *                              callback to do close(fd).
 *
 * @return -1 for error before accepting connection.
 * Will block otherwise
 *
 * @example:
 * static pthread_attr_t attr;
 * void client_thr(int fd)
 * {
 *     printf("yeah, got a friend on fd# %d\n", fd);
 *     close(fd);
 * }
 *
 *
 * void accept_hndl(int fd, void *user_data)
 * {
 *     pthread_t tid;
 *     int rc;
 *
 *     // ok, spawn a client thread and go wait for somebody else
 *     rc = pthread_create(&tid, &attr,
 *                         (void*(*)(void*))client_thr, (void*)fd);
 *     if (rc) {
 *         fprintf(stderr, "accept_hndl:pthread_create(): errno=[%d] %s\n",
 *                 rc, strerror(rc));
 *         close(fd);   // close fd
 *     }
 * }
 *
 *
 * int main(int argc, char *argv[])
 * {
 *     // initialize attribute for client threads
 *     pthread_attr_init(&attr);
 *     pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
 *
 *     // alright wait for client
 *     portmux_listen("comdb2", "replication", "dbtest", accept_hndl, NULL);
 *     fprintf(stderr, "portmux_listen: errno=[%d] %s\n",
 *             errno, strerror(errno));
 *     exit(1);
 * }
 */
int portmux_listen(const char *app, const char *service, const char *instance,
                   void (*accept_hndl)(int fd, void *user_data),
                   void *user_data);

int portmux_listen_options(const char *app, const char *service,
                           const char *instance,
                           void (*accept_hndl)(int fd, void *user_data),
                           void *user_data, uint32_t options);

/** If you dont want to use portmux_listen() (due to callback
 * mechanism or any other reason). You can use the following 3 calls
 * (portmux_listen, simply wraps them):
 *
 * 1. portmux_listen_setup() to create a portmux_fd_t
 * 2. call portmux_accept() to retrieve 1 connection. This is a
 *    blocking call.
 * 3. when done call portmux_close to cleanup.
 *
 * @example:
 * static void server(char *server_name)
 * {
 *     portmux_fd_t *portmux_hndl;
 *     int clientfd;
 *
 *     // Get a connection with portmux for our service
 *     portmux_hndl = portmux_listen_setup(server_name, server_name,
 *server_name);
 *     if (!portmux_hndl)
 *     {
 *         fprintf(stderr, "portmux_listen returned NULL\n");
 *         exit(1);
 *     }
 *
 *     while (1)
 *     {
 *         // Patiently wait for our clients to connect using portmux_connect()
 *         clientfd = portmux_accept(portmux_hndl, 0);
 *         if (clientfd <= 0)
 *         {
 *             fprintf(stderr, "error in portmux_accept\n");
 *             break;
 *         }
 *
 *         // Serve the client, this function will call close(clientfd);
 *         server_accept_hndl(clientfd);
 *     }
 *
 *     // all done cleanup
 *     portmux_close(portmux_hndl);
 *     exit(1);
 * }
 */
typedef struct portmux_fd portmux_fd_t;

/**
 * @brief  Create a "portmux File descriptor"
 *
 * @param[in]     app           Application to register.
 * @param[in]     service       Service to register.
 * @param[in]     instance      Instance to register.
 *
 * @return
 */
portmux_fd_t *portmux_listen_setup(const char *app, const char *service,
                                   const char *instance, int tcplistenfd);

/**
 * @brief  Create a "portmux File descriptor"
 *
 * @param[in]     app           Application to register.
 * @param[in]     service       Service to register.
 * @param[in]     instance      Instance to register.
 * @param[in]     options       Options.
 *
 * @return
 */
portmux_fd_t *portmux_listen_options_setup(const char *app, const char *service,
                                           const char *instance,
                                           int tcplistenfd, uint32_t options);

/**
 * @brief  Wait for a connection on portmux_fd.
 *
 * @param[in/out] fds           Created from portmux_listen_setup().
 * @param[in/out] timeoutms     Optional timeout in ms. Will block until
 *                              connection if <= 0.
 *
 * @return File descriptor of connection or -1 on error.
 *         On timeout it will return 0 (like poll()) and errno
 *         will be set to ETIMEDOUT.
 *         If connectivity with portmux is dropped and timeout
 *         expires before recovery succeed, rc will be -1
 *         and errno will be set to EAGAIN (meaning should retry).
 */
int portmux_accept(portmux_fd_t *fds, int timeoutms);

/**
 * @brief  Wait for a connection on a vector of portmux_fd.
 *
 * @param[in/out] fds           Created from portmux_listen_setup() or
 *                              portmux_listen_options_setup().
 * @param[in/out] nfds          Number of entries in fds.
 * @param[in/out] timeoutms     Optional timeout in ms. Will block until
 *                              connection if <= 0.
 * @param[in]     accept_hndl   Handler to call on connection.
 *                              It is responsability of this
 *                              callback to do close(fd).
 *
 * @return File descriptor of connection or -1 on error.
 *         On timeout it will return 0 (like poll()) and errno
 *         will be set to ETIMEDOUT.
 *         If connectivity with portmux is dropped and timeout
 *         expires before recovery succeed, rc will be -1
 *         and errno will be set to EAGAIN (meaning should retry).
 *
 *         Successful connections will be passed to accept_hndl() with
 *         'which' indicating the index into fds of the corresponding
 *         listening socket.
 *
 * @note If no callback function is given then this function will return
 * a file descriptor for a connection that we have accepted. As
 * there is no context as to which of the fds the connection was made it
 * is likely of little use.
 *
 * A non-zero return from the accept_hndl callback function will cause this
 * function to return with that value.
 */

int portmux_acceptv(portmux_fd_t **fds, int nfds, int timeoutms,
                    int (*accept_hndl)(int which, int fd, void *user_data),
                    void *user_data);

/**
 * @brief  Close and free previously attributed portmux_fd.
 *
 * @param[in/out] fds           Portmux_fd to release (invalid after call)
 */
void portmux_close(portmux_fd_t *fds);

/**
 * @brief  Using portmux_fds a TCP port may or may not be attributed
 * by portmux. One can use this call to retrieve it, knowing that it
 * can return 0 if no port# has been given.
 *
 * @param[in/out] fds           Portmux_fd to query.
 *
 * @return TCP port# if one was given else 0.
 */
int portmux_fds_get_tcpport(portmux_fd_t *fds);

/**
 * @brief  Return details of the triplet passed when fds was created.
 *
 * @param[in/out] fds           Portmux_fd to query.
 *
 * @return The application, service or instance
 */
const char *portmux_fds_get_app(portmux_fd_t *fds);
const char *portmux_fds_get_service(portmux_fd_t *fds);
const char *portmux_fds_get_instance(portmux_fd_t *fds);

void set_portmux_port(int port);
int set_portmux_bind_path(const char *path);
char *get_portmux_bind_path(void);
void clear_portmux_bind_path();

/**
 * @brief  Connects to remote_host using portmux registered
 * app/service/instance.
 *
 * @param[in]     remote_host   Where to connect to.
 * @param[in]     app           Application to connect to.
 * @param[in]     service       Service to connect to.
 * @param[in]     instance      Instance to connect to.
 *
 * @return A valid connected fd to use if >= 0, else error. (@see tcplib)
 */
int portmux_connect(const char *remote_host, const char *app,
                    const char *service, const char *instance);

/**
 * @brief  Same as portmux_connect() with timeout option.
 *
 * @param[in]     remote_host   Where to connect to.
 * @param[in]     app           Application to connect to.
 * @param[in]     service       Service to connect to.
 * @param[in]     instance      Instance to connect to.
 * @param[in]     timeoutms     Optional timeout in ms.
 *
 * @return A valid connected fd to use if >= 0, else error. (@see tcplib)
 */
int portmux_connect_to(const char *remote_host, const char *app,
                       const char *service, const char *instance,
                       int timeoutms);

/**
 * @brief  Same as portmux_connect() but use given address.
 *
 * @param[in/out] addr          Address to connect to
 * @param[in]     app           Application to connect to.
 * @param[in]     service       Service to connect to.
 * @param[in]     instance      Instance to connect to.
 *
 * @return A valid connected fd to use if >= 0, else error. (@see tcplib)
 */
int portmux_connecti(struct in_addr addr, const char *app, const char *service,
                     const char *instance);

/**
 * @brief  Same as portmux_connecti() with a timeout option
 *
 * @param[in/out] addr          Address to connect to
 * @param[in]     app           Application to connect to.
 * @param[in]     service       Service to connect to.
 * @param[in]     instance      Instance to connect to.
 * @param[in]     timeoutms     Optional timeout in ms.
 *
 * @return A valid connected fd to use if >= 0, else error. (@see tcplib)
 */
int portmux_connecti_to(struct in_addr addr, const char *app,
                        const char *service, const char *instance,
                        int timeoutms);

/******************************************************************************
 ******************************************************************************
 **                                                                          **
 ** THE APIS BELOW ARE CONSIDERED DEPRECATED                                 **
 **                                                                          **
 ** New software should use the APIs above, which support "single port"      **
 ** mode.  See {SYSI:PORTMUX<go>}.                                           **
 **                                                                          **
 ******************************************************************************
 ******************************************************************************/

/* This is called by servers to register themselves with portmux and
 * obtain a port that they can bind to and accept on.
 *
 * Returns port number, or -1 for error
 */
int portmux_register(const char *app, const char *service,
                     const char *instance);

/**
  * Tell portmux that we're now using this port
 */
int portmux_use(const char *app, const char *service, const char *instance,
                int port);

/**
 * Deregister a port number previous registered with portmux_register().
 *
 * In practice there is very little reason to ever call this.  It is certainly
 * not required that a server call this before it shuts down, as from the
 * clients point of view there is really no practical difference between
 * portmux not being able to return a port for a service, and portmux returning
 * a port but the server itself not running to accept connections.
 *
 * Returns 0 on success, non-zero if the deregistration fails.
 */
int portmux_deregister(const char *app, const char *serivce,
                       const char *instance);

/* returns port number, or -1 for error*/
int portmux_get(const char *remote_host, const char *app, const char *service,
                const char *instance);
int portmux_get_to(const char *remote_host, const char *app,
                   const char *service, const char *instance, int timeout_ms);

/* same as above by take an address rather than host name */
int portmux_geti(struct in_addr addr, const char *app, const char *service,
                 const char *instance);
int portmux_geti_to(struct in_addr addr, const char *app, const char *service,
                    const char *instance, int timeout_ms);

int portmux_hello(char *host, char *name, int *fdout);

/* override default portmux timeout */
void portmux_set_default_timeout(unsigned timeoutms);
void portmux_set_max_wait_timeout(unsigned timeoutms);

void portmux_register_reconnect_callback(int (*callback)(void *), void *);

int get_portmux_port(void);
void set_portmux_port(int);

#if defined __cplusplus
}
#endif

#endif
