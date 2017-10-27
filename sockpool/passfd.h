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

/*
 * Functions to send and receive file descriptors over unix domain sockets.
 */

#ifndef INCLUDED_PASSFD_H
#define INCLUDED_PASSFD_H

enum {
    PASSFD_SUCCESS = 0,
    PASSFD_RECVMSG = -1, /* error with recvmsg() */
    PASSFD_EOF = -2,     /* eof before message completely read */
    PASSFD_2FDS = -3,    /* received more than one file descriptor */
    PASSFD_BADCTRL = -4, /* received bad control message */
    PASSFD_TIMEOUT = -5, /* timed out */
    PASSFD_POLL = -6,    /* error with poll() */
    PASSFD_SENDMSG = -7  /* error with sendmsg() */
};

#if defined(__cplusplus)
extern "C" {
#endif

/*
 * Send a file descriptor over a unix domain socket along with a message
 * of nbytes of data.
 *
 * INPUTS
 * ------
 * sockfd     - The socket to write the message and descriptor to.
 * data       - The message to send.
 * nbytes     - The length of the message (must be > 0).
 * fd_to_send - The file descriptor to send.  This can be -1 although that's
 *              a bit pointless.
 *
 * RETURNS
 * -------
 * PASSFD_SUCCESS if the file descriptor and message were sent successfully.
 *
 * PASSFD_EOF if the message could only be partially sent
 *
 * PASSFD_SENDMSG if sendmsg() returned an error other than EINTR (which is
 * handled).  The caller should check errno for the error.
 *
 * If an error is returned then the descriptor may or may not have been
 * successfully written before the error occured.  Either way, the descriptor
 * will still be open and valid in the calling process.
 */
int send_fd(int sockfd, const void *data, size_t nbytes, int fd_to_send);

/* This is exactly like send_fd(), except that it will timeout after timeoutms
 * in which case it returns PASSFD_TIMEOUT. */
int send_fd_to(int sockfd, const void *data, size_t nbytes, int fd_to_send,
               int timeoutms);

#if defined(__cplusplus)
}
#endif
/*
 * Read nbytes of data from a unix domain socket and maybe receive a file
 * descriptor.
 *
 * INPUTS
 * ------
 * sockfd   - The socket to read.
 * data     - The buffer into which we should place the data read.
 * nbytes   - The number of bytes of data to read
 * fd_recvd - Pointer to an int into which the received file descriptor will
 *            be recorded.
 *
 * RETURNS
 * -------
 * PASSFD_SUCCESS if a full message was received.  A file decsriptor may or
 * may not have been received - check *fd_recvd.
 *
 * PASSFD_EOF if eof was encountered before a full message was read.
 *
 * PASSFD_RECVMSG if recvmsg() encountered an error other than EINTR (which
 * is handled) - caller should check errno.
 *
 * PASSFD_2FDS - if more than one descriptor is recieved in the course of
 * reading the message.  Both descriptors will be closed befoe returning
 * (this is assumed to be a violation of your protocol).
 *
 * PASSFD_BADCTRL - if unsupported ancilliary data is received.
 *
 * If an error occurs than any descriptors that have been read will be
 * close()'d before returning.
 */
int recv_fd(int sockfd, void *data, size_t nbytes, int *fd_recvd);

/*
 * Format a string with an error message based on the result of a send_fd()
 * or recv_fd() function call.
 *
 * INPUTS
 * ------
 * s            - String buffer to populate
 * slen         - Size of strin buffer in bytes.
 * passfd_rc    - Return code from a send_fd() or recv_fd() call.
 * errno        - The global errno variable, as set by send_fd() or pass_fd().
 *
 * RETURNS
 * -------
 * This function calls snprintf() and passes back its return value.
 */
int passfd_errv(char *s, size_t slen, int passfd_rc);

#endif
