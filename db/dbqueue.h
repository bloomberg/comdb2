#ifndef INCLUDED_DBQUEUE_H
#define INCLUDED_DBQUEUE_H

/* This is the maximum number of messages that dbqueue.c will shove into a
 * fstsnd buffer to send to listening offlines.  This constant should not be
 * increased as the client side code (comdb2_queue.c) has asusmptions about
 * the max number of messages it will ever receive at once (which affect the
 * size of the mmap file it uses to track recently received messages).
 * Also, sending more messages has time issues.  If it takes the offline a
 * second to process each message then it will take 64 seconds to process a full
 * buffer - which exceeds the bigsnd timeout, so the database will always
 * assume failure and try to resend. */
enum { MAXSENDQUEUE = 64 };

enum rtcpu_mode { NO_RTCPU, USE_RTCPU, DFLT_RTCPU };

struct dbtable;

/* a queue consumer */
struct consumer {
    struct dbtable *db; /* chain back to db */
    int consumern;

    volatile int active; /* nonzero if thread is running */

    int complained; /* set if we've already announced refusal to start due to
                       an illegal destination */

    int type; /* a CONSUMER_TYPE_ constant */

    int debg; /* a counter */

    enum rtcpu_mode rtcpu_mode;

    /* *** for fstsnd delivery *** */

    char *rmtmach; /* remote machine number. */
    int dbnum;     /* remote database number. */

    int ndesthosts;
    char **desthosts;
    int current_dest;
    int randomize_list;
    char *bbhost_tag;

    /* keep a queue of items that we are trying to send. */
    size_t spaceleft;
    int numitems;
    void *items[MAXSENDQUEUE];
    /* keep some statistics */
    unsigned int n_good_fstsnds;
    unsigned int ms_good_fstsnds; /* total time taken by good fstsnds */
    unsigned int n_bad_fstsnds;
    unsigned int n_bad_rtcpu_fstsnds;
    unsigned int n_rejected_fstsnds;
    unsigned int n_consumed;

    /* *** for javasp delivery *** */

    char procedure_name[MAXCUSTOPNAME];
    unsigned int n_commits;
    unsigned int n_aborts;
    unsigned int n_retries;

    /* Set this to 1 to get debug trace in the consumer */
    int debug;

    /* Used to implement a sleep/wakeup system for the consumer thread that can
     * be interrupted to make it exit instantly.
     * waiting_for_data is set if we're sleeping in the very outer loop i.e.
     * we are waiting for data to be available.  Normally we don't want to
     * wake up consumers that are sleeping between fstsnd retries just
     * because there's more data on the queue. */
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    int need_to_wake;
    int waiting_for_data;

    /* connection to consumer */
    int listener_fd;
    uint8_t buf[64 * 1024]; /* fstsnd limit is 64k and isn't likely to change
                               soon, so
                               keep this hardcoded */
    int first;              /* first request in buffer? */

    int logfile;

    int please_stop;
    int stopped;
};
#endif
