#ifndef INCLUDED_QUEUEDB_H
#define INCLUDED_QUEUEDB_H

void bdb_queuedb_init_priv(bdb_state_type *bdb_state);

/* don't need, we'll have a single consumer per queue? */
int bdb_queuedb_consumer(bdb_state_type *bdb_state, int consumer, int active,
                         int *bdberr);

/* need, but different */
int bdb_queuedb_best_pagesize(int avg_item_sz);

/* add to queue */
int bdb_queuedb_add(bdb_state_type *bdb_state, tran_type *tran, const void *dta,
                    size_t dtalen, int *bdberr, unsigned long long *out_genid);

/* no-op */
int bdb_queuedb_add_goose(bdb_state_type *bdb_state, tran_type *tran,
                          int *bdberr);

/* no-op */
int bdb_queuedb_check_goose(bdb_state_type *bdb_state, tran_type *tran,
                            int *bdberr);

/* no-op */
int bdb_queuedb_consume_goose(bdb_state_type *bdb_state, tran_type *tran,
                              int *bdberr);

int bdb_queuedb_walk(bdb_state_type *bdb_state, int flags, void **lastitem,
                     bdb_queue_walk_callback_t callback, void *userptr,
                     int *bdberr);

int bdb_queuedb_dump(bdb_state_type *bdb_state, FILE *out, int *bdberr);

int bdb_queuedb_get(bdb_state_type *bdb_state, int consumer,
                    const struct bdb_queue_cursor *prevcursor, void **fnd,
                    size_t *fnddtalen, size_t *fnddtaoff,
                    struct bdb_queue_cursor *fndcursor, unsigned int *epoch,
                    int *bdberr);

int bdb_queuedb_consume(bdb_state_type *bdb_state, tran_type *tran,
                        int consumer, const void *prevfnd, int *bdberr);

const struct bdb_queue_stats *bdb_queuedb_get_stats(bdb_state_type *bdb_state);

int bdb_trigger_subscribe(bdb_state_type *, pthread_cond_t **,
                          pthread_mutex_t **, const uint8_t **open);
int bdb_trigger_unsubscribe(bdb_state_type *);
int bdb_trigger_open(bdb_state_type *);
int bdb_trigger_close(bdb_state_type *);

#endif
