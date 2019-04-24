#ifndef bdb_queue_h
#define bdb_queue_h

/* Found items are returned in one of these (although the caller only gets
 * the memory handle and offset of the data).  All memory needed for this is
 * allocated together so it can be freed in one operation.
 */
struct bdb_queue_found {
    /* genid of this item */
    unsigned long long genid;

    /* length of found data */
    unsigned int data_len;

    /* byte offset from the beginning of this structure to the data. */
    unsigned int data_offset;

    /* how many fragments in the item found */
    union trans_t {
        /* how many fragments in the item found */
        unsigned int num_fragments;
        unsigned int tid;
    } trans;

    /* when this was enqueued */
    unsigned int epoch;

    /* array of num_fragments record numbers giving the location of
     * each fragment in order. */
    /*db_recno_t recnos[1];*/
};

const uint8_t *queue_found_get(struct bdb_queue_found *p_queue_found,
                               const uint8_t *p_buf, const uint8_t *p_buf_end);

uint8_t *queue_found_put(const struct bdb_queue_found *p_queue_found,
                         uint8_t *p_buf, const uint8_t *p_buf_end);


#endif
