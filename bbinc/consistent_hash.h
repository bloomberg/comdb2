#ifndef CH_HASH_H
#define CH_HASH_H
#include <stddef.h>
#include <stdint.h>
#include <openssl/bn.h>
#include <list.h>
typedef struct consistent_hash ch_hash_t;
typedef struct consistent_hash_node ch_hash_node_t;
typedef int (*hash_func)(uint8_t *, size_t, uint32_t *);
enum ch_err {
    CH_NOERR = 0,
    CH_ERR_PARAM = 1,
    CH_ERR_MALLOC = 2,
    CH_ERR_HASH = 3,
    CH_ERR_DUP = 4
};

enum ch_hash_func_type {
    CH_HASH_SHA = 1,
    CH_HASH_MD5 = 2,
    CH_HASH_CRC = 3
};
struct consistent_hash_node {
    uint8_t *data;
    size_t data_len;
    LINKC_T(struct consistent_hash_node) lnk;
};

typedef struct consistent_hash_keyhash {
    uint64_t hash_val; 
    struct consistent_hash_node *node;
}ch_keyhash_t;

struct consistent_hash {
    LISTC_T(struct consistent_hash_node) nodes;         /* List of nodes for key hashes to refer to */
    uint32_t num_nodes;
    struct consistent_hash_keyhash **key_hashes;   /* Array of key hashes of nodes and (optionally)
                                                      their copies. I went with an array over a hash
                                                      since we need this list to be sorted in order to look 
                                                      up the next largest keyhash that has an associated node,
                                                      given a keyhash*/
    uint32_t num_keyhashes;
    pthread_rwlock_t lock;
    hash_func func;
};

/*
 * Create an in-mem abstraction of a consistent hash.
 * num_copies -> number of copies of node to be added to the hash
 *               Db servers, cache servers are examples of a 'Node' 
 * ch_func -> pointer to the hash function to be used 
 *            It takes a byte array and it's length as the input
 *            The hash is available in the OUT parameter hash as a BIGNUM
 *            www.openssl.org/docs/man1.0.2/man3/bn.html
 */
ch_hash_t *ch_hash_create(uint64_t num_copies, hash_func func);

/*
 * Add a node to the consistent hash.
 * name -> a byte array that represents the name of the node
 *         in the calling program.
 * name_len -> length of above name
 */
int ch_hash_add_node(ch_hash_t *hash, uint8_t *name, size_t name_len, uint64_t hashval);


/*
 * Add a replica for a node at hashval 
 * node_name    -> node name for which replica is being added 
 * node_len     -> size (in bytes) of node_name
*/
int ch_hash_add_replica(ch_hash_t *hash, uint8_t *node_name, size_t node_len, uint64_t hashval);


/*
 * Remove a node from the consistent hash.
 * name -> a byte array that represents the name of the node
 *         in the calling program.
 * name_len -> length of above name
 */
int ch_hash_remove_node(ch_hash_t *hash, uint8_t *name, size_t name_len);

/*
 * Given a key and it's length, return the node on the consistent hash 
 * that the inputs hash onto
 */
ch_hash_node_t *ch_hash_find_node(ch_hash_t *hash, uint8_t *key, size_t key_len);

/*
 * Free all memory associated with a consistent hash
 */
void ch_hash_free(ch_hash_t *hash);

/*
 * SHA256 and MD5 based hash funcs
 */
int ch_hash_sha(uint8_t *buf, size_t buf_len, uint32_t *hash);
int ch_hash_md5(uint8_t *buf, size_t buf_len, uint32_t *hash);


/*
 * TEST HELPERS
 */
uint8_t *get_node_data(ch_hash_node_t *ch);
size_t get_node_data_len(ch_hash_node_t *ch);
#endif
