#include "consistent_hash.h"
#include <list.h>
#include "logmsg.h"
#include <stdlib.h>
#include <string.h>
#include <openssl/sha.h>
#include <openssl/md5.h>
#include <pthread.h>
#include <locks_wrap.h>
#define MAX_COPIES 8
struct consistent_hash_node {
    uint8_t *data;
    size_t data_len;
    LINKC_T(struct consistent_hash_node) lnk;
};

typedef struct consistent_hash_keyhash {
    BIGNUM *hash_val; 
    struct consistent_hash_node *node;
}ch_keyhash_t;

struct consistent_hash {
    LISTC_T(struct consistent_hash_node) nodes;         /* List of nodes for key hashes to refer to */
    uint32_t num_copies;
    struct consistent_hash_keyhash **key_hashes;   /* Array of key hashes of nodes and (optionally)
                                                      their copies. I went with an array over a hash
                                                      since we need this list to be sorted in order to look 
                                                      up the next largest keyhash that has an associated node,
                                                      given a keyhash*/
    uint32_t num_keyhashes;
    pthread_mutex_t lock;
    hash_func func;
};

uint8_t *get_node_data(ch_hash_node_t *ch_node) {
    return ch_node->data;
}

size_t get_node_data_len(ch_hash_node_t *ch_node) {
    return ch_node->data_len;
}



/*
 * https://stackoverflow.com/questions/2262386/generate-sha256-with-openssl-and-c
 * https://stackoverflow.com/questions/52240053/c-efficiently-get-sha256-digest-into-openssl-bignum
 */
int ch_hash_sha(uint8_t *buf, size_t buf_len, BIGNUM *hash) {
    unsigned char sha_hash[SHA256_DIGEST_LENGTH];
    SHA256_CTX sha256;

    SHA256_Init(&sha256);
    SHA256_Update(&sha256, buf, buf_len);
    SHA256_Final(sha_hash, &sha256);
    if (BN_bin2bn(sha_hash, SHA256_DIGEST_LENGTH, hash) == NULL) {
        logmsg(LOGMSG_ERROR, "%s:%d BN_bin2bn failed\n", __func__,__LINE__);
        return CH_ERR_HASH;
    }
    return CH_NOERR;
}

int ch_hash_md5(uint8_t *buf, size_t buf_len, BIGNUM *hash) {
    unsigned char md5_hash[MD5_DIGEST_LENGTH];
    MD5_CTX md5;

    MD5_Init(&md5);
    MD5_Update(&md5, buf, buf_len);
    MD5_Final(md5_hash, &md5);
    if (BN_bin2bn(md5_hash, MD5_DIGEST_LENGTH, hash) == NULL) {
        logmsg(LOGMSG_ERROR, "%s:%d BN_bin2bn failed\n", __func__,__LINE__);
        return CH_ERR_HASH;
    }
    return CH_NOERR;
}

static void ch_append_keyhash(ch_hash_t *ch, ch_keyhash_t *key_hash, int idx) {
    int arr_idx = 0;

    arr_idx = ((listc_size(&ch->nodes) - 1) * (ch->num_copies)) + idx;
    ch->key_hashes[arr_idx] = key_hash;
}


static int keyhash_cmp(const void *key1, const void *key2) {
    ch_keyhash_t *h1 = *(ch_keyhash_t **)key1;
    ch_keyhash_t *h2 = *(ch_keyhash_t **)key2;
    int result = BN_cmp(h1->hash_val, h2->hash_val);
    return result;
}

static int validate_input(ch_hash_t *ch, uint8_t *data, size_t data_len) {
    if (!ch) {
        logmsg(LOGMSG_ERROR, "%s:%d invalid input : NULL hash\n", __func__, __LINE__);
        return CH_ERR_PARAM;
    }

    if (!data) {
        logmsg(LOGMSG_ERROR, "%s:%d invalid input : NULL data\n", __func__, __LINE__);
        return CH_ERR_PARAM;
    }

    if (data_len <= 0) {
        logmsg(LOGMSG_ERROR, "%s:%d invalid input : data_len should be atleast 1\n", __func__, __LINE__);
        return CH_ERR_PARAM;
    }
    return CH_NOERR;
}

static void print_key_hashes(ch_hash_t *ch) {
    for (int i=0; i< ch->num_keyhashes; i++) {
        BN_print_fp(stdout, ch->key_hashes[i]->hash_val);
        printf("\n");
    }
}

/*
 * Add a node (and it's copies) to the hash
 */
static int ch_hash_add_node_int(ch_hash_t *ch, ch_hash_node_t *node) {

    BIGNUM *hash_key = NULL;
    int num_nodes = listc_size(&ch->nodes);
    uint8_t *buf = NULL;    /* buffer to hold input to hash function */
    char tmp[8]; /* buffer to temporarily hold a digit */
    size_t tmp_len = 0;
    ch_keyhash_t **temp = NULL;
    int rc = 0, arr_idx = 0;

    temp = (ch_keyhash_t **)realloc(ch->key_hashes, sizeof(ch_keyhash_t *) * num_nodes * ch->num_copies);
    if (!temp) {
        logmsg(LOGMSG_ERROR, ":%s:%d realloc failed!\n", __func__, __LINE__);
        goto cleanup;
    }

    for (int i=1; i<=ch->num_copies; i++) {
        tmp_len = snprintf(tmp, sizeof(tmp), "%d", i);
        buf = (uint8_t *)malloc(sizeof(uint8_t) * node->data_len + tmp_len);
        memset(buf, 0 , node->data_len + tmp_len);
        memcpy(buf, node->data, node->data_len);
        memcpy(buf+node->data_len, tmp, tmp_len);

        hash_key = BN_new();
        if (!hash_key) {
            logmsg(LOGMSG_ERROR, "%s:%d failed to allocate SSL BIGNUM\n", __func__, __LINE__);
            goto cleanup;
        }
        rc = ch->func(buf, node->data_len + sizeof(tmp), hash_key);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s:%d hash failure. rc: %d\n", __func__, __LINE__, rc);
            goto cleanup;
        }

        ch_keyhash_t *key_hash = (ch_keyhash_t *)malloc(sizeof(ch_keyhash_t));
        if (!key_hash) {
            logmsg(LOGMSG_ERROR, "%s:%d malloc error\n", __func__, __LINE__);
            goto cleanup;
        }

        key_hash->node = node;
        key_hash->hash_val = hash_key;

        arr_idx = ((listc_size(&ch->nodes) - 1) * (ch->num_copies)) + (i-1);
        temp[arr_idx] = key_hash;
        //ch_append_keyhash(ch, key_hash, i-1);
        free(buf);
    }

    ch->key_hashes = temp;
    ch->num_keyhashes += ch->num_copies;
    return CH_NOERR;

cleanup:
    if (buf) {
        free(buf);
    }

    if (temp) {
        free(temp);
    }

    return rc;
}

static ch_keyhash_t* ch_keyhash_find_next_biggest(ch_hash_t *ch, BIGNUM *hash) {
    if (ch==NULL ||  hash==NULL) {
        if (ch==NULL) {
            logmsg(LOGMSG_ERROR, "CH is NULL\n");
        }
        logmsg(LOGMSG_ERROR, "%s:%d invalid params\n", __func__, __LINE__);
        return NULL;
    }

    int l=0, mid=0, r=ch->num_keyhashes;
    while (l<r) {
        // logmsg(LOGMSG_USER, "l: %d, mid: %d, r: %d\n",l, mid, r);
        mid = (l+r) / 2;
        if (BN_cmp(ch->key_hashes[mid]->hash_val, hash) <= 0) {
            l = mid + 1;
        } else {
            r = mid;
        }
    }

    if (l==ch->num_keyhashes) {
        return ch->key_hashes[0];
    } else {
        return ch->key_hashes[l];
    }
    return NULL;
}

static int ch_hash_add_node_locked(ch_hash_t *ch, uint8_t *data, size_t data_len) {
    int rc = 0;

    ch_hash_node_t *node = (ch_hash_node_t *)malloc(sizeof(ch_hash_node_t));
    if (!node) {
        goto oom;
    }

    node->data = (uint8_t *)malloc(sizeof(uint8_t) * data_len);
    if (!node->data) {
        goto oom;
    }
    memset(node->data, 0, data_len);
    node->data_len = data_len;
    memcpy(node->data, data, data_len);
    listc_abl(&ch->nodes, node);

    rc = ch_hash_add_node_int(ch, node);

    if (rc) {
        ch_hash_remove_node(ch, node->data, node->data_len);
        return rc;
    }
    if (ch->num_keyhashes > 1) {
        qsort((void *)ch->key_hashes, ch->num_keyhashes, sizeof(ch_keyhash_t *), keyhash_cmp);
    }
    return CH_NOERR;

oom:
    if (node) {
        if (node->data) {
            free(node->data);
        }
        free(node);
    }
    return CH_ERR_MALLOC;
}

ch_hash_node_t *ch_hash_find_node_locked(ch_hash_t *ch, uint8_t *key, size_t key_len) {
    int rc = 0;
    BIGNUM *hash_key = NULL;
    ch_keyhash_t *next_biggest = NULL;
    rc = validate_input(ch, key, key_len);
    if (rc) {
        return NULL;
    }

    if (ch->num_keyhashes == 0) {
        logmsg(LOGMSG_ERROR, "Hash is empty!\n");
        return NULL;
    }
    hash_key = BN_new();
    if (!hash_key) {
        logmsg(LOGMSG_ERROR, "%s:%d failed to allocate SSL BIGNUM\n", __func__, __LINE__);
        return NULL;
    }
    rc = ch->func(key, key_len, hash_key);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s:%d hash failure. rc: %d\n", __func__, __LINE__, rc);
        return NULL;
    }

    next_biggest = ch_keyhash_find_next_biggest(ch, hash_key);
    if (next_biggest) {
        return next_biggest->node;
    }
    return NULL;
}

static int ch_hash_remove_node_locked(ch_hash_t *ch, uint8_t *data, size_t data_len) {
    struct consistent_hash_node *item, *tmp;
    int num_nodes_deleted = 0;

    LISTC_FOR_EACH_SAFE(&ch->nodes, item, tmp, lnk) {
        if (item->data_len == data_len && memcmp(item->data, data, data_len)==0) {
            /* remove all key_hashes for this node */
            int i=0;
            while(i<ch->num_keyhashes){
                if (ch->key_hashes[i] != NULL && ch->key_hashes[i]->node == item) {
                    ch->key_hashes[i]->node = NULL;
                    BN_clear_free(ch->key_hashes[i]->hash_val);
                    free(ch->key_hashes[i]);
                    /*swap with last key_hash and set current to NULL*/
                    ch->key_hashes[i] = ch->key_hashes[ch->num_keyhashes-1];
                    ch->key_hashes[ch->num_keyhashes-1] = NULL;
                    ch->num_keyhashes--;
                    continue;
                }
                i++;
            }
            /* remove node */
            listc_rfl(&ch->nodes, item);
            free(item->data);
            free(item);
            num_nodes_deleted++;
        }
    }
    qsort(ch->key_hashes, ch->num_keyhashes, sizeof(ch_keyhash_t *), keyhash_cmp);
    return CH_NOERR;
}

ch_hash_t *ch_hash_create(uint32_t num_copies, hash_func func) {
    if (num_copies < 1) {
        logmsg(LOGMSG_ERROR, "num_copies should atleast be 1\n");
        return NULL;
    }

    if (!func) {
        logmsg(LOGMSG_ERROR, "hash function must be provided!\n");
        return NULL;
    }

    ch_hash_t *ch = (ch_hash_t *)malloc(sizeof(ch_hash_t));
    if(!ch){
        logmsg(LOGMSG_ERROR, "%s:%d malloc error\n", __func__, __LINE__);
        return NULL;
    }

    ch->num_copies = num_copies;
    listc_init(&ch->nodes, offsetof(struct consistent_hash_node, lnk)); 
    ch->key_hashes = NULL;
    ch->num_keyhashes = 0;
    ch->func = func;
    Pthread_mutex_init(&(ch->lock), NULL);
    return ch;
}

int ch_hash_add_node(ch_hash_t *ch, uint8_t *data, size_t data_len) {
    int rc = 0;
    rc = validate_input(ch, data, data_len);

    if (rc) {
        return rc;
    }
    Pthread_mutex_lock(&ch->lock);
    rc = ch_hash_add_node_locked(ch, data, data_len);
    Pthread_mutex_unlock(&ch->lock);
    return rc;
}
ch_hash_node_t *ch_hash_find_node(ch_hash_t *ch, uint8_t *data, size_t data_len) {
    int rc = 0;
    ch_hash_node_t *node = NULL;
    rc = validate_input(ch, data, data_len);

    if (rc) {
        return node;
    }
    Pthread_mutex_lock(&ch->lock);
    node = ch_hash_find_node_locked(ch, data, data_len);
    Pthread_mutex_unlock(&ch->lock);
    return node;
}

int ch_hash_remove_node(ch_hash_t *ch, uint8_t *data, size_t data_len) {
    int rc = 0;
    rc = validate_input(ch, data, data_len);

    if (rc) {
        return rc;
    }
    Pthread_mutex_lock(&ch->lock);
    rc = ch_hash_remove_node_locked(ch, data, data_len);
    Pthread_mutex_unlock(&ch->lock);
    return rc;
}

void ch_hash_free(ch_hash_t *ch) {
    struct consistent_hash_node *item, *tmp;
    if (ch) {
        Pthread_mutex_lock(&ch->lock);
        for (int i=0; i<ch->num_keyhashes; i++) {
            if (ch->key_hashes[i]) {
                free(ch->key_hashes[i]);
            }
        }
        if (ch->key_hashes) {
            free(ch->key_hashes);
        }

        LISTC_FOR_EACH_SAFE(&ch->nodes, item, tmp, lnk) {
            listc_rfl(&ch->nodes, item);
            free(item->data);
            free(item);
        }
        Pthread_mutex_unlock(&ch->lock);
        Pthread_mutex_destroy(&ch->lock);
        free(ch);
    }
}
