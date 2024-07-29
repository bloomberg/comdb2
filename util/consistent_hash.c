#include "consistent_hash.h"
#include <list.h>
#include "logmsg.h"
#include <stdlib.h>
#include <string.h>
#include <openssl/sha.h>
#include <openssl/md5.h>
#include <openssl/evp.h>
#include <pthread.h>
#include <sys_wrap.h>
#include <crc32c.h>
#include <assert.h>
#define MAX_COPIES 8
#define HASH_VAL_COUNT 4294967296               // 2^32 hash values


static int debug_ch = 0;

uint8_t *get_node_data(ch_hash_node_t *ch_node) {
    return ch_node->data;
}

size_t get_node_data_len(ch_hash_node_t *ch_node) {
    return ch_node->data_len;
}

ch_keyhash_t **get_key_hashes(ch_hash_t *ch_node) {
    return ch_node->key_hashes;
}

uint32_t get_num_keyhashes(ch_hash_t *ch_node) {
    return ch_node->num_keyhashes;
}

int ch_hash_sha(uint8_t *buf, size_t buf_len, uint32_t *hash){ 
    uint8_t sha_hash[32];

#if OPENSSL_VERSION_NUMBER > 0x10100000L
    unsigned int hash_len = 0;
    EVP_MD_CTX *sha256 = EVP_MD_CTX_new();
    if (!sha256) {
        return CH_ERR_HASH;
    }
    const EVP_MD *md = EVP_get_digestbyname("SHA256");
    EVP_MD_CTX_init(sha256);
    EVP_DigestInit_ex(sha256, md, NULL);
    EVP_DigestUpdate(sha256, buf, buf_len);
    EVP_DigestFinal_ex(sha256, sha_hash, &hash_len);
    EVP_MD_CTX_free(sha256);
#else
    SHA256_CTX sha256;
    SHA256_Init(&sha256);
    SHA256_Update(&sha256, buf, buf_len);
    SHA256_Final(sha_hash, &sha256);
#endif
    uint32_t lowbits = 0;
    lowbits = sha_hash[3] << 24 | sha_hash[2] << 16 | sha_hash[1] << 8 | sha_hash[0];
    *hash = lowbits;
    return CH_NOERR;
}

int ch_hash_md5(uint8_t *buf, size_t buf_len, uint32_t *hash) {
    uint8_t md5_hash[16];
#if OPENSSL_VERSION_NUMBER > 0x10100000L
    unsigned int hash_len = 0;
    EVP_MD_CTX *md5 = EVP_MD_CTX_new();
    if (!md5) {
        return CH_ERR_HASH;
    }
    const EVP_MD *md = EVP_get_digestbyname("MD5");
    EVP_MD_CTX_init(md5);
    EVP_DigestInit_ex(md5, md, NULL);
    EVP_DigestUpdate(md5, buf, buf_len);
    EVP_DigestFinal_ex(md5, md5_hash, &hash_len);
    EVP_MD_CTX_free(md5);
#else    
    MD5_CTX md5;

    MD5_Init(&md5);
    MD5_Update(&md5, buf, buf_len);
    MD5_Final(md5_hash, &md5);
#endif
    uint32_t lowbits = 0;
    lowbits = md5_hash[3] << 24 | md5_hash[2] << 16 | md5_hash[1] << 8 | md5_hash[0];
    *hash = lowbits;
    return CH_NOERR;
}

int ch_hash_crc32(uint8_t *buf, size_t buf_len, uint32_t *hash) {
    *hash = crc32c(buf, buf_len);
    return CH_NOERR;
}

static int keyhash_cmp(const void *key1, const void *key2) {
    ch_keyhash_t *h1 = *(ch_keyhash_t **)key1;
    ch_keyhash_t *h2 = *(ch_keyhash_t **)key2;
    return h1->hash_val > h2->hash_val ? 1 : h1->hash_val < h2->hash_val ? -1 : 0;
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
        printf("%"PRIu64"\n", ch->key_hashes[i]->hash_val);
    }
}

static int ch_hash_remove_node_locked(ch_hash_t *ch, uint8_t *data, size_t data_len) {
    struct consistent_hash_node *item, *tmp;

    LISTC_FOR_EACH_SAFE(&ch->nodes, item, tmp, lnk) {
        if (item->data_len == data_len && memcmp(item->data, data, data_len)==0) {
            /* remove all key_hashes for this node */
            int i=0;
            while(i<ch->num_keyhashes){
                if (ch->key_hashes[i] != NULL && ch->key_hashes[i]->node == item) {
                    ch->key_hashes[i]->node = NULL;
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
        }
    }
    qsort(ch->key_hashes, ch->num_keyhashes, sizeof(ch_keyhash_t *), keyhash_cmp);
    return CH_NOERR;
}
/*
 * Find if a given hashval exists 
 */
ch_keyhash_t* ch_hash_find_hashval_locked(ch_hash_t *ch, uint64_t hashval) {
    if (!ch) {
        logmsg(LOGMSG_ERROR, "%s:%d Consistent hash cannot be NULL!\n", __func__, __LINE__);
        return NULL;
    }

    if (hashval<0 || hashval > HASH_VAL_COUNT) {
        logmsg(LOGMSG_ERROR, "%s:%d Invalid hash value :%"PRIu64"\n",__func__, __LINE__, hashval);
        return NULL;
    }
    if (ch->num_keyhashes==0) {
        logmsg(LOGMSG_ERROR, "%s:%d No keyhashes in the consistent hash\n",__func__, __LINE__);
        return NULL;
    }
    int32_t low = 0, mid = 0;
    uint64_t midval = 0;
    int32_t high = ch->num_keyhashes-1;
    while(low<=high) {
        mid = low + (high-low)/2;
        //logmsg(LOGMSG_USER, "l: %d, mid: %d, r: %d\n",low, mid, high);
        midval = ch->key_hashes[mid]->hash_val;
        //logmsg(LOGMSG_USER, "midval is %ld\n", midval);
        if (midval == hashval) {
            return ch->key_hashes[mid];
        } else if (midval < hashval) {
            low = mid+1;
        } else {
            high = mid-1;
        }
    }
    //logmsg(LOGMSG_USER, "%s:%d Hashval %ld was not found\n",__func__, __LINE__, hashval);
    return NULL;
}

/*
 * Add a node at hashval hash value 
 */
static int ch_hash_add_node_to_hashval_locked(ch_hash_t *ch, ch_hash_node_t *node, uint64_t hashval) {
    ch_keyhash_t **temp = NULL, *keyhash=NULL;

    keyhash = ch_hash_find_hashval_locked(ch, hashval);
    if (keyhash) {
        if (debug_ch) {
            logmsg(LOGMSG_USER, "%s:%d keyhash %"PRIu64" already exists\n",__func__, __LINE__, hashval);
        }
        if (keyhash->node) {
            logmsg(LOGMSG_ERROR, "%s:%d keyhash %"PRIu64" has a node associated with it. Not adding new node\n", __func__, __LINE__, hashval);
            return CH_ERR_DUP;
        }
    } else {        
        /* Hashval doesn't exist. Add a new one */
        keyhash = (ch_keyhash_t *)malloc(sizeof(ch_keyhash_t));
        if (!keyhash) {
            logmsg(LOGMSG_ERROR, "%s:%d malloc error\n", __func__, __LINE__);
            goto cleanup;
        }
        keyhash->hash_val = hashval;
        ch->num_keyhashes++;
        temp = (ch_keyhash_t **)realloc(ch->key_hashes, sizeof(ch_keyhash_t *) * ch->num_keyhashes);
        if (!temp) {
            logmsg(LOGMSG_ERROR, ":%s:%d realloc failed!\n", __func__, __LINE__);
            goto cleanup;
        }
        temp[ch->num_keyhashes-1] = keyhash;
        ch->key_hashes = temp;
        temp = NULL;
    }
    if (debug_ch) {
        logmsg(LOGMSG_USER, "Adding node to hashval %"PRIu64"\n", hashval);
    }
    keyhash->node = node;
    if (ch->num_keyhashes > 1) {
        qsort((void *)ch->key_hashes, ch->num_keyhashes, sizeof(ch_keyhash_t *), keyhash_cmp);
    }
    return CH_NOERR;

cleanup:
    if (keyhash) {
        free(keyhash);
    }

    if (temp) {
        free(temp);
    }

    return CH_ERR_MALLOC;
}

static ch_hash_node_t *ch_hash_lookup_node(ch_hash_t *ch, uint8_t *bytes, size_t len) {
    ch_hash_node_t *node = NULL;
    LISTC_FOR_EACH(&ch->nodes, node, lnk) {
        if (memcmp(bytes, node->data, len) == 0) {
            return node;
        }
    }
    return NULL;
}
static ch_keyhash_t* ch_keyhash_upper_bound(ch_hash_t *ch, uint64_t hash) {
    if (ch==NULL) {
        if (ch==NULL) {
            logmsg(LOGMSG_ERROR, "CH is NULL\n");
        }
        return NULL;
    }
    if (debug_ch) {
        logmsg(LOGMSG_USER, "Looking for hash %"PRIu64"\n",hash);
    }
    int l=0, mid=0, r=ch->num_keyhashes-1, ans=0;
    while (l<=r) {
        if (debug_ch) {
            logmsg(LOGMSG_USER, "l: %d, mid: %d, r: %d\n",l, mid, r);
        }
        mid = l + (r-l) / 2;
        if (ch->key_hashes[mid]->hash_val <= hash) {
            l = mid + 1;
        } else {
            ans = mid;
            r = mid-1;
        }
    }
    if (debug_ch) {
        logmsg(LOGMSG_USER, "Found the hash val at index %d\n", ans);
    }
    return ch->key_hashes[ans];
}
static ch_keyhash_t* ch_keyhash_lower_bound(ch_hash_t *ch, uint64_t hash) {
    if (ch==NULL) {
        if (ch==NULL) {
            logmsg(LOGMSG_ERROR, "CH is NULL\n");
        }
        return NULL;
    }

    if (debug_ch) {
        logmsg(LOGMSG_USER, "Looking for hash %"PRIu64"\n",hash);
    }
    int l=0, mid=0, r=ch->num_keyhashes-1, ans=0;
    while (l<=r) {
        if (debug_ch) {
            logmsg(LOGMSG_USER, "l: %d, mid: %d, r: %d\n",l, mid, r);
        }
        mid = l + (r-l)/ 2;
        if (ch->key_hashes[mid]->hash_val >= hash) {
            r = mid - 1;
        } else {
            ans = mid;
            l = mid + 1;
        }
    }

    if (debug_ch) {
        logmsg(LOGMSG_USER, "Found the hash val at index %d\n", ans);
    }
    return ch->key_hashes[ans];
}

static int ch_hash_add_node_locked(ch_hash_t *ch, uint8_t *data, size_t data_len, uint64_t hashval) {
    int rc = 0;

    ch_hash_node_t *node = NULL;
    node = ch_hash_lookup_node(ch, data, data_len);
    if (node!=NULL) {
        logmsg(LOGMSG_ERROR, "%s:%d Node already exists. Try adding a replica instead.\n",__func__, __LINE__);
        return CH_ERR_HASH;
    }
    node = (ch_hash_node_t *)malloc(sizeof(ch_hash_node_t));
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

    rc = ch_hash_add_node_to_hashval_locked(ch, node, hashval);

    if (rc) {
        if (rc==CH_ERR_DUP) {
            return CH_NOERR;
        }
        ch_hash_remove_node_locked(ch, node->data, node->data_len);
        return rc;
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
    uint32_t hash_key = 0;
    ch_keyhash_t *next_biggest = NULL;

    if (ch->num_keyhashes == 0) {
        logmsg(LOGMSG_ERROR, "Hash is empty!\n");
        return NULL;
    }
    rc = ch->func(key, key_len, &hash_key);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s:%d hash failure. rc: %d\n", __func__, __LINE__, rc);
        return NULL;
    }

    next_biggest = ch_keyhash_upper_bound(ch, hash_key);
    assert(next_biggest!=NULL);
    return next_biggest->node;
}


/*
 * Input : num_nodes -> number of nodes (ex: database servers)
 *         func      -> 32 bit hash func 
 * Output: object of type ch_hash_t with hash value ranges assigned
 *         for each node 
 */
ch_hash_t *ch_hash_create(uint64_t num_nodes, hash_func func) {
    uint64_t hash_range_increment = 0;
    if (num_nodes < 1) {
        logmsg(LOGMSG_ERROR, "num_nodes should atleast be 1\n");
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

    ch->num_nodes = num_nodes;
    hash_range_increment = HASH_VAL_COUNT / num_nodes;

    if (debug_ch) {
        logmsg(LOGMSG_USER, "the hash_range_increment is %"PRIu64"\n", hash_range_increment);
    }
    listc_init(&ch->nodes, offsetof(struct consistent_hash_node, lnk)); 
    /* 
     * Create num_nodes hash ranges, one for each node in the consistent hash
     */ 
    ch->num_keyhashes = num_nodes;
    ch->key_hashes = (ch_keyhash_t**)malloc(sizeof(ch_keyhash_t*) * ch->num_keyhashes);
    if (!ch->key_hashes) {
        logmsg(LOGMSG_ERROR, "%s:%d malloc error\n", __func__, __LINE__);
        goto cleanup;
    }
    int64_t curHashVal = 0;
    for(int i=0;i<ch->num_keyhashes;i++){
        ch->key_hashes[i] = (ch_keyhash_t *)malloc(sizeof(ch_keyhash_t));
        if (!ch->key_hashes[i]) {
            logmsg(LOGMSG_ERROR,"%s:%d malloc error\n", __func__, __LINE__);
            goto cleanup;
        }
        ch->key_hashes[i]->node = NULL;
        ch->key_hashes[i]->hash_val = curHashVal;
        curHashVal += hash_range_increment;

        if (debug_ch) {
            logmsg(LOGMSG_USER, "assigning hash val %"PRIu64"\n",ch->key_hashes[i]->hash_val);
        }
    }
    ch->func = func;
    Pthread_rwlock_init(&(ch->lock), NULL);
    return ch;

cleanup:
    if (ch) {
        if (ch->key_hashes) {
            for(int i=0;i<ch->num_keyhashes;i++) {
                if (ch->key_hashes[i]) {
                    free(ch->key_hashes[i]);
                    ch->key_hashes[i] = NULL;
                }
            }
            free(ch->key_hashes);
        }
        free(ch);
    }
    return NULL;
}

/*
 * Add a replica on the hash ring for an existing node. 
 * If node doesn't exists return error.
 */
int ch_hash_add_replica(ch_hash_t *hash, uint8_t *data, size_t data_len, uint64_t hashval) {
    int rc = 0;

    if (hashval < 0 || hashval > HASH_VAL_COUNT) {
        logmsg(LOGMSG_ERROR, "Cannot add node at %"PRIu64". Invalid hash value\n", hashval);
        return -1;
    }

    rc = validate_input(hash, data, data_len);
    Pthread_rwlock_wrlock(&hash->lock);
    ch_hash_node_t *node = ch_hash_lookup_node(hash, data, data_len);
    if (node==NULL) {
        logmsg(LOGMSG_ERROR, "%s:%d Node not found. Cannot add replica for non-existing node\n",__func__, __LINE__);
        Pthread_rwlock_unlock(&hash->lock);
        return CH_ERR_HASH;
    }

    rc = ch_hash_add_node_to_hashval_locked(hash, node, hashval);
    if (rc) {
        Pthread_rwlock_unlock(&hash->lock);
        logmsg(LOGMSG_ERROR, "%s:%d failed to add replica\n", __func__, __LINE__);
        return rc;
    }
    Pthread_rwlock_unlock(&hash->lock);
    return CH_NOERR;
}

int ch_hash_add_node(ch_hash_t *ch, uint8_t *data, size_t data_len, uint64_t hashval) {
    int rc = 0;

    if (hashval < 0 || hashval > HASH_VAL_COUNT) {
        logmsg(LOGMSG_ERROR, "Cannot add node at %"PRIu64". Invalid hash value\n", hashval);
        return -1;
    }
    rc = validate_input(ch, data, data_len);

    if (rc) {
        return rc;
    }
    Pthread_rwlock_wrlock(&ch->lock);
    rc = ch_hash_add_node_locked(ch, data, data_len, hashval);
    Pthread_rwlock_unlock(&ch->lock);
    return rc;
}

ch_hash_node_t *ch_hash_find_node(ch_hash_t *ch, uint8_t *data, size_t data_len) {
    int rc = 0;
    ch_hash_node_t *node = NULL;
    rc = validate_input(ch, data, data_len);

    if (rc) {
        return node;
    }
    Pthread_rwlock_rdlock(&ch->lock);
    node = ch_hash_find_node_locked(ch, data, data_len);
    Pthread_rwlock_unlock(&ch->lock);
    return node;
}

int ch_hash_remove_node(ch_hash_t *ch, uint8_t *data, size_t data_len) {
    int rc = 0;
    rc = validate_input(ch, data, data_len);

    if (rc) {
        return rc;
    }
    Pthread_rwlock_wrlock(&ch->lock);
    rc = ch_hash_remove_node_locked(ch, data, data_len);
    Pthread_rwlock_unlock(&ch->lock);
    return rc;
}

void ch_hash_free(ch_hash_t *ch) {
    struct consistent_hash_node *item, *tmp;
    if (ch) {
        Pthread_rwlock_wrlock(&ch->lock);
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
        Pthread_rwlock_unlock(&ch->lock);
        Pthread_rwlock_destroy(&ch->lock);
        free(ch);
    }
}
