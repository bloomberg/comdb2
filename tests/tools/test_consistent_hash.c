#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdint.h>
#include "consistent_hash.h"
#include <string.h>
enum ch_hash_func_type {
    CH_HASH_SHA = 1,
    CH_HASH_MD5 = 2
};

void test_add_and_find_one_node(int num_copies, hash_func func, int func_type) {
    ch_hash_t *ch = ch_hash_create(num_copies, func);
    if (!ch) {
        goto fail;
    }

    if (ch_hash_add_node(ch, (uint8_t *)"SHARD1", 6)) {
        goto fail;
    }

    /* Use same key */
    ch_hash_node_t *node = ch_hash_find_node(ch, (uint8_t *)"KEY1", 4);
    if (node == NULL) {
        goto fail;
    }
    if (6 != get_node_data_len(node)) {
        goto fail;
    }
    if (memcmp("SHARD1", get_node_data(node), 6) != 0) {
        goto fail;
    }

    /* Use different key -> should still map to same node
     * since num_nodes = 1
     */

    node = ch_hash_find_node(ch, (uint8_t *)"KEY2", 4);
    if (node == NULL) {
        goto fail;
    }
    if (6 != get_node_data_len(node)) {
        goto fail;
    }
    if (memcmp("SHARD1", get_node_data(node), 6) != 0) {
        goto fail;
    }

    return;
fail:
    if (ch) {
        ch_hash_free(ch);
    }
    abort();
}

/* SHARD11 -> 896d0781f29e2c95cf9ec465ac57d53fe976f1aec3f67c6bef3a61bfbc69e041
 * SHARD21 -> b645386d2787f0f3a754fc23535e64456ea3851412e51a8a71845126c09441d0
 * KEY1    -> 69787306e75e856187fea67a8d459d6bf8e491ea885afc7e700ff247a6124d97
 * KEY2    -> fd38a7ca3f645c74225919f392142b93ae5a15b91d59b8d3b1fdb96cbdcd8d6b
 * KEY15   -> 2c2d32f9d047241ea3c77517f3477cd900238938d364239a6fcbf19e9d59c2da 
 */

void test_add_and_find_multiple_nodes(int num_copies, hash_func func, int func_type) {
    ch_hash_t *ch = ch_hash_create(num_copies, func);
    if (!ch) {
        goto fail;
    }


    if (ch_hash_add_node(ch, (uint8_t *)"SHARD1", 6)) {
        goto fail;
    }

    /* Find KEY1 -> EXPECT SHARD1 SINCE THERE'S ONLY ONE NODE*/
    ch_hash_node_t *node = ch_hash_find_node(ch, (uint8_t *)"KEY1", 4);
    if (node == NULL) {
        goto fail;
    }
    switch (func_type) {
        case CH_HASH_SHA: 
            if (6 != get_node_data_len(node)) {
                goto fail;
            }
            if (memcmp("SHARD1", get_node_data(node), 6) != 0) {
                goto fail;
            }
            break;
        case CH_HASH_MD5:
            if (6 != get_node_data_len(node)) {
                goto fail;
            }
            if (memcmp("SHARD1", get_node_data(node), 6) != 0) {
                goto fail;
            }
            break;
        default:
			goto fail;
    }

    /* Find KEY2 -> EXPECT SHARD1 SINCE THERE'S ONLY ONE NODE*/
    node = ch_hash_find_node(ch, (uint8_t *)"KEY2", 4);
    if (node == NULL) {
        goto fail;
    }
    switch (func_type) {
        case CH_HASH_SHA: 
            if (6 != get_node_data_len(node)) {
                goto fail;
            }
            if (memcmp("SHARD1", get_node_data(node), 6) != 0) {
                goto fail;
            }
            break;
        case CH_HASH_MD5:
            if (6 != get_node_data_len(node)) {
                goto fail;
            }
            if (memcmp("SHARD1", get_node_data(node), 6) != 0) {
                goto fail;
            }
            break;
        default:
            goto fail;

    }

    /* Find KEY15 -> EXPECT SHARD1 SINCE THERE'S ONLY ONE NODE*/
    node = ch_hash_find_node(ch, (uint8_t *)"KEY15", 5);
    if (node == NULL) {
        goto fail;
    }
    switch (func_type) {
        case CH_HASH_SHA: 
            if (6 != get_node_data_len(node)) {
                goto fail;
            }
            if (memcmp("SHARD1", get_node_data(node), 6) != 0) {
                goto fail;
            }
            break;
        case CH_HASH_MD5:
            if (6 != get_node_data_len(node)) {
                goto fail;
            }
            if (memcmp("SHARD1", get_node_data(node), 6) != 0) {
                goto fail;
            }
            break;
        default:
            goto fail;

    }

    /* Now add another node */
    if (ch_hash_add_node(ch, (uint8_t *)"SHARD2", 6)) {
        goto fail;
    }

    /* Find KEY1 -> EXPECT SHARD2 NOW*/
    node = ch_hash_find_node(ch, (uint8_t *)"KEY1", 4);
    if (node == NULL) {
        goto fail;
    }
    switch (func_type) {
        case CH_HASH_SHA: 
            if (6 != get_node_data_len(node)) {
                goto fail;
            }
            if (memcmp("SHARD2", get_node_data(node), 6) != 0) {
                goto fail;
            }
            break;
        case CH_HASH_MD5:
            if (6 != get_node_data_len(node)) {
                goto fail;
            }
            if (memcmp("SHARD1", get_node_data(node), 6) != 0) {
                goto fail;
            }
            break;
        default:
            goto fail;

    }

    /* Find KEY2 -> EXPECT SHARD2 NOW*/
    node = ch_hash_find_node(ch, (uint8_t *)"KEY2", 4);
    if (node == NULL) {
        goto fail;
    }
    switch (func_type) {
        case CH_HASH_SHA: 
            if (6 != get_node_data_len(node)) {
                goto fail;
            }
            if (memcmp("SHARD2", get_node_data(node), 6) != 0) {
                goto fail;
            }
            break;
        case CH_HASH_MD5:
            if (6 != get_node_data_len(node)) {
                goto fail;
            }
            if ((num_copies==1 && (memcmp("SHARD2", get_node_data(node), 6) != 0)) || 
                (num_copies==2 && (memcmp("SHARD1", get_node_data(node), 6) != 0))) {
                goto fail;
            }
            break;
        default:
            goto fail;

    }

    /* Find KEY15 -> STILL EXPECT SHARD1 NOW*/
    node = ch_hash_find_node(ch, (uint8_t *)"KEY15", 5);
    if (node == NULL) {
        goto fail;
    }
    switch (func_type) {
        case CH_HASH_SHA: 
            if (6 != get_node_data_len(node)) {
                goto fail;
            }
            if (memcmp("SHARD1", get_node_data(node), 6) != 0) {
                goto fail;
            }
            break;
        case CH_HASH_MD5:
            if (6 != get_node_data_len(node)) {
                goto fail;
            }
            if (memcmp("SHARD1", get_node_data(node), 6) != 0) {
                goto fail;
            }
            break;
        default:
            goto fail;

    }
    return ;
fail:
    if (ch) {
        ch_hash_free(ch);
    }
    abort();
}

void test_remove_node(int num_copies, hash_func func, int func_type) {
    ch_hash_t *ch = ch_hash_create(num_copies, func);
    if (!ch) {
        goto fail;
    }

    if (ch_hash_add_node(ch, (uint8_t *)"SHARD1", 6)) {
        goto fail;
    }
    if (ch_hash_add_node(ch, (uint8_t *)"SHARD2", 6)) {
        goto fail;
    }
    /* Find KEY15 -> STILL EXPECT SHARD1 NOW*/
    ch_hash_node_t *node = ch_hash_find_node(ch, (uint8_t *)"KEY15", 5);
    if (node == NULL) {
        goto fail;
    }
    switch (func_type) {
        case CH_HASH_SHA: 
            if (6 != get_node_data_len(node)) {
                goto fail;
            }
            if (memcmp("SHARD1", get_node_data(node), 6) != 0) {
                goto fail;
            }
            break;
        case CH_HASH_MD5:
            if (6 != get_node_data_len(node)) {
                goto fail;
            }
            if (memcmp("SHARD1", get_node_data(node), 6) != 0) {
                goto fail;
            }
            break;
        default:
            goto fail;

    }

    if (ch_hash_remove_node(ch, (uint8_t *)"SHARD1", 6)) {
        goto fail;
    }

    /* Find KEY15 -> NOW EXPECT SHARD2 NOW*/
    node = ch_hash_find_node(ch, (uint8_t *)"KEY15", 5);
    if (node == NULL) {
        goto fail;
    }
    switch (func_type) {
        case CH_HASH_SHA: 
            if (6 != get_node_data_len(node)) {
                goto fail;
            }
            if (memcmp("SHARD2", get_node_data(node), 6) != 0) {
                goto fail;
            }
            break;
        case CH_HASH_MD5:
            if (6 != get_node_data_len(node)) {
                goto fail;
            }
            if (memcmp("SHARD2", get_node_data(node), 6) != 0) {
                goto fail;
            }
            break;
        default:
            goto fail;

    }
    return;
fail:
    if (ch) {
        ch_hash_free(ch);
    }
    abort();
}


void test_find_with_zero_nodes(int num_copies, hash_func func, int func_type){
    ch_hash_t *ch = ch_hash_create(num_copies, func);
    if (!ch) {
        goto fail;
    }
    ch_hash_node_t *node = ch_hash_find_node(ch, (uint8_t *)"KEY15", 5);
    if (node != NULL) {
        goto fail;
    }
    return;
fail:
    if (ch) {
        ch_hash_free(ch);
    }
    abort();
}

void print_distribution(int num_copies, hash_func func) {
    ch_hash_t *ch = ch_hash_create(num_copies,func);
    unsigned char bytes[4];
    int shard1=0, shard2=0, shard3=0;
    if (!ch) {
        goto fail;
    }
    if (ch_hash_add_node(ch, (uint8_t *)"SHARD1", 6)) {
        goto fail;
    }
    if (ch_hash_add_node(ch, (uint8_t *)"SHARD2", 6)) {
        goto fail;
    }
    if (ch_hash_add_node(ch, (uint8_t *)"SHARD3", 6)) {
        goto fail;
    }

    for (int i=1;i<=1000000;i++) {
        bytes[0] = (i>>24) & 0xFF;
        bytes[1] = (i>>16) & 0xFF;
        bytes[2] = (i>>8) & 0xFF;
        bytes[3] = (i) & 0xFF;
        ch_hash_node_t *node = ch_hash_find_node(ch, (uint8_t *)bytes, sizeof(int));
        if (node==NULL) {
            goto fail;
        }

        if (memcmp("SHARD1", get_node_data(node), 6) == 0) {
            shard1++;
        } else if (memcmp("SHARD2", get_node_data(node), 6) == 0) {
            shard2++;
        } else if (memcmp("SHARD3", get_node_data(node), 6) == 0) {
            shard3++;
        }
    }

    printf("The key distribution is: \n");
    printf("Shard1 : %d\n", shard1);
    printf("Shard2 : %d\n", shard2);
    printf("Shard3 : %d\n", shard3);
    return;
fail:
    if (ch) {
        ch_hash_free(ch);
    }
    abort();
}

void run_tests(int num_copies, hash_func func, int func_type) {
    test_find_with_zero_nodes(num_copies, func, func_type);
    test_add_and_find_one_node(num_copies, func, func_type);
    test_add_and_find_multiple_nodes(num_copies, func, func_type);
    test_remove_node(num_copies, func, func_type);
}

int main() 
{
    run_tests(1, ch_hash_sha, CH_HASH_SHA); 
    run_tests(2, ch_hash_sha, CH_HASH_SHA);
    run_tests(1, ch_hash_md5, CH_HASH_MD5);
    run_tests(2, ch_hash_md5, CH_HASH_MD5);

    /*int num_copies = 512;
    print_distribution(num_copies, ch_hash_sha);
    print_distribution(num_copies, ch_hash_md5);*/
    printf("SUCCESS\n");
    return 0;
}
