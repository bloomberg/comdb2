#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdint.h>
#include "consistent_hash.h"
#include <string.h>

void test_add_and_find_one_node(hash_func func, int func_type) {
    ch_hash_t *ch = ch_hash_create(1, func);
    if (!ch) {
        goto fail;
    }

    if (ch_hash_add_node(ch, (uint8_t *)"SHARD1", 6, ch->key_hashes[0]->hash_val)) {
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

void test_add_and_find_multiple_nodes(hash_func func, int func_type) {
    ch_hash_t *ch = ch_hash_create(1, func);
    if (!ch) {
        goto fail;
    }


    if (ch_hash_add_node(ch, (uint8_t *)"SHARD1", 6, ch->key_hashes[0]->hash_val)) {
        goto fail;
    }

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
    if (ch_hash_add_node(ch, (uint8_t *)"SHARD2", 6, 2863311529)) {
        goto fail;
    }

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
            if (memcmp("SHARD2", get_node_data(node), 6) != 0) {
                goto fail;
            }
            break;
        default:
            goto fail;

    }

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
            if (memcmp("SHARD2", get_node_data(node), 6) != 0) {
                goto fail;
            }
            break;
        default:
            goto fail;

    }

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
            if (memcmp("SHARD2", get_node_data(node), 6) != 0) {
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

void test_replica(hash_func func, int func_type) {
    ch_hash_t *ch = ch_hash_create(2, func);
    if (!ch) {
        goto fail;
    }

    if (ch_hash_add_node(ch, (uint8_t *)"SHARD1", 6, ch->key_hashes[0]->hash_val)) {
        goto fail;
    }
    if (ch_hash_add_node(ch, (uint8_t *)"SHARD2", 6, ch->key_hashes[1]->hash_val)) {
        goto fail;
    }

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
            if (memcmp("SHARD2", get_node_data(node), 6) != 0) {
                goto fail;
            }
            break;
        default:
            goto fail;

    }

    /*
     * Add a replica between the two ranges 
     */
    switch (func_type) {
        case CH_HASH_SHA:
            ch_hash_add_replica(ch, (uint8_t *)"SHARD2", 6, 4180816000);
            break;
        case CH_HASH_MD5:
            ch_hash_add_replica(ch, (uint8_t *)"SHARD1", 6, 49952000);
            break;
        default:
            goto fail;
    }

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
            if (memcmp("SHARD1", get_node_data(node), 6) != 0) {
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


void test_remove_node(hash_func func, int func_type) {
    ch_hash_t *ch = ch_hash_create(2, func);
    if (!ch) {
        goto fail;
    }

    if (ch_hash_add_node(ch, (uint8_t *)"SHARD1", 6, ch->key_hashes[0]->hash_val)) {
        goto fail;
    }
    if (ch_hash_add_node(ch, (uint8_t *)"SHARD2", 6, ch->key_hashes[1]->hash_val)) {
        goto fail;
    }

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
            if (memcmp("SHARD2", get_node_data(node), 6) != 0) {
                goto fail;
            }
            break;
        default:
            goto fail;

    }

    switch (func_type) {
        case CH_HASH_SHA:
            if (ch_hash_remove_node(ch, (uint8_t *)"SHARD1", 6)) {
                goto fail;
            }
            break;
        case CH_HASH_MD5:
            if (ch_hash_remove_node(ch, (uint8_t *)"SHARD2", 6)) {
                goto fail;
            }
            break;
        default:
            goto fail;
    }

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
            if (memcmp("SHARD1", get_node_data(node), 6) != 0) {
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


void test_find_with_zero_nodes(hash_func func, int func_type){
    ch_hash_t *ch = ch_hash_create(0, func);
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

void print_distribution(int num_nodes, hash_func func) {
    ch_hash_t *ch = ch_hash_create(num_nodes,func);
    unsigned char bytes[4];
    int shard1=0, shard2=0, shard3=0;
    if (!ch) {
        goto fail;
    }
    if (ch_hash_add_node(ch, (uint8_t *)"SHARD1", 6, ch->key_hashes[0]->hash_val)) {
        goto fail;
    }
    if (ch_hash_add_node(ch, (uint8_t *)"SHARD2", 6, ch->key_hashes[1]->hash_val)) {
        goto fail;
    }
    if (ch_hash_add_node(ch, (uint8_t *)"SHARD3", 6, ch->key_hashes[2]->hash_val)) {
        goto fail;
    }

    for (int i=1;i<=2000000;i++) {
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

void run_tests(hash_func func, int func_type) {
    test_add_and_find_one_node(func, func_type);
    test_add_and_find_multiple_nodes(func, func_type);
    test_remove_node(func, func_type);
    test_replica(func, func_type);
}

int main() 
{
    run_tests(ch_hash_sha, CH_HASH_SHA); 
    run_tests(ch_hash_md5, CH_HASH_MD5);

    int num_nodes = 3;
    print_distribution(num_nodes, ch_hash_sha);
    print_distribution(num_nodes, ch_hash_md5);
    printf("SUCCESS\n");
    return 0;
}
