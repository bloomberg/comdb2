#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdint.h>
#include "consistent_hash.h"
#include <string.h>
#include <sqlite3.h>
#include <getopt.h>
#include <time.h>

ch_hash_t *ch = NULL;

int print_query_result(void *a_param, int argc, char *argv[], char **column) {
    for(int i=0;i<argc;i++){
        printf("%s, \t", argv[i]);
    }
    printf("\n");
    return 0;
}

int get_query_result(void *a_param, int argc, char *argv[], char **column) {
    int *distribution = (int *)a_param;
    char *ptr;
    for(int i=0;i<argc;i++){
        *distribution = strtol(argv[i],&ptr, 10);
    }
    return 0;
}

void insert_record(const char *table, int number, sqlite3 *db) {
    if (!table) return;
    char *err_msg = NULL;
    char *sql = sqlite3_mprintf("INSERT INTO %s VALUES(%d)", table, number);
    if (sqlite3_exec(db, sql, 0, 0, &err_msg)) {
        printf("Failed inserting %d into table %s. Error: %s\n", number, table, err_msg);
        sqlite3_free(err_msg);
        sqlite3_free(sql);
        return;
    }
    sqlite3_free(sql);
    return;
}

void print_distribution(sqlite3 *db, int num_shards) {
    for(int i=1; i<=num_shards;i++) {
        char *table = sqlite3_mprintf("SHARD%d", i);
        char *err_msg = NULL;
        char *sql = sqlite3_mprintf("SELECT COUNT(*) FROM %s", table);
        if (sqlite3_exec(db, sql, print_query_result, 0, &err_msg)) {
            printf("Failed to fetch number of records from %s. Error: %s\n", table, err_msg);
            sqlite3_free(err_msg);
            sqlite3_free(sql);
            return;
        }
        sqlite3_free(sql);
    }
        return;
}

int check_distribution(sqlite3 *db, int num_shards, int num_records) {
    int *distribution = (int *)malloc(sizeof(int) * (num_shards+1));
    for(int i=1; i<=num_shards;i++) {
        char *table = sqlite3_mprintf("SHARD%d", i);
        char *err_msg = NULL;
        char *sql = sqlite3_mprintf("SELECT COUNT(*) FROM %s", table);
        if (sqlite3_exec(db, sql, get_query_result, &distribution[i], &err_msg)) {
            printf("Failed to fetch number of records from %s. Error: %s\n", table, err_msg);
            sqlite3_free(err_msg);
            sqlite3_free(sql);
            continue;
        }
        sqlite3_free(sql);
    }

    int ceiling = (num_records) / (num_shards-1);
    int floor = (num_records) / (num_shards+1);

    for (int i=1;i<=num_shards;i++) {
            printf("SHARD%d has %d records. ceiling: %d, floor: %d\n", i, distribution[i], ceiling, floor);
        if (distribution[i] > ceiling || distribution[i] < floor) {
            printf("FAILED\n");
            goto fail;
        }
    }

    free(distribution);
    return 0;
fail:
    free(distribution);
    return -1;
}

int redistribute_record(void *a_param, int argc, char *argv[], char **column) {
   // unsigned char bytes[4];
   sqlite3 *db = (sqlite3 *)a_param;
   long temp_int = 0;
   char *ptr;
    for(int j=0;j<argc;j++){
        temp_int = strtol(argv[j],&ptr, 10);
        if (ptr == argv[j]) {
            printf("Failed to convert char* to int \n");
            return -1;
        }
        ch_hash_node_t *node = ch_hash_find_node(ch, (uint8_t *)argv[j], sizeof(argv[j]));
        if (node==NULL) {
            return -1;
        }
        insert_record((char *)get_node_data(node), temp_int, db);
    }
    //printf("\n");
    return 0;
}
void drop_shard(sqlite3 *db, int shard_number) {
    char *table = sqlite3_mprintf("SHARD%d", shard_number);
    char *err_msg = NULL;
    if (ch_hash_remove_node(ch, (uint8_t *)table, sizeof(table))) {
        printf("Failed to remove node %s from the consistent hash\n", table);
        return;
    }
    char *sql = sqlite3_mprintf("SELECT * FROM %s", table);
    if (sqlite3_exec(db, sql, redistribute_record, db, &err_msg)) {
        printf("Failed to fetch number of records from %s. Error: %s\n", table, err_msg);
        sqlite3_free(err_msg);
        sqlite3_free(sql);
        return;
    }

    sqlite3_free(sql);
    sql = sqlite3_mprintf("DROP TABLE %s", table);
    if (sqlite3_exec(db, sql, 0, 0, &err_msg)) {
        printf("Failed deleting records from dropped shard %s. Error: %s\n", table, err_msg);
        sqlite3_free(err_msg);
        sqlite3_free(sql);
        return;
    }
    sqlite3_free(sql);
    return;
}

int create_consistent_hash(int numshards, hash_func func) {
    ch = ch_hash_create(numshards, func);
    if (!ch) {
        printf("Malloc error while create hash \n");
        return -1;
    }
    for(int i=1; i<=numshards; i++) {
        char *shard = sqlite3_mprintf("SHARD%d", i);
        if (ch_hash_add_node(ch, (uint8_t *)shard, sizeof(shard), ch->key_hashes[i-1]->hash_val)) {
            printf("Failed to add node for table %s\n", shard);
        }
        sqlite3_free(shard);
    }
    return 0;
}


void populate_records(int limit, sqlite3 *db) {
    unsigned char bytes[4];

    for(int i=0;i<limit;i++) {
        bytes[0] = (i>>24) & 0xFF;
        bytes[1] = (i>>16) & 0xFF;
        bytes[2] = (i>>8) & 0xFF;
        bytes[3] = (i) & 0xFF;
        ch_hash_node_t *node = ch_hash_find_node(ch, (uint8_t *)bytes, sizeof(int));
        if (node==NULL) {
            return;
        }
        insert_record((char *)get_node_data(node), i, db);
    }
}
void usage(const char *p, const char *err) {
    fprintf(stderr, "%s\n", err);
    fprintf(stderr, "Usage %s --numshards NUMSHARDS --numrecords NUMRECORDS\n", p);
    exit(1);
}

int main(int argc, char *argv[]) {
    if (argc < 3) {
        usage(argv[0], "Required parameters were NOT provided");
    }
    int numshards = 0, numrecords = 0;
    clock_t start, end;
    static struct option long_options[] =
    {
        {"numshards", required_argument, NULL, 's'},
        {"numrecords", required_argument, NULL, 'r'},
        {NULL, 0, NULL, 0}
    };
    int c;
    int index;
    while ((c = getopt_long(argc, argv, "s:r:?", long_options, &index)) != -1) {
        //printf("c '%c' %d index %d optarg '%s'\n", c, c, index, optarg);
        switch(c) {
            case 's': numshards = atoi(optarg); break;
            case 'r': numrecords = atoi(optarg); break;
            case '?':  break;
            default: break;
        }
    }

    printf("numshards: %d, numrecords: %d\n", numshards, numrecords);

    sqlite3 *db;
    char *err_msg = NULL;
    int rc = 0;
    if (create_consistent_hash(numshards, ch_hash_sha)) {
        printf("Failed to create consistent hash\n");
        return -1;
    }
    rc = sqlite3_open("test.db", &db);

    if (rc != SQLITE_OK) {
        printf("Cannot open database : %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);

        return -1;
    }
    printf("Successfully created a database \n");

    char *sql = "", *temp = NULL;
    for(int i=1;i<=numshards;i++) {
        temp = sqlite3_mprintf("%s\nCREATE TABLE SHARD%d(id INT);", sql, i); 
        sql = temp;
    }
    /*char *sql = "CREATE TABLE SHARD1(id INT);"
                "CREATE TABLE SHARD2(id INT);"
                "CREATE TABLE SHARD3(id INT);";*/

    char *tune_sqlite = "PRAGMA journal_mode = OFF;"
                        "PRAGMA synchronous = 0;"
                        "PRAGMA cache_size = 1000000;"
                        "PRAGMA locking_mode = EXCLUSIVE;"
                        "PRAGMA temp_store = MEMORY;";
    rc = sqlite3_exec(db, tune_sqlite, 0, 0, &err_msg);
    if (rc != SQLITE_OK) {
        printf("SQL ERROR: %s\n", err_msg);
        sqlite3_free(err_msg);
        sqlite3_close(db);

        return -1;
    }
    rc = sqlite3_exec(db, sql, 0, 0, &err_msg);
    if (rc != SQLITE_OK) {
        printf("SQL ERROR: %s\n", err_msg);
        sqlite3_free(err_msg);
        sqlite3_close(db);

        return -1;
    }

    printf("Successfully created tables\n");
    start = clock();
    populate_records(numrecords, db);
    end = clock();
    if(check_distribution(db, numshards, numrecords)) {
        return -1;
    }
    printf("It took %f seconds to insert %d records into %d shards.\n", ((double)(end-start))/CLOCKS_PER_SEC, numrecords, numshards);
    /*printf("Dropping SHARD5\n");
    drop_shard(db, 5);
    if (check_distribution(db, numshards-1, numrecords)) {
        return -1;
    }*/

    sqlite3_close(db);
    return 0;
}
