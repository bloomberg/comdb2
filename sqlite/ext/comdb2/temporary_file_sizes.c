#include <stdlib.h>
#include <string.h>
#include <stddef.h>

#include "comdb2.h"
#include "comdb2systblInt.h"
#include "sql.h"
#include "ezsystables.h"
#include "types.h"

#include "bdb_api.h"

struct temp_file_size {
    char *type;
    uint64_t bytes;
};

static int get_rows(void **data, int *num_points) {
    *num_points = 4;
    struct temp_file_size *rows = malloc(sizeof(struct temp_file_size) * *num_points);
    if (rows == NULL)
        return ENOMEM;
    rows[0].type = "temptables";
    rows[1].type = "sqlsorters";
    rows[2].type = "blkseqs";
    rows[3].type = "others";
    (void)bdb_tmp_size(thedb->bdb_env, &rows[0].bytes, &rows[1].bytes,
            &rows[2].bytes, &rows[3].bytes);
    *data = rows;
    return 0;
}

static void free_rows(void *data, int num_points) {
    free(data);
}

sqlite3_module systblTemporaryFileSizesModule = {
    .access_flag = CDB2_ALLOW_USER,
};

int systblTemporaryFileSizesModuleInit(sqlite3 *db) {
    return create_system_table(db, "comdb2_temporary_file_sizes",
            &systblTemporaryFileSizesModule,
            get_rows, free_rows, sizeof(struct temp_file_size),
            CDB2_CSTRING, "type",  -1, 0,
            CDB2_INTEGER, "bytes",  -1, offsetof(struct temp_file_size, bytes),
            SYSTABLE_END_OF_FIELDS);
}
