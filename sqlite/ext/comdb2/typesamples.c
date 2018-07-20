#include <stdlib.h>
#include <string.h>
#include <stddef.h>

#include "comdb2.h"
#include "comdb2systblInt.h"
#include "sql.h"
#include "ezsystables.h"
#include "cdb2api.h"

struct typesamples {
    int64_t                  integer;        // CDB2_INTEGER
    double                   real;           // CDB2_REAL
    char*                    cstring;        // CDB2_CSTRING
    systable_blobtype        blob;           // CDB2_BLOB    
    cdb2_client_datetime_t   datetime;       // CDB2_DATETIME
    cdb2_client_intv_ym_t    intervalym;     // CDB2_INTERVALYM
    cdb2_client_intv_ds_t    intervalds;     // CDB2_INTERVALDS
    cdb2_client_datetimeus_t datetimeus;     // CDB2_DATETIMEUS
    cdb2_client_intv_dsus_t  intervaldsus;   // CDB2_INTERVALDSUS
};

int get_type_samples(void **data, int *npoints) {
    *npoints = 1;
    struct typesamples *types = malloc(sizeof(struct typesamples));
    types->integer = 12;
    types->real = 3.14;
    types->cstring = "hello world";
    types->blob = (systable_blobtype) { .value = "!!!!", .size = 4 };
    types->datetime = (cdb2_client_datetime_t) {
        .tm = (cdb2_tm_t) {
            .tm_year = 118,
            .tm_mon = 2,
            .tm_mday = 7,
            .tm_hour = 8,
            .tm_min = 30,
            .tm_sec = 0
        },
        .msec = 0,
        .tzname = "America/New_York"
    };

    types->datetimeus = (cdb2_client_datetimeus_t) {
        .tm = (cdb2_tm_t) {
            .tm_year = 118,
            .tm_mon = 2,
            .tm_mday = 7,
            .tm_hour = 8,
            .tm_min = 30,
            .tm_sec = 0
        },
        .usec = 0,
        .tzname = "America/New_York"
    };

    types->intervalym = (cdb2_client_intv_ym_t) {
        .sign = 1,
        .years = 2,
        .months = 3
    };

    types->intervalds = (cdb2_client_intv_ds_t) {
        .sign = 1,
        .days = 1,
        .hours = 2,
        .mins = 3,
        .sec = 4,
        .msec = 5
    };

    types->intervaldsus = (cdb2_client_intv_dsus_t) {
        .sign = 1,
        .days = 1,
        .hours = 2,
        .mins = 3,
        .sec = 4,
        .usec = 5
    };

    *data = types;
    return 0;
}

void free_type_samples(void *p, int n) {
    free(p);
}

int systblTypeSamplesInit(sqlite3 *db) {
    return create_system_table(db, "comdb2_type_samples", get_type_samples, free_type_samples, sizeof(struct typesamples),
            CDB2_INTEGER, "integer", -1, offsetof(struct typesamples, integer),
            CDB2_REAL, "real", -1, offsetof(struct typesamples, real),
            CDB2_CSTRING, "cstring", -1, offsetof(struct typesamples, cstring),
            CDB2_BLOB    , "blob", -1, offsetof(struct typesamples, blob),
            CDB2_DATETIME, "datetime", -1, offsetof(struct typesamples, datetime),
            CDB2_INTERVALYM, "intervalym", -1, offsetof(struct typesamples, intervalym),
            CDB2_INTERVALDS, "intervalds", -1, offsetof(struct typesamples, intervalds),
            CDB2_DATETIMEUS, "datetimeus", -1, offsetof(struct typesamples, datetimeus),
            CDB2_INTERVALDSUS, "intervaldsus", -1, offsetof(struct typesamples, intervaldsus),
            SYSTABLE_END_OF_FIELDS);
}
