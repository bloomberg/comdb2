#ifndef INCLUDED_SYSTABLE_H
#define INCLUDED_SYSTABLE_H

enum {
    SYSTABLE_END_OF_FIELDS = -1
};

enum {
    SYSTABLE_FIELD_NULLABLE  = 0x1000,
};

/* All other types have enough information to determine size.  Blobs need a little help. */
typedef struct { 
    void *value;
    size_t size;
} systable_blobtype;

int create_system_table(sqlite3 *db, char *name, 
        int(*init_callback)(void **data, int *npoints), 
        void(*release_callback)(void *data, int npoints), 
        size_t struct_size, 
        // type, name, offset,  type2, name2, offset2, ..., SYSTABLE_END_OF_FIELDS
        ...);

#endif
