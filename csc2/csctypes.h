
#ifndef INCLUDED_TYPES_H
#define INCLUDED_TYPES_H

/*
 * comdb2 data types - these should match the types in db/types.h
 */

#define CLIENT_TYPES                                                           \
XMACRO_CLIENT_TYPE(CLIENT_MINTYPE,     0, "min")                               \
XMACRO_CLIENT_TYPE(CLIENT_UINT,        1, "uint")                              \
XMACRO_CLIENT_TYPE(CLIENT_INT,         2, "int")                               \
XMACRO_CLIENT_TYPE(CLIENT_REAL,        3, "real")                              \
XMACRO_CLIENT_TYPE(CLIENT_CSTR,        4, "cstr")                              \
XMACRO_CLIENT_TYPE(CLIENT_PSTR,        5, "pstr")                              \
XMACRO_CLIENT_TYPE(CLIENT_BYTEARRAY,   6, "byte")                              \
XMACRO_CLIENT_TYPE(CLIENT_PSTR2,       7, "pstr2")                             \
XMACRO_CLIENT_TYPE(CLIENT_BLOB,        8, "blob")                              \
XMACRO_CLIENT_TYPE(CLIENT_DATETIME,    9, "datetime")                          \
XMACRO_CLIENT_TYPE(CLIENT_INTVYM,     10, "intervalym")                        \
XMACRO_CLIENT_TYPE(CLIENT_INTVDS,     11, "intervalds")                        \
XMACRO_CLIENT_TYPE(CLIENT_VUTF8,      12, "vutf8")                             \
XMACRO_CLIENT_TYPE(CLIENT_BLOB2,      13, "blob2")                             \
XMACRO_CLIENT_TYPE(CLIENT_DATETIMEUS, 14, "datetimeus")                        \
XMACRO_CLIENT_TYPE(CLIENT_INTVDSUS,   15, "intervaldsus")                      \
XMACRO_CLIENT_TYPE(CLIENT_MAXTYPE,    16, "max")

/* CLIENT side types */
#ifdef XMACRO_CLIENT_TYPE
#   undef XMACRO_CLIENT_TYPE
#endif
#define XMACRO_CLIENT_TYPE(a, b, c) a,
enum { CLIENT_TYPES };

#undef XMACRO_CLIENT_TYPE
#define XMACRO_CLIENT_TYPE(a, b, c) case a: client_type_to_str = c; break;
#define CLIENT_TYPE_TO_STR(type)                                               \
({                                                                             \
    char *client_type_to_str  = "unknown";                                     \
    switch (type) {                                                            \
    CLIENT_TYPES                                                               \
    }                                                                          \
    client_type_to_str;                                                        \
})

/* ONDISK types */
enum {
    SERVER_MINTYPE = 100,
    SERVER_UINT = 101,
    SERVER_BINT = 102,
    SERVER_BREAL = 103,
    SERVER_BCSTR = 104,
    SERVER_BYTEARRAY = 105,
    SERVER_BLOB = 106,
    SERVER_DATETIME = 107,
    SERVER_INTVYM = 108,
    SERVER_INTVDS = 109,
    SERVER_VUTF8 = 110,
    SERVER_DECIMAL = 111,
    SERVER_BLOB2 = 112,
    SERVER_DATETIMEUS = 113,
    SERVER_INTVDSUS = 114,
    SERVER_MAXTYPE
};

#endif
