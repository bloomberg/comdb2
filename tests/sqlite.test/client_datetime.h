
/* datetime type definition */
typedef struct client_datetime {
        struct tm       tm;
        unsigned int    msec;
        char            tzname[36];
} client_datetime_t;


