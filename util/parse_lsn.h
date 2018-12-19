#ifndef PARSE_LSN_H
#define PARSE_LSN_H

int char_to_lsn(const char *lsnstr, unsigned int *file, unsigned int *offset);

#endif
