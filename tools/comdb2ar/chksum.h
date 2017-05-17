#ifndef INCLUDED_CHKSUM_H
#define INCLUDED_CHKSUM_H
#include <inttypes.h>
extern "C" {
uint32_t crc32c(const uint8_t* buf, uint32_t sz);
uint32_t __ham_func4(const uint8_t *key, uint32_t len);
}

#endif
