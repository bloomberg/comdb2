#ifndef INCLUDED_SRC_MD5_H
#define INCLUDED_SRC_MD5_H

/* src/md5.c */

#ifndef uint32
#  define uint32 unsigned int
#endif

struct MD5Context {
  int isInit;
  uint32 buf[4];
  uint32 bits[2];
  unsigned char in[64];
};
typedef struct MD5Context MD5Context;

void MD5Init(MD5Context *ctx);
void MD5Update(MD5Context *ctx, const unsigned char *buf, unsigned int len);
void MD5Final(unsigned char digest[16], MD5Context *ctx);
void MD5DigestToBase16(unsigned char *digest, char *zBuf);
void MD5DigestToBase10x8(unsigned char digest[16], char zDigest[50]);

#endif
