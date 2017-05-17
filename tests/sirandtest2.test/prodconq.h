#ifndef prodconq_h
#define prodconq_h

#include <pthread.h>
#define BUFFER_SIZE 10000

/* Circular buffer of void pointers. */ 
struct prodconq {
  void* buffer[BUFFER_SIZE];      /* the actual data */
  pthread_mutex_t lock;         /* mutex ensuring exclusive access to buffer */
  int readpos, writepos;        /* positions for reading and writing */
  pthread_cond_t notempty;      /* signaled when buffer is not empty */
  pthread_cond_t notfull;       /* signaled when buffer is not full */
};


extern void prodconq_syserr(int bl, const char *fmt, ...);

extern int prodconq_init(struct prodconq *b);

extern int prodconq_put(struct prodconq *b, void* data_in);

extern int prodconq_get(struct prodconq *b, void** data_out);



#endif /* prodconq_h*/
