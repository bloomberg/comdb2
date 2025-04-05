#include <stdlib.h>
#include <string.h>
#include <plhash_glue.h>
#include "dbinc/trigger_subscription.h"
#include <sys_wrap.h>

/*
 * Maintain mapping of qdb name and its signaling mechanism.
 * This needs a simple hash table (name -> pthread_cond_t).
 * Unfortunately, hash_t is defined in both libdb and plhash.
 * Isolating this hash-tbl into its own .h/.c
 */
static hash_t *htab = NULL;
static pthread_mutex_t subscription_lk = PTHREAD_MUTEX_INITIALIZER;
struct __db_trigger_subscription *__db_get_trigger_subscription(const char *name)
{
	Pthread_mutex_lock(&subscription_lk);
	if (htab == NULL) {
		htab = hash_init_strptr(0);
	}
	struct __db_trigger_subscription *s = hash_find(htab, &name);
	if (s == NULL) {
		s = calloc(1, sizeof(struct __db_trigger_subscription));
		s->name = strdup(name);
		Pthread_cond_init(&s->cond, NULL);
		Pthread_mutex_init(&s->lock, NULL);
		hash_add(htab, s);
	}
	Pthread_mutex_unlock(&subscription_lk);
	return s;
}

int __db_for_each_trigger_subscription(hashforfunc_t *func, int lock_it)
{
	Pthread_mutex_lock(&subscription_lk);
	if (htab != NULL)
		hash_for(htab, func, (void *)(intptr_t)lock_it);
	Pthread_mutex_unlock(&subscription_lk);
	return 0;
}
