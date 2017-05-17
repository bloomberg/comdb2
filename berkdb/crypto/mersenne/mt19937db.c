#include "db_int.h"
#include <openssl/rand.h>

/*
 * __db_generate_iv --
 *	Generate an initialization vector (IV)
 *
 * PUBLIC: int __db_generate_iv __P((DB_ENV *, u_int32_t *));
 */
int
__db_generate_iv(dbenv, iv)
	DB_ENV *dbenv;
	u_int32_t *iv;
{
	RAND_bytes((unsigned char *)iv, DB_IV_BYTES);
	return 0;
}
