#include <stdio.h>

/*
 * Compares two comdb2 semantic versions.
 *
 * On success, stores one of the following values in `cmp_result`:
 * 		-1 if lhs is less than rhs
 *		0 if lhs is equal to rhs
 *		1 if lhs is greater than rhs
 *
 * Returns
 *		0 on success and non-0 on if either version could not be parsed
 */
int compare_semvers(const char * const vers_lhs, const char * const vers_rhs, int * const cmp_result);
