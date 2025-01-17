#include <version_util.h>

int compare_semvers(const char * const vers_lhs, const char * const vers_rhs, int * const cmp_result) {
	unsigned major_lhs = 0, minor_lhs = 0, bugfix_lhs = 0;
	unsigned major_rhs = 0, minor_rhs = 0, bugfix_rhs = 0;

	int input_items_assigned = sscanf(vers_lhs, "%u.%u.%u", &major_lhs, &minor_lhs, &bugfix_lhs);
	if (input_items_assigned != 3) { return 1; }

	input_items_assigned = sscanf(vers_rhs, "%u.%u.%u", &major_rhs, &minor_rhs, &bugfix_rhs);
	if (input_items_assigned != 3) { return 1; }

	if (major_lhs < major_rhs) { *cmp_result = -1; }
	else if (major_lhs > major_rhs) { *cmp_result = 1; }
	else if (minor_lhs < minor_rhs) { *cmp_result = -1; }
	else if (minor_lhs > minor_rhs) { *cmp_result = 1; }
	else if (bugfix_lhs < bugfix_rhs) { *cmp_result = -1; }
	else if (bugfix_lhs > bugfix_rhs) { *cmp_result = 1; }
	else { *cmp_result = 0; }

	return 0;
}
