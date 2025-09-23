#include <cdb2_constants.h>
#include <import_util.h>
#include <str_util.h>
#include <logmsg.h>

int gbl_bulk_import_validation_werror = 1;

enum bulk_import_validation_rc validate_bulk_import_inputs(const char * const dest_tablename, const char * const bulk_import_src_mach,
    const char * const bulk_import_src_dbname, const char * const bulk_import_src_tablename)
{
    enum bulk_import_validation_rc rc = BULK_IMPORT_VALIDATION_OK;

    if (!str_is_alphanumeric(dest_tablename, NON_ALPHANUM_CHARS_ALLOWED_IN_TABLENAME)) {
        logmsg(LOGMSG_WARN, "%s: Bulk import destination table name '%s' has illegal characters\n",
                __func__, dest_tablename);
        rc = BULK_IMPORT_VALIDATION_WARN;
    }
    if (!str_is_alphanumeric(bulk_import_src_dbname, NON_ALPHANUM_CHARS_ALLOWED_IN_DBNAME)) {
        logmsg(LOGMSG_WARN, "%s: Bulk import source db name '%s' has illegal characters\n",
                __func__, bulk_import_src_dbname);
        rc = BULK_IMPORT_VALIDATION_FATAL;
    }
    if (bulk_import_src_tablename && !str_is_alphanumeric(bulk_import_src_tablename, NON_ALPHANUM_CHARS_ALLOWED_IN_TABLENAME)) {
        logmsg(LOGMSG_WARN, "%s: Bulk import source table name '%s' has illegal characters\n",
               __func__, bulk_import_src_tablename);
        rc = BULK_IMPORT_VALIDATION_WARN;
    }
    if (bulk_import_src_mach && !str_is_alphanumeric(bulk_import_src_mach, NON_ALPHANUM_CHARS_ALLOWED_IN_MACHINENAME)) {
        logmsg(LOGMSG_WARN, "%s: Bulk import source machine '%s' has illegal characters\n",
                __func__, bulk_import_src_mach);
        rc = BULK_IMPORT_VALIDATION_FATAL;
    }
    if (rc == BULK_IMPORT_VALIDATION_WARN && gbl_bulk_import_validation_werror) {
        rc = BULK_IMPORT_VALIDATION_FATAL;
    }

    return rc;
}
