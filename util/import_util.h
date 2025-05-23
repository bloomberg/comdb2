// Utility library for functions shared between old and new bulk import implementations.

enum bulk_import_validation_rc {
    BULK_IMPORT_VALIDATION_OK = 0,
    BULK_IMPORT_VALIDATION_WARN,
    BULK_IMPORT_VALIDATION_FATAL,
};

// validates bulk import inputs
// returns
//      BULK_IMPORT_VALIDATION_OK if all inputs are valid
//      BULK_IMPORT_VALIDATION_WARN if table names or database names have invalid characters
//      BULK_IMPORT_VALIDATION_FATAL if machine names have invalid characters
enum bulk_import_validation_rc validate_bulk_import_inputs(const char * const dest_tablename, const char * const bulk_import_src_mach,
    const char * const bulk_import_src_dbname, const char * const bulk_import_src_tablename);
