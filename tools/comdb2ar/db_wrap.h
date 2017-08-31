#ifndef INCLUDED_DB_WRAP
#define INCLUDED_DB_WRAP

#include <string>
#include "glue.h"

class DB_Wrap {
    std::string m_filename;
    DBTYPE m_type;
    uint32_t m_pagesize;
    uint8_t m_metaflags;
    uint8_t m_crypto;
    bool m_swapped;

public:

    DB_Wrap(const std::string& filename);

    size_t get_pagesize() const;

    bool get_checksums() const;

    bool get_crypto() const;

    bool get_sparse() const;

    bool get_swapped() const;
};

void verify_checksum(uint8_t *page, size_t pagesize, bool crypto, bool swap,
        bool *verify_bool, uint32_t *verify_cksum);
// Verify the checksum on a regular Berkeley DB page.  Returns true if
// the checksum is correct, false otherwise

uint32_t calculate_checksum(uint8_t *page, size_t pagesize);
#endif // INCLUDED_DB_WRAP
