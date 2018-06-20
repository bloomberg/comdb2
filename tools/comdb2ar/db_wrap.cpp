#include "db_wrap.h"
#include "db_page.h"
#include "error.h"

#include <iostream>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

#include "chksum.h"

uint32_t myflip(uint32_t in)
{
	union intswp
	{
	    uint32_t u32;
	    uint8_t u8[4];
	};
	typedef union intswp intswp_t;
	intswp_t in_val, flip_val;
	in_val.u32 = in;
	flip_val.u8[0] = in_val.u8[3];
	flip_val.u8[1] = in_val.u8[2];
	flip_val.u8[2] = in_val.u8[1];
	flip_val.u8[3] = in_val.u8[0];
	return flip_val.u32;
}

DB_Wrap::DB_Wrap(const std::string& filename) : m_filename(filename)
{
	// This comes straight from pgdump - can't ask berkeley to do this
	// because db might be encrypted and we don't have passwd
	uint8_t meta_buf[DBMETASIZE];
	int fd = open(filename.c_str(), O_RDONLY);
	size_t n = read(fd, meta_buf, DBMETASIZE);
	if (n == -1) {
	    perror("read");
	}
	close(fd);
	DBMETA *meta = (DBMETA *)meta_buf;
	bool swapped = false;
	uint32_t flags = meta->flags;
	uint32_t magic = meta->magic;
	uint32_t pagesize = meta->pagesize;
again:	switch (magic) {
	case DB_BTREEMAGIC:
		if (LF_ISSET(BTM_RECNO))
			m_type = DB_RECNO;
		else
			m_type = DB_BTREE;
		break;
	case DB_HASHMAGIC:
		m_type = DB_HASH;
		break;
	case DB_QAMMAGIC:
		m_type = DB_QUEUE;
		break;
	default:
		if (swapped) {
			std::ostringstream ss;
			ss << filename << ": bad meta page type 0x" << std::hex << magic << std::endl;
			throw Error(ss);
		}
		swapped = true;
		magic = myflip(magic);
		flags = myflip(flags);
		pagesize = myflip(pagesize);
		goto again;
	}
	m_swapped = swapped;
	m_metaflags = meta->metaflags;
	m_crypto = meta->encrypt_alg;
	m_pagesize = pagesize;
}

size_t DB_Wrap::get_pagesize() const
{
	return m_pagesize;
}

bool DB_Wrap::get_crypto() const
{
	return m_crypto;
}

bool DB_Wrap::get_checksums() const
{
	return m_metaflags & DBMETA_CHKSUM;
}

bool DB_Wrap::get_sparse() const
{
   int rc;
   struct stat statbuf;


   rc = stat(m_filename.c_str(), &statbuf);
   if (rc != 0)
   {
      std::ostringstream ss;
      ss << "stat failed on " << m_filename;
      throw Error(ss);
   }



   if (statbuf.st_size > (statbuf.st_blocks * statbuf.st_blksize))
      return true;

   return false;
}

bool DB_Wrap::get_swapped() const
{
	return m_swapped;
}

void verify_checksum(uint8_t *page, size_t pagesize, bool crypto, bool swapped,
                        bool *verify_bool, uint32_t *verify_cksum)
// Verify the checksum on a regular Berkeley DB page.  Returns true if
// the checksum is correct, false otherwise
//
// Also call storeIncrData to store the LSN + Checksum in a file to be
// compared against
{
    PAGE *pagep = (PAGE *)page;
    uint8_t *chksum_ptr = page;

    switch (PTYPE(pagep)) {
    case P_HASHMETA:
    case P_BTREEMETA:
    case P_QAMMETA:
        chksum_ptr = ((BTMETA *)page)->chksum;
        pagesize = DBMETASIZE;
        break;
    default:
        chksum_ptr += crypto ? SIZEOF_PAGE + SSZA(PG_CRYPTO, chksum)
                             : SIZEOF_PAGE + SSZA(PG_CHKSUM, chksum);
        break;
    }


    uint32_t orig_chksum, chksum;
    orig_chksum = chksum = *(uint32_t *)chksum_ptr;
    if (swapped)
        chksum = myflip(chksum);
    *(uint32_t *)chksum_ptr = 0;
    uint32_t calc = IS_CRC32C(page) ? crc32c(page, pagesize)
                                    : __ham_func4(page, pagesize);
    *verify_cksum = calc;
    *(uint32_t *)chksum_ptr = orig_chksum;
    *verify_bool = (calc == chksum);
    return;
}

// uint32_t calculate_checksum(uint8_t *page, size_t pagesize){
//     PAGE *pagep = (PAGE *)page;
//
//     switch (PTYPE(pagep)) {
//     case P_HASHMETA:
//     case P_BTREEMETA:
//     case P_QAMMETA:
//         pagesize = DBMETASIZE;
//         break;
//     }
//
//     return IS_CRC32C(page) ? crc32c(page, pagesize): __ham_func4(page, pagesize);
// }
