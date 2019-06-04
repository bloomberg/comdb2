#include "tar_header.h"

#include "serialiseerror.h"

#include <iostream>

#include <grp.h>
#include <pwd.h>
//#include <str0.h>
#include <cstring>
#include <cstdio>

const long long TarHeader::MAX_OCTAL_SIZE = 0100000000000LL;

TarHeader::TarHeader()
{
    clear();
}

TarHeader::~TarHeader()
{
}

void TarHeader::clear() throw()
// Reset the tar header struct ready for a new file
// Does not throw as constructor calls this
{
    m_used_gnu = false;
    memset(&m_head, 0, sizeof(m_head));
    strncpy(m_head.h.ustar, "ustar", sizeof(m_head.h.ustar) - 1);
    m_head.h.ustar_version[0] = '0';
    m_head.h.ustar_version[1] = '0';
}

void TarHeader::set_filename(const std::string& filename)
// Set the file name in the tar header.  If the filename given is too long
// (more than 100 chars) then this will throw a SerialiseError
{
    if(filename.length() >= sizeof(m_head.h.filename)) {
        throw SerialiseError(filename, "file name too long for tar header");
    }
    strncpy(m_head.h.filename, filename.c_str(),
            sizeof(m_head.h.filename) - 1);
}

void TarHeader::set_attrs(const struct stat& st)
// Set the attributes based on the information in the stat struct
{
    snprintf(m_head.h.mode,  sizeof(m_head.h.mode),  "%07o",    st.st_mode);
    snprintf(m_head.h.uid,   sizeof(m_head.h.uid),   "%07o",    st.st_uid);
    snprintf(m_head.h.gid,   sizeof(m_head.h.gid),   "%07o",    st.st_gid);
    snprintf(m_head.h.mtime, sizeof(m_head.h.mtime), "%011llo",   (long long) st.st_mtime);

    // If the size will fit within 11 octal digits then encode it that way
    // (as per classic tar file format).  Otherwise we adopt the gnu extension
    // for encoding larger +ve numbers.  First digit will be \200 (128) followed
    // by base 256 encoded number.
    if(st.st_size < MAX_OCTAL_SIZE) {
        snprintf(m_head.h.size,  sizeof(m_head.h.size),  "%011llo", (long long) st.st_size);
    } else {
        unsigned long long sz = st.st_size;
        m_head.h.size[0] = '\200';
        for(int ii = sizeof(m_head.h.size) - 1; ii > 0; ii--) {
            m_head.h.size[ii] = (char)(sz & 0xff);
            sz >>= 8;
        }
        m_used_gnu = true;
    }

    struct passwd *pwd = getpwuid(st.st_uid);
    if(pwd == NULL) {
        std::clog << "getpwuid(" << st.st_uid << ") failed." << std::endl;
    } else {
        strncpy(m_head.h.uname, pwd->pw_name, sizeof(m_head.h.uname) - 1);
    }
    struct group *grp = getgrgid(st.st_gid);
    if(grp == NULL) {
        std::clog << "getgrgid(" << st.st_gid << ") failed." << std::endl;
    } else {
        strncpy(m_head.h.gname, grp->gr_name, sizeof(m_head.h.gname) - 1);
    }
}

void TarHeader::set_checksum()
// Calculate and store checksum. The method is:
//  Set the 8 checksum bytes to spaces
//  Sum the 512 bytes of the header, treating each byte as a uint.
//  Store the size in the format "000000\0 " where the 0's represent the
//  checksum as a zero padded six digit octal number.
{
    memset(m_head.h.checksum, ' ', sizeof(m_head.h.checksum));
    unsigned int sum = 0;
    for(int ii = 0; ii < sizeof(m_head.c); ii++) {
        sum += m_head.c[ii];
    }
    snprintf(m_head.h.checksum, sizeof(m_head.h.checksum) - 1, "%06o", sum);
}
