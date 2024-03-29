#include "file_info.h"

#include "comdb2ar.h"
#include "error.h"

#include <iostream>
#include <sstream>

FileInfo::FileInfo() :
    m_type(UNKNOWN_FILE_TYPE),
    m_checksums(false),
    m_pagesize(0),
    m_sparse(false),
    m_do_direct_io(true),
    m_swapped(false),
    m_filesize(0) {}

FileInfo::FileInfo(const FileInfo& copy) :
    m_type(copy.m_type),
    m_filepath(copy.m_filepath),
    m_filename(copy.m_filename),
    m_pagesize(copy.m_pagesize),
    m_checksums(copy.m_checksums),
    m_sparse(copy.m_sparse),
    m_do_direct_io(copy.m_do_direct_io),
    m_swapped(copy.m_swapped),
    m_filesize(copy.m_filesize) {}

FileInfo::FileInfo(
        FileTypeEnum type,
        const std::string& abspath,
        const std::string& dbdir,
        size_t pagesize,
        bool checksums,
	bool crypto,
        bool sparse,
        bool do_direct_io,
	bool swapped) :
    m_type(type),
    m_filepath(abspath),
    m_pagesize(pagesize),
    m_checksums(checksums),
    m_crypto(crypto),
    m_sparse(sparse),
    m_do_direct_io(do_direct_io),
    m_swapped(swapped),
    m_filesize(0)
{
    // If file name is within the dbdir then just strip dbdir
    if(type != SUPPORT_FILE
            && abspath.length() > dbdir.length() + 1
            && abspath.compare(0, dbdir.length(), dbdir) == 0
            && abspath[dbdir.length()] == '/') {
        m_filename = abspath.substr(dbdir.length() + 1);
    } else {
        // Otherwise just get the base name.  This is only expected for
        // the lrl file; all other files should be inside the dbdir.
        // In somce cases though support files like csc2s may be elsewhere.
        size_t pos = abspath.find_last_of('/');
        if(pos != std::string::npos) {
            m_filename = abspath.substr(pos + 1);
        } else {
            m_filename = abspath;
        }
    }
}

FileInfo::~FileInfo()
{
}

FileInfo& FileInfo::operator=(const FileInfo& rhs)
// Assignment operator
{
    if(&rhs != this) {
        m_type = rhs.m_type;
        m_filepath = rhs.m_filepath;
        m_filename = rhs.m_filename;
        m_pagesize = rhs.m_pagesize;
        m_checksums = rhs.m_checksums;
        m_sparse = rhs.m_sparse;
        m_do_direct_io = rhs.m_do_direct_io;
        m_filesize = rhs.m_filesize;
    }
    return *this;
}

void FileInfo::reset()
// Reset this to an empty object
{
    m_type = UNKNOWN_FILE_TYPE;
    m_filepath.clear();
    m_filename.clear();
    m_checksums = false;
    m_sparse = false;
    m_pagesize = 0;
    m_swapped = false;
}

const char *FileInfo::get_type_string() const
// Return a string constant which represents the type of this file
{
    switch(m_type) {
        default:                    return "FileInfo::get_type_string default";
        case UNKNOWN_FILE_TYPE:     return "Unknown_file_type";
        case SUPPORT_FILE:          return "Support_file";
        case BERKDB_FILE:           return "Berkdb_file";
        case OPTIONAL_FILE:         return "Optional_file";
        case LOG_FILE:              return "Log_file";
    }
}

void FileInfo::set_type(const std::string& tok)
// Set the type from a string assumed to have come from the
// get_type_string() method.  If the string is not recognised then the
// type will be set to unknown.
{
    if(tok.compare("Support_file") == 0) {
        m_type = SUPPORT_FILE;
    } else if(tok.compare("Berkdb_file") == 0) {
        m_type = BERKDB_FILE;
    } else if(tok.compare("Log_file") == 0) {
        m_type = LOG_FILE;
    } else if (tok.compare("Optional_file") == 0) {
        m_type = OPTIONAL_FILE;
    } else {
        m_type = UNKNOWN_FILE_TYPE;
    }
}


void write_manifest_entry(std::ostream& os, const FileInfo& file)
// Serialise a FileInfo into an output stream.  This is intended for writing
// out the manifest file.
{
    os << "File " << file.get_filename();
    std::cerr << "File " << file.get_filename();
    os << " Type " << file.get_type_string();
    std::cerr << " Type " << file.get_type_string();
    if(file.get_type() == FileInfo::BERKDB_FILE) {
        os << " PageSize " << file.get_pagesize();
        std::clog << " PageSize " << file.get_pagesize();
    }

    if(file.get_checksums()) {
        os << " Checksums";
        std::clog << " Checksums";
    }

    if(file.get_sparse()) {
        os << " Sparse";
        std::clog << " Sparse";
    }

    os << std::endl;
    std::cerr << std::endl;
    //return os;
}

bool read_FileInfo(const std::string& line, FileInfo& file)
// Attempt to deserialise a FileInfo object from the provided string.
// Returns true and populates file if this is possible, otherwise returns
// false.
{
    std::istringstream ss(line);
    std::string tok;

    if(ss >> tok && tok == "File" && ss >> tok) {
        FileInfo f;
        f.set_filename(tok);
        while(ss >> tok) {
            if(tok == "Type") {
                if(!(ss >> tok)) return false;
                f.set_type(tok);

            } else if(tok == "PageSize") {
                size_t pagesize;
                if(!(ss >> pagesize)) return false;
                f.set_pagesize(pagesize);

            } else if (tok == "FileSize") {
                size_t filesize;
                if(!(ss >> filesize)) return false;
                f.set_filesize(filesize);
            } else if(tok == "Checksums") {
                f.set_checksums();

            } else if(tok == "Sparse") {
                fprintf(stderr, "tok=Sparse, calling set_sparse\n");
                f.set_sparse();

            } else if(tok == "New") {
                continue;

            } else {
                return false;
            }
        }
        file = f;
        return true;
    }
    return false;
}

bool read_incr_FileInfo(const std::string& line, FileInfo& file,
        std::vector<uint32_t>& incr_pages)
// Attempt to deserialise a FileInfo object from the provided string.
// Returns true and populates file if this is possible, otherwise returns
// false.
{
    std::istringstream ss(line);
    std::string tok;

    if(ss >> tok && tok == "File" && ss >> tok) {
        FileInfo f;
        f.set_filename(tok);
        while(ss >> tok) {
            if(tok == "Type") {
                if(!(ss >> tok)) return false;
                f.set_type(tok);

            } else if(tok == "PageSize") {
                size_t pagesize;
                if(!(ss >> pagesize)) return false;
                f.set_pagesize(pagesize);

            } else if (tok == "FileSize") {
                int64_t filesize;
                if(!(ss >> filesize)) return false;
                f.set_filesize(filesize);

            } else if(tok == "Checksums") {
                f.set_checksums();

            } else if(tok == "Sparse") {
                fprintf(stderr, "tok=Sparse, calling set_sparse\n");
                f.set_sparse();

            } else if(tok == "New") {
                continue;

            } else if(tok == "Updated") {
                continue;

            } else if(tok == "Deleted") {
                continue;

            } else if(tok == "Pages") {
                if(!(ss >> tok)) return false;
                if(tok != "[") return false;
                while(ss >> tok){
                    if (tok == "]") break;
                    try {
                        incr_pages.push_back(std::stoi(tok));
                    } catch (Error &e) {
                        return false;
                    }
                }

            } else {
                return false;
            }
        }
        file = f;
        return true;
    }
    return false;
}

