#include "comdb2ar.h"
#include "error.h"
#include "riia.h"

#include <cstring>
#include <sstream>
#include <string>
#include <ctime>

#include <sys/stat.h>

#include <errno.h>
#include <unistd.h>

std::string& makeabs(std::string& abs_out, const std::string& dirname, const std::string& filename)
// Populate abs_out with an absolute path formed by the concatenation (with the
// appropriate directory separator) of dirname and filename.  If filename
// is already absolute then no change is made and it is copied verbatim into
// abs_out.
{
    if(!filename.empty() && filename[0] == '/') {
        // Looks absolute, return it
        abs_out = filename;

    } else {
        abs_out = dirname;
        if(!abs_out.empty() && abs_out[abs_out.length() - 1] != '/') {
            abs_out.append("/");
        }
        abs_out.append(filename);
    }
    return abs_out;
}

void makebasename(std::string& path)
// Turn a pathname into just the base file name in place.  If path doesn't
// contain any /'s then it is unchanged.
{
    size_t pos = path.find_last_of('/');
    if(pos != std::string::npos) {
        path = path.substr(pos + 1);
    }
}

void makedirname(std::string& path)
// Strip a path down into just a directory name, by removing everything after
// the last / (and removing the last /).  If there is no / then this will leave
// just the empty string.
{
    size_t pos = path.find_last_of('/');
    if(pos != std::string::npos) {
        path.resize(pos);
    } else {
        path.clear();
    }
}


static void listdir_internal(std::list<std::string>& output_list,
        const std::string& dirname, bool recursive, const std::string &prefix)
// Populates output_list with the path names of all the entries found
// in directory dirname.  Each entry will be prefixed with prefix.  If
// recursive is true then this will follow non-symbolic link subdirectory
// entries recursively with a suitably altered prefix.
{
    DIR *dh;
    struct dirent *ent;

    dh = opendir(dirname.c_str());
    if(!dh) {
        std::ostringstream ss;
        ss << "Error opening directory '" << dirname << "': "
            << std::strerror(errno);
        throw Error(ss.str());
    }
    RIIA_DIR dh_guard(dh);

    errno = 0;
    while(ent = readdir(dh)) {

        if(std::strcmp(ent->d_name, ".") != 0 &&
                std::strcmp(ent->d_name, "..") != 0) {
            output_list.push_back(prefix + ent->d_name);

            if(recursive) {
                // Stat this file to see if it's a subdirectory; if so dive
                // into it.
                std::string path;
                makeabs(path, dirname, ent->d_name);
                struct stat st;
                if(stat(path.c_str(), &st) == -1) {
                    std::ostringstream ss;
                    ss << "Error stat'ing '" << path << "': "
                        << std::strerror(errno);
                    throw Error(ss.str());
                }

                // Look for directories that are not symbolic links to traverse
                if(S_ISDIR(st.st_mode) && !S_ISLNK(st.st_mode)) {
                    std::ostringstream new_prefix;
                    if(prefix.length() > 0) {
                        new_prefix << prefix;
                    }
                    new_prefix << ent->d_name << "/";

                    listdir_internal(
                            output_list,
                            path,               // search subdirectory
                            recursive,
                            new_prefix.str());  // new prefix
                }
            }
        }

        // Reset errno
        errno = 0;
    }

    if(errno != 0) {
        std::ostringstream ss;
        ss << "Error enumerating directory '" << dirname << "': "
            << std::strerror(errno);
        throw Error(ss.str());
    }
}


void listdir(std::list<std::string>& output_list, const std::string& dirname)
// Populates output_list with all the entried found in directory dirname.
// This will return subdirectories in its results, but not . or ..
{
    listdir_internal(output_list, dirname, false, "");
}

void listdir_abs(std::list<std::string>& output_list, const std::string &dirname) {
    listdir_internal(output_list, dirname, false,
            dirname.empty() || dirname[dirname.length()-1] == '/' ? dirname
            : dirname + "/");
}

void listdir_abs_recursive(std::list<std::string>& output_list,
        const std::string& dirname)
// Populates output_list with the absolute path names of all the entries found
// in directory dirname, which should be itself an absolute path.  This will
// follow non-symbolic subdirectories and include those too.
{
    listdir_internal(output_list, dirname, true,
            dirname.empty() || dirname[dirname.length()-1] == '/' ? dirname
            : dirname + "/");
}


ssize_t writeall(int fd, const void *buf, size_t nbytes)
// Write all the bytes to the given fd.  Return the number of bytes written,
// or -1 if we hit an error or 0 if we can't write (in which case it is
// possible that some bytes were written successfully; we just don't say how
// many to the caller).  Returns nbytes on success.
{
    const char *cbuf = static_cast<const char *>(buf);
    size_t total = 0;

    while(nbytes > 0) {
        ssize_t byteswritten = write(fd, cbuf, nbytes);
        if(byteswritten <= 0) {
            return byteswritten;
        }

        cbuf += byteswritten;
        nbytes -= byteswritten;
        total += byteswritten;
    }

    return total;
}


ssize_t readall(int fd, void *buf, size_t nbytes)
// Read all the bytes requested from the given fd.  Returns the number of bytes
// read, or -1 on error or 0 on eof.
{
    char *cbuf = static_cast<char *>(buf);
    size_t total = 0;

    while(nbytes > 0) {
        ssize_t bytesread = read(fd, cbuf, nbytes);
        if(bytesread <= 0) {
            return bytesread;
        }

        cbuf += bytesread;
        nbytes -= bytesread;
        total += bytesread;
    }

    return total;
}

bool isDirectory(const std::string& file) {
    struct stat st;
    int rc;

    rc = stat(file.c_str(), &st);
    if (rc == -1)
        throw Error("Error stat(\"" + file + "\")");

    if (S_ISDIR(st.st_mode))
        return true;
    else
        return false;
}


