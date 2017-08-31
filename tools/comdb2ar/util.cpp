#include "comdb2ar.h"
#include "error.h"
#include "riia.h"

#include <cstring>
#include <sstream>
#include <string>
#include <ctime>

#include <sys/stat.h>
#include <fcntl.h>
#include <sys/types.h>

#include <errno.h>
#include <unistd.h>
#include <iostream>

#include <map>

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

void listdir_internal(std::list<std::string>& output_list,
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

void make_dirs(const std::string& dirname)
// Ensure that a directory given by absolute path dirname exists
{
    struct stat st;
    static int extentsize = -1;
    static std::map<std::string, bool> files;
    bool isdir = false;

    if (stat(dirname.c_str(), &st) == 0 && ((st.st_mode & S_IFMT) == S_IFDIR))
        isdir = true;

    if (extentsize == -1) {
        FILE *f;
        int sz;

        extentsize = 0;
        f = fopen("/bb/data/comdb2_xfs_extent_size", "r");
        if (f != NULL) {
            char line[512];
            while (fgets(line, sizeof(line), f)) {
                char *s = strchr(line, '\n');
                if (*s) *s = 0;
                s = line;
                while (*s && isspace(*s)) s++;
                if (*s == '#') continue;
                sz = atoi(s);
                if (sz > 0) extentsize = sz;
            }
        }
    }
    if (extentsize > 0 && isdir && !files[dirname]) {
        std::stringstream xfscmd;
        xfscmd << "/usr/sbin/xfs_io -c 'extsize " << extentsize << "' " << dirname;

        // set default extent size for database directories
        std::clog << ">>>>>> " << xfscmd.str() << std::endl;
        std::system(xfscmd.str().c_str());
        files[dirname] = true;
    }

    if(stat(dirname.c_str(), &st) == 0) {
        if((st.st_mode & S_IFMT) == S_IFDIR) {
            // This already exists as a directory (or a symlink to a directory)
            // so leave it alone.  This check exists because "mkdir -p /bb/bin"
            // would fail on sundev5 due to /bb/bin being a symlink to
            // /bb/sys/opbin.
            return;
        }
    }


    // Yes, this is lazy.  Sorry.
    std::string cmd("mkdir -p " + dirname);

    int rc = std::system(cmd.c_str());
    if(rc != 0) {
        std::ostringstream ss;
        ss << "Command " << cmd << " failed with rcode " << rc;
        throw Error(ss);
    }

}


std::unique_ptr<fdostream> output_file(
        const std::string& filename,
        bool make_sav, bool direct)
// Create an output file stream ready to receive the data for a file.
// This will also make any intermediate directories needed.
// If make_sav is true and the file already exists then the old file will be
// moved to .sav before proceeding.
{
    std::string dirname(filename);
    makedirname(dirname);
    make_dirs(dirname);
    struct stat st;

    if(stat(filename.c_str(), &st) == 0) {
        if(make_sav) {
            // We need to make a .sav.  This isn't a fatal error if we can't
            std::string sav_filename(filename + ".sav");
            int rc = rename(filename.c_str(), sav_filename.c_str());
            if(rc == -1) {
                std::cerr << "Cannot create " << sav_filename << ": "
                    << errno << " " << strerror(errno) << std::endl;
            }
        }

        // unlink the existing file (fix for unable to overwrite a readonly file)
        if(unlink(filename.c_str()) == -1 && errno != ENOENT) {
            std::cerr << "Cannot unlink " << filename << ": "
                    << errno << " " << strerror(errno) << std::endl;
        }
    }

    int flags = O_WRONLY | O_CREAT | O_TRUNC;

#ifdef __sun
    int fd = open(filename.c_str(), flags, 0666);
    if(fd == -1) throw Error("Error opening '" + filename + "' for writing");
#else
    if (direct)
        flags |= O_DIRECT;
reopen:
    int fd = open(filename.c_str(), flags, 0666);
    if(fd == -1) {
        if (EINVAL == errno){
            std::clog << "Turning off directio, err: " << std::strerror(errno)
                      << std::endl;
            flags ^= O_DIRECT;
            goto reopen;
        } else {
            throw Error("Error opening '" + filename + "' for writing");
        }
    }

    return std::unique_ptr<fdostream>(new fdostream(fd));
}

void remove_all_old_files(std::string &datadir) {
    std::list<std::string> dirlist;
    listdir_abs(dirlist, datadir);

    for (std::list<std::string>::const_iterator ii = dirlist.begin(); ii != dirlist.end(); ++ii) {
        try {
            if (!isDirectory(*ii)) {
                std::string s(*ii);
                /* don't delete lrl file */
                if (s.find(".lrl") == s.npos) {
                    std::clog << "deleted: " << s << std::endl;
                    unlink(s.c_str());
                }
            }
        }
        catch (Error &e) {
            std::clog << "couldn't stat " << *ii << ", not removing" << std::endl;
        }
    }
}

#endif
