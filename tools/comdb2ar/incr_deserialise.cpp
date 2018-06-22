#include "comdb2ar.h"

#include "increment.h"
#include "error.h"
#include "riia.h"
#include "fdostream.h"
#include "serialiseerror.h"
#include "db_wrap.h"
#include "util.h"

#include <unistd.h>
#include <cstring>
#include <fstream>
#include <iostream>
#include <cassert>
#include <fcntl.h>
#include <memory>

#include <vector>
#include <string>
#include <set>
#include <map>
#include <utility>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/statvfs.h>

/* check once a megabyte */
#define FS_PERIODIC_CHECK (10 * 1024 * 1024)
#define write_size (1000*1024)

void update_tree(const std::string& filename,
    const std::pair<FileInfo, std::vector<uint32_t> >& file_data, bool dryrun
)
// Given a filename and a vector of pages that have been changed in that file
// overwrite the pages with the new data read in from STDIN
{

    FileInfo file_info = file_data.first;
    std::vector<uint32_t> pages = file_data.second;

    int flags = O_RDONLY;
    int fd = open(filename.c_str(), flags);

    struct stat st;
    if(fstat(fd, &st) != 0) {
        std::ostringstream ss;
        ss << "cannot stat file: " << std::strerror(errno);
        throw SerialiseError(filename, ss.str());
    }

    std::ofstream tree_file(filename, std::ofstream::out |
                            std::ofstream::in | std::ofstream::binary);

    size_t pagesize = file_info.get_pagesize();
    if(pagesize == 0) {
        pagesize = 4096;
    }

    uint8_t *pagebuf = NULL;

#if ! defined  ( _SUN_SOURCE ) && ! defined ( _HP_SOURCE )
    posix_memalign((void**) &pagebuf, 512, pagesize);
#else
    pagebuf = (uint8_t*) memalign(512, pagesize);
#endif

    for(std::vector<uint32_t>::const_iterator
            it = pages.begin();
            it != pages.end();
            ++it){

        if(readall(0, &pagebuf[0], pagesize) != pagesize) {
           std::ostringstream ss;

           ss << "Error reading for file " << filename << " page "
              << *it;
               throw Error(ss);
        }

        // Seek out the offset corresponding to the page number and overwrite
        if (!dryrun) {
            tree_file.seekp(pagesize * *it, tree_file.beg);
            tree_file.write((char *) pagebuf, pagesize);
        }
    }

    std::clog << "x " << file_info.get_filename() << " PARTIAL" << " Pages ";
    std::clog << "[" << pages[0];
    for(int i = 1; i < pages.size(); ++i){
        std::clog << " " << pages[i];
    }
    std::clog << "]";
    std::clog << " size=" << pages.size() * pagesize << " pagesize=" << pagesize;

    if (file_info.get_sparse())
       std::clog << " SPARSE ";
    else
       std::clog << " not sparse ";

    std::clog << std::endl;

    if(pagebuf) free(pagebuf);
}

void unpack_incr_data(
    const std::vector<std::string>& file_order,
    const std::map<std::string, std::pair<FileInfo, std::vector<uint32_t> > >& updated_files,
    const std::string& datadestdir, bool dryrun
)
// Driver for updating the BTree files
// Iterates through changed files and calls update_tree on them
{
    for(std::vector<std::string>::const_iterator it = file_order.begin();
        it != file_order.end();
        ++it){

        std::string filename = *it;
        std::string abs_filepath = datadestdir + "/" + filename;
        std::map<std::string, std::pair<FileInfo, std::vector<uint32_t> > >::const_iterator
            fd_it = updated_files.find(filename);

        if(fd_it == updated_files.end()){
            std::ostringstream ss;
            ss << "Unexpected updated file found";
            throw Error(ss);
        }

        const FileInfo fi = fd_it->second.first;
        if (fi.get_filesize() == 0)
            abort();
        update_tree(abs_filepath, fd_it->second, dryrun);
        // zap the file size to what we expect
        struct stat st;
        int rc = stat(abs_filepath.c_str(), &st);
        if (rc) {
            abort();
        }
        std::cerr << "truncating to " << fi.get_filesize() << " current size " << st.st_size << std::endl;
        if(truncate(abs_filepath.c_str(), fi.get_filesize()))
            perror("truncating");
    }
}

void handle_deleted_files(
    std::set<std::string>& deleted_files,
    const std::string& datadestdir,
    std::set<std::string>& table_set
)
// Delete files that have been marked as deleted in the manifest
{
    for(std::set<std::string>::const_iterator
            it = deleted_files.begin();
            it != deleted_files.end();
            ++it)
    {
        std::string abs_path = datadestdir + *it;

        try {
            unlink(abs_path.c_str());
            std::clog << "x " << *it << "DELETED";
        }
        catch (Error &e) {
            std::clog << "unable to remove deleted file " << *it << std::endl;
        }
    }
}

void unpack_full_file(
    FileInfo *file_info_pt,
    const std::string filename,
    unsigned long long filesize,
    const std::string datadestdir,
    bool is_data_file,
    unsigned percent_full,
    bool& is_disk_full
)
// For new files and sufficiently changed files (>90% of pages changed)
// we must deserialise the entire file
{
    unsigned long long skipped_bytes = 0;

    unsigned long long nblocks = (filesize + 511ULL) >> 9;

    void *empty_page = NULL;
    empty_page = malloc(65536);
    memset(empty_page, 0, 65536);

    // Verify that we will have enough disk space for this file
    struct statvfs stfs;
    int rc = statvfs(datadestdir.c_str(), &stfs);
    if(rc == -1) {
        std::ostringstream ss;
        ss << "Error running statvfs on " << datadestdir
            << ": " << strerror(errno);
        throw Error(ss);
    }

    // Calculate how full the file system would be if we were to
    // add this file to it.
    fsblkcnt_t fsblocks = filesize / stfs.f_bsize;
    double percent_free = 100.00 * ((double)(stfs.f_bavail - fsblocks) / (double)stfs.f_blocks);
    if(100.00 - percent_free >= percent_full) {
        is_disk_full = true;
        std::ostringstream ss;
        ss << "Not enough space to deserialise " << filename
            << " (" << filesize << " bytes) - would leave only "
            << percent_free << "% free space";
        throw Error(ss);
    }


    // All good?  Open file.  All non-lrls go into the data directory.
    if(datadestdir.empty()) {
        throw Error("Stream contains files for data directory before data dir is known");
    }

    size_t pagesize = 0;
    bool checksums = false;
    bool file_is_sparse = false;
    std::unique_ptr<fdostream> incr_of_ptr;
    std::string outfilename(datadestdir + "/" + filename);

    if(is_data_file){
        FileInfo file_info = *file_info_pt;
        pagesize = file_info.get_pagesize();
        checksums = file_info.get_checksums();
        file_is_sparse = file_info.get_sparse();
        incr_of_ptr = output_file(outfilename, false, true);
    } else {
        incr_of_ptr = output_file(outfilename, false, false);
    }

    if(pagesize == 0) {
        pagesize = 4096;
    }
    size_t bufsize = pagesize;

    while((bufsize << 1) <= MAX_BUF_SIZE) {
        bufsize <<= 1;
    }


    uint8_t *buf;
#if defined _HP_SOURCE || defined _SUN_SOURCE
    buf = (uint8_t*) memalign(512, bufsize);
#else
    if (posix_memalign((void**) &buf, 512, bufsize))
        throw Error("Failed to allocate output buffer");
#endif
    RIIA_malloc free_guard(buf);

    // Read the tar data in and write it out
    unsigned long long bytesleft = filesize;
    unsigned long long pageno = 0;


    // Recheck the filesystem periodically while writing
    unsigned long long recheck_count = FS_PERIODIC_CHECK;

    bool checksum_failure = false;

    unsigned long long readbytes = 0;

    while(bytesleft > 0)
    {
        readbytes = bytesleft;

        if(bytesleft > bufsize)
        {
           readbytes = bufsize;
        }

        if (file_is_sparse) {
            readbytes = pagesize;
        }
        if (readbytes > bytesleft)
            readbytes = bytesleft;

        if(readall(0, &buf[0], readbytes) != readbytes)
        {
           std::ostringstream ss;

           if (filename == "FLUFF")
              return;

           ss << "Error reading " << readbytes << " bytes for file "
              << filename << " after "
              << (filesize - bytesleft) << " bytes, with "
              << bytesleft << " bytes left to read:"
              << errno << " " << strerror(errno);
           throw Error(ss);
        }


        if (file_is_sparse &&
           (readbytes == pagesize) && (bytesleft > readbytes) )
        {
            if (memcmp(empty_page, &buf[0], pagesize) == 0)
            {
                skipped_bytes += pagesize;
                /* This data won't be counted towards file size.*/
                recheck_count += readbytes;
            }
            else
            {
                if (skipped_bytes)
                {
                    if((incr_of_ptr->skip(skipped_bytes)))
                    {
                        std::ostringstream ss;

                        if (filename == "FLUFF")
                            return;

                        ss << "Error skipping " << filename << " after "
                           << (filesize - bytesleft) << " bytes";
                        throw Error(ss);
                    }
                    skipped_bytes = 0;
                }
                if (!incr_of_ptr->write((char*) buf, pagesize))
                {
                    std::ostringstream ss;

                    if (filename == "FLUFF")
                         return;

                    ss << "Error Writing " << filename << " after "
                       << (filesize - bytesleft) << " bytes"
                       << errno << " " << strerror(errno);
                    throw Error(ss);
                }
            }
        }
        else
        {
            uint64_t off = 0;
            uint64_t nwrites = 0;
            uint64_t bytes = readbytes;

            if (file_is_sparse && skipped_bytes)
            {
                if((incr_of_ptr->skip(skipped_bytes)))
                {
                    std::ostringstream ss;

                    if (filename == "FLUFF")
                        return;

                    ss << "Error skipping " << filename << " after "
                       << (filesize - bytesleft) << " bytes";
                    throw Error(ss);
                }
                skipped_bytes = 0;
            }

            while (bytes > 0)
            {
                int lim;
                if (bytes < write_size)
                    lim = bytes;
                else
                    lim = write_size;

                if (!incr_of_ptr->write((char*) &buf[off], lim)) {

                    std::ostringstream ss;

                    if (filename == "FLUFF")
                        return;

                    ss << "Error Writing " << filename << " after "
                       << (filesize - bytesleft) << " bytes" << std::endl
                       << errno << ": " << strerror(errno);
                    throw Error(ss);
                }
                nwrites++;
                off += lim;
                bytes -= lim;
            }
            // std::cerr << "wrote " << readbytes << " bytes in " << nwrites << " chunks" << std::endl;
        }
        bytesleft -= readbytes;
        recheck_count -= readbytes;

        // don't fill the fs - copied & massaged from above
        if( recheck_count <= 0 )
        {
            struct statvfs stfs;
            int rc = statvfs(datadestdir.c_str(), &stfs);
            if(rc == -1) {
                std::ostringstream ss;
                ss << "Error running statvfs on " << datadestdir
                    << ": " << strerror(errno);
                throw Error(ss);
            }

            // Calculate how full the file system would be if we were to
            // add this file to it.
            fsblkcnt_t fsblocks = bytesleft / stfs.f_bsize;
            double percent_free = 100.00 * ((double)(stfs.f_bavail - fsblocks) / (double)stfs.f_blocks);
            if(100.00 - percent_free >= percent_full) {
                is_disk_full = true;
                std::ostringstream ss;
                ss << "Not enough space to deserialise remaining part of " << filename
                    << " (" << bytesleft << " bytes) - would leave only "
                    << percent_free << "% free space";
                throw Error(ss);
            }

            recheck_count = FS_PERIODIC_CHECK;
        }
    }

    // Read and discard the null padding
    unsigned long long padding_bytes = (nblocks << 9) - filesize;
    if(padding_bytes) {
        if(readall(0, &buf[0], padding_bytes) != padding_bytes) {
            std::ostringstream ss;

            if (filename == "FLUFF")
               return;

            ss << "Error reading padding after " << filename
                << ": " << errno << " " << strerror(errno);
            throw Error(ss);
        }
    }

    std::clog << "x " << filename << " FULL" << " size=" << filesize
              << " pagesize=" << pagesize;

    if (file_is_sparse)
       std::clog << " SPARSE ";
    else
       std::clog << " not sparse ";

    std::clog << std::endl;
}

void clear_log_folder(
    const std::string& datadestdir,
    const std::string& dbname
)
// We don't want log file holes, so after each increment, remove all existing logs
// unless in keep_all_logs mode
{
    // remove files in the txn directory
    std::string dbtxndir(datadestdir + "/" + dbname + ".txn");
    make_dirs( dbtxndir );
    remove_all_old_files( dbtxndir );
    dbtxndir = datadestdir + "/" + "logs";
    make_dirs( dbtxndir );
    remove_all_old_files( dbtxndir );
}
