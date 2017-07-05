#include "comdb2ar.h"

#include "increment.h"
#include "glue.h"
#include "db_wrap.h"
#include "serialiseerror.h"

#include <sys/stat.h>
#include <fstream>
#include <cstring>
#include <sstream>
#include <iostream>
#include <cassert>

#include <vector>
#include <set>
#include <map>
#include <pair>

#include <fcntl.h>
#include <unistd.h>


bool is_not_incr_file(std::string filename){
    return(filename.substr(filename.length() - 5) != ".incr");
}

bool assert_cksum_lsn(uint8_t *new_pagep, uint8_t *old_pagep, size_t pagesize) {
    uint32_t new_lsn_file = LSN(new_pagep).file;
    uint32_t new_lsn_offs = LSN(new_pagep).offset;
    bool verify_ret;
    uint32_t new_cksum;
    verify_checksum(new_pagep, pagesize, false, false, &verify_ret, &new_cksum);

    uint8_t cmp_arr[12];
    for (int i = 0; i < 4; ++i){
        cmp_arr[i] = ((uint8_t *) &new_lsn_file)[i];
        cmp_arr[4 + i] = ((uint8_t *) &new_lsn_offs)[i];
        cmp_arr[8 + i] = ((uint8_t *) &new_cksum)[i];
    }

    return (memcmp(cmp_arr, old_pagep, 12) != 0);
}


bool compare_checksum(FileInfo file, const std::string& incr_path,
                        std::vector<uint32_t>& pages, ssize_t *data_size,
                        std::set<std::string>& incr_files) {
    std::string filename = file.get_filename();
    std::string incr_file_name = incr_path + "/" + filename + ".incr";

    std::set<std::string>::const_iterator it = incr_files.find(filename + ".incr");
    if(it == incr_files.end()){
        std::ostringstream ss;
        std::clog << "new incr file found mid backup of file: " << filename << std::endl;
        // throw SerialiseError(filename, ss.str());
    } else {
        incr_files.erase(it);
    }

    struct stat sb;
    bool ret = false;

    int flags = O_RDONLY | O_LARGEFILE;
    int new_fd = open(file.get_filepath().c_str(), flags);
    int old_fd = open(incr_file_name.c_str(), O_RDWR);

    struct stat new_st;
    if(fstat(new_fd, &new_st) == -1) {
        std::ostringstream ss;
        ss << "cannot stat file: " << std::strerror(errno);
        throw SerialiseError(filename, ss.str());
    }

    if(stat(incr_file_name.c_str(), &sb) != 0) {
        std::clog << "New File: " << filename << std::endl;
        *data_size = new_st.st_size;
        return true;
    } else {


        size_t pagesize = file.get_pagesize();
        if(pagesize == 0) {
            pagesize = 4096;
        }

        uint8_t *new_pagebuf = NULL;
        uint8_t old_pagebuf[12];

#if ! defined  ( _SUN_SOURCE ) && ! defined ( _HP_SOURCE )
        posix_memalign((void**) &new_pagebuf, 512, pagesize);
#else
        new_pagebuf = (uint8_t*) memalign(512, pagesize);
#endif

        off_t bytesleft = new_st.st_size;

        while(bytesleft >= pagesize) {
            ssize_t new_bytesread = read(new_fd, &new_pagebuf[0], pagesize);
            ssize_t old_bytesread = read(old_fd, &old_pagebuf[0], 12);
            if(new_bytesread <= 0 || old_bytesread <= 0) {
                std::ostringstream ss;
                ss << "read error after " << bytesleft << " bytes";
                throw SerialiseError(filename, ss.str());
            }

            if (assert_cksum_lsn(new_pagebuf, old_pagebuf, pagesize)) {
                pages.push_back(PGNO(new_pagebuf));
                *data_size += pagesize;
                ret = true;
            }

            bytesleft -= pagesize;
        }

        if(new_pagebuf) free(new_pagebuf);
    }
    return ret;
}

void write_incr_manifest_entry(std::ostream& os, const FileInfo& file,
                                const std::vector<uint32_t>& pages)
// Serialise a FileInfo into an output stream during an incremental backup.
// This is intended for writing out the manifest file.
{

    os << "File " << file.get_filename();
    std::clog << "File " << file.get_filename();
    os << " Type " << file.get_type_string();
    std::clog << " Type " << file.get_type_string();
    if(file.get_type() == FileInfo::BERKDB_FILE) {
        os << " PageSize " << file.get_pagesize();
        std::clog << " PageSize " << file.get_pagesize();
    }

    if(!pages.empty()){
        os << " Pages [";
        std::clog << " Pages [";
        for(size_t i = 0; i < pages.size(); ++i){
            os << pages[i];
            std::clog << pages[i];
            if(i != pages.size() - 1){
                os << ", ";
                std::clog << ", ";
            } else {
                os << "] ";
                std::clog << "] ";
            }
        }
    } else {
        os << " Pages All ";
        std::clog << " Pages All ";
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
    std::clog << std::endl;
    //return os;

    return;
}

void write_del_manifest_entry(std::ostream& os, const std::string& incr_filename){
    std::string true_filename = incr_filename.substr(0, incr_filename.length() - 5);
    os << "File " << true_filename;
    std::cerr << "File " << true_filename;
    os << " DELETED" << std::endl;
    std::cerr << " DELETED" << std::endl;

    return;
}


ssize_t write_incr_file(const FileInfo& file, std::vector<uint32_t> pages,
                            std::string incr_path){
    const std::string& filename = file.get_filename();

    int flags;
    bool skip_iomap = false;
    std::ostringstream ss;

    // Ensure large file support
    assert(sizeof(off_t) == 8);

    struct stat st;
    if(stat(filename.c_str(), &st) == -1) {
        ss << "cannot stat file: " << std::strerror(errno);
        throw SerialiseError(filename, ss.str());
    }

    size_t pagesize = file.get_pagesize();
    if(pagesize == 0) {
        pagesize = 4096;
    }

    std::ifstream ifs (filename, std::ifstream::in | std::ifstream::binary);

    char *char_pagebuf = NULL;
    uint8_t *pagebuf = NULL;

#if ! defined  ( _SUN_SOURCE ) && ! defined ( _HP_SOURCE )
    posix_memalign((void**) &char_pagebuf, 512, pagesize);
    posix_memalign((void**) &pagebuf, 512, pagesize);
#else
    char_pagebuf = (char*) memalign(512, pagesize);
    pagebuf = (uint8_t*) memalign(512, pagesize);
#endif

    ssize_t total_read = 0;
    ssize_t total_written = 0;

    std::string incrFilename = incr_path + "/" + filename + ".incr";

    // if(stat(incrFilename.c_str(), &st) == -1) {
        // std::clog << "here" << std::endl;
        // ss << "cannot stat file: " << std::strerror(errno);
        // throw SerialiseError(incrFilename, ss.str());
    // }


    std::ofstream incrFile(incrFilename, std::ofstream::out |
                            std::ofstream::in | std::ofstream::binary);

    for(std::vector<uint32_t>::const_iterator
            it = pages.begin();
            it != pages.end();
            ++it){

        ifs.seekg(pagesize * *it, ifs.beg);
        ifs.read(&char_pagebuf[0], pagesize);
        ssize_t bytes_read = ifs.gcount();
        if(bytes_read < pagesize) {
            std::ostringstream ss;
            ss << "read error";
            throw SerialiseError(filename, ss.str());
        }

        std::memcpy(pagebuf, char_pagebuf, pagesize);

        uint32_t cksum;
        bool cksum_verified;
        verify_checksum(pagebuf, pagesize, false, false,
                            &cksum_verified, &cksum);
        if(!cksum_verified){
            std::ostringstream ss;
            ss << "serialise_file:page failed checksum verification";
            throw SerialiseError(filename, ss.str());
        }


        incrFile.seekp(12 * *it, incrFile.beg);
        PAGE * pagep = (PAGE *) pagebuf;

        incrFile.write((char *) &(LSN(pagep).file), 4);
        incrFile.write((char *) &(LSN(pagep).offset), 4);
        incrFile.write((char *) &cksum, 4);

        ssize_t bytes_written = writeall(1, pagebuf, pagesize);
        if(bytes_written != pagesize){
            std::ostringstream ss;
            ss << "error writing text data: " << std::strerror(errno);
            throw SerialiseError(filename, ss.str());
        }

        total_read += bytes_read;
        total_written += bytes_written;
        if(total_written != total_read){
            std::ostringstream ss;
            ss << "different amounts written and read. wrote " << total_written
                << " bytes and read " << total_read << " bytes "
                << std::strerror(errno);
            throw SerialiseError(filename, ss.str());
        }
    }

    if (pagebuf)
        free(pagebuf);

    return total_read;
}


std::string getDTString() {
    time_t rawtime;
    struct tm *timeinfo;
    char buffer[80];

    time(&rawtime);
    timeinfo = localtime(&rawtime);

    strftime(buffer, sizeof(buffer), "%Y%m%d%I%M%S", timeinfo);
    std::string str(buffer);

    return str;
}

void incr_deserialise_database(
    const std::string lrldestdir,
    const std::string datadestdir,
    std::set<std::string>& table_set,
    unsigned percent_full,
    bool force_mode,
    bool legacy_mode,
    bool& is_disk_full,
    const std::string& incr_path
)
{
    std::map<std::string, std::pair<size_t, std::vector<uint32_t>>> manifest_map;

    while(true) {

        // Read the tar block header
        tar_block_header head;
        if(readall(0, head.c, sizeof(head.c)) != sizeof(head.c)) {
            // Failed to read a full block header
            std::ostringstream ss;
            ss << "Error reading tar block header: "
                << errno << " " << strerror(errno);
            throw Error(ss);
        }

        // If the block is entirely blank then we're done with the increment
        if(std::memcmp(head.c, zero_head, 512) == 0) {
            continue;
        }

        // Get the file name
        if(head.h.filename[sizeof(head.h.filename) - 1] != '\0') {
            throw Error("Bad block: filename is not null terminated");
        }
        const std::string filename(head.h.filename);

        // Try to find this file in our manifest
        std::map<std::string, FileInfo>::const_iterator manifest_it = manifest_map.find(filename);

        // Get the file size
        unsigned long long filesize;
        if(!read_octal_ull(head.h.size, sizeof(head.h.size), filesize)) {
            throw Error("Bad block: bad size");
        }
        unsigned long long nblocks = (filesize + 511ULL) >> 9;


        // If this is an .lrl file then we have to read it into memory and
        // then rewrite it to disk.  In getting the extension it is important
        // to use the **first** dot, as this will affect the data file
        // recognition code below where we don't want "fstblk.dta" to
        // match the pattern "dta*"
        std::string ext;
        size_t dot_pos = filename.find_first_of('.');
        bool is_lrl = false;
        if(dot_pos != std::string::npos) {
            ext = filename.substr(dot_pos + 1);
        }
        if(ext == "lrl") {
            is_lrl = true;
        }

        bool is_manifest = false;
        if(filename == "MANIFEST") {
            is_manifest = true;
        }

        if(filename == "INCR_MANIFEST) {
            n


        // Gather the text of the file for lrls and manifests
        std::string text;
        bool is_text = is_lrl || is_manifest;

        // If it's not a text file then look at the extension to see if it looks
        // like a data file.  If it does then add it to our list of tables if
        // not already present.  I was going to trust the lrl file for this
        // (and use the dbname_file_vers_map for llmeta dbs) but the old
        // comdb2backup script doesn't serialise the file_vers_map, so I can't
        // do that yet. This way the onus is on the serialising side to get
        // the list of files right.
        if(!is_text && filename.find_first_of('/') == std::string::npos) {
            bool is_data_file = false;
            bool is_queue_file = false;
            bool is_queuedb_file = false;
            std::string table_name;

            if(recognise_data_file(filename, false, is_data_file,
                        is_queue_file, is_queuedb_file, table_name) ||
               recognise_data_file(filename, true, is_data_file,
                        is_queue_file, is_queuedb_file, table_name)) {
                if(table_set.insert(table_name).second) {
                    std::clog << "Discovered table " << table_name
                        << " from data file " << filename << std::endl;
                }
            }
        }

        std::unique_ptr<fdostream> of_ptr;

        if(is_text) {
            text.reserve(filesize);
        } else {
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

            bool direct = false;

            if (manifest_it != manifest_map.end() && manifest_it->second.get_type() == FileInfo::BERKDB_FILE)
                direct = 1;

            std::string outfilename(datadestdir + "/" + filename);
            of_ptr = output_file(outfilename, false, direct);
            extracted_files.insert(outfilename);
        }


        // Determine buffer size to read this data in.  If there is a known
        // page size then use that so that we can verify checksums as we go.
        size_t pagesize = 0;
        bool checksums = false;
        file_is_sparse = false;
        if(manifest_it != manifest_map.end()) {
            pagesize = manifest_it->second.get_pagesize();
            checksums = manifest_it->second.get_checksums();
            file_is_sparse = manifest_it->second.get_sparse();
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


            if(is_text)
            {
               text.append((char*) &buf[0], readbytes);
            }
            else if (file_is_sparse &&
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
                     if((of_ptr->skip(skipped_bytes)))
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
                  if (!of_ptr->write((char*) buf, pagesize))
                  {
                     std::ostringstream ss;

                     if (filename == "FLUFF")
                        return;

                     ss << "Error Writing " << filename << " after "
                        << (filesize - bytesleft) << " bytes";
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
                  if((of_ptr->skip(skipped_bytes)))
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
                  if (!of_ptr->write((char*) &buf[off], lim))
                  {
                     std::ostringstream ss;

                     if (filename == "FLUFF")
                        return;

                     ss << "Error Writing " << filename << " after "
                        << (filesize - bytesleft) << " bytes";
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

        std::clog << "x " << filename << " size=" << filesize
                  << " pagesize=" << pagesize;

        if (file_is_sparse)
           std::clog << " SPARSE ";
        else
           std::clog << " not sparse ";


        std::clog << std::endl;

        if(checksum_failure && !force_mode) {
            std::ostringstream ss;
            ss << "Checksum verification failures in " << filename;
            throw Error(ss);
        }

        if(is_manifest) {
            process_manifest(text, manifest_map, origlrlname, options);

        } else if(is_lrl) {

            std::string outfilename;
            // If the lrl was renames, restore the orginal name
            if (origlrlname != "")
               outfilename = (lrldestdir + "/" + origlrlname);
            else
               outfilename = (lrldestdir + "/" + filename);

            // Parse the lrl file and then write it out
            of_ptr = output_file(outfilename, true, false);
            if(main_lrl_file.empty()) {
                main_lrl_file = outfilename;
            }

            process_lrl(
                    *of_ptr,
                    filename,
                    text,
                    strip_cluster_info,
                    strip_consumer_info,
                    datadestdir,
                    lrldestdir,
                    dbname,
                    table_set);

            extracted_files.insert(outfilename);

            if (!datadestdir.empty())
                make_dirs(datadestdir);
            if (!datadestdir.empty() && check_dest_dir(datadestdir)) {
                /* Remove old log files.  This used to remove all files in the directory,
                   which can be problematic if hi. */
                if (!legacy_mode) {
                   // remove files in the txn directory
                   std::string dbtxndir(datadestdir + "/" + dbname + ".txn");
                   make_dirs( dbtxndir );
                   remove_all_old_files( dbtxndir );
                   dbtxndir = datadestdir + "/" + "logs";
                   make_dirs( dbtxndir );
                   remove_all_old_files( dbtxndir );

                }
            }
        }
    }
}
