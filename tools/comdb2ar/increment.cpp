#include "comdb2ar.h"

#include "increment.h"
#include "glue.h"
#include "db_wrap.h"
#include "serialiseerror.h"
#include "error.h"
#include "tar_header.h"
#include "riia.h"

#include <sys/stat.h>
#include <fstream>
#include <cstring>
#include <sstream>
#include <iostream>
#include <cassert>

#include <vector>
#include <set>
#include <map>
#include <utility>
#include <fcntl.h>
#include <unistd.h>

bool is_not_incr_file(std::string filename){
    return((filename.substr(filename.length() - 5) != ".incr") &&
            (filename.substr(filename.length() - 4) != ".sha"));
}

// Read the SHA file to get the fingerprint of the previous increment
std::string get_sha_fingerprint(std::string filename, std::string incr_path) {
    std::string filepath = incr_path + "/" + filename;
    std::ifstream ifs (filepath, std::ifstream::in);

    char sha_buffer[40];
    ifs.read(&sha_buffer[0], 40);

    std::clog << "Found previous fingerprint as " << std::string(sha_buffer) << std::endl;

    return std::string(sha_buffer);
}

// Read from STDIN a serialised text file with the SHA fingerprint
std::string read_serialised_sha_file() {
    char fingerprint[40];

    size_t bufsize = 4096;
    uint8_t *buf;
#if defined _HP_SOURCE || defined _SUN_SOURCE
    buf = (uint8_t*) memalign(512, bufsize);
#else
    if (posix_memalign((void**) &buf, 512, bufsize))
        throw Error("Failed to allocate output buffer");
#endif
    RIIA_malloc free_guard(buf);

    if(readall(0, &fingerprint[0], 40) != 40){
        std::ostringstream ss;
        ss << "Could not read sha fingerprint file";
        throw Error(ss);
    }

    // Read and discard the null padding
    unsigned long long nblocks = (40 + 511ULL) >> 9;
    unsigned long long padding_bytes = (nblocks << 9) - 40;
    if(padding_bytes) {
        if(readall(0, &buf[0], padding_bytes) != padding_bytes) {
            std::ostringstream ss;

            ss << "Error reading padding after in SHA file";
            throw Error(ss);
        }
    }

    std::clog << "Read SHA as: " << std::string(fingerprint, 40) << std::endl;
    return std::string(fingerprint, 40);
}

// Determine whether the file has changed, ie if the page's current LSN + Checksum
// is the same as it is in the .incr diff file
bool assert_cksum_lsn(FileInfo &file, uint8_t *new_pagep, uint8_t *old_pagep, size_t pagesize) {
    uint32_t new_lsn_file = LSN(new_pagep).file;
    uint32_t new_lsn_offs = LSN(new_pagep).offset;
    bool verify_ret;
    uint32_t new_cksum;
    bool crypto = file.get_crypto();
    bool swapped = file.get_swapped();
    verify_checksum(new_pagep, pagesize, crypto, swapped, &verify_ret, &new_cksum);

    uint8_t cmp_arr[12];
    for (int i = 0; i < 4; ++i){
        cmp_arr[i] = ((uint8_t *) &new_lsn_file)[i];
        cmp_arr[4 + i] = ((uint8_t *) &new_lsn_offs)[i];
        cmp_arr[8 + i] = ((uint8_t *) &new_cksum)[i];
    }

    return (memcmp(cmp_arr, old_pagep, 12) != 0);
}

// Compare the page with the diff file to determine whether it has changed - driver
// For each file, populate pages with the page numbers fo the changed pages
// populate data_size with the total amount of data that needs to be serialised
// Returns true if any part of the file needs to be serialised
bool compare_checksum(
    FileInfo &file,
    const std::string& incr_path,
    std::vector<uint32_t>& pages,
    ssize_t *data_size,
    std::set<std::string>& incr_files
) {
    std::string filename = file.get_filename();
    std::string incr_file_name = incr_path + "/" + filename + ".incr";

    std::cerr << filename << " <- " << incr_file_name << std::endl;

    // Keep a set of all .incr diff files to determine if there are any remaining at the end
    // Any remaining .incr files indiciate deleted files
    std::set<std::string>::iterator it = incr_files.find(filename + ".incr");
    if(it != incr_files.end()){
        incr_files.erase(it);
    }

    struct stat sb;
    bool ret = false;

    int flags = O_RDONLY;
    int new_fd = open(file.get_filepath().c_str(), flags);
    int old_fd = open(incr_file_name.c_str(), O_RDWR);

    struct stat new_st;
    if(fstat(new_fd, &new_st) == -1) {
        std::ostringstream ss;
        ss << "cannot stat file: " << std::strerror(errno);
        throw SerialiseError(filename, ss.str());
    }

    // wrong, but a good estimate, and good enough if we truncate BEFORE
    // writing pages on restore
    file.set_filesize(new_st.st_size); 
    {
        std::ostringstream ss;
        ss << "ls -l " << file.get_filepath() << " >&2";
        system(ss.str().c_str());
        std::cerr << "stat says size " << new_st.st_size << std::endl;
    }

    // For a new file, make pages empty (upon returning, an empty pages list
    // denotes a new file) and mark the size as the full size
    if(stat(incr_file_name.c_str(), &sb) != 0) {
        std::clog << "New File: " << filename << std::endl;
        *data_size = new_st.st_size;
        return true;
    // For returning files, go through each page and compare with the diff files
    } else {

        size_t pagesize = file.get_pagesize();
        if(pagesize == 0) {
            pagesize = 4096;
        }

        uint8_t *new_pagebuf = NULL;
        uint8_t old_pagebuf[12];
        bool file_expanded = false;

#if ! defined  ( _SUN_SOURCE ) && ! defined ( _HP_SOURCE )
        if(posix_memalign((void**) &new_pagebuf, 512, pagesize))
            throw Error("Failed to allocate output buffer");
#else
        new_pagebuf = (uint8_t*) memalign(512, pagesize);
#endif

        size_t original_size = new_st.st_size;
        off_t bytesleft = new_st.st_size;
        int64_t filesize = 0;
        int64_t pgno = -1;

        while(bytesleft >= pagesize) {
            ssize_t new_bytesread = read(new_fd, &new_pagebuf[0], pagesize);

            filesize += new_bytesread;
            pgno += 1;

            if(new_bytesread <= 0) {
                std::ostringstream ss;
                ss << "read error after " << new_bytesread << " bytes";
                throw SerialiseError(filename, ss.str());
            }

            // File has expamded by a reasonable amount, so add all remaining pages
            if(file_expanded) {
                pages.push_back(pgno);
                *data_size += pagesize;
                bytesleft -= pagesize;
                continue;
            }

            ssize_t old_bytesread = read(old_fd, &old_pagebuf[0], 12);

            if(old_bytesread < 0){
                std::ostringstream ss;
                ss << "read error after " << new_bytesread << " bytes";
                throw SerialiseError(filename, ss.str());
            }

            if(old_bytesread == 0) {
                // If the size has more than multiplied by a factor of 10, just treat it as a new file
                if ((float) bytesleft / (float) original_size > .9) {
                    pages.clear();
                    return true;
                // otherwise just add the remaining pages
                } else {
                    pages.push_back(pgno);
                    file_expanded = true;
                    *data_size += pagesize;
                    bytesleft -= pagesize;
                    ret = true;
                    continue;
                }
            }

            // If a diff has been selected in a page, keep track of that page
            if (assert_cksum_lsn(file, new_pagebuf, old_pagebuf, pagesize)) {
                pages.push_back(pgno);
                *data_size += pagesize;
                ret = true;
            }

            bytesleft -= pagesize;
        }

        if(new_pagebuf) free(new_pagebuf);
    }
    return ret;
}

ssize_t serialise_incr_file(
    const FileInfo& file,
    std::vector<uint32_t> pages,
    std::string incr_path
)
// For a file with changed pages, go through each changed page and serialise it
{
    const std::string& filename = file.get_filename();
    const std::string& filepath = file.get_filepath();

    int flags;
    bool skip_iomap = false;
    std::ostringstream ss;

    // Ensure large file support
    assert(sizeof(off_t) == 8);

    struct stat st;
    if(stat(filepath.c_str(), &st) == -1) {
        ss << "cannot stat file: " << std::strerror(errno);
        throw SerialiseError(filename, ss.str());
    }

    size_t pagesize = file.get_pagesize();
    if(pagesize == 0) {
        pagesize = 4096;
    }

    std::ifstream ifs (filepath, std::ifstream::in | std::ifstream::binary);

    char *char_pagebuf = NULL;
    uint8_t *pagebuf = NULL;

#if ! defined  ( _SUN_SOURCE ) && ! defined ( _HP_SOURCE )
    if(posix_memalign((void**) &pagebuf, 512, pagesize))
        throw Error("Failed to allocate output buffer");
#else
    pagebuf = (uint8_t*) memalign(512, pagesize);
#endif

    ssize_t total_read = 0;
    ssize_t total_written = 0;
    int retry = 5;

    std::string incrFilename = incr_path + "/" + filename + ".incr";

    std::ofstream incrFile(incrFilename, std::ofstream::out |
                            std::ofstream::in | std::ofstream::binary);

    for(std::vector<uint32_t>::iterator
            it = pages.begin();
            it != pages.end();
            ++it){
tryagain:
        ifs.seekg(pagesize * *it, ifs.beg);
        ifs.read((char *)&pagebuf[0], pagesize);
        ssize_t bytes_read = ifs.gcount();
        if(bytes_read < pagesize) {
            std::ostringstream ss;
            ss << "read error";
            throw SerialiseError(filename, ss.str());
        }

        uint32_t cksum;
        bool cksum_verified = false;
        bool crypto = file.get_crypto();
        bool swapped = file.get_swapped();
        verify_checksum(pagebuf, pagesize, crypto, swapped,
                            &cksum_verified, &cksum);
        if(!cksum_verified){
            if(--retry == 0) {
                std::ostringstream ss;
                ss << "serialise_file:page failed checksum verification";
                throw SerialiseError(filename, ss.str());
            }

            goto tryagain;
        }

        retry = 5;

        // Update the diff .incr file
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

    std::clog << "a " << filename << " pages=[";
    for(size_t i = 0; i < pages.size(); ++i){
        std::clog << pages[i];
        if(i != pages.size() - 1){
            std::clog << " ";
        } else {
            std::clog << "] ";
        }
    }
    std::clog << "pagesize=" << pagesize << std::endl;

    return total_read;
}

// Get the datetime as a string YYYYMMDDHHMMSS
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
    const std::string& lrldestdir,
    const std::string& datadestdir,
    const std::string& dbname,
    std::set<std::string>& table_set,
    std::string& sha_fingerprint,
    unsigned percent_full,
    bool force_mode,
    std::vector<std::string>& options,
    bool& is_disk_full,
    bool& dryrun
)
// Read from STDIN to deserialise an incremental backup
{
    static const char zero_head[512] = {0};

    bool manifest_read = false;
    bool done_with_incr = true;

    std::map<std::string, FileInfo> new_files;
    std::map<std::string, std::pair<FileInfo, std::vector<uint32_t> > > updated_files;
    std::set<std::string> deleted_files;
    std::vector<std::string> file_order;

    while(true) {

        // Read the tar block header
        tar_block_header head;
        size_t bytes_read = readall(0, head.c, sizeof(head.c));
        if(bytes_read == 0){
            // Lazy check that all increments are done (ie STDIN has no additional input)
            break;
        } else if(bytes_read != sizeof(head.c)) {

            // Failed to read a full block header
            std::ostringstream ss;
            ss << "Error reading tar block header: "
                << errno << " " << strerror(errno);
            throw Error(ss);
        }

        // If the block is entirely blank then we're done with the increment
        if(std::memcmp(head.c, zero_head, 512) == 0) {
            if(!done_with_incr){
                std::clog << "done with increment" << std::endl << std::endl;
                done_with_incr = true;
                new_files.clear();
                updated_files.clear();
                deleted_files.clear();
                file_order.clear();
            }
            continue;
        }

        done_with_incr = false;

        // Get the file name
        if(head.h.filename[sizeof(head.h.filename) - 1] != '\0') {
            throw Error("Bad block: filename is not null terminated");
        }
        std::string filename(head.h.filename);

        // Get the file size
        unsigned long long filesize;
        if(!read_octal_ull(head.h.size, sizeof(head.h.size), filesize)) {
            throw Error("Bad block: bad size");
        }

        bool is_manifest = false;
        if(filename == "INCR_MANIFEST") {
            is_manifest = true;
        }

        std::string ext;
        size_t dot_pos = filename.find_first_of('.');
        if(dot_pos != std::string::npos) {
            ext = filename.substr(dot_pos + 1);
        }

        bool is_incr_data = false;
        bool is_data_file = false;
        bool is_queue_file = false;
        bool is_queuedb_file = false;

        if(ext == "data") {
            is_incr_data = true;
        } else if (ext == "sha") {
            sha_fingerprint = read_serialised_sha_file();
            std::clog << "Fingerprint: " << sha_fingerprint << std::endl;
            continue;
        } else {
            // If it's not a text file then look at the extension to see if it looks
            // like a data file.  If it does then add it to our list of tables if
            // not already present.  I was going to trust the lrl file for this
            // (and use the dbname_file_vers_map for llmeta dbs) but the old
            // comdb2backup script doesn't serialise the file_vers_map, so I can't
            // do that yet. This way the onus is on the serialising side to get
            // the list of files right.
            if(filename.find_first_of('/') == std::string::npos) {
                std::string table_name;

                if(recognize_data_file(filename, is_data_file,
                            is_queue_file, is_queuedb_file, table_name)) {
                    if(table_set.insert(table_name).second) {
                        std::clog << "Discovered table " << table_name
                            << " from data file " << filename << std::endl;
                    }
                }
            }
        }

        if(is_manifest) {

            // Manifest is first file in each increment, so if one is seen,
            // delete previous logs
            std::clog << "Deleting old log files" << std::endl;
            clear_log_folder(datadestdir, dbname);

            manifest_read = true;
            std::string manifest_sha;
            std::string manifest_text = read_incr_manifest(filesize);
            options.clear();
            if(!process_incr_manifest(manifest_text, datadestdir,
                    updated_files, new_files, deleted_files,
                    file_order, options, manifest_sha)){
                // Failed to read a coherant manifest
                std::ostringstream ss;
                ss << "Error reading manifest";
                throw Error(ss);
            }

            if(strncmp(manifest_sha.c_str(), sha_fingerprint.c_str(), 40) != 0){
                std::ostringstream ss;
                ss << "Mismatched SHA fingerprints: " << std::endl;
                ss << "Found: " << sha_fingerprint << std::endl;
                ss << "Manifest: " << manifest_sha;
                throw Error(ss);
            }
        // All incremental changes are stored in a single .data file, so unpack that
        // using the file order to keep track of which file is currently being read
        } else if(is_incr_data) {
            unpack_incr_data(file_order, updated_files, datadestdir, dryrun);
        // Unpack a full file
        } else if (is_data_file || is_queue_file || is_queuedb_file) {
            std::map<std::string, FileInfo>::iterator file_it = new_files.find(filename);

            if(file_it == new_files.end()){
                std::ostringstream ss;
                ss << "Unable to locate file " << filename
                   << " in manifest map during incremental update" << std::endl;
                throw Error(ss);
            }

            unpack_full_file(&(file_it->second), filename, filesize, datadestdir,
                   true, percent_full, is_disk_full);


        } else {
            // ASSUMING THIS IS LOG OR CHECKPOINT FILE
            unpack_full_file(NULL, filename, filesize, datadestdir, false,
                    percent_full, is_disk_full);
        }

        // Delete deleted files
        handle_deleted_files(deleted_files, datadestdir, table_set);
    }
}
