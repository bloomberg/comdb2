#include <cstring>

#include "comdb2ar.h"
#include "error.h"
#include "file_info.h"
#include "fdostream.h"
#include "lrlerror.h"
#include "tar_header.h"
#include "riia.h"
#include "increment.h"
#include "util.h"

#include <cstdlib>
#include <map>
#include <set>
#include <string>
#include <sstream>
#include <iostream>
#include <fstream>
#include <vector>
#include <memory>

#include <sys/stat.h>
#include <sys/types.h>
#include <sys/statvfs.h>
#include <dirent.h>

#include <sys/wait.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <ctype.h>

/* check once a megabyte */
#define FS_PERIODIC_CHECK (10 * 1024 * 1024)

static bool check_dest_dir(const std::string& dir);

static void remove_old_files(const std::list<std::string>& dirlist,
        const std::set<std::string>& extracted_files,
        const std::string& pattern)
// Remove files in the directory listing dirlist that match the given file
// pattern, provided that we have not already overwritten them with new files
// (i.e. do not remove any file that appears in the extracted_files set).
// All inputs to this function should be absolute paths.
// pattern should be an absolute path optionally ending in '*' to indicate
// a wildcard.
{
    size_t matchlen = pattern.length();
    bool exact_match = true;
    if(matchlen > 0 && pattern[matchlen-1] == '*') {
        exact_match = false;
        matchlen--;
    }
    for(std::list<std::string>::const_iterator ii = dirlist.begin();
            ii != dirlist.end();
            ++ii) {

        if((exact_match && ii->compare(pattern)==0)
                ||
           (!exact_match && ii->compare(0, matchlen, pattern, 0, matchlen)==0)
           ) {

            if(extracted_files.find(*ii) == extracted_files.end()) {
                if(-1 == unlink(ii->c_str())) {
                    std::cerr << "Error unlinking " << *ii << ": "
                        << strerror(errno) << std::endl;
                } else {
                    std::clog << "unlinked " << *ii << std::endl;
                }
            }
        }
    }
}



bool read_octal_ull(const char *str, size_t len, unsigned long long& number)
{
    if(*str == '\200') {
        /* This is encoded in base 256 as per the GNU extensions */
        number = 0;
        len--;
        str++;
        while(len) {
            number = (number << 8) | *((unsigned char *)str);
            len--;
            str++;
        }
    } else {
        char *endptr = NULL;

        if(str[len - 1] != '\0') {
            return false;
        }

        errno = 0;
        number = strtoull(str, &endptr, 8);
        if(errno != 0 || !endptr || *endptr != '\0') {
            return false;
        }
    }
    return true;
}


static void process_manifest(
        const std::string& text,
        std::map<std::string, FileInfo>& manifest_map,
        bool& run_full_recovery,
        std::string& origlrlname,
        std::vector<std::string> &options
        )
// Decode the manifest into a map of files and their associated file info
{
    FileInfo tmp_file;

    std::istringstream in(text);
    std::string line;

    int lineno = 0;
    while(!in.eof()) {
        std::getline(in, line);
        lineno++;

        size_t pos = line.find_first_of('#');
        if(pos != std::string::npos) {
            line.resize(pos);
        }

        std::istringstream ss(line);
        std::string tok;

        if(ss >> tok) {
            if(tok == "File") {
                if(read_FileInfo(line, tmp_file)) {
                    manifest_map[tmp_file.get_filename()] = tmp_file;
                    tmp_file.reset();
                } else {
                    std::clog << "Bad File directive on line "
                        << lineno << " of MANIFEST" << std::endl;
                }
            } else if(tok == "SupportFilesOnly") {
                // If serialisation contains sources only then no need to run
                // full recovery.
                run_full_recovery = false;
            } else if(tok == "OrigLrlFile") {

               // llmeta renames the lrl file
               // metafile contains the original file name
               // correct this here
               ss >> origlrlname;
            } else if (tok == "Option") {
                while (ss >> tok) {
                    options.push_back(tok);
                }
            } else {
                std::clog << "Unknown directive '" << tok << "' on line "
                    << lineno << " of MANIFEST" << std::endl;
            }
        }
    }
}


static void process_lrl(
        std::ostream& of,
        const std::string& filename,
        const std::string& lrltext,
        bool strip_cluster_info,
        bool strip_consumer_info,
        std::string& datadestdir,
        const std::string& lrldestdir,
        std::string& dbname,
        std::set<std::string>& table_set
        )
// Parse the lrl file that has been read from the stream, set database
// parameters as they are discovered, and write out a rewritten lrl file
// to the output stream of.  dir, table and resource lines will be rewritten
// with the new data directory.
// dbname and datadestdir, if empty, will be populated with the values read
// from the lrltext.
// table_set will be updated with the names of any tables found
{
    std::istringstream lrlss(lrltext);
    int lineno = 0;
    while(!lrlss.eof()) {
        std::string line;
        std::getline(lrlss, line);
        lineno++;

        std::string bareline(line);
        size_t pos = bareline.find_first_of('#');
        if(pos != std::string::npos) {
            bareline.resize(pos);
        }
        std::istringstream liness(bareline);

        std::string tok;
        if(liness >> tok) {

            if(tok == "name" && dbname.empty()) {
                // Get the db name from the first lrl file found,
                // which should be the primary.
                if(!(liness >> dbname)) {
                    throw LRLError(filename, lineno, "missing database name directive");
                }
            } else if(tok == "dir") {
                if(!(liness >> tok)) {
                    throw LRLError(filename, lineno, "missing database directory");
                }
                // If we don't have a directory yet then use this one
                if(datadestdir.empty()) {
                    datadestdir = tok;
                }
                line = "dir " + datadestdir;

            } else if(tok == "table") {
                std::string name;
                std::string schema_path, new_path;
                std::string dbnum;
                if(!(liness >> name)) {
                    throw LRLError(filename, lineno, "missing table name");
                }
                if(!(liness >> schema_path)) {
                    throw LRLError(filename, lineno, "missing table schema path");
                }
                liness >> dbnum; // optional database number

                std::string ext;
                size_t dot_pos = schema_path.find_last_of('.');
                if(dot_pos != std::string::npos) {
                    ext = schema_path.substr(dot_pos + 1);
                }
                makebasename(schema_path);
                if(ext == "lrl") {
                    // lrl files get put in the lrl destination dir..
                    makeabs(new_path, lrldestdir, schema_path);
                } else {
                    if(datadestdir.empty()) {
                        throw LRLError(filename, lineno, "table directive before dir; cannot infer data directory");
                    }
                    makeabs(new_path, datadestdir, schema_path);
                }

                line = "table " + name + " " + new_path + " " + dbnum;

                table_set.insert(name);

            } else if(tok == "resource") {
                std::string path, new_path, name;
                if(!(liness >> name >> path)) {
                    throw LRLError(filename, lineno, "missing resource name or path");
                }

                if(datadestdir.empty()) {
                    throw LRLError(filename, lineno, "resource directive before dir; cannot infer data directory");
                }

                makebasename(path);
                makeabs(new_path, datadestdir, path);

                line = "resource " + name + " " + new_path;
            } else if(tok == "spfile") {
                std::string path,new_path;
                if(!(liness >> path)) {
                    throw LRLError(filename, lineno, "missing spfile");
                }
                makebasename(path);
                makeabs(new_path, datadestdir, path);
                line = "spfile " + new_path;
            } else if(tok == "cluster" && (liness >> tok)
                    && tok == "nodes" && strip_cluster_info) {
                // Strip this line from the output.  Since this test above
                // changes tok, it needs to be the last test
                line.insert(0, "# ");
            } else if (strip_consumer_info) {
                if (tok == "if") {
                    liness >> tok; /* consume machine type */
                    liness >> tok; /* read real option */
                }

                if (tok == "queue" || tok == "procedure" || tok == "consumer")
                    line.insert(0, "# ");
            }
        }

        if(!(of << line << std::endl)) {
            std::ostringstream ss;
            ss << "Error writing " << filename;
            throw Error(ss);
        }
    }
}

static bool empty_dir(const std::string& dir) {
    DIR *d;
    struct dirent *f;
    d = opendir(dir.c_str());
    if (d == NULL)
        return false;
    while ((f = readdir(d)) != NULL) {
        if (strcmp(f->d_name, ".") == 0 || strcmp(f->d_name, "..") == 0)
            continue;
        return false;
    }
    return true;
}

static bool check_dest_dir(const std::string& dir)
// REALLY REALLY REALLY important safety check - without this
// we may end up deleting *.dta from /bb/data!
// Only allow 3rd level subdirectories or greater (e.g. /bb/data/mydb)
{
    size_t pos = 0;
    int levels = 0;

    /* skip the check if the destination directory is empty */
    if (empty_dir(dir))
        return false;

    while(pos < dir.length()) {
        size_t match = dir.find('/', pos);

        if(match == std::string::npos) {
            // No more matches
            match = dir.length();
        }

        if(match > pos) {
            levels++;
        }

        pos = match + 1;
    }

    if(levels < 3) return false;

    return true;
}

#define write_size (1000*1024)

void deserialise_database(
        const std::string *p_lrldestdir,
        const std::string *p_datadestdir,
        bool strip_cluster_info,
        bool strip_consumer_info,
        bool run_full_recovery,
        const std::string& comdb2_task,
        unsigned percent_full,
        bool force_mode,
        bool legacy_mode,
        bool& is_disk_full,
        bool run_with_done_file,
        bool incr_mode
)
// Deserialise a database from serialised from received on stdin.
// If lrldestdir and datadestdir are not NULL then the lrl and data files
// will be placed accordingly.  Otherwise the lrl file and the data files will
// be placed in the same directory as specified in the serialised form.
// The lrl file written out will be updated to reflect the resulting directory
// structure.  If the destination disk reaches or exceeds the specified
// percent_full during the deserialisation then the operation is halted.
{
    static const char zero_head[512] = {0};
    int stlen;
    is_disk_full = false;
    void *empty_page = NULL;
    unsigned long long skipped_bytes = 0;
    int rc =0;
    std::vector<std::string> options;

    bool file_is_sparse = false;

    std::string fingerprint;

    std::string done_file_string;

    // This is the directory listing of the data directory taken at the time
    // when we find the lrl file in out input stream and can therefore fix
    // the data directory.
    std::list<std::string> data_dir_files;

    // The absolute path names of the files that we have already extracted.
    std::set<std::string> extracted_files;

    // List of known tables
    std::set<std::string> table_set;

    std::string datadestdir;


    empty_page = malloc(65536);
    memset(empty_page, 0, 65536);

    if (p_datadestdir == NULL && p_lrldestdir)
        p_datadestdir = p_lrldestdir;

    if(p_datadestdir) {
        datadestdir = *p_datadestdir;
        stlen=datadestdir.length();
        if( datadestdir[stlen - 1] == '/' )
        {
            datadestdir.resize( stlen - 1 );
        }
    }
    const std::string& lrldestdir = p_lrldestdir ? *p_lrldestdir : datadestdir;

    bool inited_txn_dir = false;
    std::string copylock_file;
    std::string main_lrl_file;
    std::string dbname;
    std::string origlrlname("");

    std::string sha_fingerprint = "";

    // The manifest map
    std::map<std::string, FileInfo> manifest_map;

    if (run_with_done_file)
    {
       /* remove the DONE file before we start copying */
       done_file_string = datadestdir + "/DONE";
       unlink(done_file_string.c_str());
    }


    while(true) {

        if(!datadestdir.empty() && !dbname.empty() && !inited_txn_dir) {
            // Init the .txn dir.  We need to make sure that we remove:
            // * any queue extent files lying around
            // * any log files lying around
            // * __db.rep.db
            // Any database like files from the destination directory dir

            if(datadestdir[0] != '/') {
                std::ostringstream ss;
                ss << "Cannot deserialise into " << datadestdir
                    << " - destination directory must be an absolute path";
                throw Error(ss);
            }

            // Create the copylock file to indicate that this copy isn't
            // done yet.
            copylock_file = lrldestdir + "/" + dbname + ".copylock";
#if 0
            struct stat statbuf;
            rc = stat(copylock_file.c_str(), &statbuf);
            if (rc == 0)
            {
               std::ostringstream ss;
               ss << "Error copylock file exists " << copylock_file
                  << " " << std::strerror(errno);
               throw Error(ss);
            }
#endif
            unlink(copylock_file.c_str());
            int fd = creat(copylock_file.c_str(), 0666);
            if(fd == -1) {
                std::ostringstream ss;
                ss << "Error creating copylock file " << copylock_file
                    << " " << strerror(errno);
                throw Error(ss);
            }

            inited_txn_dir = true;
        }

        // Read the tar block header
        tar_block_header head;
        if(readall(0, head.c, sizeof(head.c)) != sizeof(head.c)) {
            // Failed to read a full block header
            std::ostringstream ss;
            ss << "Error reading tar block header: "
                << errno << " " << strerror(errno);
            throw Error(ss);
        }

        // If the block is entirely blank then we're done
        // Alternativelyh, if we're running in incremental mode, then
        // we know we are moving on the the incremental backups
        if(std::memcmp(head.c, zero_head, 512) == 0) {
            if(incr_mode){
                std::clog << "Done with base backup, moving on to increments"
                          << std::endl << std::endl;

                incr_deserialise_database(
                    lrldestdir,
                    datadestdir,
                    dbname,
                    table_set,
                    sha_fingerprint,
                    percent_full,
                    force_mode,
                    options,
                    is_disk_full
                );
            }
            break;
        }

        // TODO: verify the block check sum

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

        if(ext == "sha") {
            sha_fingerprint = read_serialised_sha_file();
            continue;
        }

        bool is_manifest = false;
        if(filename == "MANIFEST") {
            is_manifest = true;
        }

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
            process_manifest(text, manifest_map, run_full_recovery, origlrlname, options);

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

    // If we never inited the txn dir then we must never have had a valid lrl;
    // fail in this case.
    if(!inited_txn_dir) {
        throw Error("No valid lrl file seen or txn dir not inited");
    }

    // Run full recovery
    if(run_full_recovery) {
        std::ostringstream cmdss;

        if (run_with_done_file)
           cmdss << "comdb2_stdout_redir ";
        
        cmdss<< comdb2_task << " " 
             << dbname << " -lrl "
             << main_lrl_file 
             << " -fullrecovery";

        for (int i = 0; i < options.size(); i++) {
            cmdss << " " << options[i];
        }

        std::clog << "Running full recovery: " << cmdss.str() << std::endl;

        errno = 0;
        int rc = std::system(cmdss.str().c_str());
        if(rc != 0) {
            std::ostringstream ss;
            ss << "Full recovery command '" << cmdss.str() << "' failed rc "
                << rc << " errno " << errno << std::endl;
            throw Error(ss);
        }
        std::clog << "Full recovery successful" << std::endl;
    }




    // Remove the copylock file
    if(!copylock_file.empty()) {
        unlink(copylock_file.c_str());
    }

    // If it's a nonames db, remove the txn directory
    if (empty_dir(datadestdir + "/" + dbname + ".txn"))
        rmdir((datadestdir + "/" + dbname + ".txn").c_str());

    std::string fluff_file;
    fluff_file = datadestdir + "/FLUFF";
    unlink(fluff_file.c_str());
}
