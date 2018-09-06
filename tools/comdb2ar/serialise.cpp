#include "comdb2ar.h"

#include "db_wrap.h"
#include "error.h"
#include "file_info.h"
#include "logholder.h"
#include "repopnewlrl.h"
#include "lrlerror.h"
#include "riia.h"
#include "serialiseerror.h"
#include "tar_header.h"
#include "increment.h"
#include "util.h"

#include <cassert>
#include <cstring>
#include <fstream>
#include <iostream>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <utility>
#include <vector>
#include <algorithm>

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
extern "C" {
#include <unistd.h>
}
#include <poll.h>
#include <sys/mman.h>
#include <time.h>
#include <openssl/sha.h>
#include <iomanip>
#include <sys/stat.h>

#include <stdlib.h>

#if defined(_AIX)
#define DO_DIRECT O_DIRECT
#elif defined (__linux__)
#define DO_DIRECT O_DIRECT
#else
#define DO_DIRECT 0
#endif



static void writepadding(size_t nbytes)
{
    static const char zeroes[1024] = {0};
    while(nbytes > 0) {
        ssize_t num = nbytes;
        if(num > sizeof(zeroes)) {
            num = sizeof(zeroes);
        }
        if(writeall(1, zeroes, num) != num) {
            std::ostringstream ss;
            ss << "error writing zero padding: " << std::strerror(errno);
            throw Error(ss);
        }
        nbytes -= num;
    }
}


static void serialise_string(const std::string& filename,
        const std::string& data)
{
    struct stat st;

    st.st_mode = 0664;
    st.st_uid = getuid();
    st.st_gid = getgid();
    st.st_mtime = time(NULL);
    st.st_size = data.length();

    // Write the header
    TarHeader head;
    head.set_filename(filename);
    head.set_attrs(st);
    head.set_checksum();

    if(writeall(1, head.get().c, sizeof(tar_block_header))
            != sizeof(tar_block_header)) {
        std::ostringstream ss;
        ss << "error writing tar block header: " << std::strerror(errno);
        throw SerialiseError(filename, ss.str());
    }

    // Write the character data
    if(writeall(1, data.data(), data.length()) != data.length()) {
        std::ostringstream ss;
        ss << "error writing text data: " << std::strerror(errno);
        throw SerialiseError(filename, ss.str());
    }

    // Write out padding
    off_t bytesleft = st.st_size & (512 - 1);
    bytesleft = 512 - bytesleft;
    if(bytesleft > 0 && bytesleft < 512) {
        writepadding(bytesleft);
    }

    std::clog << "a " << filename << " size=" << st.st_size;
    if(head.used_gnu()) {
        std::clog << " (encoded using gnu extension)";
    }
    std::clog << std::endl;
}


/* dlmalloc clashes with malloc definitions, so can't include malloc.h
 * that defines this properly */
void *memalign(size_t boundary, size_t size);

static void serialise_file(FileInfo& file, volatile iomap *iomap=NULL, const std::string altpath="",
                            const std::string incr_path="", bool incr_create = false)
// Serialise a single file, in tape archive format, onto stdout.  The input
// filename is expected to be an absolute path.  The name recorded in the
// tape archive will be relative to dbdir.  Input files outside of dbdir
// (usually the lrl) will be recorded in the archive as having come from
// dbdir.
{
    const std::string& filename = file.get_filename();
    int flags;
    bool skip_iomap = false;
    std::ostringstream ss;

    // Ensure large file support
    assert(sizeof(off_t) == 8);

    flags = O_RDONLY;

    if (file.get_type() == FileInfo::BERKDB_FILE && file.get_direct_io())
        flags |= DO_DIRECT;

    int fd, fdalt = -1;
reopen:
    if (altpath != "") {
        fd = open(altpath.c_str(), flags);
        fdalt = open(file.get_filepath().c_str(), flags);
    }
    else {
        fd = open(file.get_filepath().c_str(), flags);
    }
    RIIA_fd fd_guard(fd);

    if(fd == -1) {
        /* If this is a log file, we can't ignore it - we need it to run recovery. */
        if (file.get_type() == FileInfo::LOG_FILE) {
            ss << "missing log file " << filename << std::endl;
            throw SerialiseError(filename, ss.str());
        }
        else if (EINVAL == errno){
            std::clog << "Turning off directio, err: " << std::strerror(errno)
                      << std::endl;
            flags ^= DO_DIRECT;
            goto reopen;
        }
        else if (ENOENT == errno) {
            /* Any other files can go missing intraday (eg: after a schema change).
             * If it turns out it's needed, recovery will fail anyway.  So continue
             * despite this file being unavailable. */
            std::clog << "Error opening file " << file.get_filepath()
                      <<", err: " << std::strerror(errno) << std::endl;
            return;
        }
        else
            throw SerialiseError(filename, ss.str());
    }

    struct stat st, stalt;
    if(fstat(fd, &st) == -1) {
        ss << "cannot stat file: " << std::strerror(errno);
        throw SerialiseError(filename, ss.str());
    }

    // Use original lrl file to determine uid, guid & permissions (but not size)
    if(fdalt != -1 && fstat(fdalt, &stalt) != -1) {
        st.st_mode = stalt.st_mode;
        st.st_uid = stalt.st_uid;
        st.st_gid = stalt.st_gid;
    }

    // Ignore special files
    if(!S_ISREG(st.st_mode)) {
        throw SerialiseError(filename, "not a regular file");
    }

    // Write the header
    TarHeader head;
    head.set_filename(filename);
    head.set_attrs(st);
    head.set_checksum();

    if(writeall(1, head.get().c, sizeof(tar_block_header))
            != sizeof(tar_block_header)) {
        std::ostringstream ss;
        ss << "error writing tar block header: " << std::strerror(errno);
        throw SerialiseError(filename, ss.str());
    }

    // Read the file a page at a time and copy to the output.
    // Use a large buffer if possible
    size_t pagesize = file.get_pagesize();
    if(pagesize == 0) {
        pagesize = 4096;
    }
    size_t bufsize = pagesize;
    int num_waits = 0;
    int64_t filesize = 0;

    while((bufsize << 1) <= MAX_BUF_SIZE) {
        bufsize <<= 1;
    }
#if 0
    */
    std::cerr<<"Computed bufsize="<<bufsize<<" for pagesize="<<pagesize<<" file="<<filename<<"\n";

    std::vector<char> pagebuf(bufsize);
#endif
    uint8_t *pagebuf = NULL;
    off_t bytesleft = st.st_size;
    unsigned long long pageno = 0;

#if ! defined  ( _SUN_SOURCE ) && ! defined ( _HP_SOURCE )
        if(posix_memalign((void**) &pagebuf, 512, bufsize))
            throw Error("Failed to allocate output buffer");
#else
        pagebuf = (uint8_t*) memalign(512, bufsize);
#endif

    std::string incrFilename = incr_path + "/" + filename + ".incr";
    std::ofstream incrFile(incrFilename,
            std::ofstream::binary |
            std::ofstream::trunc);

    while(bytesleft > 0) {
        int now;
        unsigned long long nbytes = bytesleft > bufsize ? bufsize : bytesleft;

        while (!skip_iomap && iomap != NULL && iomap->memptrickle_time) {
            now = time(NULL);
            if ((now - iomap->memptrickle_time) > 5*60) {
                std::clog << "long memptrickle (" << now - iomap->memptrickle_time << " seconds), continuing" << std::endl;
                skip_iomap = true;
                break;
            }
            num_waits++;
            poll(0, 0, 100);
        }

        ssize_t bytesread = read(fd, &pagebuf[0], nbytes);
        if(bytesread <= 0) {
            std::ostringstream ss;
            ss << "read error after " << bytesleft << " bytes, tried to read " << nbytes << " bytes "
                << std::strerror(errno);
            throw SerialiseError(filename, ss.str());
        }
        filesize += bytesread;

        if (file.get_checksums()) {
            // Save current offset
            const off_t offset = lseek(fd, 0, SEEK_CUR);
            if (offset == (off_t) -1) {
                std::ostringstream ss;
                ss << "serialise_file:lseek:initial: " << std::strerror(errno);
                throw SerialiseError(filename, ss.str());
            }

            int retry = 5;
            ssize_t n = 0;

            while (n < bytesread && retry) {
                bool verify_bool = false;
                PAGE * pagep = (PAGE *) (pagebuf + n);
                uint32_t verify_cksum;
                verify_checksum(pagebuf + n, pagesize, file.get_crypto(), file.get_swapped(), &verify_bool, &verify_cksum);

                if(verify_bool){
                    // checksum verified
                    n += pagesize;
                    retry = 5;


                    // If we are in incremental mode, on initial backup creation we want to create the diff files
                    if(incr_create){
                        incrFile.write((char *) &(LSN(pagep).file), 4);
                        incrFile.write((char *) &(LSN(pagep).offset), 4);
                        incrFile.write((char *) &verify_cksum, 4);
                    }

                    continue;
                }

                // Partial page read. Read the page again to see if it passes
                // checksum verification.
                if (--retry == 0) {
                    //giving up on this page
                    std::ostringstream ss;
                    ss << "serialise_file:page failed checksum verification";
                    throw SerialiseError(filename, ss.str());
                }

                // wait 500ms before reading page again
                poll(0, 0, 500);

                // rewind and read the page again
                off_t rewind = offset - (bytesread - n);
                rewind = lseek(fd, rewind, SEEK_SET);
                if (rewind == (off_t) -1) {
                    std::ostringstream ss;
                    ss << "serialise_file:lseek:rewind: " << std::strerror(errno);
                    throw SerialiseError(filename, ss.str());
                }

                ssize_t nread, totalread = 0;
                while (totalread < pagesize) {
                    nread = read(fd, &pagebuf[0] + n + totalread,
                                 pagesize - totalread);
                    if (nread <= 0) {
                        std::ostringstream ss;
                        ss << "serialise_file:read: " << std::strerror(errno);
                        throw SerialiseError(filename, ss.str());
                    }
                    totalread += nread;
                }
            }

            // Restore to original offset
            if (offset != lseek(fd, offset, SEEK_SET)) {
                std::ostringstream ss;
                ss << "serialise_file:lseek:reset: " << std::strerror(errno);
                throw SerialiseError(filename, ss.str());
            }
        }

        ssize_t byteswritten = writeall(1, &pagebuf[0], bytesread);
        if(byteswritten != bytesread) {
            std::ostringstream ss;
            ss << "write error after " << bytesleft << "bytes: "
                << std::strerror(errno);
            throw SerialiseError(filename, ss.str());
        }

        bytesleft -= bytesread;
    }

    file.set_filesize(filesize);

    if (num_waits)
        std::clog <<  "paused " << num_waits << " times because db is busy writing." << std::endl;

    // This is a fatal error as it will leave the archive corrupt if the
    // header says the file is longer than it really is.
    if(bytesleft > 0) {
        throw SerialiseError(filename, "file shrank while being archived!");
    }

    // The length of the output must be a multiple of 512 bytes
    bytesleft = st.st_size & (512 - 1);
    bytesleft = 512 - bytesleft;
    if(bytesleft > 0 && bytesleft < 512) {
        writepadding(bytesleft);
    }

    std::clog << "a " << filename << " size=" << st.st_size
              << " pagesize=" << pagesize;

    if (file.get_sparse())
       std::clog << " not sparse ";


    if(head.used_gnu()) {
        std::clog << " (encoded using gnu extension)";
    }


    std::clog << std::endl;
    if (pagebuf)
        free(pagebuf);
}


static void serialise_log_files(
        const std::string& dbtxndir,
        const std::string& dbdir,
        long long& log_number, bool complete_only)
// Scan the dbtxndir directory for log files that we can archive, starting with
// log file log_number.  If complete_only is set then we will only archive
// completed logs (a log is complete if the next log file in the sequence
// already exists).  Otherwise we keep going until there are no more log files.
{
    char logfile[16];
    std::string absfile, storename;
    struct stat st;

    // Find the highest existing log file
    long long highest_log = log_number - 1;
    while(true) {
        snprintf(logfile, sizeof(logfile), "log.%010lld", highest_log + 1);
        makeabs(absfile, dbtxndir, logfile);
        if(stat(absfile.c_str(), &st) == -1) {
            break;
        }
        highest_log++;
    }

    // If we only want complete files, don't include it
    if(complete_only) {
        highest_log--;
    }

    // Archive whatever's in between
    while(log_number <= highest_log) {
        snprintf(logfile, sizeof(logfile), "log.%010lld", log_number);
        std::cerr<<"Serializing "<<logfile<<std::endl;
        makeabs(absfile, dbtxndir, logfile);
        FileInfo fi(FileInfo::LOG_FILE, absfile, dbdir);
        serialise_file(fi);
        log_number++;
    }
}

static void strip_cluster(const std::string& lrlpath,
        const std::string& lrldest)
{
    std::ifstream instream(lrlpath.c_str());
    if(!instream.is_open()) {
        throw LRLError(lrlpath, 0, "cannot open file");
    }

    std::ofstream outstream(lrldest.c_str());
    if(!outstream.is_open()) {
        throw LRLError(lrldest, 0, "cannot open file");
    }

    while(!instream.eof()) {
        std::string line;
        std::getline(instream, line);

        std::istringstream liness(line);

        std::string tok;

        if(liness >> tok && tok == "cluster") {
            outstream << "#" << line << std::endl;
        } else {
            outstream  << line << std::endl;
        }
    }
}

void parse_lrl_file(const std::string& lrlpath,
        std::string* p_dbname,
        std::string* p_dbdir,
        std::list<std::string>* p_support_files,
        std::set<std::string>* p_table_names,
        std::set<std::string>* p_queue_names,
        bool* p_nonames,
        bool* has_cluster_info)
{
    // First, load the lrl file into memory.
    std::ifstream lrlstream(lrlpath.c_str());
    if(!lrlstream.is_open()) {
        throw LRLError(lrlpath, 0, "cannot open file");
    }
    int lineno = 0;
    while(!lrlstream.eof()) {
        std::string line;
        std::getline(lrlstream, line);
        lineno++;

        // Parse the line that we just read.  First strip off comments.
        size_t pos = line.find_first_of('#');
        if(pos != std::string::npos) {
            line.resize(pos);
        }
        std::istringstream liness(line);

        std::string tok;
        if(liness >> tok) {

            if(tok == "name") {
                if(!(liness >> tok)) {
                    throw LRLError(lrlpath, lineno, "missing database name");
                }
                *p_dbname = tok;

            } else if(tok == "dir") {
                if(!(liness >> tok)) {
                    throw LRLError(lrlpath, lineno, "missing database directory");
                }
                *p_dbdir = tok;
            } else if(tok == "table") {
                std::string name;
                std::string schema_path;
                if(!(liness >> name)) {
                    throw LRLError(lrlpath, lineno, "missing table name");
                }
                if(!(liness >> schema_path)) {
                    throw LRLError(lrlpath, lineno, "missing table schema path");
                }
                p_table_names->insert(name);
                p_support_files->push_back(schema_path);

            } else if(tok == "queue") {
                std::string name;
                if(!(liness >> name)) {
                    throw LRLError(lrlpath, lineno, "missing queue name");
                }
                p_queue_names->insert(name);

            } else if(tok == "resource") {
                std::string path;
                if(!(liness >> tok >> path)) {
                    throw LRLError(lrlpath, lineno, "missing resource name or path");
                }
                p_support_files->push_back(path);
            } else if(tok == "spfile") {
                std::string path;
                if(!(liness >> path)) {
                    throw LRLError(path, lineno, "missing spfile");
                }
                p_support_files->push_back(path);
            } else if(tok == "nonames") {
                *p_nonames = true;
            } else if (tok == "usenames") {
                *p_nonames = false;
            } else if(tok == "cluster") {
                *has_cluster_info = true;
            }
        }
    }
}

std::string generate_fingerprint(void) 
{
    unsigned char obuf[20];

    std::string dt_string = getDTString();

    // SHA the date time to get a pseudorandom fingerprint to check increment order
    SHA1((unsigned char *) dt_string.c_str(), dt_string.length() + 1, obuf);

    std::ostringstream ss;
    for(int i = 0; i < 20; ++i){
        ss << std::hex << std::setw(2) << std::setfill('0') << +obuf[i];
    }

    return ss.str();
}

void serialise_database(
  std::string lrlpath,
  const std::string& comdb2_task,
  bool disable_log_deletion,
  bool strip_cluster_info,
  bool support_files_only,
  bool run_with_done_file,
  bool do_direct_io,
  bool incr_create,
  bool incr_gen,
  const std::string& incr_path
)
// Serialise a database into tape archive format and write it to stdout.
// If support_only is true then only support files (lrl and schema) will
// be serialised.  If disable_log_deletion and the database is running then
// it will be advised to hold log file deletion until the backup is complete
// (highly recommended!)
{
    std::string dbname;
    std::string dbdir;
    std::string dbtxndir;
    bool nonames = false;
    bool has_cluster_info = false;
    std::string origlrlname("");
    std::string strippedpath("");

    // All support files - csc2s, extra lrls, resources for stored procedures
    // etc.
    std::list<std::string> support_files;

    // All tables.  In llmeta mode we don't have a table list
    std::set<std::string> table_names;

    // All queues
    std::set<std::string> queue_names;
    std::set<std::string> queuedb_names;

    // Optional files.  Nice if found, but not nececssary
    std::list<std::string> optional_files;

    // Vector of logfile numbers to detect gaps
    std::vector<int> logvec;

    // Iterator for logvec
    std::vector<int>::iterator logit;

    // Current logfile, log errors
    int curlog=0, logerr=0;

    parse_lrl_file(lrlpath, &dbname, &dbdir, &support_files,
            &table_names, &queue_names, &nonames, &has_cluster_info);

    // Ignore lrl names setting if we know better
    if (!dbdir.empty() && !dbname.empty()) {
        nonames = check_usenames(dbname, dbdir, nonames);
    }

    // Create the directory for incremental backups if needed
    if (incr_create) {
        struct stat sb;

        if(!(stat(incr_path.c_str(), &sb) == 0 && S_ISDIR(sb.st_mode))){
            std::string cmd("mkdir -p " + incr_path);
            system(cmd.c_str());
        }
    }

    if (strip_cluster_info && has_cluster_info) {
        std::string newlrlpath;
        support_files.clear();
        table_names.clear();
        queue_names.clear();

        if (nonames) {
            newlrlpath = dbdir + "/" + dbname + ".tmpdbs/" + dbname
                + ".nocluster.lrl";
        } else {
            newlrlpath = dbdir + "/tmp." + dbname + ".nocluster.lrl";
        }

        struct stat st;
        if(stat(newlrlpath.c_str(), &st) == 0
                && unlink(newlrlpath.c_str()) == -1 && errno != ENOENT) {
            std::cerr << "Cannot unlink " << newlrlpath << ": "
                << errno << " " << std::strerror(errno) << std::endl;
        }

        strip_cluster(lrlpath, newlrlpath);
        strippedpath = newlrlpath;
    }



    // TODO you could install from an un-initialized lrl that does have
    // use_llmeta but still has it's table declarations, in that case you'd want
    // to skip this if block, but people are supposed to install from a db
    // that's up and running (and has been tested etc) so we'll leave this out
    // for now -cpick
    if(support_files_only) {
        // Reset
        support_files.clear();
        table_names.clear();
        queue_names.clear();

        // Stash the new lrl and csc2 files in the tmpdbs dir so they'll be
        // cleaned up on the db's next startup
        std::string newlrlpath;
        if (nonames) {
            newlrlpath = dbdir + "/tmp/" + dbname + ".lrl";
        } else {
            newlrlpath = dbdir + "/" + dbname + ".tmpdbs/" + dbname
                + ".lrl";
        }

        struct stat st;
        if(stat(newlrlpath.c_str(), &st) == 0
                && unlink(newlrlpath.c_str()) == -1 && errno != ENOENT) {
            std::cerr << "Cannot unlink " << newlrlpath << ": "
                << errno << " " << std::strerror(errno) << std::endl;
        }

        // Throws on error
        RepopNewLrl(dbname, lrlpath, newlrlpath, comdb2_task);

        origlrlname = dbname + ".lrl"; /* save the current path */
        lrlpath = newlrlpath;

        parse_lrl_file(lrlpath, &dbname, &dbdir,
                &support_files, &table_names, &queue_names, &nonames, &has_cluster_info);
    }


    // lrl file is always the first to be serialised
    support_files.push_front(lrlpath);

    // Make sure we get required things from the lrl file
    if(dbname.empty()) {
        throw LRLError(lrlpath, "missing name line");
    }
    if(dbdir.empty()) {
        throw LRLError(lrlpath, "missing dir line");
    }

    // Strip trailing / in dbdir
    if(dbdir[dbdir.length() - 1] == '/') {
        dbdir.resize(dbdir.length() - 1);
    }

    // Calculate path of .txn directory
    if (nonames) {
        dbtxndir = dbdir + "/logs";
    } else {
        dbtxndir = dbdir + "/" + dbname + ".txn";
    }

    // We will need to list the files in the data directory and txn dir
    std::list<std::string> dbdir_files;
    std::list<std::string> dbtxndir_files;
    if(!support_files_only) {
        listdir(dbdir_files, dbdir);
        listdir(dbtxndir_files, dbtxndir);
    }

    // List of incremental comparison files to determine new/updated/deleted files
    std::list<std::string> incr_files_list;
    std::set<std::string> incr_files;
    if(incr_gen) {
        listdir(incr_files_list, incr_path);
        incr_files_list.remove_if(is_not_incr_file);
        incr_files = std::set<std::string>(incr_files_list.begin(), incr_files_list.end());
    }

    // If it's an llmeta db then look for the file version map too.
    // Read it in to get a list of tables.
    if(!support_files_only) {
        if (nonames) {
            optional_files.push_back("file_vers_map");
        } else {
            optional_files.push_back(dbname + "_file_vers_map");
        }

        // All btrees must have a datas0 file
        for (std::list<std::string>::const_iterator it = dbdir_files.begin();
             it != dbdir_files.end(); ++it) {
            size_t len = it->length();
            if (len < 23) continue;
            if (it->substr(len - 7) != ".datas0") continue;
            if ((*it)[len - 24] != '_') continue;
            size_t ii;
            for (ii = 0; ii < 16; ++ii) {
                if (!std::isxdigit((*it)[len - 23 + ii])) {
                    break;
                }
            }
            if (ii == 16) {
                std::string table(it->substr(0, len - 24));
                if (table_names.insert(table).second) {
                    std::clog << "Inferred table " << table << " from " << *it
                              << std::endl;
                }
            }
        }
    }

    // If the database is running, connect to it and instruct it to pause
    // log file deletion.  This must be done before we construct our picture
    // of what files to backup since if new files are created after this
    // that we don't serialise they will need to be recoverable from the
    // logs that we save.
    std::unique_ptr<LogHolder> log_holder;
    if(disable_log_deletion && !support_files_only) {
        log_holder = std::unique_ptr<LogHolder>(new LogHolder(dbname));
    }


    // Based on what we read from the lrl, it's time to create the definitive
    // set of files to back up.  For data files we record the file names and
    // the page sizes to use.
    std::list<FileInfo> data_files;

    std::string abspath;
    long long lowest_log = -1;

    // Vector of files that need to be backed up in incremental mode
    std::vector<FileInfo> incr_data_files;
    // Vector of pages that need to be backed up for the corresponding files
    std::vector<FileInfo> new_files;

    // Vector of pages index aligned with each file to serialise
    std::vector<std::vector<uint32_t> > page_number_vec;

    // Total size of backed up pages
    ssize_t total_data_size = 0;

    if(!support_files_only) {

        logvec.clear();

        // Find the lowest log file number in the txn dir.
        for(std::list<std::string>::const_iterator it = dbtxndir_files.begin();
                it != dbtxndir_files.end();
                ++it) {
            if(it->compare(0, 4, "log.") == 0 && it->length() == 14) {
                int logno = 0;
                for(int ii = 4; ii < 14; ++ii) {
                    if(!std::isdigit((*it)[ii])) {
                        logno = -1;
                        break;
                    }
                    logno = (logno * 10) + ((*it)[ii] - '0');
                }
                if(logno < lowest_log || lowest_log == -1) {
                    lowest_log = logno;
                }

                logvec.push_back( logno );
            }
        }

        // sort
        std::sort( logvec.begin(), logvec.end () );

        for ( logit = logvec.begin() ; logit != logvec.end() ; logit++ )
        {
            // First iteration
            if( logit == logvec.begin() )
            {
                // curlog = *logit;
            }

            // Found a gap in the logfile sequence!
            else if( *logit != curlog + 1 )
            {
                std::cerr << "Logfile gap: expected " << curlog + 1 <<
                        " got " << *logit << std::endl;
                logerr++;
            }

            // Remember this logfile
            curlog = *logit;
        }

        // throw an error if there was a gap
        if( logerr )
        {
            throw SerialiseError( "Database-logfiles", "gap in logfile sequence" );
        }


        std::clog << "Lowest log file is " << lowest_log << std::endl;

        // Look for files to copy.  This is the dumb version that just takes
        // everything that might be a db file.
        std::string templ_fstblk;
        std::string templ_llmeta;
        std::string templ_metadata;
        std::string templ_blkseq_dta;
        std::string templ_blkseq_freerec;
        std::string templ_blkseq_ix0;

        if(nonames) {
            templ_fstblk = "comdb2_fstblk.dta";
            templ_llmeta = "comdb2_llmeta.dta";
            templ_metadata = "comdb2_metadata.dta";
            templ_blkseq_dta = "comdb2_blkseq.dta";
            templ_blkseq_freerec = "comdb2_blkseq.freerec";
            templ_blkseq_ix0 = "comdb2_blkseq.ix0";
        } else {
            templ_fstblk = dbname + ".fstblk.dta";
            templ_llmeta = dbname + ".llmeta.dta";
            templ_metadata = dbname + ".metadata.dta";
            templ_blkseq_dta = dbname + ".blkseq.dta";
            templ_blkseq_freerec = dbname + ".blkseq.freerec";
            templ_blkseq_ix0 = dbname + ".blkseq.ix0";
        }

        for(std::list<std::string>::const_iterator it = dbdir_files.begin();
                it != dbdir_files.end();
                ++it) {
            const std::string& filename(*it);

            bool is_data_file = false;
            bool is_queue_file = false;
            bool is_queuedb_file = false;
            std::string table_name;

            // First see if this file relates to any of our tables or queues
            if(recognize_data_file(filename,
                        is_data_file, is_queue_file, is_queuedb_file, table_name)) {
                if(is_data_file &&
                        table_names.find(table_name) == table_names.end()) {
                    continue;
                }
                if(is_queue_file &&
                        queue_names.find(table_name) == queue_names.end()) {
                    continue;
                }
            } else if(filename != templ_fstblk &&
                        filename != templ_llmeta &&
                        filename != templ_metadata &&
                        filename != templ_blkseq_dta &&
                        filename != templ_blkseq_freerec &&
                        filename != templ_blkseq_ix0) {
                // We don't want this.
                continue;
            }

            // Looks like we need this file..
            makeabs(abspath, dbdir, filename);

            try {
                DB_Wrap db(abspath);
                size_t pagesize = db.get_pagesize();
                bool checksums = db.get_checksums();
                bool crypto = db.get_crypto();
                bool sparse = db.get_sparse();
                bool swapped = db.get_swapped();

                data_files.push_back(FileInfo(FileInfo::BERKDB_FILE,
                      abspath, dbdir, pagesize, checksums, crypto, sparse, do_direct_io, swapped));

                // Look for queue extents in the txn directory, which will
                // all have the same page size as the parent queue file.
                if(is_queue_file) {
                    std::string cmp("__dbq." + filename);
                    for(std::list<std::string>::const_iterator txn_it =
                            dbtxndir_files.begin();
                            txn_it != dbtxndir_files.end();
                            ++txn_it) {
                        makeabs(abspath, dbtxndir, *txn_it);
                        if(txn_it->compare(0, cmp.length(), cmp) == 0) {
                            data_files.push_back(FileInfo(
                                    FileInfo::BERKDB_FILE,
                                    abspath, dbdir, pagesize, checksums, crypto, sparse, swapped));
                        }
                    }
                }
            } catch(Error& e) {
                // If we can't get the pagesize with Berkeley then it
                // can't be a Berkeley DB file, so move on.
                std::cerr << ">>>>> " << abspath << ":" << e.what() << std::endl;
                std::clog << e.what() << std::endl;
            }
        }
    }


    // Construct a manifest which will give the page sizes of all the files
    std::ostringstream manifest;

    // Non-incremental mode or increment creation mode
    if(!incr_gen){
        manifest << "# Manifest for serialisation of " << dbname << std::endl;
        if(support_files_only) {
            manifest << "SupportFilesOnly" << std::endl;
            if (origlrlname != "")
            {
               manifest <<"OrigLrlFile "<< origlrlname<< std::endl;
            }
        }

        for(std::list<FileInfo>::const_iterator
                it = data_files.begin();
                it != data_files.end();
                ++it) {
                write_manifest_entry(manifest, *it);
        }

        // Find a recovery point after the copy, and record it in the manifest
        if (!support_files_only) {
            std::clog << "logdelete version " << log_holder->version() << std::endl;
            if (log_holder->version() >= 3) {
                std::string recovery_options = log_holder->recovery_options();
                if (!recovery_options.empty()) {
                    manifest << "Option " << recovery_options <<std::endl;
                }
            }
        }

        // Serialise the manifest file
        serialise_string("MANIFEST", manifest.str());

    // Incremental Mode
    } else {
        manifest << "# Manifest for serialisation of increment produced on "
            << getDTString() << std::endl;


        for(std::list<FileInfo>::iterator
                it = data_files.begin();
                it != data_files.end();
                ++it) {

            std::vector<uint32_t> pages_list;
            ssize_t data_size = 0;

            // Diff the page checksums for each file to find what has been changed
            if(compare_checksum(*it, incr_path, pages_list, &data_size, incr_files)) {
                // If pages list is empty but compare_checksum returned true, it's a new file
                if(pages_list.empty()){
                    new_files.push_back(*it);
                    write_incr_manifest_entry(manifest, *it, pages_list);
                // Keep track of the file name and the list of pages to be backed-up
                } else {
                    std::cerr << it->get_filename() <<  " " << pages_list.size() << " pages" << std::endl;
                    incr_data_files.push_back(*it);
                    page_number_vec.push_back(pages_list);
                    write_incr_manifest_entry(manifest, *it, pages_list);
                    total_data_size += data_size;
                }
            }
        }

        if(incr_data_files.size() != page_number_vec.size()){
            std::ostringstream ss;
            ss << "data files and page vectors don't align";
            throw Error(ss);
        }

        // filename for the SHA fingerprint that asserts that increments are applied in the correct order
        std::string sha_filename = "";
        // Any leftover .incr files indicate that that file has been deleted or that it is the SHA file
        for(std::set<std::string>::const_iterator
                it = incr_files.begin();
                it != incr_files.end();
                ++it){
            // If it's the SHA fingerprint file, mark it then move onto the next file
            if((*it).substr((*it).length() - 4) == ".sha"){
                sha_filename = *it;
                continue;
            }

            // Remove diff files corresponding to deleted files and mark them
            std::string true_filename = incr_path + "/" + *it;
            if(remove(true_filename.c_str()) != 0){
                std::ostringstream ss;
                ss << "error deleting file: " << *it;
                throw Error(ss);
            }
            write_del_manifest_entry(manifest, *it);
        }

        // Find a recovery point after the copy, and record it in the manifest
        std::clog << "logdelete version " << log_holder->version() << std::endl;
        if (log_holder->version() >= 3) {
            std::string recovery_options = log_holder->recovery_options();
            if (!recovery_options.empty()) {
                manifest << "Option " << recovery_options <<std::endl;
            }
        }

        // Read and note the previous increment's SHA fingerprint
        if(sha_filename.empty()){
            std::ostringstream ss;
            ss << "No previous SHA fingerprint";
            throw Error(ss);
        } else {
            std::string sha_fingerprint = get_sha_fingerprint(sha_filename, incr_path);
            manifest << "PREV " << sha_fingerprint << std::endl;
        }

        // Serialise the manifest file
        serialise_string("INCR_MANIFEST", manifest.str());

    }


    std::string iomapfile;
    std::ostringstream ss;
    ss << "/bb/data/" << dbname << ".iomap";
    iomap *iom = NULL;

    int fd = open(ss.str().c_str(), O_RDONLY);

    if (fd != -1) {
        iom = (iomap*) mmap(NULL, sizeof(struct iomap), PROT_READ, MAP_SHARED, fd, 0);
        if (iom == (void*) -1)
            iom = NULL;
    }

    RIIA_fd fd_guard(fd);

    // Serialise files in normal mode/initial backup mode
    if(!incr_gen){

        // Serialise the files that we found to stdout.  First do support files.
        // We may have to make the paths absolute in some cases as this wasn't
        // done before.  Never touch the lrl path though.
        bool islrl = true;
        for(std::list<std::string>::const_iterator it = support_files.begin();
                it != support_files.end();
                ++it) {
            abspath = *it;

            if(!abspath.empty() && abspath[0] != '/') {
                // Behave like comdb2 - look for support files (resource files,
                // .csc2 files and .lrl files) relative to the lrl file.
                std::string lrldir(lrlpath);
                makedirname(lrldir);
                if(!lrldir.empty()) {
                    makeabs(abspath, lrldir, *it);
                }
            }
            if (islrl && strippedpath != "") {
                FileInfo fi(FileInfo::SUPPORT_FILE, abspath, dbdir);
                serialise_file(fi, iom, strippedpath);
            } else {
                FileInfo fi(FileInfo::SUPPORT_FILE, abspath, dbdir);
                serialise_file(fi, iom);
            }
            islrl = false;
        }

        // Now do optional files (if any)
        for (std::list<std::string>::const_iterator it = optional_files.begin();
                it != optional_files.end(); ++it) {
            try {
                abspath = *it;
                makeabs(abspath, dbdir, *it);
                FileInfo fi(FileInfo::OPTIONAL_FILE, abspath, dbdir);
                serialise_file(fi, iom);
            }
            catch (SerialiseError &err) {
                std::cerr << "Warning: " << *it << ": " << err.what() << std::endl;
            }
        }

        // Now do data files
        if(!support_files_only) {

            // Grab the checkpoint file, pretend its a logfile
            std::string absfile;
            std::cerr<<"Serializing checkpoint"<<std::endl;
            makeabs(absfile, dbtxndir, "checkpoint");
            FileInfo fi(FileInfo::LOG_FILE, absfile, dbdir);
            serialise_file(fi);

            long long log_number(lowest_log);
            for(std::list<FileInfo>::iterator
                    it = data_files.begin();
                    it != data_files.end();
                    ++it) {

                // First, serialise any complete log files that are in the .txn
                // directory and notify the running database that they can now be
                // archived.
                long long old_log_number(log_number);
                serialise_log_files(dbtxndir, dbdir, log_number, true);
                if(log_number != old_log_number && log_holder.get()) {
                    log_holder->release_log(log_number - 1);
                }

                // Ok, now serialise this file.
                serialise_file(*it, iom, "", incr_path, incr_create);
            }

            // Serialise all remaining log files, including incomplete ones
            serialise_log_files(dbtxndir, dbdir, log_number, false);
        }

    // Serialise files for incremental backup
    } else {
        struct stat st;

        st.st_mode = 0664;
        st.st_uid = getuid();
        st.st_gid = getgid();
        st.st_mtime = time(NULL);
        st.st_size = total_data_size;

        // Grab the checkpoint file, pretend its a logfile
        std::string absfile;
        std::cerr<<"Serializing checkpoint"<<std::endl;
        makeabs(absfile, dbtxndir, "checkpoint");
        FileInfo fi(FileInfo::LOG_FILE, absfile, dbdir);

        serialise_file(fi);

        long long log_number(lowest_log);

        // First, serialise any complete log files that are in the .txn
        // directory and notify the running database that they can now be
        // archived.
        // TODO: Figure out how to serialise logs after each file without
        // disrupting the page data file
        long long old_log_number(log_number);
        serialise_log_files(dbtxndir, dbdir, log_number, true);
        if(log_number != old_log_number && log_holder.get()) {
            log_holder->release_log(log_number - 1);
        }

        // Write the header
        TarHeader head;
        head.set_filename(getDTString() + ".data");
        head.set_attrs(st);
        head.set_checksum();

        if(writeall(1, head.get().c, sizeof(tar_block_header))
                != sizeof(tar_block_header)) {
            std::ostringstream ss;
            ss << "error writing tar block header: " << std::strerror(errno);
            throw Error(ss.str());
        }

        // Iterate through the files, index-aligning with the list of changed pages
        std::vector<FileInfo>::const_iterator data_it = incr_data_files.begin();
        std::vector<std::vector<uint32_t> >::const_iterator
            page_it = page_number_vec.begin();

        ssize_t data_written = 0;

        // Serialise the database's changed pages
        while(data_it != incr_data_files.end()){
            data_written += serialise_incr_file(*data_it, *page_it, incr_path);

            ++data_it;
            ++page_it;
        }

        // The length of the output must be a multiple of 512 bytes
        size_t bytesleft = total_data_size & (512 - 1);
        bytesleft = 512 - bytesleft;
        if(bytesleft > 0 && bytesleft < 512) {
            writepadding(bytesleft);
        }

        if(data_written != total_data_size){
            ss <<"file sizes changed during incremental backup";
            throw Error(ss);
        }

        // Serialise new files
        for(std::vector<FileInfo>::iterator new_it = new_files.begin();
                new_it != new_files.end();
                *new_it++
        ) {

            // First, serialise any complete log files that are in the .txn
            // directory and notify the running database that they can now be
            // archived.
            long long old_log_number(log_number);
            serialise_log_files(dbtxndir, dbdir, log_number, true);
            if(log_number != old_log_number && log_holder.get()) {
                log_holder->release_log(log_number - 1);
            }

            // Ok, now serialise this file.
            serialise_file(*new_it, iom, "", incr_path, true);
        }


        // Serialise all remaining log files, including incomplete ones
        serialise_log_files(dbtxndir, dbdir, log_number, false);
    }

    // Generate fingerprint SHA file
    if(incr_create || incr_gen){
        std::string sha = generate_fingerprint();

        std::clog << "Calculated SHA fingerprint as: " << sha << std::endl;

        // Serialise the SHA file
        serialise_string("fingerprint.sha", sha);

        // Write the SHA file so that the next increment can know it
        std::string sha_filename = incr_path + "/fingerprint.sha";
        std::ofstream sha_file(sha_filename, std::ofstream::trunc);

        sha_file.write(sha.c_str(), 40);
    }

    // Release the database for log file deletion.
    if(log_holder.get()) {
        log_holder->close();
    }


    // Complete the archive with two blank 512 byte blocks
    writepadding(2 * 512);

    // Success, all done!
}

extern uint32_t myflip(uint32_t in);

void write_incremental_file (
        const std::string &dbdir, 
        const std::string &abspath, 
        const std::string &incrname, 
        struct stat *st
) 
// Write checksum/LSN information for a database file
{
    DB_Wrap db(abspath);
    int flags = O_RDONLY; 
    size_t pagesize;
    int fd;

    RIIA_fd fd_guard(fd);

    // TODO: optionally O_DIRECT?
    fd = open(abspath.c_str(), flags);
    if (fd == -1) {
        std::ostringstream ss;
        ss << "Can't open" << abspath << " " << strerror(errno) << std::endl;
        throw SerialiseError(abspath, ss.str());
    }

    pagesize = db.get_pagesize();
    if (pagesize == 0)
        pagesize = 4096;
    size_t bufsize = pagesize;
    while((bufsize << 1) <= MAX_BUF_SIZE) {
        bufsize <<= 1;
    }
    off_t uptosize = st->st_size;

    // st->st_size is going to be the size of the incremental file
    // save the size of the real file here
    // 12 bytes per page
    st->st_size = 12 * (st->st_size / pagesize);

    TarHeader head;
    head.set_filename(incrname);
    head.set_attrs(*st);
    head.set_checksum();
    tar_block_header hdr = head.get();

    // Write tar header
    if (writeall(1, &hdr, sizeof(tar_block_header)) != sizeof(tar_block_header)) {
        std::ostringstream ss;
        ss << "error writing tar block header: " << std::strerror(errno);
        throw Error(ss.str());
    }

    uint8_t *pagebuf = NULL;
    off_t bytesleft = uptosize;
    unsigned long long pageno = 0;
    off_t offset = 0;

#if ! defined  ( _SUN_SOURCE ) && ! defined ( _HP_SOURCE )
    if(posix_memalign((void**) &pagebuf, 512, bufsize))
        throw Error("Failed to allocate output buffer");
#else
    pagebuf = (uint8_t*) memalign(512, bufsize);
#endif
    while (bytesleft > 0) {
        off_t nbytes = bytesleft > bufsize ? bufsize : bytesleft;

        // We don't need to check checksums or retry, since if the page disagrees 
        // with the source it'll be overwritten
        size_t rc;
        rc = read(fd, pagebuf, nbytes);
        if (rc != nbytes) {
            std::ostringstream ss;
            ss << "Can't read" << abspath << " " << nbytes << " bytes at offset " << offset << " : " << strerror(errno) << std::endl;
            throw SerialiseError(abspath, ss.str());
        }

        for (off_t n = 0; n < nbytes / pagesize; n++) {
            PAGE *pagep = (PAGE *) (pagebuf + (pagesize * n));
            bool verify_bool;
            uint32_t verify_cksum;

            verify_checksum(pagebuf + (pagesize * n), pagesize, db.get_crypto(), db.get_swapped(), &verify_bool, &verify_cksum);
            if (db.get_swapped()) {
                verify_cksum = myflip(verify_cksum);
                pagep->lsn.file = myflip(pagep->lsn.file);
                pagep->lsn.offset = myflip(pagep->lsn.offset);
            }

            write(1, &(LSN(pagep).file), 4);
            write(1, &(LSN(pagep).offset), 4);
            write(1, &verify_cksum, 4);

            offset += pagesize;
        }

        bytesleft -= nbytes;
    }
}

void create_partials(
    const std::string &lrlpath, bool do_direct_io
) 
// create a tarball of incremental data files for all data files, and nothing else
{
    std::string dbname, dbdir;
    std::list<std::string> support_files;
    std::set<std::string> table_names;
    std::set<std::string> queue_names;
    bool nonames;
    bool has_cluster_info;
    std::list<std::string> dbdir_files;
    std::string abspath;

    std::string templ_fstblk;
    std::string templ_llmeta;
    std::string templ_metadata;
    std::string templ_blkseq_dta;
    std::string templ_blkseq_freerec;
    std::string templ_blkseq_ix0;


    parse_lrl_file(lrlpath, &dbname, &dbdir, 
                   &support_files, &table_names, 
                   &queue_names, &nonames, &has_cluster_info);

    if (!dbdir.empty() && !dbname.empty()) {
        nonames = check_usenames(dbname, dbdir, nonames);
    }

    if(nonames) {
        templ_fstblk = "comdb2_fstblk.dta";
        templ_llmeta = "comdb2_llmeta.dta";
        templ_metadata = "comdb2_metadata.dta";
        templ_blkseq_dta = "comdb2_blkseq.dta";
        templ_blkseq_freerec = "comdb2_blkseq.freerec";
        templ_blkseq_ix0 = "comdb2_blkseq.ix0";
    } else {
        templ_fstblk = dbname + ".fstblk.dta";
        templ_llmeta = dbname + ".llmeta.dta";
        templ_metadata = dbname + ".metadata.dta";
        templ_blkseq_dta = dbname + ".blkseq.dta";
        templ_blkseq_freerec = dbname + ".blkseq.freerec";
        templ_blkseq_ix0 = dbname + ".blkseq.ix0";
    }

    // We just care about dbdir.  Ignore everything else.
    listdir(dbdir_files, dbdir);

    // Incremental backups generate a fingerprint to establish parent/child
    // relationships. We don't really need this, but the restore process
    // expects it, so generate it.
    std::string sha = generate_fingerprint();
    serialise_string("fingerprint.sha", sha);

    for (std::list<std::string>::const_iterator it  = dbdir_files.begin();  it != dbdir_files.end(); ++it) {
        bool is_data_file, is_queue_file, is_queuedb_file;
        std::string table;

        std::ostringstream ss;
        ss << dbdir << "/" << *it;
        abspath = ss.str();
        struct stat st;
        int rc = stat(abspath.c_str(), &st);
        if (rc) {
            std::ostringstream ss;
            ss << "Can't stat " << ss.str() << " " << strerror(errno) << std::endl;
            throw Error(ss.str());

        }
        else if (!(st.st_mode & S_IFREG)) {
            std::cerr << "notafile " << *it << std::endl;
            continue;
        }

        if (recognize_data_file(*it, is_data_file, is_queue_file, is_queuedb_file, table) || 
                *it == templ_fstblk || *it == templ_llmeta || *it == templ_metadata || 
                *it == templ_blkseq_dta || *it == templ_blkseq_freerec || *it == templ_blkseq_ix0) {
            std::ostringstream ss;
            ss << *it << ".incr";


            std::cerr << "p " << *it << std::endl;


            try {
                // Write checksums
                write_incremental_file(dbdir, abspath, ss.str(), &st);
                int rem = st.st_size % 512;
                if (rem) {
                    rem = 512 - rem;
                    writepadding(rem);
                }
            }
            catch (Error &e) {
                std::cerr << e.what() << std::endl;
            }
        }
        else {
            std::cerr << "notadatafile " << *it << std::endl;
        }
    }
}
