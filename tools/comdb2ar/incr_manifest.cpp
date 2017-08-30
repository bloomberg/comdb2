#include "comdb2ar.h"

#include "increment.h"
#include "error.h"
#include "riia.h"
#include "file_info.h"

#include <string>
#include <cerrno>
#include <cstring>

void write_incr_manifest_entry(std::ostream& os, const FileInfo& file,
                                const std::vector<uint32_t>& pages)
// Serialise a FileInfo into an output stream during an incremental backup.
// This is intended for writing out the manifest file.
{
    bool updated = !pages.empty();

    os << "File " << file.get_filename();
    std::clog << "File " << file.get_filename();

    if(updated){
        os << " Updated ";
        std::clog << " Updated ";
    } else {
        os << " New ";
        std::clog << " New ";
    }

    os << " Type " << file.get_type_string();
    std::clog << " Type " << file.get_type_string();
    if(file.get_type() == FileInfo::BERKDB_FILE) {
        os << " PageSize " << file.get_pagesize();
        std::clog << " PageSize " << file.get_pagesize();
    }

    if(updated){
        os << " Pages [ ";
        std::clog << " Pages [ ";
        for(size_t i = 0; i < pages.size(); ++i){
            os << pages[i];
            std::clog << pages[i];
            if(i != pages.size() - 1){
                os << " ";
                std::clog << " ";
            } else {
                os << " ] ";
                std::clog << " ] ";
            }
        }
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

    return;
}

void write_del_manifest_entry(std::ostream& os, const std::string& incr_filename){
    std::string true_filename = incr_filename.substr(0, incr_filename.length() - 5);
    os << "File " << true_filename;
    std::cerr << "File " << true_filename;
    os << " Deleted" << std::endl;
    std::cerr << " Deleted" << std::endl;

    return;
}

std::string read_incr_manifest(unsigned long long filesize){
    std::string text;

    size_t bufsize = 4096;

    unsigned long long nblocks = (filesize + 511ULL) >> 9;

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


    unsigned long long readbytes = 0;
    while(bytesleft > 0)
    {
        readbytes = bytesleft;

        if(bytesleft > bufsize)
        {
           readbytes = bufsize;
        }

        if(readall(0, &buf[0], readbytes) != readbytes)
        {
           std::ostringstream ss;

           ss << "Error reading " << readbytes
              << " bytes for incremental manifest after "
              << (filesize - bytesleft) << " bytes, with "
              << bytesleft << " bytes left to read";
               throw Error(ss);
        }

        text.append((char*) &buf[0], readbytes);

        bytesleft -= readbytes;
    }

    unsigned long long padding_bytes = (nblocks << 9) - filesize;
    if(padding_bytes) {
        if(readall(0, &buf[0], padding_bytes) != padding_bytes) {
            std::ostringstream ss;

            ss << "Error reading padding while processing manifest: "
               << errno << " " << strerror(errno);
            throw Error(ss);
        }
    }

    return text;
}

bool process_incr_manifest(
    std::string text,
    std::string datadestdir,
    std::map<std::string, std::pair<FileInfo, std::vector<uint32_t>>>&
        updated_files,
    std::map<std::string, FileInfo>& new_files,
    std::set<std::string>& deleted_files,
    std::vector<std::string>& file_order,
    std::vector<std::string>& options,
    std::string& manifest_sha
) {
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
        std::string filename;

        if(ss >> tok) {
            if(tok == "File") {
                if(!(ss >> filename)) return false;
                file_order.push_back(filename);

                if(!(ss >> tok)) return false;

                std::vector<uint32_t> pages;

                if (tok == "New" && read_FileInfo(line, tmp_file)){
                    new_files[tmp_file.get_filename()] = tmp_file;
                    tmp_file.reset();
                    file_order.pop_back();

                } else if (tok == "Updated" && read_incr_FileInfo(line, tmp_file, pages)){
                    updated_files[filename] = std::make_pair(tmp_file, pages);
                    tmp_file.reset();

                } else if (tok == "Deleted"){
                    deleted_files.insert(filename);
                    file_order.pop_back();

                } else {
                    std::clog << "Bad File directive on line "
                        << lineno << " of MANIFEST: "
                        << tok << std::endl;
                }

            } else if (tok == "Option") {
                while (ss >> tok) {
                    options.push_back(tok);
                }

            } else if (tok == "PREV") {
                if(!(ss >> manifest_sha)) return false;

            } else {
                std::clog << "Unknown directive '" << tok << "' on line "
                    << lineno << " of MANIFEST" << std::endl;
            }
        }
    }

    return true;
}


