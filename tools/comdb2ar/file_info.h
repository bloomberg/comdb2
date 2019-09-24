/*
   Copyright 2015 Bloomberg Finance L.P.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */

#ifndef INCLUDED_FILE_INFO
#define INCLUDED_FILE_INFO

#include <string>
#include <ostream>
#include <iostream>
#include <stdio.h>
#include <vector>

class FileInfo {
// This stores info about a file that is going to be serialised

public:
    enum FileTypeEnum {
        UNKNOWN_FILE_TYPE,
        SUPPORT_FILE,
        BERKDB_FILE,
        LOG_FILE,
        OPTIONAL_FILE,
        RULESET_FILE
    };


private:
    FileTypeEnum m_type;

    std::string m_filepath;
    // Path to the file e.g. /bb/bin/mydb.lrl or /bb/data/mydb/mydb.dta

    std::string m_filename;
    // The relative filename that will be recorded in the serialised stream
    // e.g. mydb.lrl or mydb/mydb.dta

    size_t m_pagesize;
    // The recommended page size for this file, or 0 if it doesn't matter

    bool m_checksums;
    // true if checksums are enabled on this Berkeley file

    bool m_crypto;

    bool m_sparse;
    // true if the file is sparse

    bool m_do_direct_io;
    // true if we're going to open file with O_DIRECT

    bool m_swapped;
    // true if checksum needs to be swapped

    int64_t m_filesize;
    // file size as recorded in manifest
public:

    FileInfo();
    // Default constructor

    FileInfo(const FileInfo& copy);
    // Copy constructor

    FileInfo(FileTypeEnum type,
            const std::string& abspath,
            const std::string& dbdir,
            size_t pagesize = 4096,
       bool checksums = false,
       bool crypto = false,
       bool sparse = false,
       bool do_direct_io = true,
       bool swapped = false);
    // Convenient constructor

    virtual ~FileInfo();
    // Destructor

    FileInfo& operator=(const FileInfo& rhs);
    // Assignment operator

    void reset();


    // Setters
    void set_filepath(const std::string& filepath) { m_filepath = filepath; }
    void set_filename(const std::string& filename) { m_filename = filename; }
    void set_pagesize(size_t pagesize) { m_pagesize = pagesize; }
    void set_type(FileTypeEnum type) { m_type = type; }
    void set_checksums(bool checksums = true) { m_checksums = checksums; }

    void set_type(const std::string& tok);
    // Set the type from a string assumed to have come from the
    // get_type_string() method.  If the string is not recognised then the
    // type will be set to unknown.

    void set_filesize(int64_t filesize) { m_filesize = filesize; }

    void set_sparse(bool sparse = true)
    {
       fprintf(stderr, "set_sparse fileinfo called...\n");

       if (sparse)
          fprintf(stderr, "set_sparse fileinfo called with sparse true\n");

       m_sparse = sparse;
    }

    // Getters
    const std::string& get_filepath() const { return m_filepath; }
    const std::string& get_filename() const { return m_filename; }
    size_t get_pagesize() const { return m_pagesize; }
    FileTypeEnum get_type() const { return m_type; }
    bool get_checksums() const { return m_checksums; }
    bool get_crypto() const { return m_crypto; }
    bool get_sparse()const { return m_sparse; }
    bool get_swapped() const { return m_swapped; }
    int64_t get_filesize() const { return m_filesize; }


    const char *get_type_string() const;
    // Return a string constant which represents the type of this file

    bool get_direct_io() const { return m_do_direct_io; }
};


void write_manifest_entry(std::ostream& os, const FileInfo& file);
// Serialise a FileInfo into an output stream.  This is intended for writing
// out the manifest file.

bool read_FileInfo(const std::string& line, FileInfo& file);
// Attempt to deserialise a FileInfo object from the provided string.
// Returns true and populates file if this is possible, otherwise returns
// false.

bool read_incr_FileInfo(const std::string& line, FileInfo& file,
        std::vector<uint32_t>& incr_pages);


bool recognize_data_file(const std::string& filename,
        bool& is_data_file, bool& is_queue_file, bool& is_queuedb_file,
        std::string& out_table_name);
// Determine if the given filename looks like a table or queue file.  If it does
// then return true and set the is_ flags appropriately, and put the name of the
// object in out_table_name.

#endif // INCLUDED_FILE_INFO
