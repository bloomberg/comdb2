#ifndef INCLUDED_INCREMENT
#define INCLUDED_INCREMENT

#include <string>
#include <vector>
#include <set>
#include <map>
#include <utility>


#include "file_info.h"

bool is_not_incr_file(std::string filename);
// Determine whether a file is not .incr or .sha

std::string get_sha_fingerprint(std::string filename, std::string incr_path);
// Read a .sha file to get the SHA fingerprint

std::string read_serialised_sha_file();
// read from STDIN to get the fingerprint from a serialised .SHA file

bool compare_checksum(
    FileInfo file,
    const std::string& incr_path,
    std::vector<uint32_t>& pages,
    ssize_t *data_size,
    std::set<std::string>& incr_files
);
// Compare a file's checksum and LSN with it's diff file to determine whether pages
// have been changed

void write_incr_manifest_entry(
    std::ostream& os,
    const FileInfo& file,
    const std::vector<uint32_t>& pages
);
// Write the manifest entry for a file that has been changed

void write_del_manifest_entry(std::ostream& os, const std::string& incr_filename);
// Write the manifest entry for a file that has been deleted

ssize_t serialise_incr_file(
    const FileInfo& file,
    std::vector<uint32_t> pages,
    std::string incr_path
);
// Given a file and a list of changed pages, serialise those pages to STDOUT

std::string getDTString();
// Get the YYYYMMDDHHMMSS string of the current timestamp

void incr_deserialise_database(
    const std::string& lrldestdir,
    const std::string& datadestdir,
    const std::string& dbname,
    std::set<std::string>& table_set,
    std::string& sha_fingerprint,
    unsigned percent_full,
    bool force_mode,
    std::vector<std::string>& options,
    bool& is_disk_full
);
// Deserialise an incremental backup from STDOUT

std::string read_incr_manifest(unsigned long long filesize);
// Read from STDIN the manifest file

bool process_incr_manifest(
    const std::string text,
    const std::string datadestdir,
    std::map<std::string, std::pair<FileInfo, std::vector<uint32_t>>>&
        updated_files,
    std::map<std::string, FileInfo>& new_files,
    std::set<std::string>& deleted_files,
    std::vector<std::string>& file_order,
    std::vector<std::string>& options,
    std::string& process_incr_manifest
);
// Process the tokens from the manifest file to create a map of
// changed, new, and deleted files as well as the order the changed files
// will be read in from STDIN

void unpack_incr_data(
    const std::vector<std::string>& file_order,
    const std::map<std::string, std::pair<FileInfo, std::vector<uint32_t>>>& updated_files,
    const std::string& datadestdir
);
// Unpack the changed files from the .data file read in from STDIN

void handle_deleted_files(
    std::set<std::string>& deleted_files,
    const std::string& datadestdir,
    std::set<std::string>& table_set
);
// Delete delted files

void unpack_full_file(
    FileInfo *file_info_pt,
    const std::string filename,
    unsigned long long filesize,
    const std::string datadestdir,
    bool is_data_file,
    unsigned percent_full,
    bool& is_disk_full
);
// Deserialise a full file

void clear_log_folder(const std::string& datadestdir, const std::string& dbname);
// Delete everything in the log folder to ensure there aren't log gaps

#endif
