#ifndef INCLUDED_INCREMENT
#define INCLUDED_INCREMENT

#include <string>
#include "file_info.h"
#include <vector>
#include <set>

bool is_not_incr_file(std::string filename);

bool compare_checksum(FileInfo file, const std::string& incr_path,
        std::vector<uint32_t>& pages, ssize_t *data_size,
        std::set<std::string>& incr_files);

void write_incr_manifest_entry(std::ostream& os, const FileInfo& file,
                                const std::vector<uint32_t>& pages);

void write_del_manifest_entry(std::ostream& os, const std::string& incr_filename);

ssize_t write_incr_file(const FileInfo& file, std::vector<uint32_t> pages,
                            std::string incr_path);

std::string getDTString();

void incr_deserialise_database(
    const std::string lrldestdir,
    const std::string datadestdir,
    std::set<std::string>& table_set,
    unsigned percent_full,
    bool force_mode,
    bool legacy_mode,
    bool& is_disk_full,
    const std::string& incr_path
);

#endif
