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

#ifndef INCLUDED_COMDB2AR
#define INCLUDED_COMDB2AR

#include <stdint.h>
#include <stddef.h>

#include <sys/types.h>

#include <list>
#include <string>
#include <memory>

#include "fdostream.h"


const size_t MAX_BUF_SIZE = 4 * 1024 * 1024;


void errexit(int code = 1);
// Exits the program with a fatal error.  First it prints a single line
// "Error" to stderr.  This is important because the utilities that use us
// (copycomdb2 particularly) invoke us through rsh and don't easily get
// access to out exit status, so they rely on the error output clearly
// indicating that an error occured.


std::string& makeabs(std::string& abs_out, const std::string& dirname, const std::string& filename);
// Populate abs_out with an absolute path formed by the concatenation (with the
// appropriate directory separator) of dirname and filename.  If filename
// is already absolute then no change is made and it is copied verbatim into
// abs_out.

void makebasename(std::string& path);
// Turn a pathname into just the base file name in place.  If path doesn't
// contain any /'s then it is unchanged.

void makedirname(std::string& path);
// Strip a path down into just a directory name, by removing everything after
// the last / (and removing the last /).  If there is no / then this will leave
// just the empty string.

void listdir(std::list<std::string>& output_list, const std::string& dirname);
// Populates output_list with all the entries found in directory dirname.
// This will return subdirectories in its results, but not . or ..

void listdir_abs(std::list<std::string>& output_list, const std::string &dirname);
// Populate output_list with all the entries found in directory dirname.
// This will not recurse into subdirectories


void listdir_abs_recursive(std::list<std::string>& output_list,
        const std::string& dirname);
// Populates output_list with the absolute path names of all the entries found
// in directory dirname, which should be itself an absolute path.  This will
// follow non-symbolic subdirectories and include those too.


ssize_t writeall(int fd, const void *ptr, size_t nbytes);
// Write all the bytes to the given fd.  Return the number of bytes written,
// or -1 if we hit an error or 0 if we can't write (in which case it is
// possible that some bytes were written successfully; we just don't say how
// many to the caller).  Returns nbytes on success.


ssize_t readall(int fd, void *buf, size_t nbytes);
// Read all the bytes requested from the given fd.  Returns the number of bytes
// read, or -1 on error or 0 on eof.

bool read_octal_ull(const char *str, size_t len, unsigned long long& number);

std::unique_ptr<fdostream> output_file(
  const std::string& filename,
  bool make_sav, bool direct
);

void make_dirs(const std::string& dirname);

void remove_all_old_files(std::string& datadir);

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
);
// Serialise a database into tape archive format and write it to stdout.
// If support_only is true then only support files (lrl and schema) will
// be serialised.  If disable_log_deletion and the database is running then
// it will be advised to hold log file deletion until the backup is complete
// (highly recommended!)
// If legacy_mode is enabled, old file format are not removed after restore


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
);
// Deserialise a database from serialised form received on stdin.
// If lrldestdir and datadestdir are not NULL then the lrl and data files
// will be placed accordingly.  Otherwise the lrl file will be placed in
// /bb/bin and the data files will be placed in the same directory as
// specified in the serialised form.  The lrl file written out will be updated
// to reflect the resulting directory structure.  If run_full_recovery is
// true then full recovery is run on the resulting database using the binary
// given by comdb2_task.  If the destination disk reaches or exceeds the
// specified percent_full during the deserialisation then the operation is
// halted.

bool isDirectory(const std::string& file);

/* this definition needs to agree with DBENV_MAP in berkdb */
struct iomap {
    int memptrickle_time;
};


#endif
