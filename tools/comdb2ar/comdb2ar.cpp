// Comdb2 archive program
//
// This program can serialise a comdb2 database into tape archive format (tar)
// or deserialise it from the same.  It is the basis for copycomdb2,
// comdb2backup and comdb2restore.

#include "comdb2ar.h"

#include <exception>
#include <iostream>
#include <string>
#include <sstream>

#include <cstdlib>
#include <cstring>
#include <crc32c.h>

#include <sys/types.h>
#include <sys/stat.h>

// For getopt()
#include <unistd.h>
#include <stdio.h>

extern "C" {
#include "mem.h"
}

const char *help_text[] = {
"To serialise a db: comdb2ar [opts] c /bb/bin/mydb.lrl >output",
"",
"  Database mydb is serialised into tape archive format on to stdout.",
"  -s   serialise support files only (lrl, csc2 etc, no data or log files)",
"  -L   do not disable log file deletion (dangerous)",
"",
"To deserialise a db: comdb2ar.tsk [opts] x [/bb/bin /bb/data/mydb] <input",
"",
"  The serialised database read from stdin is deserialised.  You can",
"  optionally specify destination directories for the lrl file and data",
"  directory; if ommitted the paths will be taken from the lrl in the",
"  serialised input stream.",
"  -C strip     strip cluster nodes lines from lrl file",
"  -C preserve  preserve cluster nodes lines in lrl file",
"  -x <path>    path to comdb2 binary to use for full recovery",
"  -r/R         do/do-not run full recovery after extracting",
"  -u %         do not allow disk usage to exceed this percentage",
"  -f           force deserialisation even if checksums fail",
"  -O           legacy mode, does not delete old format files",
"  -D           turn off directio",
NULL
};

static void usage()
{
    for(int ii = 0; help_text[ii]; ii++) {
        std::cerr << help_text[ii] << std::endl;
    }
}

void errexit(int code)
// Exits the program with a fatal error.  First it prints a single line
// "Error" to stderr.  This is important because the utilities that use us
// (copycomdb2 particularly) invoke us through rsh and don't easily get
// access to out exit status, so they rely on the error output clearly
// indicating that an error occured.
{
    std::cerr << "Error" << std::endl;
    std::exit(code);
}

#define QUOTE_(x) #x
#define QUOTE(x) QUOTE_(x)

extern "C" {
int tool_comdb2ar_main(int argc, char *argv[])
{
    comdb2ma_init(0, 0);
    crc32c_init(0);

    extern char *optarg;
    extern int optind, optopt;

    enum modes_enum {CREATE_MODE, EXTRACT_MODE, NO_MODE};
    modes_enum mode = NO_MODE;

    int c;

    bool disable_log_deletion = true;
    bool support_files_only = false;
    bool strip_cluster_info = false;
    bool strip_consumer_info = false;
    bool run_full_recovery = true;
    bool run_with_done_file = false;
    bool kludge_write = true;
    bool force_mode = false;
    unsigned percent_full = 95;
    bool legacy_mode = false;
    bool do_direct_io = true;

    // TODO: should really consider using comdb2file.c
    char *s = getenv("COMDB2_ROOT");
    std::string root;
    if (s == NULL)
        root = QUOTE(COMDB2_ROOT);
    else
        root = s;

    std::ostringstream ss;
    ss << root << "/bin/comdb2";
    std::string comdb2_task(ss.str());

    while((c = getopt(argc, argv, "hsSLC:x:u:rRSKfOD")) != EOF) {
        switch(c) {
            case 'O':
                legacy_mode = true;
                break;

            case 'h':
                usage();
                std::exit(0);
                break;

            case 's':
                support_files_only = true;
                break;

            case 'C':
                if(std::strcmp(optarg, "strip") == 0) {
                    strip_cluster_info = true;
                } else if(std::strcmp(optarg, "preserve") == 0) {
                    strip_cluster_info = false;
                } else if(std::strcmp(optarg, "qa") == 0) {
                    strip_consumer_info = true;
                    strip_cluster_info = true;
                } else {
                    std::cerr << "Unrecognised parameter to -C: " << optarg
                        << std::endl;
                    std::exit(2);
                }
                break;

            case 'x':
                comdb2_task = optarg;
                break;

            case 'u':
                percent_full = std::atoi(optarg);
                break;

            case 'L':
                disable_log_deletion = false;
                break;

            case 'r':
                run_full_recovery = true;
                break;

            case 'R':
                run_full_recovery = false;
                break;

                // run over rSh
                // serialize writes extra DONE file
                // as last file, then blocks on stdin for string "DONE"
                // deserialize writes "DONE" to stdout when it recieves the
                // DONE file.
            case 'S':
                run_with_done_file = true;
                break;

            case 'K':
                kludge_write = true;
                break;


            case 'f':
                force_mode = true;
                break;

            case 'D':
                do_direct_io = false;
                break;

            case '?':
                std::cerr << "Unrecognised option: -" << (char)c << std::endl;
                usage();
                std::exit(2);
        }
    }

    argc -= optind;
    argv += optind;

    if(argc < 1) {
        usage();
        std::exit(2);
    }

    for(const char *cp = argv[0]; *cp; ++cp) {
        switch(*cp) {
            case 'c':
                mode = CREATE_MODE;
                break;

            case 'x':
                mode = EXTRACT_MODE;
                break;

            default:
                std::cerr << "Unknown mode command '" << *cp << "'" << std::endl;
                std::exit(2);
        }
    }

    // Just in case.. we can be invoked via rsh, which sometimes has been
    // known to set an odd umask
    umask(02);

    if(mode == NO_MODE) {
        std::cerr << "Must specify a valid mode" << std::endl;
        std::exit(2);

    } else if(mode == CREATE_MODE) {
        // In create mode we expect one more parameter, the path to the lrl
        // file that we must back up.
        if(argc != 2) {
            std::cerr << "Expected exactly two arguments for create mode" << std::endl;
            std::exit(2);
        }

        const std::string lrlpath(argv[1]);

        try {
           serialise_database(
             lrlpath,
             comdb2_task, 
             disable_log_deletion,
             strip_cluster_info,
             support_files_only, 
             run_with_done_file,
             kludge_write,
             do_direct_io
           );
        } catch(std::exception& e) {
            std::cerr << e.what() << std::endl;
            errexit();
        }

    } else if(mode == EXTRACT_MODE) {

        // In extract mode we expect two parameters or none
        std::string lrldest, datadest;
        std::string *p_lrldest(NULL);
        std::string *p_datadest(NULL);
        if(argc <= 3) {
            if(argc >= 2) {
                lrldest = argv[1];
                p_lrldest = &lrldest;
            }
            if(argc >= 3) {
                datadest = argv[2];
                p_datadest = &datadest;
            }
        } else  {
            std::cerr << "Unexpected extra arguments" << std::endl;
            std::exit(2);
        }
        bool is_disk_full = false;
        try {
           deserialise_database(
             p_lrldest,
             p_datadest,
             strip_cluster_info,
             strip_consumer_info,
             comdb2_task,
             percent_full,
             force_mode,
             legacy_mode,
             is_disk_full,
             run_with_done_file
           );
        } catch(std::exception& e) {
            std::cerr << e.what() << std::endl;
            if (is_disk_full) {
              errexit(3);
            } else {
              errexit();
            }
        }
    }

    return 0;
}
}
