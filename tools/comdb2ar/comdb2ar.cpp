// Comdb2 archive program
//
// This program can serialise a comdb2 database into tape archive format (tar)
// or deserialise it from the same.  It is the basis for copycomdb2,
// comdb2backup and comdb2restore.

#include "comdb2ar.h"
#include "util.h"

#include <exception>
#include <iostream>
#include <string>
#include <sstream>

#include <cstdlib>
#include <cstring>
#include <ctime>
#include <crc32c.h>

#include <sys/types.h>
#include <sys/stat.h>

// For getopt()
#include <unistd.h>
#include <stdio.h>

#include "str_util.h" /* QUOTE */

const char *help_text[] = {
"To serialise a db: comdb2ar [opts] c /bb/bin/mydb.lrl >output",
"To serialise a db incrementally:",
"  First, create a full backup in incremental mode-",
"     comdb2ar c -I create -b /bb/bin/increment [opts] /bb/bin/mydb.lrl > output",
"  Then, create an increment-",
"     comdb2ar c -I inc -b /bb/bin/increment [opts] /bb/bin/mydb.lrl > output",
"",
"  Database mydb is serialised into tape archive format on to stdout.",
"  -s   serialise support files only (lrl, csc2 etc, no data or log files)",
"  -L   do not disable log file deletion (dangerous)",
"",
"To deserialise a db: comdb2ar.tsk [opts] x [/bb/bin /bb/data/mydb] < input",
"To deserialise a db incrementally:",
"  cat db.tar in1.tar in2.tar | comdb2ar x -I restore [/bb/bin/ /bb/data/mydb] ",
"Where input is each increment concatenated together",
"  i.e. cat mydb.tar mydb_incr1.tar mydb_incr2.tar",
"",
"  The serialised database read from stdin is deserialised.  You can",
"  optionally specify destination directories for the lrl file and data",
"  directory; if ommitted the paths will be taken from the lrl in the",
"  serialised input stream.",
"  -C strip     strip cluster nodes lines from lrl file",
"  -C preserve  preserve cluster nodes lines in lrl file",
"  -I create    create the incremental meta files while serialising",
"  -I inc       create an increment for the incremental backup",
"  -I restore   restore from a sequence of base_backup | increments",
"  -b <path>    location to store/load the incremental backup",
"  -x <path>    path to comdb2 binary to use for full recovery",
"  -r/R         do/do-not run full recovery after extracting",
"  -u \%       do not allow disk usage to exceed this percentage",
"  -f           force deserialisation even if checksums fail",
"  -O           legacy mode, does not delete old format files",
"  -D           turn off directio",
"  -E dbname    create replicant with dbname",
"  -T type      override physrep type",
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

int main(int argc, char *argv[])
{
    crc32c_init(0);
    extern char *optarg;
    extern int optind, optopt;

    enum class Mode {create, extract, partial_create, partial_restore, none};
    Mode mode = Mode::none;

    int c;

    bool disable_log_deletion = true;
    bool support_files_only = false;
    bool strip_cluster_info = false;
    bool strip_consumer_info = false;
    bool run_full_recovery = true;
    bool run_with_done_file = false;
    bool force_mode = false;
    unsigned percent_full = 95;
    bool legacy_mode = false;
    bool add_latency = false;
    bool do_direct_io = true;
    bool incr_create = false;
    bool incr_gen = false;
    bool incr_ex = false;
    std::string incr_path;
    bool incr_path_specified = false;
    bool dryrun = false;
    bool copy_physical = false;

    std::string new_db_name = "";
    std::string new_type = "default";

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

    while((c = getopt(argc, argv, "hsSLC:I:b:x:u:rRSkKfODE:T:A")) != EOF) {
        switch(c) {
            case 'O':
                legacy_mode = true;
                break;

            case 'A':
                add_latency = true;
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

            case 'f':
                force_mode = true;
                break;

            case 'D':
                do_direct_io = false;
                break;

            case 'I':
                if(std::strcmp(optarg, "create") == 0) {
                    incr_create = true;
                } else if(std::strcmp(optarg, "inc") == 0) {
                    incr_gen = true;
                } else if(std::strcmp(optarg, "restore") == 0) {
                    incr_ex = true;
                } else {
                    std::cerr << "Unrecognized parameter to -I: " << optarg
                        << std::endl;
                    std::exit(2);
                }
                break;

            case 'b':
                incr_path_specified = true;
                incr_path = std::string(optarg);
                if(incr_path[incr_path.length() - 1] == '/'){
                    incr_path.resize(incr_path.length() - 1);
                }
                break;

            case 'y':
                dryrun = true;
                break;

            case 'E':
                new_db_name = std::string(optarg);
                copy_physical = true;
                strip_cluster_info = true;
                break;

            case 'T':
                new_type = std::string(optarg);
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

    if((incr_gen || incr_create) && !incr_path_specified){
        std::cerr << "If running in incremental mode, a path to the directory where the"
            << " incremental files will be stored is required" << std::endl;
        std::exit(2);
    }

    for(const char *cp = argv[0]; *cp; ++cp) {
        switch(*cp) {
            case 'c':
              mode = Mode::create;
                break;

            case 'p':
              mode = Mode::partial_create;
                break;

            case 'P':
              mode = Mode::partial_restore;
                break;

            case 'x':
              mode = Mode::extract;
                break;

            default:
                std::cerr << "Unknown mode command '" << *cp << "'" << std::endl;
                std::exit(2);
        }
    }

    // Just in case.. we can be invoked via rsh, which sometimes has been
    // known to set an odd umask
    umask(02);

    if(mode == Mode::none) {
        std::cerr << "Must specify a valid mode" << std::endl;
        std::exit(2);

    } else if(mode == Mode::create) {
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
                new_type,
                new_db_name,
                comdb2_task,
                disable_log_deletion,
                strip_cluster_info,
                support_files_only,
                run_with_done_file,
                do_direct_io,
                incr_create,
                incr_gen,
                copy_physical,
                add_latency,
                incr_path
            );
        } catch(std::exception& e) {
            std::cerr << e.what() << std::endl;
            errexit();
        }

    } else if(mode == Mode::extract) {

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
             run_full_recovery,
             comdb2_task,
             percent_full,
             force_mode,
             legacy_mode,
             is_disk_full,
             run_with_done_file,
             incr_ex,
             dryrun
           );
        } catch(std::exception& e) {
            std::cerr << e.what() << std::endl;
            if (is_disk_full) {
              errexit(3);
            } else {
              errexit();
            }
        }
    } else if (mode == Mode::partial_create) {
        // A lot like create, we create a tarball, but we don't do
        // lots of other things that create does, so it's a great
        // deal easier to skip that code
        try {
            const std::string lrlpath(argv[1]);
            create_partials(lrlpath, do_direct_io);
        }
        catch(std::exception& e) {
            std::cerr << e.what() << std::endl;
            errexit();
        }
    } else if (mode == Mode::partial_restore) {
        // Also a lot like restore, with enough differences
        // that it's incredibly ... unclean ... to make the
        // restore code do what we need
        if (argc != 2) {
            std::cerr << "Expected lrl path." << std::endl;
            std::exit(2);
        }
        std::string lrlpath = argv[1];
        try {
            restore_partials(lrlpath, comdb2_task, run_full_recovery, false, dryrun);
        }
        catch(std::exception& e) {
            std::cerr << e.what() << std::endl;
            errexit();
        }
    }

    return 0;
}
