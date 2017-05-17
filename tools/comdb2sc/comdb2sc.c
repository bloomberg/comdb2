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

/*
 * Comdb2 synchronous socket based schema changes.  The objective of this
 * program is to reduce Paul Graham's job to copy and paste.
 *
 * $Id$
 */

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <alloca.h>
#include <time.h>
#include <unistd.h>

/* including this ruins debugging for sun (?) */

#include <netinet/in.h>
#include <portmuxusr.h>
#include <tcputil.h>
#include <sbuf2.h>
#include <str0.h>
#include <bb_oscompat.h>

#include <mem.h>

static int verbose = 0;
static int report_seconds = 0;
static int quiet = 0;
static char tempfile[1024];
static int attempt_fix = 0;
static int have_table_dbnum = 0;
static int table_dbnum;
static int bulk_import = 0;
static int verify = 0;

enum
{
   ALIAS_NO    = 0,
   ALIAS_SET   = 1,
   ALIAS_GET   = 2,
   ALIAS_REM   = 3,
};


static const char *usage_text[] =
{
"Usage: comdb2sc.tsk [options] dbname <command> [[machine:]/path/to/schemafile.csc2]",
"Where <command> can be one of:",
"    add        tablename schemafile.csc2",
"    addsp",
"    alter      tablename schemafile.csc2",
"    analyze    [tablename]",
"    analyzecoverage  tablename [sample-percent]",
"    analyzethreshold  tablename [threshold]",
"    bulkimport tablename srcmachine srcdbname [srctablename]",
"    defaultsp",
"    drop       tablename",
"    fastinit   tablename [schemafile.csc2]",
"    rebuild    tablename",
"    rebuildindex  tablename indexname",
"    rebuilddata   tablename ",
"    rebuildblobsdata  tablename ",
"    rowlocks   ['enable'/'disable']",
"    send",
"    showsp",
"    spname",
"    stat       tablename",
"    testcompr  [tablename]",
"    verify     tablename",
"    setcompr   tablename",
"    debug      filename",
"    alias      set tablename tablename_url",
"    alias      get tablename",
"    alias      rem tablename",
"    trigger    add name configfile destinations",
"    trigger    drop name",
"    trigger    alter name configfile destinations",
"",
"This program performs a synchronous schema change on the specified comdb2",
"database.  The database must be up and running with all nodes connected.",
"",
"The new schema file is only required if 'add' or 'alter' is specified.",
"Otherwise the database will use its existing schema for the table.",
"",
/*"Note that you can specify a new schema with a fastinit, in which case the database",
"does a combined schema change and initialization.",*/
"",
"If tablename is not provided with analyze or testcompr, then all tables in the ",
"database will be processed.",
"",
"Valid options include:-",
"   -o      Enable ODH (on-disk headers) for table.",
"   -O      Remove ODH for table.",
"   -z <blob|rec>:<none|rle|zlib|crle|lz4>",
"           Set compression option for blobs or records. Valid options are:",
"           blob:type  where type is one of none, rle, zlib or lz4",
"           rec:type   where type is one of none, rle, zlib, lz4 or crle (comdb2 rle).",
"           Enabling compression for either blobs or data records implies the -o option.",
"   -i on|off",
"           Set or remove in-place updates for table.  Enabling in-place ",
"           updates implies the -o option.",
"   -n on|off",
"           Set or remove instant schema-change for table.  Enabling instant",
"           schema-change implies the -o option.",
"   -l      Perform live schema change - database is available for reads",
"           and writes throughout.",
"   -L      Disable live schema change - database goes readonly while it",
"           rebuilds the table.",
"   -p      Use an optimized schema change plan rather than rebuilding all",
"           btrees even if they haven't changed.",
"   -P      Do not use optimised plan; rebuild all btrees.",
"   -d      Live schema change only - delay the commit of the new schema until",
"           the 'sccommit' message trap is received.  Allows you to have split",
"           second precision over when the new schema will take effect.",
"   -s index|stripe|parallel|dump",
"           Set table scan mode.  Usually you should just accept the default.",
"   -r      Force a full rebuild of the table regardless of the extent of the",
"           changes.",
"   -f      Force the schema change to go ahead even if disk space is low.",
"   -A      Disable advisory filename mode i.e. database will choose it's",
"           own filenames.",
"   -c sample-percent",
"           Run analyze sampling a percent of the records in the index.",
"           The sampling-percent default value is 20.",
"           If analyzecoverage was previously called to save in llmeta the sample-percent,",
"           the -c parameter will take precendence. Note that if table size is ",
"           less than the stored threshold (analyzethreshold) then coverage is 100%.",
"   -b      Backout analysis.  Reverts table to the previous analyze-stats.",
"   -t tblthds",     
"           Set number of concurrent analyze table-threads.",
"   -T cmpthds",
"           Set number of concurrent analyze compression-threads.",
"   -H threshld",
"           Set the sampling-threshold.  Tables smaller than this will be",
"           completely scanned.",
"   -y      Dry run mode. Don't actually perform the schemachange.",
"   -v      Verbose mode.",
"   -V sec  Verbose mode for verify, progress report every <sec> seconds.",
"   -q      Quiet mode.",
"   -F      For verify mode, attempt to fix errors.",
"   -m      Force command to go to master",
"   -D node Execute on node.",
"   -g db#  Set db# for comdbg compatibility",
NULL
};

static void
usage(void)
{
    int ii;
    for(ii = 0; usage_text[ii]; ii++)
        fprintf(stderr, "%s\n", usage_text[ii]);
    exit(2);
}

static void
remove_tempfile(void)
{
    if(verbose)
        printf("removing %s\n", tempfile);
    unlink(tempfile);
}

static const char *
make_schema_local(const char *src)
{
    char *colon;
    char cmd[1024];
    struct stat st;

    colon = strchr(src, ':');
    
    if(!colon)
        return src;

    /* Looks like this is a remote file. */
    snprintf(tempfile, sizeof(tempfile), "/tmp/comdb2_sc_%d.csc2", (int)getpid());
    snprintf(cmd, sizeof(cmd), "/bb/bin/bb_filexfer %s %s", src, tempfile);
    if(verbose)
        printf("%s\n", cmd);
    if(system(cmd) == -1)
    {
        fprintf(stderr, "cannot run %s: %d %s\n",
                cmd, errno, strerror(errno));
        exit(1);
    }

    if(stat(tempfile, &st) == -1)
    {
        fprintf(stderr, "%s failed\n", cmd);
        exit(1);
    }
    
    atexit(remove_tempfile);
    return tempfile;
}

static void wait_for_sc(SBUF2 *sb);

static SBUF2 *
connect_db(char *dbname, int tomaster, char *direct, int prefer_local)
{
    int fd;
    char buf[255];
    SBUF2 *sb;
    char *s;
    char *host = NULL;
    int port = 0;

    if (direct)
        host = direct;
    else {
        s = strchr(dbname, ':');
        if (s) {
            *s = 0;
            host = dbname;
            dbname = s+1;
        }
        s = strchr(dbname, ':');
        if (s) {
            *s = 0;
            port = atoi(s+1);
        }
    }

    if (host == NULL)
        host = "localhost";

    if (port == 0)
        fd = portmux_connect(host, "comdb2", "replication", dbname);
    else 
        fd = tcpconnecth(host, port, 0);
    if (fd < 0) {
        fprintf(stderr, "Can't connect to %s: %d\n", dbname, fd);
        return NULL;
    }
    sb = sbuf2open(fd, 0);
    if (sb == NULL) {
        close(fd);
        fprintf(stderr, "Can't allocate buffer for connection to %s\n", dbname);
        return NULL;
    }
    if (tomaster) {
        sbuf2printf(sb, "whomasterhost\n");
        sbuf2flush(sb);
        if (sbuf2gets(buf, sizeof(buf), sb) < 0) {
            fprintf(stderr, "Can't discover master for %s\n", dbname);
            return NULL;
        }
        char *s;
        s = strchr(buf, '\n');
        if (s) *s = 0;
        sbuf2close(sb);
        fd = portmux_connect(buf, "comdb2", "replication", dbname);
        if (fd < 0) {
            fprintf(stderr, "Can't connect to master for %s: %d\n", dbname, fd);
            return NULL;
        }
        sb = sbuf2open(fd, 0);
        if (sb == NULL) {
            close(fd);
            fprintf(stderr, "Can't allocate buffer for connection to %s\n", dbname);
            return NULL;
        }
    }
    return sb;
}

void send_schema_change_info(SBUF2 *sb,
        const char *addalter,
        const char *tablename,
        const char *scanmode,
        const char *livemode,
        int delay_commit,
        int full_rebuild,
        int ignore_diskspace,
        const char *planmode,
        const char *odh_opt,
        const char *rec_compr_opt,
        const char *blob_compr_opt,
        int inplace_updates,
        int instant_sc,
        int dryrun,
        int stat_command,
        const char *extrastuff,
        FILE *fh,
        int delsp,
        int defaultsp,
        int showsp,
        int use_aname,
        const char *saved_schemapath,
        int have_table_dbnum) 
{

    char line[1024] = {0};

    sbuf2printf(sb, "schemachange\n");
    sbuf2printf(sb, "%s table:%s", addalter, tablename);
    if(scanmode)
        sbuf2printf(sb, " %s", scanmode);
    if(livemode)
        sbuf2printf(sb, " %s", livemode);
    if(delay_commit)
        sbuf2printf(sb, " waitcommit");
    if(full_rebuild)
        sbuf2printf(sb, " fullrebuild");
    if(ignore_diskspace)
        sbuf2printf(sb, " ignoredisk");
    if(planmode)
        sbuf2printf(sb, " %s", planmode);
    if(odh_opt)
        sbuf2printf(sb, " %s", odh_opt);
    if(rec_compr_opt)
        sbuf2printf(sb, " rec_%s", rec_compr_opt);
    if(blob_compr_opt)
        sbuf2printf(sb, " blob_%s", blob_compr_opt);
    if(1 == inplace_updates)
        sbuf2printf(sb, " ipu_on");
    if(2 == inplace_updates)
        sbuf2printf(sb, " ipu_off");
    if(1 == instant_sc)
        sbuf2printf(sb, " instant_sc_on");
    if(2 == instant_sc)
        sbuf2printf(sb, " instant_sc_off");
    if(1 == dryrun)
        sbuf2printf(sb, " dryrun");
    if(1 == stat_command)
        sbuf2printf(sb, " stat");
    if(extrastuff[0])
        sbuf2printf(sb, " %s", extrastuff);
    if(!fh && !delsp && !defaultsp && !showsp)
        sbuf2printf(sb, " noschema");
    if(use_aname && saved_schemapath)
    {
        char aname[128];
        char *start;

        /* Send the advised schema file name to the db to match the
         * supplied file name. */
        start = strrchr(saved_schemapath, '/');
        if(!start)
            strncpy0(aname, saved_schemapath, sizeof(aname));
        else
            strncpy0(aname, start+1, sizeof(aname));
        /* strip .csc2 extension */
        start = strrchr(aname, '.');
        if(start && strcmp(start, ".csc2") == 0)
            *start = 0;
        sbuf2printf(sb, " aname:%s", aname);
    }
    if (have_table_dbnum) {
        sbuf2printf(sb, " dbnum:%d", table_dbnum);
    }
    sbuf2printf(sb, "\n");

    /* Send new schema down the wire.  Terminate with a . all on its own. */
    if(delsp || defaultsp || showsp) {
        if (saved_schemapath)  
            sbuf2printf(sb, "%s", saved_schemapath);
    } else if(fh) {
        while(fgets(line, sizeof(line), fh))
        {
            sbuf2printf(sb, "%s", line);
        }
    }
    sbuf2printf(sb, "\n.\n");
    sbuf2flush(sb);
}

int do_trigger_cmd(int argc, char *argv[]) {
    char *dbname = argv[0];
    const char *name;
    SBUF2* sb;
    const char *action;
    const char *path = NULL; 
    const char *localpath = NULL;
    const char *destinations = NULL;
    char *all_destinations = "";
    FILE *fh = NULL;

#if 0
    comdb2sc.tsk dbname trigger add name configfile destinations
    comdb2sc.tsk dbname trigger del name 
    comdb2sc.tsk dbname trigger alter name configfile destinations
#endif

    if (argc <= 3)
        usage();

    argc -= 2;
    argv += 2;

    /* Connect to database. */
    sb = connect_db(dbname, 1, 0, 0);
    if (sb == NULL)
        exit(1);
    action = argv[0];
    name = argv[1];

    if (strcmp(action, "add") == 0 || strcmp(action, "alter") == 0) {
        if (argc < 4)
            usage();
        path = argv[2];
        localpath = make_schema_local(path);
        
        if (strcmp(action, "add") == 0)
            action = "trigger add";
        else
            action = "trigger alter";

        if (localpath == NULL) {
            fprintf(stderr, "Can't get local copy of config file\n");
            return 1;
        }
        fh = fopen(localpath, "r");
        if (fh == NULL) {
            fprintf(stderr, "Can't open local copy of config file %s %d %s\n", localpath, errno, strerror(errno));
            return 1;
        }

        int destlen = 0;
        for (int i = 3; i < argc; i++) {
            destlen += strlen(argv[i]);
            destlen += 5;  /* dest: */
            if (i != argc-1)
                destlen++; /* space */
        }
        destlen++; /* null */
        all_destinations = calloc(1, destlen);
        for (int i = 3; i < argc; i++) {
            strcat(all_destinations, "dest:");
            strcat(all_destinations, argv[i]);
            strcat(all_destinations, " ");
        }
    }
    else if (strcmp( action, "drop") == 0) {
        action = "trigger drop";
        if (argc != 2)
            usage();
    }
    else
        usage();

    send_schema_change_info(
        /* SBUF2 *sb */sb,
        /* const char *addalter */ action,
        /* const char *tablename */ name,
        /* const char *scanmode */ NULL,
        /* const char *livemode */ NULL,
        /* int delay_commit */ 0,
        /* int full_rebuild */  0,
        /* int ignore_diskspace */ 0,
        /* const char *planmode */ NULL,
        /* const char *odh_opt */ NULL,
        /* const char *rec_compr_opt */ NULL,
        /* const char *blob_compr_opt */ NULL,
        /* int inplace_updates */ 0,
        /* int instant_sc */ 0,
        /* int dryrun */ 0,
        /* int stat_command */ 0,
        /* const char *extrastuff */ all_destinations,
        /* FILE *fh */ fh,
        /* int delsp */ 0,
        /* int defaultsp */ 0,
        /* int showsp */ 0,
        /* int use_aname */ 0,
        /* const char *saved_schemapath */ NULL,
        /* int have_table_dbnum */ 0);

    wait_for_sc(sb);

    return 0;
}

static void wait_for_sc(SBUF2 *sb) {
    char line[1024];
    int rc;

    /* Await status updates */
    while((rc = sbuf2gets(line, sizeof(line), sb)) >= 0)
    {
        if(rc == 0)
            ;
        else if(strcmp(line, "SUCCESS\n") == 0)
            break;
        else if(strcmp(line, "FAILED\n") == 0)
        {
            fprintf(stderr, "%s failed\n",
                    (bulk_import) ? "Bulk Import" : verify ? "Verify" : "Schema change");
            exit(1);
        }
        else if(line[0] == '?')
        {
            /* status update that we should parse, ignore if we don't
             * understand */
            if(!quiet && strncmp(line + 1, "progress ", 9) == 0)
                printf("%s", line + 1);
            else if(verbose)
                printf("Info> %s", line + 1);
        }
        else if(line[0] == '!')
        {
            /* Error message, write to stderr */
            fprintf(stderr, "%s", line + 1);
        }
        else if (line[0] == '>') {
            /* just echo these */
            fprintf(stdout, "%s", line+1);
        }
        else
        {
            fprintf(stderr, "Unexpected response: '%s'\n", line);
            exit(1);
        }
    }
}

int
main(int argc, char *argv[])
{
    int c, rc;
    extern char *optarg;
    extern int optind, optopt;
    int newtable = 0;
    int newsp = 0;
    int delsp = 0;
    int defaultsp = 0;
    char *direct = NULL;
    int showsp = 0;
    int tomaster = 1;
    char *dbname;
    const char *addalter;
    const char *tablename = NULL;
    const char *schemapath = NULL;
    const char *bulk_import_src_mach;
    const char *bulk_import_src_dbname;
    const char *bulk_import_src_tablename;
    const char *saved_schemapath;
    SBUF2 *sb;
    FILE *fh = NULL;
    char line[1024];
    const char *livemode = NULL;
    char scanmodebuf[16];
    const char *scanmode = NULL;
    int delay_commit = 0;
    int full_rebuild = 0;
    int ignore_diskspace = 0;
    int schema_required = 1;
    int schema_provided = 0;
    int fastinit = 0;
    int rowlocks = 0;
    int drop = 0;
    int stat_command = 0;
    char extrastuff[64] = "";
    const char *planmode = NULL;
    int use_aname = 1;
    const char *odh_opt = NULL;
    const char *rec_compr_opt = NULL;
    const char *blob_compr_opt = NULL;
    int inplace_updates = 0;
    int instant_sc = 0;
    int compress_opts = 0;
    int dryrun = 0;
    int analyze = 0;
    uint8_t analyzecoverage = 0;
    int analyzecoveragevalue = 0;
    uint8_t analyzethreshold = 0;
    long long analyzethresholdval = 0;
    int scaling_factor = 20;
    int override_scaling_factor = 0; /* override llmeta val with this param */
    int backout_analysis = 0;
    int analyze_tblthds = -1;
    int analyze_cmpthds = -1;
    int analyze_threshd = -1;
    char analyze_subcmd[100]={0};
    int send = 0;
    int forcemaster = 0;
    int testcompr = 0;
    int setcompr = 0;
    int debug = 0;
    int is_schema_change = 0;
    int indices_changed = 0;
    const char *algo = NULL;
    int        alias_op = 0;
    char       *alias_to = NULL;

    comdb2ma_init(0, 0);

    /* seed random */
    srand(time(NULL) * getpid());

    while((c = getopt(argc, argv, "vV:hlLpPs:qdrfx:Ac:t:T:H:boOz:i:n:yFmD:g:")) != EOF)
    {
        switch(c)
        {
            default:
            case 'h':
                usage();
                /* does not return, but break anyway for good style */
                break;

            case 'l':
                livemode = "live";
                break;
            case 'L':
                livemode = "nolive";
                break;

            case 'p':
                planmode = "useplan";
                break;
            case 'P':
                planmode = "noplan";
                break;

            case 's':
                if(strcmp(optarg, "index") == 0 ||
                   strcmp(optarg, "dump") == 0 ||
                   strcmp(optarg, "stripe") == 0 ||
                   strcmp(optarg, "parallel") == 0 ||
                   strcmp(optarg, "old") == 0)
                {
                    snprintf(scanmodebuf, sizeof(scanmodebuf),
                            "%sscan", optarg);
                    scanmode = scanmodebuf;
                }
                else
                {
                    usage();
                }
                break;

            case 'x':
                strncpy0(extrastuff, optarg, sizeof(extrastuff));
                break;

            case 'd':
                delay_commit = 1;
                break;

            case 'r':
                full_rebuild = 1;
                break;

            case 'f':
                ignore_diskspace = 1;
                break;
                
            case 'v':
                verbose = 1;
                break;

            case 'V':
                report_seconds = atoi(optarg);
                break;

            case 'F':
                attempt_fix = 1;
                break;

            case 'q':
                quiet = 1;
                break;

            case 'A':
                use_aname = 0;
                break;

            case 'o':
                odh_opt = "add_headers";
                break;

            case 'O':
                odh_opt = "remove_headers";
                break;

            case 'c':
                scaling_factor = atoi(optarg);
                if(scaling_factor < 1)
                {
                    fprintf(stderr,"invalid scaling factor\n");
                    usage();
                }
                override_scaling_factor = 1;
                break;

            case 'b':
                backout_analysis = 1;
                break;

            case 't':
                analyze_tblthds = atoi( optarg );
                break;

            case 'T':
                analyze_cmpthds = atoi( optarg );
                break;

            case 'H':
                analyze_threshd = atoi( optarg );
                break;

            case 'i':
                if( strncmp( optarg, "on", 2 ) == 0 )
                {
                    odh_opt = "add_headers";
                    inplace_updates = 1;
                }
                else if( strncmp( optarg, "off", 3 ) == 0 )
                    inplace_updates = 2;
                else
                    usage();
                break;
            case 'n':
                if( strncmp( optarg, "on", 2 ) == 0 )
                {
                    odh_opt = "add_headers";
                    instant_sc = 1;
                }
                else if( strncmp( optarg, "off", 3 ) == 0 )
                    instant_sc = 2;
                else
                    usage();
                break;
            case 'z':
                {
                    const char *colon;
                    const char **opt;
                    if(strncmp(optarg, "blob:", 5) == 0)
                        opt = &blob_compr_opt;
                    else if(strncmp(optarg, "rec:", 4) == 0)
                        opt = &rec_compr_opt;
                    else
                        usage();
                    colon = strchr(optarg, ':') + 1;
                    if (strcmp(colon, "none") == 0) {
                        *opt = "nocompress";
                    } else if (strcmp(colon, "zlib") == 0) {
                        *opt = "zlib";
                        odh_opt = "add_headers";
                    } else if (strcmp(colon, "rle") == 0 || strcmp(colon, "rle8") == 0) {
                        *opt = "rle8";
                        odh_opt = "add_headers";
                    } else if (strcmp(colon, "crle") ==0) {
                        if (opt == &blob_compr_opt) {
                            usage();
                        }
                        *opt = "crle";
                        odh_opt = "add_headers";
                    } else if (strcmp(colon, "lz4") == 0) {
                        *opt = "lz4";
                        odh_opt = "add_headers";
                    } else {
                        usage();
                    }
                    compress_opts = 1;
                }
                break;
            case 'y':
                dryrun = 1;
                break;

            case 'm':
                forcemaster = 1;
                break;

            case 'D':
                direct=optarg;
                break;

            case 'g':
                have_table_dbnum = 1;
                table_dbnum =  atoi(optarg);
                break;
        }
    }

    argc -= optind;
    argv += optind;

    if(argc < 2 || (strcmp(argv[1], "send") && argc > 6))
        usage();


    dbname = argv[0];
    addalter = argv[1];

    if (strcmp(addalter, "trigger") == 0)
        return do_trigger_cmd(argc, argv);

    if(argc >= 3)
        tablename = argv[2];
    if (argc > 3)
        schemapath = argv[3];
    if(strcmp(addalter, "add") == 0)
    {
        newtable = 1;
        schema_required = 1;
    }
    else if(strcmp(addalter, "addsp") == 0)
    { 
        newsp = 1;
        schema_required = 1;
    }
    else if(strcmp(addalter, "delsp") == 0)
    { 
        delsp = 1;
        schema_required = 0;
    }    
    else if(strcmp(addalter, "defaultsp") == 0)
    { 
        defaultsp = 1;
        schema_required = 0;
    }  
    else if(strcmp(addalter, "showsp") == 0)
    { 
        if (!tablename)
            tablename = "";
        showsp = 1;
        schema_required = 0;
    }      
    else if(strcmp(addalter, "alter") == 0)
    {
        schema_required = (instant_sc || inplace_updates || compress_opts) ? 0 : 1;
        if (schemapath)
            schema_provided = 1;
        is_schema_change = 1;
    }
    else if (strcmp(addalter, "stat") == 0)
    {
        schema_required = 0;
        if (NULL == tablename)
            usage();
    }
    else if(strcmp(addalter, "rebuild") == 0)
    {
        addalter = "alter";
        full_rebuild = 1;
        schema_required = 0;
    }    
    else if(strcmp(addalter, "rebuildindex") == 0)
    {
        schema_required = 0;
    }
    else if(strcmp(addalter, "rebuilddata") == 0)
    {
        schema_required = 0;
    }
    else if(strcmp(addalter, "rebuildblobsdata") == 0)
    {
        schema_required = 0;
    }
    else if(strcmp(addalter, "rowlocks") == 0)
    {
        schema_required = 0;
        if (tablename==NULL)
            usage();
        if (strcmp(tablename, "enable") == 0)
            rowlocks = 1;
        else if (strcmp(tablename, "disable") == 0)
            rowlocks = 2;
        else
            usage();
    }
    else if(strcmp(addalter, "fastinit") == 0)
    {
        fastinit = 1;
        schema_required = 0;
        if (schemapath)
            schema_provided = 1;
    }
    else if(strcmp(addalter, "drop") == 0)
    {
        drop = 1;
        fastinit = 1;
        schema_required = 0;
    }    
    else if(strcmp(addalter, "bulkimport") == 0)
    {
        if(optind != 1 || (argc != 5 && argc != 6))
            usage();

        bulk_import = 1;
        bulk_import_src_mach = argv[3];
        bulk_import_src_dbname = argv[4];
        bulk_import_src_tablename = argv[5];
        schema_required = 0;
        schemapath = NULL;

        if(!bulk_import_src_mach || !bulk_import_src_dbname)
            usage();
    }
    else if (strcmp(addalter, "verify") == 0)
    {
        schema_required = 0;
        verify = 1;
    }
    else if (strcmp(addalter, "analyze") == 0)
    {
        /* analyze defaults to '-all' without a tablename */
        if( !tablename ) 
            tablename="-all";

        schema_required = 0;
        analyze = 1;

        /* send backouts to master */
        if( backout_analysis )
        {
            tomaster = 1 ;
        }
        else
        {
            tomaster = 0 ;
        }
    }    
    else if (strcmp(addalter, "analyzethreshold") == 0)
    {
        if( !tablename) {
            usage();
            return 0;
        }

        schema_required = 0;
        analyzethreshold = 1;
        if(argc > 3)
            analyzethresholdval = atoll(argv[3]);
        else
            analyzethresholdval = -1;

        if( analyzethresholdval < -1 ) {
            fprintf(stderr, "Analyze threshold value '%lld'needs to be >= 0, leave blank to remove entry for table\n", analyzethresholdval);
            return 0;
        }
    }
    else if (strcmp(addalter, "analyzecoverage") == 0)
    {
        if( !tablename) {
            usage();
            return 0;
        }

        schema_required = 0;
        analyzecoverage = 1;
        if(argc > 3)
            analyzecoveragevalue = atoi(argv[3]);
        else
            analyzecoveragevalue = -1;

        if( analyzecoveragevalue < -1 || analyzecoveragevalue > 100 ) {
            fprintf(stderr, "Analyze percent value needs to be between 0 and 100; leave blank to remove entry for table\n");
            return 0;
        }
    }
    else if (strcmp(addalter, "send") == 0) 
    {
        schema_required = 0;
        send = 1;
        tomaster = 0;
    }
    else if (strcmp(addalter, "testcompr") == 0)
    {
        schema_required = 0;
        if(!tablename) {
            tablename = "-all";
        }
        testcompr = 1;
    }
    else if (strcmp(addalter, "setcompr") == 0)
    {
        schema_required = 0;
        setcompr = 1;
    }
    else if (strcmp(addalter, "debug") == 0)
    {
        debug = 1;
        schema_required = 1;
    }
    else if (strcmp(addalter, "alias") == 0)
    {
        schema_required = 0;

        if( argc < 4  || argc > 5 )
        {
            usage();
        }

        tablename = argv[3];

        if (strcmp(argv[2], "set") == 0)
        {
           if (argc != 5) usage();
           alias_op = ALIAS_SET;
           alias_to = argv[4];
        }
        else if (strcmp(argv[2], "rem") == 0)
        {
           if (argc != 4) usage();
           alias_op = ALIAS_REM;
        }
        else if (strcmp(argv[2], "get") == 0)
        {
           if (argc != 4) usage();
           alias_op = ALIAS_GET;
        }
        else
        {
           usage();
        }
    }
    else
    {
        usage();
    }

    if( (!alias_op && !bulk_import && !send && !rowlocks && argc == 5) || 
        (debug && argc != 3) ||
        (schema_required && !debug && argc != 4))
        usage();

    if (debug)
        schemapath = tablename;

    /* If source is from a remote machine, bbxfer it over to here.  Keep
     * original schema path as we need it to advise the database what to call
     * the file. */
    saved_schemapath = schemapath;
    if(schemapath != NULL && !send)
    {
        schemapath = make_schema_local(schemapath);
    }


    if (forcemaster)
        tomaster = 1;

    if ((schema_required || schema_provided) && !send) {
        if(schemapath == NULL)
        {
            fh = NULL;
        }
        else 
        {
            if (debug && strcmp(schemapath, "-") == 0)
                fh = stdin;
            else
                fh = fopen(schemapath, "r");
            if(!fh)
            {
                fprintf(stderr, "error opening '%s': %d %s\n",
                        schemapath, errno, strerror(errno));
                exit(1);
            }
        }
    }
    
    /* Connect to database. */
    sb = connect_db(dbname, tomaster, direct, send ? 1 : 0);
    if(sb == NULL)
        exit(1);
    
    /* Send instructions down the wire. */
    if(bulk_import)
    {
       if (bulk_import_src_tablename)
       {
          sbuf2printf(sb, "bulkimport\n"
                "tablename:%s foreigndbmach:%s foreigndbname:%s foreigntablename:%s\n", tablename,
                bulk_import_src_mach, bulk_import_src_dbname, bulk_import_src_tablename);
       }
       else
       { 
          sbuf2printf(sb, "bulkimport\n"
                "tablename:%s foreigndbmach:%s foreigndbname:%s\n", tablename,
                bulk_import_src_mach, bulk_import_src_dbname);
       }
       sbuf2flush(sb);
    }
    else if (verify) {
        sbuf2printf(sb, "verify %s %d %d\n", tablename, report_seconds, attempt_fix);
        sbuf2flush(sb);
    }
    else if (rowlocks == 1) {
        sbuf2printf(sb, "rowlocks enable\n");
        sbuf2flush(sb);
    }
    else if (rowlocks == 2) {
        sbuf2printf(sb, "rowlocks disable\n");
        sbuf2flush(sb);
    }
    else if (analyze) {
        char buf[80];

        if( override_scaling_factor )
        {
            strcat( analyze_subcmd, " override" );
        }

        if( analyze_tblthds > 0 )
        {
            snprintf( buf, sizeof( buf ), " tblthds %d", analyze_tblthds );
            strcat( analyze_subcmd, buf );
        }

        if( analyze_cmpthds > 0 )
        {
            snprintf( buf, sizeof( buf ), " cmpthds %d", analyze_cmpthds );
            strcat( analyze_subcmd, buf );
        }

        if( analyze_threshd > 0 )
        {
            snprintf( buf, sizeof( buf ), " threshd %d", analyze_threshd );
            strcat( analyze_subcmd, buf );
        }

        if(backout_analysis)
        {
            sbuf2printf(sb, "analyze %s backout\n", tablename);
        }
        else
        {
            sbuf2printf(sb, "analyze %s scale %d%s\n", tablename, 
                    scaling_factor, analyze_subcmd );
        }
        sbuf2flush(sb);
    }
    else if(analyzethreshold) {
        printf("sending analyzethreshold %s %lld\n", tablename, analyzethresholdval);
        sbuf2printf(sb, "analyzethreshold %s %lld\n", tablename, analyzethresholdval);
        sbuf2flush(sb);
    }    
    else if(analyzecoverage) {
        sbuf2printf(sb, "analyzecoverage %s %d\n", tablename, 
                    analyzecoveragevalue);
        sbuf2flush(sb);
    }
    else if (testcompr) {
        sbuf2printf(sb, "testcompr %s\n", tablename);
        sbuf2flush(sb);
    }
    else if (setcompr) {
        sbuf2printf(sb, "setcompr\n");
        sbuf2flush(sb);
        if (rec_compr_opt) sbuf2printf(sb, " rec %s", rec_compr_opt);
        if (blob_compr_opt) sbuf2printf(sb, " blob %s", blob_compr_opt);
        sbuf2printf(sb, " tbl %s\n", tablename);
        sbuf2flush(sb);
    }
    else if (send) {
        int i;
        if (argc < 3) {
            printf("Expected arguments to send\n");
            exit(1);
        }
        sbuf2printf(sb, "mtrap ");
        for (i = 2; i < argc; i++)
            sbuf2printf(sb, "%s ", argv[i]);
        sbuf2printf(sb, "\n");
        sbuf2flush(sb);
    }
    else if (alias_op)
    {
        if (alias_op == ALIAS_SET)
        {
            sbuf2printf(sb, "alias set %s %s\n", tablename, alias_to);
        } 
        else if (alias_op == ALIAS_GET)
        {
            sbuf2printf(sb, "alias get %s\n", tablename);
        }
        else if (alias_op == ALIAS_REM)
        {
            sbuf2printf(sb, "alias rem %s\n", tablename);
        }
        sbuf2flush(sb);
    }
    else if (debug) { 
        char f[1024];
        int rc;

        sbuf2printf(sb, "llops\n");
        if (fh == stdin) {
            sbuf2printf(sb, "interactive\n");
            sbuf2flush(sb);
            printf("debug> ");
            while (fgets(f, sizeof(f), fh)) {
                sbuf2printf(sb, "%s", f);
                sbuf2flush(sb);
                while (sbuf2gets(line, sizeof(line), sb) >= 0) {
                    if (line[0] == '>')
                        fprintf(stdout, "%s", line+1);
                    else if (line[0] == '!')
                        fprintf(stderr, "%s", line+1);
                    else if (strcmp(line, "SUCCESS\n") == 0) {
                        return 0;
                    }
                    else if (strcmp(line, "FAILED\n") == 0) {
                        return 1;
                    }
                    else if (strcmp(line, ".\n") == 0)
                        break;
                    else {
                        fprintf(stderr, "Unexpected response: '%s'", line);
                    }
                }
                printf("debug> ");
            }
            sbuf2close(sb);
            fclose(fh);
            return 0;
        }
        else {
            while (fgets(f, sizeof(f), fh)) {
                sbuf2printf(sb, "%s", f);
            }
            sbuf2printf(sb, "\n.\n");
            sbuf2flush(sb);
        }
    }
    else
    {
        send_schema_change_info(sb, addalter, tablename, scanmode, livemode, delay_commit,
                full_rebuild, ignore_diskspace, planmode, odh_opt, rec_compr_opt, blob_compr_opt,
                inplace_updates, instant_sc, dryrun, stat_command, extrastuff,
                fh, delsp, defaultsp, showsp, use_aname, saved_schemapath,
                have_table_dbnum);
    }
    
    wait_for_sc(sb);

    if (fh) 
        fclose(fh);

    if(rc < 0)
    {
        /* Some kind of I/O error occured */
        fprintf(stderr, "I/O error: prematurely lost connection with database\n");
        exit(1);
    }

    return 0;
}
