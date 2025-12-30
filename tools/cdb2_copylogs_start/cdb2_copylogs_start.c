#include <stdlib.h>
#include <stdio.h>

#include <cdb2api.h>
#include <string.h>
#include <limits.h>

#include <sys/types.h>
#include <dirent.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

#include <getopt.h>

#define CONFIG_TOKSEP " \t\n"

char *db_binary = NULL;

int find_logs(const char *dir, int *first_log, int *num_logs) {
    DIR *d = opendir(dir);
    struct dirent *entry;
    if (!d) {
        perror("opendir");
        return -1;
    }
    *first_log = INT_MAX;
    *num_logs = 0;
    while ((entry = readdir(d)) != NULL) {
        if (strncmp(entry->d_name, "log.", 4) == 0) {
            int lognum = atoi(entry->d_name + 4);
            if (lognum < *first_log)
                *first_log = lognum;
            (*num_logs)++;
        }
    }
    return 0;
}

char *checksum_file(const char *file) {
    char *checksum = NULL;
    char *cmd;
    if (asprintf(&cmd, "md5sum %s", file) == -1) {
        fprintf(stderr, "Failed to allocate md5sum command string\n");
        return NULL;
    }
    char *line = NULL;
    size_t linelen = 0;
    FILE *cmdf = popen(cmd, "r");
    if (cmdf == NULL) {
        fprintf(stderr, "Failed to run md5sum command\n");
        free(line);
        return NULL;
    }
    line = NULL;
    if (getline(&line, &linelen, cmdf) == -1) {
        fprintf(stderr, "Failed to read md5sum output for %s\n", file);
        pclose(cmdf);
        free(line);
        return NULL;
    }
    pclose(cmdf);
    if (line) {
        char *tok = strtok(line, " ");
        if (tok == NULL) {
            fprintf(stderr, "Failed to parse md5sum output for %s\n", file);
            free(line);
            return NULL;
        }
        checksum = strdup(tok);
    }
    free(line);
    return checksum;
}

int checksum_logs(char ***chksums_out, const char *dir, int first_log, int num_logs) {
    char **chksums;
    chksums = malloc(num_logs * sizeof(char *));
    printf("getting local checksums\n");
    for (int i = 0; i < num_logs; i++) {
        char *logfile;
        if (asprintf(&logfile, "%s/log.%010d", dir, first_log + i) == -1) {
            fprintf(stderr, "Failed to allocate md5sum command string\n");
            return 1;
        }
        chksums[i] = checksum_file(logfile);
        if (chksums[i] == NULL) {
            free(logfile);
            return 1;
        }
        printf("%d <- %s\n", first_log + i, chksums[i]);
    }
    *chksums_out = chksums;
    return 0;
}

int get_db_log_checksums(const char *dbname, const char *tier, int direct, int first_log, int num_logs, char **chksums, int *outnlogs) {
    cdb2_hndl_tp *hndl = NULL;
    char *sql = NULL;
    int fd = -1;
    int success = 0;

    printf("getting remote checksums\n");
    int rc = cdb2_open(&hndl, dbname, tier, direct ? CDB2_DIRECT_CPU : 0);
    if (rc != 0) {
        fprintf(stderr, "cdb2_open failed: %s\n", cdb2_errstr(hndl));
        return 1;
    }
    if (asprintf(&sql, "SELECT cast(substr(filename, 5) as int) as lognum, offset, size, content FROM comdb2_files WHERE dir='logs' and lognum between %d AND %d AND chunk_size=%d ORDER BY filename, offset ASC", first_log, first_log + num_logs - 1, 1024*1024) == -1) {
        perror("out of memory? ");
        goto done;
    }
    printf(">> %s\n", sql);
    rc = cdb2_run_statement(hndl, sql);
    if (rc) {
        fprintf(stderr, "cdb2_run_statement failed: %s\n", cdb2_errstr(hndl));
        goto done;
    }
    int n = 0;
    *outnlogs = 0;
    char tmpfile[30];
    // TODO: getenv("TMPDIR")
    strcpy(tmpfile, "logchksumXXXXXX");
    fd = mkstemp(tmpfile);
    if (fd == -1) {
        fprintf(stderr, "can't create temporary file to checksum logs: %d %s\n", (int) errno, strerror(errno));
        goto done;
    }
    int nchunks = 0;
    int expected_offset = 0;
    int prevlog = -1;
    int lognum, offset, chunksize;
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK) {
        lognum = (int)*(int64_t*)cdb2_column_value(hndl, 0);
        offset = (int)*(int64_t*)cdb2_column_value(hndl, 1);
        chunksize = (int)*(int64_t*)cdb2_column_value(hndl, 2);
        nchunks++;
        if (prevlog == -1)
            prevlog = lognum;
        // printf("lognum=%d offset=%d (expecting %d) chunksize=%d\n", lognum, offset, expected_offset, chunksize);
        if (lognum != prevlog) {
            if (fd != -1) {
                close(fd);
                fd = -1;
            }
            chksums[n] = checksum_file(tmpfile);
            if (chksums[n] == NULL) {
                printf("checksum_file failed for log %d?\n", lognum);
                char c[100];
                sprintf(c, "ls -l %s", tmpfile);
                if (system(c)) {
                    fprintf(stderr, "system(%s) failed\n", c);
                }
                goto done;
            }
            printf("%d -> %s\n", lognum, (char*) chksums[n]);
            nchunks = 0;
            n++;
            if (lognum != n + first_log) {
                fprintf(stderr, "Log number mismatch: expected %d, got %d\n", n + first_log, lognum);
                goto done;
            }
            expected_offset = 0;
            unlink(tmpfile);
            fd = open(tmpfile, O_RDWR | O_TRUNC | O_CREAT, 0600);
            if (fd == -1) {
                fprintf(stderr, "can't create temporary file to checksum logs: %d %s\n", (int) errno, strerror(errno));
                goto done;
            }
            (*outnlogs)++;
            prevlog = lognum;
        }
        if (offset != expected_offset) {
            fprintf(stderr, "Offset mismatch for log %d: expected %d, got %d\n", lognum, expected_offset, offset);
            goto done;
        }
        expected_offset += chunksize;
        const char *content = (const char*) cdb2_column_value(hndl, 3);
        ssize_t written = write(fd, content, chunksize);
        if (written != chunksize) {
            fprintf(stderr, "Failed to write chunk to temporary file for log %d\n", lognum);
            goto done;
        }

        if (n > num_logs) {
            break;
        }
    }
    if (fd != -1) {
        close(fd);
        fd = -1;
    }
    if (nchunks > 0 && *outnlogs < num_logs) {
        chksums[n] = checksum_file(tmpfile);
        if (chksums[n] == NULL) {
            fprintf(stderr, "checksum_file failed for log %d?\n", lognum);
            goto done;
        }
        (*outnlogs)++;
    }
    if (rc != CDB2_OK_DONE) {
        fprintf(stderr, "cdb2_next_record failed: %s\n", cdb2_errstr(hndl));
        goto done;
    }
    else {
        success = 1;
    }
done:
    if (hndl)
        cdb2_close(hndl);
    free(sql);
    if (fd != -1) {
        close(fd);
        unlink(tmpfile);
    }
    return success ? 0 : 1;
}

int copy_logs(const char *dir, int start_log, const char *dbname, const char *tier, int direct) {
    cdb2_hndl_tp *hndl;
    int rc = cdb2_open(&hndl, dbname, tier, direct ? CDB2_DIRECT_CPU : 0);
    if (rc != 0) {
        fprintf(stderr, "cdb2_open failed: %s\n", cdb2_errstr(hndl));
        return 1;
    }
    char *sql;
    if (asprintf(&sql, "SELECT lognum, logfile FROM comdb2_logfiles WHERE lognum >= %d ORDER BY lognum ASC", start_log) == -1) {
        perror("out of memory? ");
        cdb2_close(hndl);
        return 1;
    }
    rc = cdb2_run_statement(hndl, sql);
    if (rc) {
        fprintf(stderr, "cdb2_run_statement failed: %s\n", cdb2_errstr(hndl));
        free(sql);
        cdb2_close(hndl);
        return 1;
    }
    char *logfname = NULL;
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK) {
        int lognum = (int)*(int64_t*)cdb2_column_value(hndl, 0);
        const char *logfile = (const char*) cdb2_column_value(hndl, 1);
        if (asprintf(&logfname, "%s/log.%010d", dir, lognum) == -1) {
            perror("out of memory? ");
            free(sql);
            cdb2_close(hndl);
            return 1;
        }
        FILE *f = fopen(logfname, "wb");
        if (ftruncate(fileno(f), 0)) {
            fprintf(stderr, "Failed to truncate %s\n", logfname);
            return 1;
        }
        if (!f) {
            fprintf(stderr, "Failed to open %s for writing\n", logfname);
            free(logfname);
            free(sql);
            cdb2_close(hndl);
            return 1;
        }
        if (fwrite(logfile, 1, cdb2_column_size(hndl, 1), f) != cdb2_column_size(hndl, 1)) {
            fprintf(stderr, "Failed to write log file %s\n", logfname);
            return 1;
        }
        fclose(f);
        printf("Copied log.%010d\n", lognum);
        free(logfname);
    }

    cdb2_close(hndl);
    return 0;
}

enum {
    OPTION_LRL = 1,
    OPTION_DIRECT = 2,
    OPTION_DBBINARY = 3
};

void usage() {
    printf("Usage: cdb2_copylogs_start [options] <database> <tier>\n");
    printf("Options:\n");
    printf("  --lrl=/path/to/lrl           - Path to db lrl file - otherwise assumes $PWD\n");
    printf("  --direct                     - Tier argument is a machine, not a tier\n");
    printf("  --dbbinary=/path/to/comdb2   - Path to db binary to use for recovery\n");
    exit(1);
}

int main(int argc, char *argv[]) {
    char *lrlfname = NULL;
    int direct = 0;

    struct option long_options[] = {
        {"lrl", required_argument, NULL, OPTION_LRL},
        {"direct", no_argument, NULL, OPTION_DIRECT},
        {"dbbinary", required_argument, NULL, OPTION_DBBINARY},
        {0, 0, 0, 0}
    };

    int opt;
    while ((opt = getopt_long(argc, argv, "l:d", long_options, NULL)) != -1) {
        switch (opt) {
            case OPTION_LRL:
                lrlfname = optarg;
                break;
            case OPTION_DIRECT:
                direct = 1;
                break;
            case OPTION_DBBINARY:
                db_binary = optarg;
                break;
            default:
                usage();
        }
    }

    if ((argc-optind) != 2) {
        usage();
    }
    const char *dbname = argv[optind];
    const char *tier = argv[optind + 1];

    if (db_binary == NULL) {
        db_binary = "comdb2";
    }

    if (lrlfname == NULL) {
        if (asprintf(&lrlfname, "%s.lrl", dbname) == -1) {
            perror("out of memory? ");
            return 1;
        }
    }
    FILE *lrl = fopen(lrlfname, "r");
    if (!lrl) {
        fprintf(stderr, "Failed to open lrl file %s\n", lrlfname);
        return 1;
    }
    char *line = NULL;
    size_t linelen;
    char *dbdir = NULL;
    while (getline(&line, &linelen, lrl) != -1) {
        char *tok = strtok(line, CONFIG_TOKSEP);
        if (strcmp(line, "dir") == 0) {
            tok = strtok(NULL, CONFIG_TOKSEP);
            if (tok == NULL)
                continue;
            dbdir = strdup(tok);
        }
    }
    if (dbdir == NULL) {
        fprintf(stderr, "No dir specified in lrl\n");
        return 1;
    }
    free(line);
    fclose(lrl);

    int first_log;
    int num_logs;

    char *logdir;
    if (asprintf(&logdir, "%s/logs", dbdir) == -1) {
        perror("out of memory? ");
        return 1;
    }
    if (find_logs(logdir, &first_log, &num_logs))
        return 1;
    char **chksums;
    char **rmtchecksums;
    if (checksum_logs(&chksums, logdir, first_log, num_logs))
        return 1;
    int nrmtchecksums;
    rmtchecksums = malloc(num_logs * sizeof(char *));
    if (get_db_log_checksums(dbname, tier, first_log, direct, num_logs, rmtchecksums, &nrmtchecksums)) {
        fprintf(stderr, "Failed to get log checksums from database\n");
        return 1;
    }
    int lognum;
    if (nrmtchecksums < num_logs) {
        fprintf(stderr, "Cluster has fewer logs (%d) than local (%d). Cannot proceed.\n", nrmtchecksums, num_logs);
        return 1;
    }
    for (lognum = 0; lognum < nrmtchecksums; lognum++) {
        if (lognum > num_logs)
            break;
        if (strcmp(chksums[lognum], rmtchecksums[lognum]) != 0) {
            printf("Checksum mismatch for log.%d: file=%s db=%s, will run recovery to that log.\n", first_log + lognum, chksums[lognum], rmtchecksums[lognum]);
            break;
        }
    }
    if (lognum == num_logs) {
        printf("All log files match. No need to run recovery.\n");
        return 0;
    }
    else {
        char *cmd;
        if (asprintf(&cmd, "%s --recovertolsn %d:28 --lrl %s %s", db_binary, first_log + lognum, lrlfname, dbname) == -1) {
            perror("out of memory? ");
            return 1;
        }
        printf("Running db recovery: %s\n", cmd);

        int rc = system(cmd);
        if (rc) {
            fprintf(stderr, "Recovery command failed: %s\n", cmd);
            return 1;
        }
        free(cmd);
    }
    if (copy_logs(logdir, first_log + lognum, dbname, tier, direct)) {
        fprintf(stderr, "Failed to copy logs from database\n");
        return 1;
    }

    char *cmd;
    if (asprintf(&cmd, "%s --fullrecovery --lrl %s %s", db_binary, lrlfname, dbname) == -1) {
        perror("out of memory? ");
        return 1;
    }
    printf("Running full recovery: %s\n", cmd);
    int rc = system(cmd);
    if (rc) {
        fprintf(stderr, "Full recovery command failed: %s\n", cmd);
        return 1;
    }
    return 0;
}
