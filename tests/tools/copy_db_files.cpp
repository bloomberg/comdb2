#include <string>

#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <cdb2api.h>
#include <ftw.h>

static char *argv0 = NULL;

char *dbname;
char *hostname;
char *tmpdir;

char *datadir;
int remove_file_cb(const char *fpath, const struct stat *sb,
                   int typeflag, struct FTW *ftwbuf)
{
    if (strcmp(fpath, datadir) != 0) {
        printf("%s:%d removing %s\n", __func__, __LINE__, fpath);
        return remove(fpath);
    }
    return 0;
}

int cleanup_datadir(const char *dir)
{
    datadir = (char *)dir;
    return nftw(dir, remove_file_cb, 10, FTW_PHYS | FTW_DEPTH | FTW_MOUNT);
}

void usage(void)
{
    fprintf(stderr, "Usage: %s -d <dbname> -t <tmpdir>\n", argv0);
    exit(1);
}

int main(int argc, char *argv[])
{
    cdb2_hndl_tp *cdb2 = NULL;
    char datadir[4096];
    char c;
    int rc;
    int err = 0;

    argv0 = argv[0];

    while ((c = getopt(argc, argv, "d:h:t:a:f:")) != EOF) {
        switch (c) {
        case 'd':
            dbname = optarg;
            break;
        case 'h':
            hostname = optarg;
            break;
        case 't':
            tmpdir = optarg;
            break;
        default:
            fprintf(stderr, "Unknown option, '%c'\n", c);
            err++;
            break;
        }
    }
    if (!dbname) {
        fprintf(stderr, "Dbname is unset\n");
        err++;
    }

    if (!hostname) {
        fprintf(stderr, "hostname is unset\n");
       err++;
    }

    if (!tmpdir) {
        fprintf(stderr, "tmpdir is unset\n");
        err++;
    }

    if (err)
        usage();

    snprintf(datadir, sizeof(datadir), "%s/%s", tmpdir, dbname);
    rc = mkdir(datadir, 0755);
    if (rc != 0) {
        printf("mkdir() failed (rc: %d, err: %s)\n", rc, strerror(errno));
        exit(1);
    }

    rc = cdb2_open(&cdb2, dbname, hostname, 0);
    if (rc != 0) {
        printf("failed to open connection (rc: %d)\n", rc);
        exit(1);
    }

    char cmd[4000];
    snprintf(cmd, sizeof(cmd), "select filename, dir, content, size from comdb2_files");

    printf("executing %s\n", cmd);
    rc = cdb2_run_statement(cdb2, cmd);
    if (rc != 0) {
        printf("failed to execute command (rc: %d)\n", rc);
        exit(1);
    }

    while ((rc = cdb2_next_record(cdb2) == CDB2_OK)) {
        char *file = (char *)cdb2_column_value(cdb2, 0);
        char *dir = (char *)cdb2_column_value(cdb2, 1);
        void *content = cdb2_column_value(cdb2, 2);
        int content_len = cdb2_column_size(cdb2, 2);

        std::string fullpath(datadir);
        if (strlen(dir) > 0) {
            fullpath += "/";
            fullpath += dir;
            mkdir(fullpath.c_str(), 0755);
        }
        fullpath += "/";
        fullpath += file;
        int fd = open(fullpath.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0755);
        if (fd  == -1) {
            printf("failed to open file %s (errno: %s)\n", fullpath.c_str(), strerror(errno));
            exit(1);
        }
        ssize_t bytes_written = write(fd, content, content_len);
        if (bytes_written != content_len) {
            printf("failed to write to the file (expected: %d got: %ld)\n",
                   content_len, bytes_written);
            exit(1);
        }
        close(fd);
    }

    cdb2_close(cdb2);
    return 0;
}
