#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <cdb2api.h>

static char *argv0 = NULL;

char *dbname;
char *tmpdir;

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

    while ((c = getopt(argc, argv, "d:t:")) != EOF) {
        switch (c) {
        case 'd':
            dbname = optarg;
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

    rc = cdb2_open(&cdb2, dbname, "local", 0);
    if (rc != 0) {
        printf("failed to open connection (rc: %d)\n", rc);
        exit(1);
    }

    rc = cdb2_run_statement(cdb2, "select * from comdb2_files");
    if (rc != 0) {
        printf("failed to execute command (rc: %d)\n", rc);
        exit(1);
    }

    while ((rc = cdb2_next_record(cdb2) == CDB2_OK)) {
        char *file = (char *)cdb2_column_value(cdb2, 0);
        char *dir = (char *)cdb2_column_value(cdb2, 1);
        void *content = cdb2_column_value(cdb2, 2);
        int content_len = cdb2_column_size(cdb2, 2);

        char path[4096];

        if (strlen(dir) > 0) {
            snprintf(path, sizeof(path), "%s/%s", datadir, dir);
            /* Create directory if it does not exist */
            mkdir(path, 0755);
            snprintf(path, sizeof(path), "%s/%s/%s", datadir, dir, file);
        } else {
            snprintf(path, sizeof(path), "%s/%s", datadir, file);
        }

        int fd = open(path, O_WRONLY | O_CREAT);
        if (fd == -1) {
            printf("failed to open file %s (errno: %d)\n", path, errno);
            exit(1);
        }

        rc = write(fd, content, content_len);
        if (rc == -1) {
            printf("failed to write to file %s (err: %s)\n", path,
                   strerror(errno));
            exit(1);
        }
        fchmod(fd, 0755);
        close(fd);

        printf("%s created successfully\n", path);
    }

    cdb2_close(cdb2);
    return 0;
}
