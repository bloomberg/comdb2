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

#include <comdb2rle.h>

#include <stdio.h>
#include <dirent.h>
#include <string.h>
#include <stdint.h>

static const int yes = 1;
static const int no = 0;
static uint32_t pass = 0;
static uint32_t fail = 0;

void runtest(char *name, int expected)
{
    char inbuf[32768];
    FILE *bigstr = fopen(name, "r");
    if (bigstr == NULL) {
        printf("Failed %s (input not found)\n", name);
        ++fail;
        return;
    }
    size_t n = fread(inbuf, 1, sizeof(inbuf), bigstr);
    fclose(bigstr);

    char compbuf[n];
    Comdb2RLE compress = {
        .in = inbuf,
        .insz = n,
        .out = compbuf,
        .outmax = n
    };

    int rc = compressComdb2RLE(&compress);
    if(rc != 0) {
        if (expected == no) {
            ++pass;
            return;
        } else {
            printf("Failed %s (couldn't compress)\n", name);
            ++fail;
            return;
        }
    } else {
        /* We were able to compress */
        if (expected == no) {
            /* But we weren't supposed to */
            printf("Failed %s (compress rc: %d insz: %lu -> outsz: %lu)\n", name, rc, compress.insz, compress.outsz);
            ++fail;
            return;
        }
    }

    if (compress.outsz > compress.insz) {
        printf("Failed %s (insz: %lu -> outsz: %lu)\n", name, compress.insz, compress.outsz);
        ++fail;
        return;
    }

    /* decompress and compare output to input */
    char decompbuf[n];
    Comdb2RLE decompress = {
        .in = compress.out,
        .insz = compress.outsz,
        .out = decompbuf,
        .outmax = sizeof(decompbuf)
    };
    rc = decompressComdb2RLE(&decompress);
    if (rc) {
        printf("Failed %s rc: %d (cound't decompress)\n", name, rc);
        ++fail;
        return;
    }

    if (decompress.outsz != n) {
        printf("Failed %s (input & ouput sizes don't match)\n", name);
        ++fail;
        return;
    }

    if (memcmp(inbuf, decompbuf, n) != 0) {
        printf("Failed %s (input & output don't match)\n", name);
        ++fail;
        return;
    }

    ++pass;
    return;
}

void runtestdir(const char *dirpath)
{
    char testfile[NAME_MAX];
    DIR *dir = opendir(dirpath);
    if (dir == NULL) {
        fprintf(stderr, "%s ", dirpath);
        perror("opendir");
        ++fail;
        return;
    }

    struct dirent *ent;
    while ((ent = readdir(dir)) != NULL) {
        if (ent->d_name[0] == '.') {
            continue;
        }
        sprintf(testfile, "%s/%s", dirpath, ent->d_name);
        runtest(testfile, yes);
    }
    closedir(dir);
}

int main(int argc, char *argv[])
{
    runtestdir(".");
    runtestdir("inputs");

    runtest("bigstr", no);

    printf("Total pass: %d\n", pass);
    printf("Total fail: %d\n", fail);
    return fail;
}
