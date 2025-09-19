#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <arpa/inet.h>
#include <stdlib.h>
#include <assert.h>
#include <signal.h>
#include <openssl/sha.h>
#include <openssl/aes.h>
#include <inttypes.h>
#include <ctype.h>

#include <flibc.h>
#include <build/db.h>
#include <build/db_int.h>
#include <dbinc/db_swap.h>
#include <dbinc/db_page.h>
#include <dbinc/btree.h>
#include <crc32c.h>
#include <tohex.h>

#include <btree/bt_prefix.c>
#include <btree/bt_prefix.h>
#include <build/db_dbt.h>

int get_bh_gen;
static char *passwd = NULL;

static const char *type2str(int type)
{
    switch (type) {
    case P_INVALID: return "P_INVALID";
    case __P_DUPLICATE: return "__P_DUPLICATE";
    case P_HASH: return "P_HASH";
    case P_IBTREE: return "P_IBTREE";
    case P_IRECNO: return "P_IRECNO";
    case P_LBTREE: return "P_LBTREE";
    case P_LRECNO: return "P_LRECNO";
    case P_OVERFLOW: return "P_OVERFLOW";
    case P_HASHMETA: return "P_HASHMETA";
    case P_BTREEMETA: return "P_BTREEMETA";
    case P_QAMMETA: return "P_QAMMETA";
    case P_QAMDATA: return "P_QAMDATA";
    case P_LDUP: return "P_LDUP";
    case P_PAGETYPE_MAX: return "P_PAGETYPE_MAX";
    default: return "???";
    }
}

static void swap_meta(DBMETA *m)
{
    uint8_t *p = (uint8_t *)m;
    SWAP32(p); /* lsn.file */
    SWAP32(p); /* lsn.offset */
    SWAP32(p); /* pgno */
    SWAP32(p); /* magic */
    SWAP32(p); /* version */
    SWAP32(p); /* pagesize */
    p += 4;    /* unused, page type, unused, unused */
    SWAP32(p); /* free */
    SWAP32(p); /* alloc_lsn part 1 */
    SWAP32(p); /* alloc_lsn part 2 */
    SWAP32(p); /* cached key count */
    SWAP32(p); /* cached record count */
    SWAP32(p); /* flags */
    p = (u_int8_t *)m + sizeof(DBMETA);
    SWAP32(p); /* maxkey */
    SWAP32(p); /* minkey */
    SWAP32(p); /* re_len */
    SWAP32(p); /* re_pad */
    SWAP32(p); /* root */
    p += 92 * sizeof(u_int32_t); /* unused */
    SWAP32(p); /* crypto_magic */
}

static uint32_t *chksum_ptr(DB *dbp, PAGE *p)
{
    switch (TYPE(p)) {
    case P_INVALID:
        /*
         * We assume that we've read a file hole if we have
         * a zero LSN, zero page number and P_INVALID.  Otherwise
         * we have an invalid page that might contain real data.
         */
        if (IS_ZERO_LSN(LSN(p)) && PGNO(p) == 0)
            return NULL;
    /* fallthrough */
    case P_HASH:
    case P_IBTREE:
    case P_IRECNO:
    case P_LBTREE:
    case P_LRECNO:
    case P_OVERFLOW: return (uint32_t *)P_CHKSUM(dbp, p);

    case P_HASHMETA: return (uint32_t *)&((HMETA *)p)->chksum;

    case P_BTREEMETA: return (uint32_t *)&((BTMETA *)p)->chksum;

    case P_QAMMETA: return (uint32_t *)&((QMETA *)p)->chksum;

    case P_QAMDATA: return (uint32_t *)&((QPAGE *)p)->chksum;

    case P_LDUP:
    case __P_DUPLICATE:
    case P_PAGETYPE_MAX:
    default: return NULL;
    }
}

static int chksum_pgsize(DB *dbp, PAGE *p)
{
    switch (TYPE(p)) {
    case P_HASHMETA:
    case P_BTREEMETA:
    case P_QAMMETA: return 512;
    default: return dbp->pgsize;
    }
}

extern u_int32_t __ham_func4 (DB *, const void *, u_int32_t);
static uint64_t zero_pg[64 / sizeof(uint64_t) * 1024] = {0}; // 64K zero page
static int check_chksum(DB *dbp, PAGE *p)
{
    uint32_t *chksump;
    if ((chksump = chksum_ptr(dbp, p)) == NULL) {
        if (memcmp(p, zero_pg, dbp->pgsize) == 0) {
            printf("ENTIRE PAGE is 0s\n");
            return -1;
        } else {
            printf("PGTYPE: %s - skipping chksum\n", type2str(TYPE(p)));
            return 0;
        }
    }
    uint32_t chksum = *chksump;
    uint32_t orig_chksum = chksum;
    if (F_ISSET(dbp, DB_AM_SWAP))
        chksum = flibc_intflip(chksum);
    memset(chksump, 0, sizeof(chksum));
    int pgsize = chksum_pgsize(dbp, p);
    uint32_t calc = IS_CRC32C(p) ? crc32c((uint8_t *)p, pgsize)
                                 : __ham_func4(NULL, (uint8_t *)p, pgsize);
    int rc = 0;
    if (chksum != calc) {
        fprintf(stderr, "pg:%u IS_CRC32C:%s failed chksum expected:%u got:%u\n",
                PGNO(p), YESNO(IS_CRC32C(p)), chksum, calc);
        rc = -2;
    } else {
        printf("VALID CHKSUM (%s)\n", IS_CRC32C(p) ? "CRC32C" : "HAM_FUNC4");
    }
    *chksump = orig_chksum;
    return rc;
}

static uint32_t *iv_ptr(DB *dbp, PAGE *p)
{
    switch (TYPE(p)) {
    case P_HASH:
    case P_IBTREE:
    case P_IRECNO:
    case P_LBTREE:
    case P_LRECNO:
    case P_OVERFLOW: return (uint32_t *)P_IV(dbp, p);
    case P_HASHMETA: return (uint32_t *)&((HMETA *)p)->iv;
    case P_BTREEMETA: return (uint32_t *)&((BTMETA *)p)->iv;
    case P_QAMMETA: return (uint32_t *)&((QMETA *)p)->iv;
    case P_QAMDATA: return (uint32_t *)&((QPAGE *)p)->iv;
    case P_LDUP:
    case P_INVALID:
    case __P_DUPLICATE:
    case P_PAGETYPE_MAX:
    default: return NULL;
    }
}

#define HMAC_OUTPUT_SIZE 20
#define HMAC_BLOCK_SIZE 64
#define DB_MAC_MAGIC "mac derivation key magic value"
#define DB_ENC_MAGIC "encryption and decryption key value magic"
#define DB_AES_KEYLEN 128
static int decrypt_page(DB *dbp, PAGE *p)
{
    SHA_CTX ctx;
    size_t passwd_len = strlen(passwd) + 1;

#ifdef USE_HMAC
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
    // VERIFY MAC ON DISK
    uint8_t mac_key[SHA_DIGEST_LENGTH] = {0};
    SHA1_Init(&ctx);
    SHA1_Update(&ctx, passwd, passwd_len);
    SHA1_Update(&ctx, DB_MAC_MAGIC, strlen(DB_MAC_MAGIC));
    SHA1_Update(&ctx, passwd, passwd_len);
    SHA1_Final(mac_key, &ctx);
#pragma GCC diagnostic pop

    size_t len = DB_MAC_KEY;
    uint8_t old[DB_MAC_KEY], new[DB_MAC_KEY];
    void *chksump = chksum_ptr(dbp, p);
    memcpy(old, chksump, DB_MAC_KEY);
    bzero(chksump, DB_MAC_KEY);
    __db_hmac(mac_key, (uint8_t *)p, chksum_pgsize(dbp, p), new);
    if (memcmp(old, new, DB_MAC_KEY) != 0) {
        fprintf(stderr, "bad mac\n");
        print_hex_nl(old, DB_MAC_KEY, 1);
        print_hex_nl(new, DB_MAC_KEY, 1);
        return -1;
    } else {
        printf("good mac\n");
    }
#else
    int ret;
    if ((ret = check_chksum(dbp, p)) != 0) {
        return ret;
    }
#endif

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
    // DECRYPT PAGE
    uint8_t tmp_key[SHA_DIGEST_LENGTH] = {0};
    SHA1_Init(&ctx);
    SHA1_Update(&ctx, passwd, passwd_len);
    SHA1_Update(&ctx, DB_ENC_MAGIC, strlen(DB_ENC_MAGIC));
    SHA1_Update(&ctx, passwd, passwd_len);
    SHA1_Final(tmp_key, &ctx);

    AES_KEY key;
    bzero(&key, sizeof(key)); //= {0};
    AES_set_decrypt_key(tmp_key, DB_AES_KEYLEN, &key);

    void *iv = iv_ptr(dbp, p); // DB_IV_BYTES
    uint8_t tmp_iv[DB_IV_BYTES];
    memcpy(tmp_iv, iv, DB_IV_BYTES);
    memset(iv, 0, DB_IV_BYTES);
    int skip = P_OVERHEAD(dbp);
    void *data = ((uint8_t *)p) + skip;
    AES_cbc_encrypt(data, data, chksum_pgsize(dbp, p) - skip, &key, tmp_iv,
                    AES_DECRYPT);
#pragma GCC diagnostic pop
    return 0;
}

static int process_hdr(DB *dbp, PAGE *p)
{
    if (memcmp(p, zero_pg, dbp->pgsize) == 0) {
        printf("ENTIRE PAGE is 0s\n");
        return 0;
    }

    if (F_ISSET(dbp, DB_AM_ENCRYPT))
        return decrypt_page(dbp, p);

    if (F_ISSET(dbp, DB_AM_CHKSUM))
        return check_chksum(dbp, p);

    return 0;
}

static void pg2cpu(DB *dbp, PAGE *p)
{
    process_hdr(dbp, p);
    if (!F_ISSET(dbp, DB_AM_SWAP))
        return;
    switch (TYPE(p)) {
    case P_HASHMETA:
    case P_BTREEMETA:
    case P_QAMMETA: swap_meta((DBMETA *)p); return;
    }
    p->lsn.file = flibc_intflip(p->lsn.file);
    p->lsn.offset = flibc_intflip(p->lsn.offset);
    p->pgno = flibc_intflip(p->pgno);
    p->prev_pgno = flibc_intflip(p->prev_pgno);
    p->next_pgno = flibc_intflip(p->next_pgno);
    p->entries = flibc_shortflip(p->entries);
    p->hf_offset = flibc_shortflip(p->hf_offset);
    if (IS_PREFIX(p))
        prefix_tocpu(dbp, p);
    db_indx_t *inp = P_INP(dbp, p);
    if (ISLEAF(p)) {
        int i;
        for (i = 0; i < NUM_ENT(p); i++) {
            inp[i] = flibc_shortflip(inp[i]);
            BKEYDATA *bk = GET_BKEYDATA(dbp, p, i);
            BOVERFLOW *bo;
            switch (B_TYPE(bk)) {
            case B_KEYDATA: bk->len = flibc_shortflip(bk->len); break;
            case B_OVERFLOW:
                bo = (BOVERFLOW *)bk;
                bo->pgno = flibc_intflip(bo->pgno);
                bo->tlen = flibc_intflip(bo->tlen);
            }
        }
    } else {
        int i;
        for (i = 0; i < NUM_ENT(p); i++) {
            inp[i] = flibc_shortflip(inp[i]);
            BINTERNAL *bi = GET_BINTERNAL(dbp, p, i);
            bi->len = flibc_shortflip(bi->len);
            bi->pgno = flibc_intflip(bi->pgno);
            bi->nrecs = flibc_intflip(bi->nrecs);
        }
    }
}

#ifdef USE_HMAC
static void __db_hmac(u_int8_t *k, u_int8_t *data, size_t data_len, u_int8_t *mac)
{
  SHA_CTX ctx;
  u_int8_t key[HMAC_BLOCK_SIZE];
  u_int8_t ipad[HMAC_BLOCK_SIZE];
  u_int8_t opad[HMAC_BLOCK_SIZE];
  u_int8_t tmp[HMAC_OUTPUT_SIZE];
  int i;

  memset(key, 0x00, HMAC_BLOCK_SIZE);
  memset(ipad, 0x36, HMAC_BLOCK_SIZE);
  memset(opad, 0x5C, HMAC_BLOCK_SIZE);

  memcpy(key, k, HMAC_OUTPUT_SIZE);

  for (i = 0; i < HMAC_BLOCK_SIZE; i++) {
    ipad[i] ^= key[i];
    opad[i] ^= key[i];
  }

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
  SHA1_Init(&ctx);
  SHA1_Update(&ctx, ipad, HMAC_BLOCK_SIZE);
  SHA1_Update(&ctx, data, data_len);
  SHA1_Final(tmp, &ctx);
  SHA1_Init(&ctx);
  SHA1_Update(&ctx, opad, HMAC_BLOCK_SIZE);
  SHA1_Update(&ctx, tmp, HMAC_OUTPUT_SIZE);
  SHA1_Final(mac, &ctx);
#pragma GCC diagnostic pop
  return;
}
#endif

static void do_meta(DB *dbp, PAGE *p)
{
    DBMETA *meta = (DBMETA *)p;
    BTMETA *bm;
    printf("MAGIC:0x%x %s ENDIAN\n", meta->magic,
#if defined(__x86_64) || defined(__x86)
           F_ISSET(dbp, DB_AM_SWAP) ? "BIG" : "LITTLE"
#else
           F_ISSET(dbp, DB_AM_SWAP) ? "LITTLE" : "BIG"
#endif
           );
    printf("PGSIZE: %" PRIu32 " OFFSET BIAS:%d\n", dbp->pgsize,
           dbp->offset_bias);
    printf("FILEID: ");
    print_hex_nl(meta->uid, DB_FILE_ID_LEN, 1);
    printf("PGNO:%u\n", meta->pgno);

    switch (TYPE(p)) {
    case P_HASHMETA: printf("LAST_PAGE:%u\n", meta->last_pgno); break;
    case P_BTREEMETA:
        bm = (BTMETA *)meta;
        printf("ROOT PGNO:%u\n", bm->root);
        printf("FREELIST: %u\n", meta->free);
        printf("LAST_PAGE:%u\n", meta->last_pgno);
        printf("CHKSUM:%s\n", YESNO(F_ISSET(dbp, DB_AM_CHKSUM)));
        printf("LSN:%u:%u\n", p->lsn.file, p->lsn.offset);
        break;
    case P_QAMMETA: printf("LAST_PAGE:%u\n", meta->last_pgno); break;
    default: abort();
    }
}

static void process_meta(DB *dbp, PAGE *p, int fatal)
{
    F_CLR(dbp, DB_AM_SWAP);
    DBMETA *meta = (DBMETA *)p;
    uint32_t magic = meta->magic;

again:
    switch (magic) {
    case DB_BTREEMAGIC:
    case DB_HASHMAGIC:
    case DB_QAMMAGIC:
    case DB_RENAMEMAGIC: break;
    default:
        if (F_ISSET(dbp, DB_AM_SWAP))
            goto bad;
        F_SET(dbp, DB_AM_SWAP);
        magic = flibc_intflip(magic);
        goto again;
    }
    if (F_ISSET(dbp, DB_AM_SWAP)) {
        puts("swapped");
        swap_meta(meta);
    }
    if (FLD_ISSET(meta->metaflags, DBMETA_CHKSUM)) {
        puts("chksummed");
        F_SET(dbp, DB_AM_CHKSUM);
    }
    if (meta->encrypt_alg != 0 || FLD_ISSET(meta->metaflags, DB_AM_ENCRYPT)) {
        puts("encrypted");
        F_SET(dbp, DB_AM_ENCRYPT);
        if (passwd == NULL) {
            fprintf(stderr, "PROVIDE PASSWD FOR ENCRYPTED BTREE\n");
            exit(EXIT_FAILURE);
        }
    }
    dbp->pgsize = meta->pagesize;
    if (meta->pagesize > 64 * 1024)
        dbp->offset_bias = meta->pagesize / (64 * 1024);
    else
        dbp->offset_bias = 1;
    printf("pgsize:%d offset_bias:%d\n", meta->pagesize, dbp->offset_bias);
    return;
bad:
    fprintf(stderr, "BAD META PAGE\n");
    if (fatal)
        exit(EXIT_FAILURE);
}

void inspect_page_hdr(DB *, PAGE *);
static void inspect_internal_page(DB *dbp, PAGE *p)
{
    inspect_page_hdr(dbp, p);
    int i;
    for (i = 0; i < NUM_ENT(p); i++) {
        BINTERNAL *bi = GET_BINTERNAL(dbp, p, i);
        printf("%d. pgno:%u nrecs:%u\n", i, bi->pgno, bi->nrecs);
    }
}

static void pdump_inspect_bk(BKEYDATA *bk)
{
    if (bk == NULL) {
        printf("%s: bk is NULL\n", __func__);
        return;
    }
    db_indx_t len;
    u_int32_t tlen;
    db_pgno_t pgno;
    BOVERFLOW *bo;
    switch (B_TYPE(bk)) {
    case B_KEYDATA:
        ASSIGN_ALIGN(db_indx_t, len, bk->len);
        print_hex_nl(bk->data, len, 0);
        printf(" [%s%s%s ]", B_PISSET(bk) ? "P" : " ", B_RISSET(bk) ? "R" : " ",
               B_DISSET(bk) ? "X" : " ");
        break;
    case B_OVERFLOW:
        bo = (BOVERFLOW *)bk;
        ASSIGN_ALIGN(u_int32_t, tlen, bo->tlen);
        ASSIGN_ALIGN(db_pgno_t, pgno, bo->pgno);
        printf(" overflow len:%u pg:%d [  %sO]", tlen, pgno,
               B_DISSET(bk) ? "X" : " ");
        break;
    default:
        printf("huh weird type here -> type:%d B_TYPE:%d B_TYPE_NOCOMP:%d\n",
               bk->type, B_TYPE(bk), B_TYPE_NOCOMP(bk));
        // raise(SIGINT);
        break;
    }
}

static void pdump_inspect_page_dta(DB *dbp, PAGE *h)
{
    printf("values:");
    int value = 0;
    int i;
    //db_indx_t len;
    for (i = 0; i < NUM_ENT(h); ++i) {
        BKEYDATA *bk = GET_BKEYDATA(dbp, h, i);
        if (B_TYPE(bk) == B_KEYDATA) {
            uint8_t *a, *b;
            //ASSIGN_ALIGN(db_indx_t, len, bk->len);
            a = (uint8_t *)bk;
            b = (uint8_t *)h;
            if (a < b || (a > b + dbp->pgsize)) {
                printf("\nthis page don't smell right anymore @i=%u\n", i);
                raise(SIGINT);
                break;
            }
            // if (B_RISSET(bk) || B_PISSET(bk)) {
            //  format = "\n%3d. ";
            //  value = 0;
            //} else if (B_DISSET(bk)) {
            //  format = "\n%3d. ";
            //  value = 1;
            //} else if (value && (len == 8 || B_TYPE(bk) == B_OVERFLOW)) {
            //  format = " -> ";
            //} else {
            //  format = "\n%3d. ";
            //}
            // printf(format, i);
            printf("\n%d [@%d]:", i, P_INP(dbp, h)[i]);
            pdump_inspect_bk(bk);
            value = !value;
        }
        else if (B_TYPE(bk) == B_OVERFLOW) {
            BOVERFLOW *bo = GET_BOVERFLOW(dbp, h, i);
            printf(" overflow %u next %u\n", bo->tlen, bo->pgno);
        }
    }
}

static void pdump_inspect_page(DB *dbp, PAGE *h)
{
    DB db;
    if (dbp->fname == NULL) { // got a dummy dbp
        db = *dbp;
        dbp = &db;
        // all new pages should have checksums
        F_SET(dbp, DB_AM_CHKSUM);
    }
    inspect_page_hdr(dbp, h);
    if (ISLEAF(h)) {
        pdump_inspect_page_dta(dbp, h);
    }
    printf("\n");
}

static void do_page_int(DB *dbp, PAGE *p)
{
    uint8_t type = TYPE(p);
    printf("PAGE TYPE: %s\n", type2str(type));
    printf("PAGE LEVEL: %d\n", LEVEL(p));
    switch (type) {
    case P_HASHMETA:
    case P_BTREEMETA:
    case P_QAMMETA: do_meta(dbp, p); break;
    case P_LBTREE: pdump_inspect_page(dbp, p); break;
    case P_IBTREE: inspect_internal_page(dbp, p); break;
    case P_INVALID: inspect_page_hdr(dbp, p); break;
    }
}

static void do_page(DB *dbp, PAGE *p)
{
    pg2cpu(dbp, p);
    do_page_int(dbp, p);
}

static void cdb2_pgdump_usage()
{
    puts("Print header and key-value pairs for Comdb2 btrees.\n\n"
         "Usage: pgdump [options] <db_file> [pgno]\n\n"
         "Options:\n"
         "  -c 0|1      - Override chksum flag obtained from meta-page\n"
         "  -h          - Print this usage message\n"
         "  -P password - Set password\n"
         "  -p pagesize - Override pagesize obtained from meta-page\n"
         "  -s 0|1      - Override swap flag obtained from meta-page\n"
         "  -m          - Don't process meta page\n"
         "  -f          - Find new pfx and show resulting page()\n"
         "  -t 0|1      - Btree type: Index (0) or Data (1) btree\n"
        );
}

int 
tool_cdb2_pgdump_main(argc, argv)
    int argc;
    char *argv[];
{
    crc32c_init(0);
    logmsg_set_time(0);
    io_override_set_std(stdout);
    int arg, pgsize, swap, chksum, skipmeta, findpfx, data;
    pgsize = swap = chksum = skipmeta = findpfx = data = 0;
    while ((arg = getopt(argc, argv, "c:s:P:p:mft:h")) != -1) {
        switch (arg) {
        case 'c': chksum = atoi(optarg) + 1; break;
        case 'P': passwd = strdup(optarg); break;
        case 'p': pgsize = atoi(optarg); break;
        case 's': swap = atoi(optarg) + 1; break;
        case 'm': skipmeta = 1; break;
        case 'f': findpfx = 1; break;
        case 't': data = atoi(optarg) + 1; break;
        default: cdb2_pgdump_usage(); return (arg == 'h') ? EXIT_SUCCESS : EXIT_FAILURE;
        }
    }
    if (argc < 2) {
        cdb2_pgdump_usage();
        return EXIT_FAILURE;
    }

    /* process meta pg to build dbp */
    int fd;
    char *fname = argv[optind++];
    if ((fd = open(fname, O_RDONLY)) < 0) {
        fprintf(stderr, "Can not open file '%s'\n", fname);
        return EXIT_FAILURE;
    }
    size_t n;
    char pgbuf[256 * 1024];
    PAGE *p = (PAGE *)pgbuf;
    if ((n = pread(fd, p, 512, 0)) != 512) {
        fprintf(stderr, "Error reading file header for '%s'\n", argv[1]);
        return EXIT_FAILURE;
    }
    DB dbp = {0};
    dbp.fname = fname;
    puts(dbp.fname);
    if (skipmeta == 0)
        process_meta(&dbp, p, !pgsize);

    /* process overrides */
    if (pgsize) {
        dbp.offset_bias = 1;
        dbp.pgsize = pgsize;
    }
    if (swap--) {
        if (swap) {
            F_SET(&dbp, DB_AM_SWAP);
        } else {
            F_CLR(&dbp, DB_AM_SWAP);
        }
    }
    if (chksum--) {
        if (chksum) {
            F_SET(&dbp, DB_AM_CHKSUM);
        } else {
            F_CLR(&dbp, DB_AM_CHKSUM);
        }
    }
    FLD_SET(dbp.compression_flags, DB_PFX_COMP);
    if (data--) {
        if (data) {
            FLD_SET(dbp.compression_flags, DB_SFX_COMP);
        } else {
            FLD_SET(dbp.compression_flags, DB_RLE_COMP);
        }
    } else {
        size_t l = strlen(fname);
        const char idx[] = "index";
        if (l > sizeof(idx) && strcmp(fname + l - sizeof(idx) + 1, idx) == 0) {
            FLD_SET(dbp.compression_flags, DB_RLE_COMP);
        } else {
            FLD_SET(dbp.compression_flags, DB_SFX_COMP);
        }
    }

    /* dump just the requested page */
    if (optind < argc) {
        uint64_t pgno = strtoull(argv[optind++], NULL, 10);
        if ((n = pread(fd, p, dbp.pgsize, pgno * dbp.pgsize)) == dbp.pgsize) {
            if (findpfx) {
                uint8_t obuf[dbp.pgsize];
                PAGE *o = (PAGE *)obuf;
                pfx_t pfx;
                memcpy(o, p, dbp.pgsize);
                pg2cpu(&dbp, o);
                find_pfx(&dbp, o, &pfx);
                pfx_compress_pages(&dbp, p, o, &pfx, 1, o);
                do_page_int(&dbp, p);
            } else {
                do_page(&dbp, p);
            }
        } else {
            fprintf(stderr, "FAILED TO READ PG:%" PRIu64 "\n", pgno);
            return EXIT_FAILURE;
        }
        return EXIT_SUCCESS;
    }

    /* dump all pages */
    uint64_t counter = 0;
    uint64_t offset = 0;
    while ((n = pread(fd, p, dbp.pgsize, dbp.pgsize * offset)) == dbp.pgsize) {
        do_page(&dbp, p);
        ++counter;
        offset += 1;
    }
    if (n < 0) {
        fprintf(stderr, "FAILED TO READ PG:%" PRIu64 "\n", counter);
        return EXIT_FAILURE;
    }
    printf("\n\nTOTAL PAGES READ: %" PRIu64 "\n", counter);
    return EXIT_SUCCESS;
}
