#include "comdb2.h"
#include "sql.h"
#include "sqliteInt.h"
#include "vdbeInt.h"
#include <ctrace.h>

/**
 * sqlite master global entries
 * NOTE: there is no rootpage here, entries are indexed by tblname&ixnum
 * rootpages are local to an sqlite engine, and are indexes in the
 * sql thread cache of sqlmaster;
 */
typedef struct master_entry {
    char *tblname;
    int ixnum;
    int entry_size;
    void *entry;
} master_entry_t;

/*********** GLOBAL SQLITE MASTER ********************************************/

/* array */
static master_entry_t *sqlmaster;
static int sqlmaster_nentries;

static void *create_sqlite_master_row(int rootpage, char *csc2_schema,
                                      int tblnum, int ixnum, int *sz);

static int destroy_sqlite_master(master_entry_t *arr, int arr_len)
{
    master_entry_t *ent;
    int i;

    if (!arr) return 0;

    for (i = 0; i < arr_len; i++) {
        ent = &arr[i];
        if (ent->ixnum == -1) free(ent->tblname);
        free(ent->entry);
    }

    free(arr);

    return 0;
}

/**
 * Create sqlite_master row and populate the associated hash
 *
 */
int create_sqlite_master(void)
{
    master_entry_t *new_arr;
    master_entry_t *old_arr;
    master_entry_t *ent;
    struct dbtable *db;
    int tblnum, ixnum;
    int local_nentries;
    int tbl_idx;
    int i;

    local_nentries = 0;
    for (tblnum = 0; tblnum < thedb->num_dbs; tblnum++)
        local_nentries += 1 + thedb->dbs[tblnum]->nsqlix;

    old_arr = sqlmaster;
    new_arr = calloc(local_nentries, sizeof(master_entry_t));
    if (!new_arr) {
        fprintf(stderr, "MALLOC OOM\n");
        return -1;
    }

    for (i = 0, tblnum = 0; tblnum < thedb->num_dbs; tblnum++) {
        ent = &new_arr[i];
        db = thedb->dbs[tblnum];
        ent->tblname = strdup(db->dbname);
        ent->ixnum = -1;
        ent->entry = create_sqlite_master_row(i + RTPAGE_START, db->csc2_schema,
                                              tblnum, -1, &ent->entry_size);
        tbl_idx = i;
        i++;

        for (ixnum = 0; ixnum < db->nix; ixnum++) {
            ent = &new_arr[i];
            /* skip indexes that we aren't advertising to sqlite */
            if (db->ixsql[ixnum] != NULL) {
                ent->tblname = new_arr[tbl_idx].tblname;
                ent->ixnum = ixnum; /* comdb2 index number */
                ent->entry = create_sqlite_master_row(
                    i + RTPAGE_START, NULL, tblnum, ixnum, &ent->entry_size);
                i++;
            }
        }
    }

    assert(i == local_nentries);

    destroy_sqlite_master(old_arr, sqlmaster_nentries);

    sqlmaster = new_arr;
    sqlmaster_nentries = local_nentries;

    return 0;
}

inline static void fill_mem_str(Mem *m, char *str)
{
    if (str) {
        m->z = str;
        m->n = strlen(str);
        m->flags = MEM_Str | MEM_Ephem;
    } else {
        m->flags = MEM_Null;
    }
}
inline static void fill_mem_int(Mem *m, int val)
{
    m->u.i = val;
    m->flags = MEM_Int;
}

inline static int serialize_mems(Mem *m, int nmems, char **out, int *outlen)
{
    unsigned char *hdrbuf, *dtabuf;
    int datasz;
    int hdrsz;
    int fnum;
    int type;
    u32 sz;

    datasz = 0;
    hdrsz = 0;
    for (fnum = 0; fnum < nmems; fnum++) {
        type = sqlite3VdbeSerialType(&m[fnum], SQLITE_DEFAULT_FILE_FORMAT, &sz);
        datasz += sz;
        hdrsz += sqlite3VarintLen(type);
    }
    hdrsz += sqlite3VarintLen(hdrsz);

    *outlen = datasz + hdrsz;
    *out = malloc(*outlen);
    if (!*out) return -1;

    hdrbuf = (char *)*out;
    dtabuf = (char *)*out + hdrsz;

    sz = sqlite3PutVarint(hdrbuf, hdrsz);
    hdrbuf += sz;

    for (fnum = 0; fnum < nmems; fnum++) {
        u32 serial_type =
            sqlite3VdbeSerialType(&m[fnum], SQLITE_DEFAULT_FILE_FORMAT, &sz);
        sz = sqlite3VdbeSerialPut(dtabuf, &m[fnum], serial_type);
        dtabuf += sz;
        sz = sqlite3PutVarint(
            hdrbuf,
            sqlite3VdbeSerialType(&m[fnum], SQLITE_DEFAULT_FILE_FORMAT, &sz));
        hdrbuf += sz;
        assert(hdrbuf <= (*out + hdrsz));
    }

    return 0;
}

static void *create_sqlite_master_row(int rootpage, char *csc2_schema,
                                      int tblnum, int ixnum, int *sz)
{
    /* text type, text name, text tbl_name, integer rootpage, text sql, text
     * csc2 */
    Mem mems[6] = {0};
    struct dbtable *db;
    char *etype;
    char name[128];
    char *dbname;
    char *sql;
    char *rec;
    int rc;

    assert(tblnum < thedb->num_dbs);

    db = thedb->dbs[tblnum];
    dbname = db->dbname;

    if (ixnum == -1) {
        strcpy(name, dbname);
        sql = db->sql;
        etype = "table";
    } else {
        snprintf(name, sizeof(name), ".ONDISK_ix_%d", ixnum);
        struct schema *schema = find_tag_schema(dbname, name);
        if (schema->sqlitetag) {
            strcpy(name, schema->sqlitetag);
        } else {
            sql_index_name_trans(name, sizeof name, schema, db, ixnum, NULL);
        }

        sql = db->ixsql[ixnum];
        etype = "index";
    }
    ctrace("rootpage %d sql %s\n", rootpage, sql);

    fill_mem_str(&mems[0], etype);
    fill_mem_str(&mems[1], name);
    fill_mem_str(&mems[2], dbname);
    fill_mem_int(&mems[3], rootpage);
    fill_mem_str(&mems[4], sql);
    fill_mem_str(&mems[5], csc2_schema);

    rc = serialize_mems(mems, 6, &rec, sz);
    if (rc) return NULL;

    return rec;
}

/*********** PER SQL THREAD ROOTPAGE->TABLE MAPPING **************************/

struct dbtable *get_sqlite_db(struct sql_thread *thd, int iTable, int *ixnum)
{
    struct dbtable *db;
    char *tblname;

    assert(thd->rootpages);

    if (iTable < RTPAGE_START ||
        iTable >= (thd->rootpage_nentries + RTPAGE_START) ||
        ((tblname = thd->rootpages[iTable - RTPAGE_START].tblname) == NULL)) {
        return NULL;
    }

    db = get_dbtable_by_name(tblname);
    if (!db) return NULL;

    if (ixnum) *ixnum = thd->rootpages[iTable - RTPAGE_START].ixnum;

    return db;
}

int get_sqlite_entry_size(struct sql_thread *thd, int n)
{
    return thd->rootpages[n].entry_size;
}

void *get_sqlite_entry(struct sql_thread *thd, int n)
{
    return thd->rootpages[n].entry;
}

/* deep copy of sqlite master */
int get_copy_rootpages_nolock(struct sql_thread *thd)
{
    int i;
    if (thd->rootpages)
        destroy_sqlite_master(thd->rootpages, thd->rootpage_nentries);

    thd->rootpages = calloc(sqlmaster_nentries, sizeof(master_entry_t));
    if (!thd->rootpages) return -1;
    memcpy(thd->rootpages, sqlmaster,
           sqlmaster_nentries * sizeof(master_entry_t));
    for (i = 0; i < sqlmaster_nentries; i++) {
        thd->rootpages[i].tblname = strdup(sqlmaster[i].tblname);
        if (!thd->rootpages[i].tblname) return -1;
        thd->rootpages[i].entry = malloc(thd->rootpages[i].entry_size);
        if (!thd->rootpages[i].entry) return -1;
        memcpy(thd->rootpages[i].entry, sqlmaster[i].entry,
               thd->rootpages[i].entry_size);
    }
    thd->rootpage_nentries = sqlmaster_nentries;

    return 0;
}

/* copy rootpage info so a sql thread as a local copy
 */
inline int get_copy_rootpages(struct sql_thread *thd)
{
    int ret;
    pthread_rwlock_rdlock(&schema_lk);
    ret = get_copy_rootpages_nolock(thd);
    pthread_rwlock_unlock(&schema_lk);
    return ret;
}

pthread_rwlock_t sqlite_rootpages = PTHREAD_RWLOCK_INITIALIZER;

/* used by dynamic remote tables only */
int get_rootpage_numbers(int nums)
{
    static int crt_rootpage_number = RTPAGE_START;
    int tmp;

    pthread_rwlock_wrlock(&sqlite_rootpages);

    tmp = crt_rootpage_number + nums;
    if (tmp < crt_rootpage_number) {
        abort();
    } else {
        tmp = crt_rootpage_number;
        crt_rootpage_number += nums;
    }
    pthread_rwlock_unlock(&sqlite_rootpages);

    /*fprintf(stderr, "XXX allocated [%d:%d]\n", tmp, crt_rootpage_number-1);*/

    return tmp | 0x40000000; /* we allocate these nodes separately from local
                                rootpages */
}
