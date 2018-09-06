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
struct master_entry {
    char *tblname;
    int isstrdup; /* True if tblname is obtained from strdup(). */
    int ixnum;
    int entry_size;
    void *entry;
};

/*********** GLOBAL SQLITE MASTER ********************************************/

/* array */
static master_entry_t *sqlmaster;
static int sqlmaster_nentries;

static void *create_master_row(struct dbtable **dbs, int num_dbs, int rootpage,
                               char *csc2_schema, int tblnum, int ixnum,
                               int *sz);

inline int destroy_sqlite_master(master_entry_t *arr, int arr_len)
{
    if (!arr)
        return 0;

    for (int i = 0; i < arr_len; i++) {
        master_entry_t *ent = &arr[i];
        if (ent->isstrdup)
            free(ent->tblname);
        free(ent->entry);
    }
    free(arr);
    return 0;
}

/* accessor called from comdb2.c clean_exit() */
void cleanup_sqlite_master()
{
    destroy_sqlite_master(sqlmaster, sqlmaster_nentries);
    sqlmaster = NULL;
    sqlmaster_nentries = 0;
}

master_entry_t *create_master_entry_array(struct dbtable **dbs, int num_dbs,
                                          int *nents)
{
    int tblnum;
    int tbl_idx;
    int i;
    int local_nentries = 0;

    *nents = 0;
    /* for each table, account for table and all its indices */
    for (tblnum = 0; tblnum < num_dbs; tblnum++)
        local_nentries += 1 + dbs[tblnum]->nsqlix;

    master_entry_t *new_arr = calloc(local_nentries, sizeof(master_entry_t));
    if (!new_arr) {
        logmsg(LOGMSG_ERROR, "%s: MALLOC OOM\n", __func__);
        return NULL;
    }

    for (i = 0, tblnum = 0; tblnum < num_dbs; tblnum++) {
        master_entry_t *ent = &new_arr[i];
        struct dbtable *tbl = dbs[tblnum];
        ent->tblname = strdup(tbl->tablename);
        ent->isstrdup = 1;
        ent->ixnum = -1;
        ent->entry =
            create_master_row(dbs, num_dbs, i + RTPAGE_START, tbl->csc2_schema,
                              tblnum, -1, &ent->entry_size);
        tbl_idx = i;
        i++;

        for (int ixnum = 0; ixnum < tbl->nix; ixnum++) {
            ent = &new_arr[i];
            /* skip indexes that we aren't advertising to sqlite */
            if (tbl->ixsql[ixnum] == NULL)
                continue;
            ent->isstrdup = 0;
            assert(ent->tblname == NULL);
            ent->tblname = new_arr[tbl_idx].tblname;
            ent->ixnum = ixnum; /* comdb2 index number */
            ent->entry = create_master_row(dbs, num_dbs, i + RTPAGE_START, NULL,
                                           tblnum, ixnum, &ent->entry_size);
            i++;
        }
    }

    assert(i == local_nentries);

    *nents = local_nentries;

    return new_arr;
}

/**
 * Create sqlite_master row and populate the associated hash
 *
 */
void create_sqlite_master()
{
    master_entry_t *new_arr = NULL;
    int local_nentries = 0;

    new_arr =
        create_master_entry_array(thedb->dbs, thedb->num_dbs, &local_nentries);
    if (!new_arr) {
        logmsg(LOGMSG_ERROR, "%s: MALLOC OOM\n", __func__);
        abort();
    }

    destroy_sqlite_master(sqlmaster, sqlmaster_nentries);

    sqlmaster = new_arr;
    sqlmaster_nentries = local_nentries;
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
    if (!*out)
        return -1;

    hdrbuf = (unsigned char *)*out;
    dtabuf = (unsigned char *)*out + hdrsz;

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
        assert(((char *)hdrbuf) <= (*out + hdrsz));
    }

    return 0;
}

static void *create_master_row(struct dbtable **dbs, int num_dbs, int rootpage,
                               char *csc2_schema, int tblnum, int ixnum,
                               int *sz)
{
    /* text type, text name, text tbl_name, integer rootpage, text sql, text
     * csc2 */
    Mem mems[6] = {0};
    struct dbtable *tbl;
    char *etype;
    char name[128];
    char *dbname;
    char *sql;
    char *rec;
    int rc;

    assert(tblnum < num_dbs);

    tbl = dbs[tblnum];
    dbname = tbl->tablename;

    if (ixnum == -1) {
        strcpy(name, dbname);
        sql = tbl->sql;
        etype = "table";
    } else {
        struct schema *schema = tbl->schema->ix[ixnum];
        if (schema->sqlitetag) {
            strcpy(name, schema->sqlitetag);
        } else {
            sql_index_name_trans(name, sizeof name, schema, tbl, ixnum, NULL);
        }

        sql = tbl->ixsql[ixnum];
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
    if (rc)
        return NULL;

    return rec;
}

/*********** PER SQL THREAD ROOTPAGE->TABLE MAPPING **************************/

struct dbtable *get_sqlite_db(struct sql_thread *thd, int iTable, int *ixnum)
{
    struct dbtable *tbl;
    char *tblname;

    assert(thd->rootpages);

    if (iTable < RTPAGE_START ||
        iTable >= (thd->rootpage_nentries + RTPAGE_START) ||
        ((tblname = thd->rootpages[iTable - RTPAGE_START].tblname) == NULL)) {
        return NULL;
    }

    tbl = get_dbtable_by_name(tblname);
    if (!tbl)
        return NULL;

    if (ixnum)
        *ixnum = thd->rootpages[iTable - RTPAGE_START].ixnum;

    return tbl;
}

int get_sqlite_entry_size(struct sql_thread *thd, int n)
{
    return thd->rootpages[n].entry_size;
}

void *get_sqlite_entry(struct sql_thread *thd, int n)
{
    return thd->rootpages[n].entry;
}

int get_copy_rootpages_custom(struct sql_thread *thd, master_entry_t *ents,
                              int nents)
{
    int i;
    if (thd->rootpages)
        destroy_sqlite_master(thd->rootpages, thd->rootpage_nentries);

    thd->rootpages = calloc(nents, sizeof(master_entry_t));
    if (!thd->rootpages)
        return -1;
    memcpy(thd->rootpages, ents, nents * sizeof(master_entry_t));
    for (i = 0; i < nents; i++) {
        thd->rootpages[i].tblname = strdup(ents[i].tblname);
        if (!thd->rootpages[i].tblname)
            return -1;
        thd->rootpages[i].isstrdup = 1;
        thd->rootpages[i].entry = malloc(thd->rootpages[i].entry_size);
        if (!thd->rootpages[i].entry)
            return -1;
        memcpy(thd->rootpages[i].entry, ents[i].entry,
               thd->rootpages[i].entry_size);
    }
    thd->rootpage_nentries = nents;

    return 0;
}

/* deep copy of sqlite master */
int get_copy_rootpages_nolock(struct sql_thread *thd)
{
    return get_copy_rootpages_custom(thd, sqlmaster, sqlmaster_nentries);
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
