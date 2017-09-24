# Local defs
berkdb_MEMGEN:=berkdb/mem_berkdb.h

# Berkdb individual modules
BTREE_SOURCES:=berkdb/btree/bt_compare.c berkdb/btree/bt_conv.c		\
berkdb/btree/bt_curadj.c berkdb/btree/bt_cursor.c			\
berkdb/btree/bt_delete.c berkdb/btree/bt_method.c			\
berkdb/btree/bt_open.c berkdb/btree/bt_prefix.c berkdb/btree/bt_put.c	\
berkdb/btree/bt_rec.c berkdb/btree/bt_reclaim.c				\
berkdb/btree/bt_recno.c berkdb/btree/bt_rsearch.c			\
berkdb/btree/bt_search.c berkdb/btree/bt_split.c			\
berkdb/btree/bt_stat.c berkdb/btree/bt_upgrade.c			\
berkdb/btree/bt_verify.c berkdb/btree/bt_cache.c berkdb/btree/bt_pf.c   \
berkdb/btree/bt_pgcompact.c
COMMON_SOURCES:=berkdb/common/db_byteorder.c berkdb/common/db_err.c	\
berkdb/common/db_getlong.c berkdb/common/db_idspace.c			\
berkdb/common/db_log2.c berkdb/common/util_cache.c			\
berkdb/common/util_sig.c
CRYPTO_SOURCES:=berkdb/crypto/aes_method.c berkdb/crypto/crypto.c	\
berkdb/crypto/mersenne/mt19937db.c
DB_SOURCES:=berkdb/db/crdel_rec.c berkdb/db/db.c berkdb/db/db_am.c	\
berkdb/db/db_cam.c berkdb/db/db_conv.c berkdb/db/db_dispatch.c		\
berkdb/db/db_dup.c berkdb/db/db_iface.c berkdb/db/db_join.c		\
berkdb/db/db_meta.c berkdb/db/db_method.c berkdb/db/db_open.c		\
berkdb/db/db_overflow.c berkdb/db/db_ovfl_vrfy.c berkdb/db/db_pr.c	\
berkdb/db/db_rec.c berkdb/db/db_reclaim.c berkdb/db/db_remove.c		\
berkdb/db/db_rename.c berkdb/db/db_ret.c berkdb/db/db_truncate.c	\
berkdb/db/db_upg.c berkdb/db/db_upg_opd.c berkdb/db/db_vrfy.c		\
berkdb/db/db_vrfyutil.c berkdb/db/db_pgdump.c berkdb/db/db_pgcompact.c	\
berkdb/db/trigger_subscription.c
DBREG_SOURCES:=berkdb/dbreg/dbreg.c berkdb/dbreg/dbreg_rec.c	\
berkdb/dbreg/dbreg_util.c
ENV_SOURCES:=berkdb/env/db_malloc.c berkdb/env/db_salloc.c	\
berkdb/env/db_shash.c berkdb/env/env_attr.c			\
berkdb/env/env_file.c berkdb/env/env_method.c		\
berkdb/env/env_open.c berkdb/env/env_recover.c		\
berkdb/env/env_region.c berkdb/env/env_pgcompact.c
FILEOPS_SOURCES:=berkdb/fileops/fop_basic.c berkdb/fileops/fop_rec.c	\
berkdb/fileops/fop_util.c
HASH_SOURCES:=berkdb/hash/hash.c berkdb/hash/hash_conv.c	\
berkdb/hash/hash_dup.c berkdb/hash/hash_func.c			\
berkdb/hash/hash_meta.c berkdb/hash/hash_method.c		\
berkdb/hash/hash_open.c berkdb/hash/hash_page.c			\
berkdb/hash/hash_rec.c berkdb/hash/hash_reclaim.c		\
berkdb/hash/hash_stat.c berkdb/hash/hash_upgrade.c		\
berkdb/hash/hash_verify.c
HMAC_SOURCES:=berkdb/hmac/hmac.c
LOCK_SOURCES:=berkdb/lock/lock.c berkdb/lock/lock_deadlock.c	\
berkdb/lock/lock_method.c berkdb/lock/lock_region.c		\
berkdb/lock/lock_stat.c berkdb/lock/lock_util.c
LOG_SOURCES:=berkdb/log/log.c berkdb/log/log_archive.c			\
berkdb/log/log_compare.c berkdb/log/log_get.c berkdb/log/log_method.c	\
berkdb/log/log_put.c
MP_SOURCES:=berkdb/mp/mp_alloc.c berkdb/mp/mp_bh.c		\
berkdb/mp/mp_fget.c berkdb/mp/mp_fopen.c berkdb/mp/mp_fput.c	\
berkdb/mp/mp_fset.c berkdb/mp/mp_method.c berkdb/mp/mp_region.c	\
berkdb/mp/mp_register.c berkdb/mp/mp_stat.c berkdb/mp/mp_sync.c	\
berkdb/mp/mp_trickle.c
MUTEX_SOURCES:=berkdb/mutex/mut_pthread.c berkdb/mutex/mutex.c
OS_SOURCES:=berkdb/os/os_abs.c berkdb/os/os_alloc.c			\
berkdb/os/os_clock.c berkdb/os/os_config.c berkdb/os/os_dir.c		\
berkdb/os/os_errno.c berkdb/os/os_fid.c berkdb/os/os_fsync.c		\
berkdb/os/os_handle.c berkdb/os/os_id.c berkdb/os/os_map.c		\
berkdb/os/os_method.c berkdb/os/os_oflags.c berkdb/os/os_open.c		\
berkdb/os/os_region.c berkdb/os/os_rename.c berkdb/os/os_root.c		\
berkdb/os/os_rpath.c berkdb/os/os_rw.c berkdb/os/os_seek.c		\
berkdb/os/os_sleep.c berkdb/os/os_spin.c berkdb/os/os_stat.c		\
berkdb/os/os_tmpdir.c berkdb/os/os_unlink.c berkdb/os/os_falloc.c
QAM_SOURCES:=berkdb/qam/qam.c berkdb/qam/qam_conv.c			\
berkdb/qam/qam_files.c berkdb/qam/qam_method.c berkdb/qam/qam_open.c	\
berkdb/qam/qam_rec.c berkdb/qam/qam_stat.c berkdb/qam/qam_upgrade.c	\
berkdb/qam/qam_verify.c
REP_SOURCES:=berkdb/rep/rep_method.c berkdb/rep/rep_record.c	\
berkdb/rep/rep_region.c berkdb/rep/rep_util.c			\
berkdb/rep/rep_lc_cache.c
TXN_SOURCES:=berkdb/txn/txn.c berkdb/txn/txn_auto.c			\
berkdb/txn/txn_method.c berkdb/txn/txn_rec.c berkdb/txn/txn_recover.c	\
berkdb/txn/txn_region.c berkdb/txn/txn_stat.c berkdb/txn/txn_util.c
XA_SOURCES:=berkdb/xa/xa.c berkdb/xa/xa_db.c berkdb/xa/xa_map.c

BERKDB_SOURCES=$(BTREE_SOURCES) $(COMMON_SOURCES) $(CRYPTO_SOURCES)	\
$(DB_SOURCES) $(DBREG_SOURCES) $(ENV_SOURCES) $(FILEOPS_SOURCES)	\
$(HASH_SOURCES) $(HMAC_SOURCES) $(LOCK_SOURCES) $(LOG_SOURCES)		\
$(MP_SOURCES) $(MUTEX_SOURCES) $(OS_SOURCES) $(QAM_SOURCES)		\
$(REP_SOURCES) $(TXN_SOURCES) $(XA_SOURCES)

all: berkdb/libdb.a

berkdb_CPPFLAGS:=-Iberkdb -Iberkdb/build -Idlmalloc -Icomdb2rle		\
-Icrc32c $(OPTBBINCLUDE) -DPARANOID -DLOCKMGRDBG -DSTUB_REP_ENTER	\
-DTRACE_ON_ADDING_LOCKS -DCONFIG_TEST -D_DEFAULT_SOURCE -DDEBUG
berkdb_CFLAGS:=$(CFLAGS_OPT)

#autogen include directory
AINCDIR=berkdb/dbinc_auto
#autogen source and headers
AUTOGEN=berkdb/dist/genrec.sh
#autogen ext headers
AUTOINC=berkdb/dist/geninc.sh
#autogen template directory
TMPLDIR=berkdb/dist/template

GENERATED_TEMPLATES=		   \
berkdb/dist/template/rec_btree	   \
berkdb/dist/template/rec_crdel	   \
berkdb/dist/template/rec_db	   \
berkdb/dist/template/rec_dbreg	   \
berkdb/dist/template/rec_fileops   \
berkdb/dist/template/rec_hash	   \
berkdb/dist/template/rec_qam	   \

BERKDB_GENERATED_H=			   \
$(AINCDIR)/btree_auto.h			   \
$(AINCDIR)/crdel_auto.h			   \
$(AINCDIR)/db_auto.h			   \
$(AINCDIR)/dbreg_auto.h			   \
$(AINCDIR)/fileops_auto.h		   \
$(AINCDIR)/hash_auto.h			   \
$(AINCDIR)/qam_auto.h			   \

BERKDB_GENERATED_C=		\
berkdb/btree/btree_auto.c	\
berkdb/db/crdel_auto.c		\
berkdb/db/db_auto.c		\
berkdb/dbreg/dbreg_auto.c	\
berkdb/fileops/fileops_auto.c	\
berkdb/hash/hash_auto.c		\
berkdb/qam/qam_auto.c		\

# Generate _auto.c files from .src files.
%_auto.c: %.src
	$(AUTOGEN) $< $@ $(AINCDIR)/$(*F)_auto.h $(TMPLDIR)/rec_$(*F)

# Now that we're generating .h pre-requisites, we must build the _auto.h
# files before compiling any .c from the sub-module
$(BERKDB_GENERATED_H): $(BERKDB_GENERATED_C)

BERKDB_EXT_H =				   \
$(AINCDIR)/hash_ext.h			   \
$(AINCDIR)/crypto_ext.h			   \
$(AINCDIR)/hmac_ext.h			   \
$(AINCDIR)/xa_ext.h			   \
$(AINCDIR)/rep_ext.h			   \
$(AINCDIR)/qam_ext.h			   \
$(AINCDIR)/os_ext.h			   \
$(AINCDIR)/mutex_ext.h			   \
$(AINCDIR)/mp_ext.h			   \
$(AINCDIR)/log_ext.h			   \
$(AINCDIR)/lock_ext.h			   \
$(AINCDIR)/hmac_ext.h			   \
$(AINCDIR)/hash_ext.h			   \
$(AINCDIR)/fileops_ext.h		   \
$(AINCDIR)/env_ext.h			   \
$(AINCDIR)/dbreg_ext.h			   \
$(AINCDIR)/db_ext.h			   \
$(AINCDIR)/crypto_ext.h			   \
$(AINCDIR)/common_ext.h			   \
$(AINCDIR)/btree_ext.h

# Generate external interfaces from *.c files.
$(AINCDIR)/btree_ext.h: berkdb/btree/btree_auto.c $(BTREE_SOURCES)
	$(AUTOINC) btree $@ $^
$(AINCDIR)/common_ext.h: berkdb/common/crypto_stub.c $(COMMON_SOURCES)
	$(AUTOINC) common $@ $^
$(AINCDIR)/crypto_ext.h: $(CRYPTO_SOURCES)
	$(AUTOINC) crypto $@ $^
$(AINCDIR)/db_ext.h: berkdb/db/crdel_auto.c berkdb/db/db_auto.c $(DB_SOURCES)
	$(AUTOINC) db $@ $^
$(AINCDIR)/dbreg_ext.h: berkdb/dbreg/dbreg_auto.c $(DBREG_SOURCES)
	$(AUTOINC) dbreg $@ $^
$(AINCDIR)/env_ext.h: $(ENV_SOURCES)
	$(AUTOINC) env $@ $^
$(AINCDIR)/fileops_ext.h: berkdb/fileops/fileops_auto.c $(FILEOPS_SOURCES)
	$(AUTOINC) fileops $@ $^
$(AINCDIR)/hash_ext.h: berkdb/hash/hash_auto.c $(HASH_SOURCES)
	$(AUTOINC) hash $@ $^
$(AINCDIR)/hmac_ext.h: $(HMAC_SOURCES)
	$(AUTOINC) hmac $@ $^
$(AINCDIR)/lock_ext.h: $(LOCK_SOURCES)
	$(AUTOINC) lock $@ $^
$(AINCDIR)/log_ext.h: $(LOG_SOURCES)
	$(AUTOINC) log $@ $^
$(AINCDIR)/mp_ext.h: $(MP_SOURCES)
	$(AUTOINC) mp $@ $^
$(AINCDIR)/mutex_ext.h: $(MUTEX_SOURCES)
	$(AUTOINC) mutex $@ $^
$(AINCDIR)/os_ext.h: $(OS_SOURCES)
	$(AUTOINC) os $@ $^
$(AINCDIR)/qam_ext.h: berkdb/qam/qam_auto.c $(QAM_SOURCES)
	$(AUTOINC) qam $@ $^
$(AINCDIR)/rep_ext.h: $(REP_SOURCES)
	$(AUTOINC) rep $@ $^
$(AINCDIR)/xa_ext.h: $(XA_SOURCES)
	$(AUTOINC) xa $@ $^

berkdb_OBJS=$(patsubst %.c,%.o,$(BERKDB_SOURCES)) $(patsubst %.c,%.o,$(BERKDB_GENERATED_C))

# Defined in the top level makefile
ARS+=berkdb/libdb.a
OBJS+=$(berkdb_OBJS)
GENC+=$(BERKDB_GENERATED_C)
GENH+=$(BERKDB_EXT_H) $(BERKDB_GENERATED_H) $(berkdb_MEMGEN)
GENMISC+=$(GENERATED_TEMPLATES)
SOURCES+=$(BERKDB_SOURCES)

$(berkdb_OBJS): $(BERKDB_EXT_H) $(berkdb_MEMGEN)

# Custom defines
berkdb/libdb.a: CPPFLAGS+=$(berkdb_CPPFLAGS)
berkdb/libdb.a: CFLAGS+=$(berkdb_CFLAGS)

berkdb/libdb.a: $(berkdb_OBJS)
	$(AR) $(ARFLAGS) $@ $^


# For dependencies
berkdb/%.d: CPPFLAGS+=$(berkdb_CPPFLAGS)
