set(BERK_C
  btree/bt_cache.c
  btree/bt_compare.c
  btree/bt_conv.c
  btree/bt_curadj.c
  btree/bt_cursor.c
  btree/bt_delete.c
  btree/bt_method.c
  btree/bt_open.c
  btree/bt_pf.c
  btree/bt_pgcompact.c
  btree/bt_prefix.c
  btree/bt_put.c
  btree/bt_rec.c
  btree/bt_reclaim.c
  btree/bt_recno.c
  btree/bt_rsearch.c
  btree/bt_search.c
  btree/bt_split.c
  btree/bt_stat.c
  btree/bt_upgrade.c
  btree/bt_verify.c

  common/db_byteorder.c
  common/db_err.c
  common/db_getlong.c
  common/db_idspace.c
  common/db_log2.c
  common/util_cache.c
  common/util_sig.c

  crypto/aes_method.c
  crypto/crypto.c
  crypto/mersenne/mt19937db.c

  db/crdel_rec.c
  db/db.c
  db/db_am.c
  db/db_cam.c
  db/db_conv.c
  db/db_dispatch.c
  db/db_dup.c
  db/db_iface.c
  db/db_join.c
  db/db_meta.c
  db/db_method.c
  db/db_open.c
  db/db_overflow.c
  db/db_ovfl_vrfy.c
  db/db_pgcompact.c
  db/db_pgdump.c
  db/db_pr.c
  db/db_rec.c
  db/db_reclaim.c
  db/db_remove.c
  db/db_rename.c
  db/db_ret.c
  db/db_truncate.c
  db/db_upg.c
  db/db_upg_opd.c
  db/db_vrfy.c
  db/db_vrfyutil.c
  db/trigger_subscription.c

  dbreg/dbreg.c
  dbreg/dbreg_rec.c
  dbreg/dbreg_util.c

  env/db_malloc.c
  env/db_salloc.c
  env/db_shash.c
  env/env_attr.c
  env/env_file.c
  env/env_method.c
  env/env_open.c
  env/env_pgcompact.c
  env/env_recover.c
  env/env_region.c

  fileops/fop_basic.c
  fileops/fop_rec.c
  fileops/fop_util.c

  hash/hash_func.c
  hash/hash_stub.c

  hmac/hmac.c

  lock/lock.c
  lock/lock_deadlock.c
  lock/lock_method.c
  lock/lock_region.c
  lock/lock_stat.c
  lock/lock_util.c

  log/log.c
  log/log_archive.c
  log/log_compare.c
  log/log_get.c
  log/log_method.c
  log/log_put.c

  mp/mp_alloc.c
  mp/mp_bh.c
  mp/mp_fget.c
  mp/mp_fopen.c
  mp/mp_fput.c
  mp/mp_fset.c
  mp/mp_method.c
  mp/mp_region.c
  mp/mp_register.c
  mp/mp_stat.c
  mp/mp_sync.c
  mp/mp_trickle.c

  mutex/mut_pthread.c
  mutex/mutex.c

  os/os_abs.c
  os/os_alloc.c
  os/os_clock.c
  os/os_config.c
  os/os_dir.c
  os/os_errno.c
  os/os_falloc.c
  os/os_fid.c
  os/os_fsync.c
  os/os_handle.c
  os/os_id.c
  os/os_map.c
  os/os_method.c
  os/os_oflags.c
  os/os_open.c
  os/os_region.c
  os/os_rename.c
  os/os_root.c
  os/os_rpath.c
  os/os_rw.c
  os/os_seek.c
  os/os_sleep.c
  os/os_spin.c
  os/os_stat.c
  os/os_tmpdir.c
  os/os_unlink.c

  qam/qam.c
  qam/qam_conv.c
  qam/qam_files.c
  qam/qam_method.c
  qam/qam_open.c
  qam/qam_rec.c
  qam/qam_stat.c
  qam/qam_upgrade.c
  qam/qam_verify.c

  rep/rep_lc_cache.c
  rep/rep_method.c
  rep/rep_record.c
  rep/rep_region.c
  rep/rep_util.c

  txn/txn.c
  txn/txn_auto.c
  txn/txn_method.c
  txn/txn_rec.c
  txn/txn_recover.c
  txn/txn_region.c
  txn/txn_stat.c
  txn/txn_util.c

  xa/xa.c
  xa/xa_db.c
  xa/xa_map.c
)

file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/btree)
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/common)
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/db)
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/dbinc_auto)
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/dbreg)
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/fileops)
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/hash)
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/qam)
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/txn)

set(module berkdb)
set(MODULE BERKDB)
configure_file(${PROJECT_SOURCE_DIR}/mem/mem.h.in mem_berkdb.h @ONLY)
configure_file(dist/RELEASE dist/RELEASE COPYONLY)
configure_file(dist/genrec.sh dist/genrec.sh COPYONLY)
configure_file(dist/geninc.sh dist/geninc.sh COPYONLY)
configure_file(dist/gen_rec.awk dist/gen_rec.awk COPYONLY)
configure_file(dist/gen_inc.awk dist/gen_inc.awk COPYONLY)
configure_file(dist/template/rec_ctemp dist/template/rec_ctemp COPYONLY)

set(BERK_SRC
  btree/btree.src 
  db/crdel.src
  db/db.src
  dbreg/dbreg.src
  fileops/fileops.src
  hash/hash.src
  qam/qam.src
)

foreach(src ${BERK_SRC})
  get_filename_component(name ${src} NAME)
  string(REPLACE ".src" "" name ${name})
  string(REPLACE ".src" "_auto.c" c ${src})
  string(CONCAT h "dbinc_auto/" ${name} "_auto.h")
  list(APPEND GEN_C ${c})
  add_custom_command(
    OUTPUT ${c} ${h} ${e}
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/${src} dist/genrec.sh dist/gen_rec.awk
    COMMAND dist/genrec.sh ${CMAKE_CURRENT_SOURCE_DIR}/${src} ${c} ${h} dist/template/rec_${name}
  )
endforeach()

# Only want hash_auto.h
list(REMOVE_ITEM GEN_C hash/hash_auto.c)

set(EXT_DIRS
  btree
  common
  crypto
  db
  dbreg
  env
  fileops
  hash
  hmac
  lock
  log
  mp
  mutex
  os
  qam
  rep
  xa
)

foreach(EXT_DIR ${EXT_DIRS})
  file(GLOB_RECURSE EXT_SRCS "${EXT_DIR}/*.c")
  file(GLOB    EXT_AUTO_SRCS "${EXT_DIR}/*.src")
  foreach(src ${EXT_AUTO_SRCS})
    get_filename_component(src ${src} NAME)
    string(REPLACE ".src" "_auto.c" src ${src})
    string(CONCAT src "${CMAKE_CURRENT_BINARY_DIR}/${EXT_DIR}/" ${src})
    list(APPEND EXT_SRCS ${src})
  endforeach()
  string(CONCAT HEXT "dbinc_auto/" ${EXT_DIR} "_ext.h")
  list(APPEND GEN_EXT_H ${HEXT})
  add_custom_command(
    OUTPUT ${HEXT}
    DEPENDS ${EXT_SRCS} dist/geninc.sh dist/gen_inc.awk
    COMMAND dist/geninc.sh ${EXT_DIR} ${HEXT} ${EXT_SRCS}
  )
endforeach()

add_library(db ${BERK_C} ${GEN_C} ${GEN_EXT_H})
include_directories(
  build
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_BINARY_DIR}
  ${PROJECT_SOURCE_DIR}/util
  ${PROJECT_SOURCE_DIR}/bbinc
  ${PROJECT_SOURCE_DIR}/comdb2rle
  ${PROJECT_SOURCE_DIR}/crc32c
  ${PROJECT_SOURCE_DIR}/dlmalloc
  ${PROJECT_SOURCE_DIR}/mem
  ${PROJECT_BINARY_DIR}/mem
  ${LZ4_INCLUDE_DIR}
  ${OPENSSL_INCLUDE_DIR}
)
if(${CMAKE_SYSTEM_NAME} STREQUAL "Darwin")
  set_target_properties(db PROPERTIES COMPILE_FLAGS -Wno-knr-promoted-parameter)
endif()
add_definitions(-DSTDC_HEADERS)
add_dependencies(db mem)
