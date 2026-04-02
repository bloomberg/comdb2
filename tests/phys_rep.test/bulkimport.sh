#!/bin/bash

bash -n "$0" || exit 1
source ${TESTSROOTDIR}/tools/runit_common.sh
set -o pipefail
set -x 

DBNAME=$1 
DBDIR=$2
destdb=${TESTCASE}dest${TESTID} #from runit
tmpdb_name="tmp${DBNAME}"
tmpdb_dir="${DBDIR}/${tmpdb_name}"

function check_rc() {
    local cmd="$1"
    eval $cmd
    if [[ $? -ne 0 ]]; then
        echo "failed running $cmd"
        cleanup
        exit 1
    fi
}

# assumes we have comdb2db.cfg on testdir 
# cause we sourcing runit_common.sh
function cp_comdb2dbcfg() { 
    dir=$1
    name=$2
    echo "comdb2_config:default_type=local" > "$dir/comdb2db.cfg"
    echo "comdb2_config:ssl_cert_path=$TESTDIR" >>"$dir/comdb2db.cfg"
    echo "comdb2_config:allow_pmux_route:true" >> "$dir/comdb2db.cfg"
}


function cleanup() {
    # cleanup tmpdb only - parent and physrep are managed by runit
    local name="$tmpdb_name" dir="$tmpdb_dir"
    if [[ -f "$dir/${name}.pid" ]]; then
        kill -9 $(cat "$dir/${name}.pid") 2>/dev/null || true
    fi
}

function create_db() {
    local name=$1 dir=$2
    mkdir -p "$dir"
    check_rc "$COMDB2_EXE --create $name -dir $dir"
    $COMDB2_EXE $name --lrl $dir/${name}.lrl --pidfile $dir/${name}.pid >"$DBDIR/${name}.log" 2>&1 &
    cp_comdb2dbcfg "$dir" "$name"
    # Also add tmpdb to parent's config so destdb can find it
    echo "$name $dir $dir/${name}.lrl" >> "$DBDIR/comdb2db.cfg"
    sleep 5
    check_rc "${CDB2SQL_EXE} --cdb2cfg $dir/comdb2db.cfg $name default 'select 1' "
    check_rc "${CDB2SQL_EXE} --cdb2cfg $dir/comdb2db.cfg $name -f ${TESTDIR}/${TESTCASE}.test/1-create-table.src.sql default "
    check_rc "${CDB2SQL_EXE} --cdb2cfg $dir/comdb2db.cfg $name default \"insert into t1 (id) values (1)\" "
}

function test_bulkimport_into_physrep() {
    # Given: destdb is already a physrep (set up by runit)

    # When: bulk import into the physrep (destdb)
    local tbl="t1"
    local output
    output=$(${CDB2SQL_EXE} --cdb2cfg $DBDIR/comdb2db.cfg $destdb --host localhost \
        "replace table $tbl with LOCAL_$tmpdb_name.$tbl" 2>&1)
    local rc=$?

    # Then: bulkimport should fail with the correct error message
    if [[ $rc -eq 0 ]] || ! echo "$output" | grep -q "physical replicant" ; then
        echo "Fail, rc=$rc, output: $output"
        cleanup
        exit 1
    fi

    echo "SUCCESS: bulkimport rejected with error: $output"
}

function test_bulkimport_from_physrep() {
    # Given: destdb is a physrep with replicated data from DBNAME
    # We reuse tmpdb as the destination and import FROM the physrep
    local tbl="t1"
    
    check_rc "${CDB2SQL_EXE} ${CDB2_OPTIONS} $DBNAME default \"drop table if exists $tbl\" "
    check_rc "${CDB2SQL_EXE} ${CDB2_OPTIONS} $DBNAME default -f ${TESTDIR}/${TESTCASE}.test/1-create-table.src.sql"
    check_rc "${CDB2SQL_EXE} ${CDB2_OPTIONS} $DBNAME default \"insert into $tbl (id) values (100), (200), (300)\" "
    sleep 5
    check_rc "${CDB2SQL_EXE} --cdb2cfg $tmpdb_dir/comdb2db.cfg $tmpdb_name default \"delete from $tbl where 1\" "

    # When: bulk import FROM the physrep to the tmpdb
    local output
    output=$(${CDB2SQL_EXE} --cdb2cfg $tmpdb_dir/comdb2db.cfg $tmpdb_name default \
        "replace table $tbl with LOCAL_$destdb.$tbl" 2>&1)
    local rc=$?

    # Then: bulkimport should succeed
    if [[ $rc -ne 0 ]]; then
        echo "FAIL: bulkimport from physrep failed with rc=$rc"
        echo "Output: $output"
        cleanup
        exit 1
    fi

    # Verify data was imported
    local cnt
    cnt=$(${CDB2SQL_EXE} --tabs --cdb2cfg $tmpdb_dir/comdb2db.cfg $tmpdb_name default "select count(*) from $tbl")
    if [[ $cnt -eq 0 ]]; then
        echo "FAIL: no records found in $tmpdb_name.$tbl after import from physrep"
        cleanup
        exit 1
    fi

    echo "SUCCESS: bulk import from physrep succeeded, imported $cnt records"
}

function main() {
    create_db "$tmpdb_name" "$tmpdb_dir"

    test_bulkimport_into_physrep

    test_bulkimport_from_physrep

    echo "All tests passed"
    cleanup
}

main 
