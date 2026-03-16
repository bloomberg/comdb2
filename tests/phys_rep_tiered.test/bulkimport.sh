#!/bin/bash

bash -n "$0" || exit 1
source ${TESTSROOTDIR}/tools/runit_common.sh
set -o pipefail

physrc=$1        
physrc_host=$2    
physrep=$3        
physrep_host=$4  

tmpdb_name="tmp${physrep}"
tmpdb_dir="${TESTDIR}/${tmpdb_name}"
echo "physrc: $physrc@$physrc_host, physrep: $physrep@$physrep_host, tmpdb: $tmpdb_name"

function cp_cdb2cfg() {
    local dir=$1
    echo "comdb2_config:default_type=local" > "$dir/comdb2db.cfg"
    echo "comdb2_config:ssl_cert_path=$TESTDIR" >> "$dir/comdb2db.cfg"
    echo "comdb2_config:allow_pmux_route:true" >> "$dir/comdb2db.cfg"
    if [[ -n "$PMUXPORT" ]]; then
        echo "comdb2_config:portmuxport=$PMUXPORT" >> "$dir/comdb2db.cfg"
    fi
}

function check_rc() {
    local cmd="$1"
    eval $cmd
    if [[ $? -ne 0 ]]; then
        echo "failed running $cmd"
        cleanup
        exit 1
    fi
}

function cleanup() {
    local name="$tmpdb_name" dir="$tmpdb_dir"
    if [[ -f "$dir/${name}.pid" ]]; then
        kill -9 $(cat "$dir/${name}.pid") 2>/dev/null || true
    fi
}

function create_db() {
    local name=$1 dir=$2
    mkdir -p "$dir"
    check_rc "$COMDB2_EXE --create $name -dir $dir"
    if [[ -n "$PMUXPORT" ]]; then
        echo "portmux_port $PMUXPORT" >> "$dir/${name}.lrl"
        echo "portmux_bind_path $pmux_socket" >> "$dir/${name}.lrl"
        echo "logmsg level debug >> $dir/${name}.lrl"
        echo "do semver 8.1.0" >> "$dir/${name}.lrl"
        echo "enable_bulk_import 1" >> "$dir/${name}.lrl"
    fi
    $COMDB2_EXE $name --lrl $dir/${name}.lrl --pidfile $dir/${name}.pid \
        >"$TESTDIR/logs/${name}.log" 2>&1 &
    cp_cdb2cfg "$dir"
    sleep 5
    check_rc "${CDB2SQL_EXE} --cdb2cfg $dir/comdb2db.cfg $name --host localhost 'select 1'"
    check_rc "${CDB2SQL_EXE} --cdb2cfg $dir/comdb2db.cfg $name --host localhost \
        -f ${TESTDIR}/${TESTCASE}.test/1-create-table.src.sql"
    check_rc "${CDB2SQL_EXE} --cdb2cfg $dir/comdb2db.cfg $name --host localhost \
        \"insert into t1 (id) values (1)\""
}

function test_bulkimport_into_physrep() {
    # Given: physrep is a physrep replicant (read-only)
    check_rc "${CDB2SQL_EXE} ${CDB2_OPTIONS} $physrep --host $physrep_host \
        \"exec procedure sys.cmd.send('enable_bulk_import 1')\""

    local tbl="t1"
    local output
    output=$(${CDB2SQL_EXE} ${CDB2_OPTIONS} $physrep --host $physrep_host \
        "replace table $tbl with LOCAL_$tmpdb_name.$tbl" 2>&1)
    local rc=$?

    if [[ $rc -eq 0 ]] || ! echo "$output" | grep -q "physical replicant"; then
        echo "Fail, rc=$rc, output: $output"
        cleanup
        exit 1
    fi

    echo "SUCCESS: bulkimport into physrep rejected: $output"
}

function test_bulkimport_into_physrep_src() {

    local tbl="t1"
    local output
    output=$(${CDB2SQL_EXE} ${CDB2_OPTIONS} $physrc --host $physrc_host \
        "replace table $tbl with LOCAL_$tmpdb_name.$tbl" 2>&1)
    local rc=$?

    if [[ $rc -eq 0 ]] || ! echo "$output" | grep -q "physical replicant"; then
        echo "Fail, rc=$rc, output: $output"
        cleanup
        exit 1
    fi

    echo "SUCCESS: bulkimport into physrep src rejected: $output"
}

function test_bulkimport_from_physrep_and_physrep_src() {
    local tbl="t1"

    check_rc "${CDB2SQL_EXE} ${CDB2_OPTIONS} $physrc --host $physrc_host \
        \"drop table if exists $tbl\""
    check_rc "${CDB2SQL_EXE} ${CDB2_OPTIONS} $physrc --host $physrc_host \
        -f ${TESTDIR}/${TESTCASE}.test/1-create-table.src.sql"
    check_rc "${CDB2SQL_EXE} ${CDB2_OPTIONS} $physrc --host $physrc_host \
        \"insert into $tbl (id) values (100), (200), (300)\""

    sleep 5

    check_rc "${CDB2SQL_EXE} --cdb2cfg $tmpdb_dir/comdb2db.cfg $tmpdb_name --host localhost \
        \"delete from $tbl where 1\""

    # Import from physrep source
    output_src=$(${CDB2SQL_EXE} --cdb2cfg $tmpdb_dir/comdb2db.cfg $tmpdb_name --host localhost \
        "replace table $tbl with LOCAL_$physrc.$tbl" 2>&1)
    local rc_src=$?
    echo "$output_src"

    sleep 5

    # Import from physrep
    output_rep=$(${CDB2SQL_EXE} --cdb2cfg $tmpdb_dir/comdb2db.cfg $tmpdb_name --host localhost \
        "replace table $tbl with LOCAL_$physrep.$tbl" 2>&1)
    local rc_rep=$?

    if [[ $rc_rep -ne 0 ]]; then
        echo "FAIL: bulkimport from physrep failed with rc_src=$rc_src, rc_rep=$rc_rep"
        echo "Output src: $output_src, rep: $output_rep"
        cleanup
        exit 1
    fi

    local cnt
    cnt=$(${CDB2SQL_EXE} --tabs --cdb2cfg $tmpdb_dir/comdb2db.cfg $tmpdb_name \
        --host localhost "select count(*) from $tbl")
    if [[ $cnt -eq 0 ]]; then
        echo "FAIL: no records found in $tmpdb_name.$tbl after import"
        cleanup
        exit 1
    fi

    echo "SUCCESS: bulk import from physrep succeeded, imported $cnt records"
}

function main() {
    create_db "$tmpdb_name" "$tmpdb_dir"

    tests=$(compgen -A function | grep -oh "test_\w*")
    for testcase in $tests; do
        echo "Running test case: $testcase"
        $testcase
    done

    echo "All bulkimport tests passed"
    cleanup
}

main
