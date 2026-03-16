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

if [[ -n "$CLUSTER" ]]; then
    tmpdb_host="$physrc_host"
else
    tmpdb_host=$(hostname)
fi

echo "physrc: $physrc@$physrc_host, physrep: $physrep@$physrep_host, tmpdb: $tmpdb_name@$tmpdb_host"

function cp_cdb2cfg() {
    local dir=$1
    local cfg="comdb2_config:default_type=local
comdb2_config:ssl_cert_path=$TESTDIR
comdb2_config:allow_pmux_route:true"
    if [[ -n "$PMUXPORT" ]]; then
        cfg="$cfg
comdb2_config:portmuxport=$PMUXPORT"
    fi

    mkdir -p "$dir"
    echo "$cfg" > "$dir/comdb2db.cfg"
    if [[ -n "$CLUSTER" ]]; then
        ssh $tmpdb_host "echo '$cfg' > $dir/comdb2db.cfg" < /dev/null
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
    if [[ -n "$CLUSTER" ]]; then
        ssh $tmpdb_host "if [[ -f $dir/${name}.pid ]]; then kill -9 \$(cat $dir/${name}.pid) 2>/dev/null; fi" < /dev/null || true
    else
        if [[ -f "$dir/${name}.pid" ]]; then
            kill -9 $(cat "$dir/${name}.pid") 2>/dev/null || true
        fi
    fi
}

function create_db() {
    local name=$1 dir=$2
    local lrl_extras=""
    if [[ -n "$PMUXPORT" ]]; then
        lrl_extras="portmux_port $PMUXPORT
portmux_bind_path $pmux_socket"
    fi
    lrl_extras="$lrl_extras
logmsg level debug
do semver 8.1.0
enable_bulk_import 1
foreign_db_resolve_local 1"

    if [[ -n "$CLUSTER" ]]; then
        ssh $tmpdb_host "mkdir -p $dir" < /dev/null
        check_rc "ssh $tmpdb_host '$COMDB2_EXE --create $name -dir $dir' < /dev/null"
        ssh $tmpdb_host "echo '$lrl_extras' >> $dir/${name}.lrl" < /dev/null
        ssh $tmpdb_host "source ${REP_ENV_VARS} ; $COMDB2_EXE $name --lrl $dir/${name}.lrl --pidfile $dir/${name}.pid" >> $TESTDIR/logs/${name}.log 2>&1 < /dev/null &
    else
        mkdir -p "$dir"
        check_rc "$COMDB2_EXE --create $name -dir $dir"
        echo "$lrl_extras" >> "$dir/${name}.lrl"
        COMDB2_CONFIG_PORTMUXPORT=$PMUXPORT COMDB2_CONFIG_ALLOW_PMUX_ROUTE=1 \
            $COMDB2_EXE $name --lrl $dir/${name}.lrl --pidfile $dir/${name}.pid \
            >"$TESTDIR/logs/${name}.log" 2>&1 &
    fi
    cp_cdb2cfg "$dir"
    sleep 5
    check_rc "${CDB2SQL_EXE} --cdb2cfg $dir/comdb2db.cfg $name --host $tmpdb_host 'select 1'"
    check_rc "${CDB2SQL_EXE} --cdb2cfg $dir/comdb2db.cfg $name --host $tmpdb_host \
        -f ${TESTDIR}/${TESTCASE}.test/1-create-table.src.sql"
    check_rc "${CDB2SQL_EXE} --cdb2cfg $dir/comdb2db.cfg $name --host $tmpdb_host \
        \"insert into t1 (id) values (1)\""
}

function test_bulkimport_into_physrep() {
    # Given: physrep is a physrep replicant (read-only)
    check_rc "${CDB2SQL_EXE} ${CDB2_OPTIONS} $physrep --host $physrep_host \
        \"exec procedure sys.cmd.send('enable_bulk_import 1')\""

    local tbl="t1"
    local output

    # The physrep host must also be able to find the tmpdb via LOCAL_.
    # Since tmpdb runs on $tmpdb_host (same as physrc_host in cluster mode),
    # and physrep also runs on the same cluster, LOCAL_ resolves via its pmux.
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

    check_rc "${CDB2SQL_EXE} --cdb2cfg $tmpdb_dir/comdb2db.cfg $tmpdb_name --host $tmpdb_host \
        \"delete from $tbl where 1\""

    # Import from physrep source
    output_src=$(${CDB2SQL_EXE} --cdb2cfg $tmpdb_dir/comdb2db.cfg $tmpdb_name --host $tmpdb_host \
        "replace table $tbl with LOCAL_$physrc.$tbl" 2>&1)
    local rc_src=$?
    echo "$output_src"

    sleep 5

    # Import from physrep replicant
    output_rep=$(${CDB2SQL_EXE} --cdb2cfg $tmpdb_dir/comdb2db.cfg $tmpdb_name --host $tmpdb_host \
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
        --host $tmpdb_host "select count(*) from $tbl")
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
