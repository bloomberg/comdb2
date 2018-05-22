#!/usr/bin/env bash

remove_good_test=0

while [[ -n "$1" ]]; do
    case $1 in

        -d)
            echo "ENABLED DEBUG TRACE"
            export CDB2JDBC_DEBUG_TRACE=1
            shift
            ;;

        -t)
            echo "ENABLED TEST TRACE"
            export CDB2JDBC_TEST_TRACE=1
            shift
            ;;

        -s)
            echo "PERFORMING SELECT TEST"
            export CDB2JDBC_SELECT_TEST=1
            shift
            ;;

        -r)
            echo "REMOVING GOOD TESTS"
            remove_good_test=1;
            shift
            ;;

        *)
            db=$1
            shift
            ;;

    esac

done

if [[ -z "$db" ]]; then
    echo "the db variable is not set"
    exit 1
fi

basedir=${COMDB2_BASEDIR:-/home/ubuntu}
pathbase=${COMDB2_PATHBASE:-$basedir/comdb2}
runbase=$pathbase/linearizable/jdbctest
scripts=$pathbase/linearizable/scripts
. $scripts/setvars
outbase=${COMDB2_OUTBASE:-/db/testout}

export CDB2JDBC_TEST_SSH_PASSWORD=shadow
export CDB2JDBC_TEST_SSH_USER=root
export CDB2JDBC_TEST_DBNAME=$db
export CDB2JDBC_TEST_CLUSTER=dev

iter=0
insper=0

# prepare the select test
ctest=$pathbase/linearizable/ctest/test
#echo "Preparing select test"
#$ctest -d $db -Y

while :; do 
    outfile=$outbase/jdbc_test.$db.$iter
    modval=$(( iter % 4 ))
    insper=$(( 10 ** modval ))
    echo "$(date) running jdbc-test iteration $iter for $db using $insper records per insert"
    export CDB2JDBC_TEST_INCREMENT=$insper
    cdb2sql $db dev "delete from jepsen where 1"

    echo "java -cp $runbase/target/jdbctest-1.0-SNAPSHOT.jar:$pathbase/cdb2jdbc/target/cdb2jdbc-2.0.0-dist/libexec/cdb2jdbc/cdb2jdbc-2.0.0.jar:$pathbase/cdb2jdbc/target/cdb2jdbc-2.0.0-dist/libexec/cdb2jdbc/protobuf-java-2.6.1.jar:$basedir/.m2/repository/com/jcraft/jsch/0.1.54/jsch-0.1.54.jar com.bloomberg.comdb2.App"
    java -cp $runbase/target/jdbctest-1.0-SNAPSHOT.jar:$pathbase/cdb2jdbc/target/cdb2jdbc-2.0.0-dist/libexec/cdb2jdbc/cdb2jdbc-2.0.0.jar:$pathbase/cdb2jdbc/target/cdb2jdbc-2.0.0-dist/libexec/cdb2jdbc/protobuf-java-2.6.1.jar:$basedir/.m2/repository/com/jcraft/jsch/0.1.54/jsch-0.1.54.jar com.bloomberg.comdb2.App 2>&1 | tee $outfile

    grep "LOST" $outfile
    if [[ $? == 0 ]]; then
        echo "!!! LOST VALUE IN ITERATION $iter !!!"
        break 2
    fi

    grep "^XXX " $outfile
    if [[ $? == 0 ]]; then
        echo "!!! FAILURE IN ITERATION $iter !!!"
        break 2
    fi

    #egrep -i cnonce $outfile | awk '{print $5, $4}' | egrep -v "snapshotOffset=0" | sort -u | awk '{print $1}' | uniq -c | egrep -v "      1 " 
    #egrep -i cnonce $outfile | awk '{print $6, $5, $4}' | egrep -v "snapshotOffset=0" | sort  -u | awk '{print $1}' | uniq -c | egrep -v  "      1 "
    egrep -i cnonce $outfile | awk '{print $5, $4}' | egrep -v "snapshotOffset=0" | sort -u | uniq -c  | egrep -v "      1"


    if [[ $? = 0 ]]; then
        echo "!!! CNONCE AT MULTIPLE LSNS FAILURE IN ITERATION $iter !!!"
        break 2
    fi


    let iter=iter+1

    echo "COMPLETED: HEALING"

    $pathbase/linearizable/scripts/heal $db
    ma=$(cdb2sql -tabs $db dev "exec procedure sys.cmd.send('bdb cluster')" | egrep MASTER | egrep lsn) ; m=${ma%%:*}
    c=$(ssh $m "/opt/bb/bin/cdb2sql -tabs $db @localhost 'exec procedure sys.cmd.send(\"bdb cluster\")'")
    echo "$c"

    echo "$c" | egrep COHERENT
    r=$?
    while [[ $r == 0 ]] ; do
        echo "$(date) waiting for $db cluster to become coherent"
        $pathbase/linearizable/scripts/heal $db
        sleep 1
        ma=$(cdb2sql -tabs $db dev "exec procedure sys.cmd.send('bdb cluster')" | egrep MASTER | egrep lsn) ; m=${ma%%:*}
        c=$(ssh $m "/opt/bb/bin/cdb2sql -tabs $db @localhost 'exec procedure sys.cmd.send(\"bdb cluster\")'")
        echo "$c"
        echo "$c" | egrep COHERENT
        r=$?
    done

    sleep 1

    if [[ "$remove_good_test" = "1" ]]; then
        echo "removing good test run"
        rm -f $outfile
    fi

done
