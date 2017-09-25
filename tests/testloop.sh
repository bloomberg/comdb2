#!/bin/bash

email="mhannum72@gmail.com mhannum@bloomberg.net"
tests="cinsert_linearizable jdbc_insert_linearizable jepsen_bank_nemesis jepsen_bank jepsen_dirty_reads jepsen_register_nemesis jepsen_register jepsen_sets_nemesis jepsen_sets register_linearizable"
mailperiod=100

# I saw a failure on this i haven't been able to reproduce
# tests="jepsen_sets_nemesis"

i=0 
while :; do 
    let i=i+1 
    echo "$(date) ITERATION $i" 
    rm -Rf $(find . -type d -mmin +60 | egrep test_)
    for x in $tests 
    do echo "$(date) - starting $x" 
        make $x > out ; r=$? 
        looktest=1
        cat out 
        egrep "setup failed" out 
        if [[ $? == 0 ]] ; then 
            echo "TEST DID NOT SET UP" 
            looktest=0
        fi

        egrep "timeout" out
        if [[ $? == 0 ]] ; then 
            echo "TEST TIMED OUT" 
            looktest=0
        fi

        if [[ $r != 0 && $looktest == 1 ]]; then
            ll=$(egrep "logs in" out | awk '{print $NF}') 
            l=${ll%%\)}

            # Okay .. some errors don't indicate a jepsen failure 
            # & I'm not going to 

            err=1
            egrep "java.lang.OutOfMemoryError" $l
            if [[ $? == 0 ]]; then
                echo "java out of memory error: continuing"
                err=0
            fi

            egrep "actual: com.jcraft.jsch.JSchException: java.net.ConnectException: Connection refused" $l
            if [[ $? == 0 ]]; then
                echo "actual: Connection refused error: continuing"
                err=0
            fi

            egrep "actual: java.sql.SQLNonTransientConnectionException: " $l
            if [[ $? == 0 ]]; then
                echo "TransientConnectionException: continuing"
                err=0
            fi

            if [[ $err == 1 ]]; then
                echo "ERROR IN ITERATION $i" 
                err=1
                for addr in $email ; do
                    mail -s "JEPSEN FAILURE ITERATION $i !!" $addr < $l
                done
                break 5
            fi
        fi
    done

    if [[ $(( i % mailperiod )) == 1 ]]; then 
        for addr in $email ; do
            mail -s "Successfully tested $i iterations" $addr < /dev/null
        done
    fi

done

