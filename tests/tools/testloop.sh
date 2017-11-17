#!/bin/bash

debug=1
[[ "$debug" == "1" ]] && set -x

export CORE_ON_TIMEOUT=1
#export NOKILL_ON_TIMEOUT=1
email="mhannum72@gmail.com"
tests="jepsen_atomic_writes jepsen_a6_nemesis jepsen_a6 jepsen_bank_nemesis jepsen_bank jepsen_dirty_reads jepsen_g2 jepsen_register_nemesis jepsen_register jepsen_sets_nemesis jepsen_sets cinsert_linearizable register_linearizable"

# mailperiod=86400
mailperiod=7200
lasttime=0

i=0 

export setup_failures=0
export timeouts=0
export nomemory=0
export noconn=0
export sshfail=0
export goodtests=0
export test_linger=$(( 60 * 2 ))

function print_status
{
    [[ "$debug" == "1" ]] && set -x
    echo "Good test count:  $goodtests" 
    echo "Setups failures:  $setup_failures" 
    echo "Test timeouts  :  $timeouts" 
    echo "Out-of-memory  :  $nomemory"
    echo "Connection fail:  $noconn" 
    echo "SSH fail       :  $sshfail"
}

while :; do 
    let i=i+1 
    print_status
    echo "$(date) ITERATION $i" 
    rm -Rf $(find . -type d -mmin +$test_linger | egrep test_)
    for x in $tests 
    do echo "$(date) - starting $x" 

        for m in $CLUSTER; do ssh $m 'sudo iptables -F -w; sudo iptables -X -w';  done
        for m in $CLUSTER; do ssh $m 'killall -s 9 comdb2';  done

        export out=test_$x_$(date '+%Y%m%d%H%M%S')
        make $x > $out ; r=$? 

        for m in $CLUSTER; do ssh $m 'sudo iptables -F -w; sudo iptables -X -w';  done
        for m in $CLUSTER; do ssh $m 'killall -s 9 comdb2';  done

        looktest=1
        cat $out 
        egrep "setup failed" $out 
        if [[ $? == 0 ]] ; then 
            echo "TEST DID NOT SET UP" 
            let setup_failures=setup_failures+1
            looktest=0
        fi

        # Kyle's tests sometime crash & leave the network partitioned.  This 
        # looks like a timeout.  Detect and continue
        if [[ "$x" == "jepsen"* ]] ; then
            egrep "timeout \(logs in" $out
                if [[ $? == 0 ]] ; then
                echo "TEST TIMED OUT"
                let timeout=timeout+1
                looktest=0
            fi
        fi

        if [[ $looktest == 1 && $r == 0 ]]; then
            let goodtests=goodtests+1
        fi

        if [[ $r != 0 && $looktest == 1 ]]; then

            # Okay .. some errors don't indicate a jepsen failure 
            # & I'm not going to 
            ll=$(egrep "logs in" $out | awk '{print $NF}') 
            l=${ll%%\)}

            err=1
            egrep "java.lang.OutOfMemoryError" $l
            if [[ $? == 0 ]]; then
                echo "java out of memory error: continuing"
                let nomemory=nomemory+1
                err=0
            fi

            egrep "actual: com.jcraft.jsch.JSchException: java.net.ConnectException: Connection refused" $l
            if [[ $? == 0 ]]; then
                echo "actual: Connection refused error: continuing"
                let sshfail=sshfail+1
                err=0
            fi

            egrep "actual: java.sql.SQLNonTransientConnectionException: " $l
            if [[ $? == 0 ]]; then
                echo "TransientConnectionException: continuing"
                let noconn=noconn+1
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

    export now=$(date +%s)
    echo "now is $now lasttime is $lasttime"

    if [ $(( now - lasttime )) -gt $mailperiod ]; then

        lasttime=now
        echo "Mailing results"

        print_status > body.txt

        for addr in $email ; do
            mail -s "Successfully tested $i iterations" $addr < body.txt
        done
        lasttime=$now
    fi

done

