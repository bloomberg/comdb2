#!/bin/bash

# Analyze test for the comdb2 testsuite
#
#
# This test is designed to test comdb2/sqlite3's analyze function.  It intends
# to test and document the difference in performance between a non-analyzed 
# database and and analyzed database for a set of testcases designed
# specifically to benefit from analyze.  
#
# This test module is also designed to test the features of comdb2's improved-
# analyze/compressed-btrees.  These features include a) the ability to backout
# to the previous stats, b) the ability to request the database to analyze
# compressed-btrees rather than full-btrees.
# 
#
#
# I. Testing Basic Analyze Functionality
#
# The methodolgy is to add records to tables t1, t2, and t3, benchmark the 
# time that a series of 'select' statments take both before and after the 
# analyze is complete.  Prior to running analyze, the database should perform 
# significantly slower than after running analyze.
# 
# Most of the time for this test will be used in filling the database, which
# will be used again when testing the improved-analyze / compressed btree 
# functionality
#
#
#
# II. Testing Improved-Analyze
#
# If there are no previous sqlite1_stats, the rollback function should restore 
# the table to the defaults (empty).  The methodology will be to save the 
# current (non-compressed) result set, and compare this against the results 
# returned by running improved-analyze using a series of compression-arguments.
# We should be able to note that the time taken to perform analyze decreases
# as the amount of compression increases.  A perfect test would show that an
# analysis performed against a compressed btree returns the same results as 
# the analysis performed against the non-compressed btree.  It's expected that
# higher compression will produce less accurate, but usable results.
#
################################################################################
#
#   Table t1 
#
#   schema 
#   {
#       int a
#       int b
#       int c
#       int d
#   }
#         
#   keys 
#   {
#       dup     "a" = a
#       dup     "b" = b
#       dup     "c" = c
#       dup     "d" = d
#   }
# 
################################################################################
# 
#   Table t2
#
#   schema 
#   {
#       int a
#       int b
#       int c
#       int d
#   }
#         
#   keys 
#   {
#       dup     "ab" = a + b
#       dup     "bc" = b + c
#       dup     "cd" = c + d
#   }
#
################################################################################
#
#   Table t3
#
#   schema 
#   {
#       int a
#       int b
#       int c
#       int d
#   }
#         
#   keys 
#   {
#       dup     "abc" = a + b + c
#       dup     "bcd" = b + c + d
#   }
#
################################################################################
#
# Adding records
#
# The 'x'_scale numbers are chosen so that sqlite will favor the later keys in 
# these tables rather than the earlier keys after the table is analyzed.  On 
# table t1, after adding 500 records with a strtv of 0 using this setup, analyze
# appeared to do extactly this:
#
# (tbl='t1', idx='t1_ix_0', stat='500 167')     // a
# (tbl='t1', idx='t1_ix_1', stat='500 72')      // b
# (tbl='t1', idx='t1_ix_2', stat='500 30')      // c
# (tbl='t1', idx='t1_ix_3', stat='500 18')      // d
#
# The '167' for key a shows that a select using up this key will return on
# average 167 records.  This gels with what we've added as 500/3 is 167.
# Before running analyze, each key has the same weight:
#
# stat='1000000 10 9 8 7 6 5 5 5 5 5 5 5 ..'
#
# The default behavior is that sqlite will prefer indexes which contain columns 
# that match the greatest number of matching columns in the where-clause up to 
# the 6th index.  For multiple indexes which match 6 or more columns and are 
# otherwise equal, the sqlite query-optimizer will simply use the first.  
#
# The select query for the test on t1 will use all of the keys.  Before running
# analyze on t1, the optimizer will choose the first key (a).  This is a bad,
# choice, and should cause the query to run approximately 10-times slower than 
# the post-analyze selection of key d.

args=$1
dbnm=$2

# debug-flag
debug=

# verbose-flag
verbose=

# testtrace
ttrc=

# Each integer value added to x-column in any table is mod'ed by 'x'_scale.
a_scale=3
b_scale=7
c_scale=17
d_scale=29

# todo contains a list of tasks to perform with each task followed by a colon(:)
todo=""

# variables to contain 'baseline' stats for table t1
base_t1_ix_0=
base_t1_ix_1=
base_t1_ix_2=
base_t1_ix_3=

# variables to contain 'baseline' stats for table t2
base_t2_ix_0=
base_t2_ix_1=
base_t2_ix_2=

# variables to contain 'baseline' stats for table t3
base_t3_ix_0=
base_t3_ix_1=

TMPDIR=${TMPDIR:-/tmp}


# full usage menu with a use-case example
function full_usage
{
    echo "
Usage: ${0##*/} \"<args>\" <dbname>

You are interested in <args> while testing this.  The driver-flags in the <args>
variable are executed in-order.  Valid args are:

    Driver-flags:
    -a <cyc>        Driver-flag to add <cyc> cycles of records to tables.  
                    1-cycle==10353 records.
    -f <cnt>        Driver-flag to find <cnt> records from t1, t2, and t3.
    -n <tbl> <cmp>  Driver-flag to perform an 'analyze' on <tbl> sampling <cmp>
                    percent of the records.
    -N <cmp>        Driver-flag to analyze all tables sampling <cmp> percent
                    of the records.
    -o <tbl>-<cmp>-<cmptds>-<thrsh>
                    Driver-flag to analyze table <tbl> sampling <cmp> percent
                    of the records, using <cmptds> compression-threads, and a 
                    threshold of <thrsh> bytes.
    -O <cmp>-<tbtds>-<cmptds>-<thrsh>
                    Driver-flag to analyze the database sampling <cmp> percent
                    of the records, and using <tbtds> table-threads, <cmptds> 
                    compression threads, and a threshold of <thrsh> bytes.
    -s              Driver-flag to collect and store baseline stats from 
                    sqlite_stat1.
    -S <rng>        Driver-flag to compare current stats against the baseline.
                    Complain if one of the values is outside of <rng> %.
    -b <tbl>        Driver-flag to backout an analysis from <tbl>.
    -B              Driver-flag to backout an analysis from all tables.
    -e              Driver-flag to dump query-plan commands for a select to each 
                    table.

    Other-flags:
    -d              Enable debug trace.
    -t              Enable function-start trace.
    -v              Print verbose trace.
    -h              Print quick help-menu.
    -H              Print this menu.

A typical use-case might be to set args to '-a 10 -f 20 -n t1 25 -f 20 -v':
-
    1) Enable verbose trace (order doesn't matter for a non-driver)
    2) Add 10-cycles of records to each table (10*10353 records * 3 tables)
    3) Perform 20 random find-counts on each table
    4) Analyze table 't1' using a sample-size of 25% of the total records
    5) Perform 20 random find-counts on each table
"
}


# usage menu
function usage
{
    echo "
Usage: ${0##*/} \"<args>\" <dbname>

You are interested in <args> while testing this.  The driver-flags in the <args>
variable are executed in-order.  Valid args are:

    Driver-flags:
    -a <cyc>        Driver-flag to add <cyc> cycles of records to tables.  
                    1-cycle==10353 records.
    -f <cnt>        Driver-flag to find <cnt> records from t1, t2, and t3.
    -n <tbl>-<cmp>  Driver-flag to perform an 'analyze' on <tbl> sampling <cmp>
                    percent of the records.
    -N <cmp>        Driver-flag to analyze all tables sampling <cmp> percent
                    of the records.
    -o <tbl>-<cmp>-<cmptds>-<thrsh>
                    Driver-flag to analyze table <tbl> sampling <cmp> percent
                    of the records, using <cmptds> compression-threads, and a 
                    threshold of <thrsh> bytes.
    -O <cmp>-<tbtds>-<cmptds>-<thrsh>
                    Driver-flag to analyze the database sampling <cmp> percent
                    of the records, and using <tbtds> table-threads, <cmptds> 
                    compression threads, and a threshold of <thrsh> bytes.
    -s              Driver-flag to collect and store baseline stats from 
                    sqlite_stat1.
    -S <rng>        Driver-flag to compare current stats against the baseline.
                    Complain if one of the values is outside of <rng> %.
    -b <tbl>        Driver-flag to backout an analysis from <tbl>.
    -B              Driver-flag to backout an analysis from all tables.
    -e              Driver-flag to dump query-plan commands for a select to each 
                    table.

    Other-flags:
    -d              Enable debug trace.
    -t              Enable function-start trace.
    -v              Print verbose trace.
    -h              Print this menu.
    -H              Print extended help menu.
"
}

# archcode function
function myarch
{
    u=$(uname)
    a="<unknown>"
    [[ "$u" == "SunOS" ]]   && a="sundev1"
    [[ "$u" == "AIX" ]]     && a="ibm"
    [[ "$u" == "HP-UX" ]]   && a="hp"
    [[ "$u" == "Linux" ]]   && a="linux"

    echo $a
    return 0
}



# print the number of records in the table
function countrecs
{
    # print debug trace
    [[ "$debug" == "1" ]] && set -x

    # args
    typeset db=$1
    typeset tbl=$2

    # print enter-function
    [[ "$ttrc" == "1" ]] && echo "countrecs $1 $2" 2>&1

    x=$($CDB2SQL_EXE --tabs ${CDB2_OPTIONS} $db default "select count(*) from $tbl" 2>&1)

    echo $x
    return 0
}

# waitfull: wait until the table is filled & print messages so that we know
# it's still chugging along
function waitfull
{
     # print debug trace
    [[ "$debug" == "1" ]] && set -x

    # args
    typeset db=$1
    typeset tbl=$2
    typeset st=$3
    typeset max=$4
    typeset inc=$5

    # vars
    typeset lastp=$st
    typeset cnt=0

    echo "$tbl: n_recs = $st / target=$max"

    # print enter-function
    [[ "$ttrc" == "1" ]] && echo >&2 "waitfull $1 $2 $3 $4"

    while :; do

        cnt=$(countrecs $dbnm $tbl)

        while (( cnt > ( lastp + inc ) )) ; do

            lastp=$(( lastp + inc ))
            echo "> Added $(( lastp - st )) records to $tbl"

        done

        (( cnt >= max )) && break

        sleep $((inc / 200))

    done

    echo "> Added $(( max - st )) records to $tbl"
    return 0
}


# select counts(*) of random records
function countrand
{
    # print debug trace
    [[ "$debug" == "1" ]] && set -x

    # args
    typeset db=$1
    typeset tbl=$2
    typeset iter=$3

    # vars
    typeset ii=0
    typeset a
    typeset b
    typeset c
    typeset d

    # print enter-function
    [[ "$ttrc" == "1" ]] && echo >&2 "countrand $1 $2 $3"

    # This trace is useful
    echo >&2 "Counting $iter random records from $tbl"

    # pick values and run select in a loop
    while (( ii < iter )) ; do
        
        a=$(( RANDOM % a_scale ))
        b=$(( RANDOM % b_scale ))
        c=$(( RANDOM % c_scale ))
        d=$(( RANDOM % d_scale ))
        x=$( $CDB2SQL_EXE ${CDB2_OPTIONS} $db default "select count(*) from $tbl where a=$a and b=$b and c=$c and d=$d" 2>&1 )

        [[ "$verbose" == "1" ]] && echo >&2 "$x"

        let ii=ii+1

    done
    return 0
}


# Time a find of <cnt> records in a table
function time_countrand
{
    # print debug trace
    [[ "$debug" == "1" ]] && set -x

    # args
    typeset db=$1
    typeset tbl=$2
    typeset cnt=$3

    # timer vars
    typeset tot_time
    typeset st_time=$SECONDS

    [[ "$ttrc" == "1" ]] && echo >&2 "time_countrand $1 $2 $3"

    # count'em
    countrand $db $tbl $cnt
   
    # grab the time
    tot_time=$(( SECONDS - st_time ))
        
    echo "$tot_time"

    return 0
}


# Wrapper for explain
function explain_query
{
    # print debug trace
    [[ "$debug" == "1" ]] && set -x

    # args
    typeset db=$1
    typeset tbl=$2

    # run command
    $CDB2SQL_EXE ${CDB2_OPTIONS} $db default "explain select * from $tbl where a=1 and b=2 and c=3 and d=4"
}


# Benchmark selects
function findrecs_driver
{
    # print debug trace
    [[ "$debug" == "1" ]] && set -x

    # args
    typeset db=$1
    typeset cnt=$2

    # vars
    typeset t1_time
    typeset t2_time
    typeset t3_time

    # print enter-function
    [[ "$ttrc" == "1" ]] && echo >&2 "findrecs_driver $1 $2 $3"

    # run the tests
    t1_time=$(time_countrand $db t1 $cnt)
    t2_time=$(time_countrand $db t2 $cnt)
    t3_time=$(time_countrand $db t3 $cnt)

    # print totals
    tot_time=$(( t1_time + t2_time + t3_time ))

    # verbose-output
    if [[ "$verbose" == "1" ]]; then
        echo >&2 "t1_time = $t1_time seconds"
        echo >&2 "t2_time = $t2_time seconds"
        echo >&2 "t3_time = $t3_time seconds"
        echo >&2 "tot_time = $tot_time seconds"
    fi

    echo "$tot_time seconds"

    return 0
}



# Add alot of records for a test
function addrecs_driver
{
    # print debug trace
    [[ "$debug" == "1" ]] && set -x

    # args
    typeset db=$1
    typeset cyc=$2

    # counter
    typeset ii=0

    # the number of possible unique records
    typeset n_uniq_recs=$(( a_scale * b_scale * c_scale * d_scale ))

    # total number of records this function will add
    typeset tot_recs=$(( cyc * n_uniq_recs ))

    # grab the current total number of records
    typeset t1_cnt=$(countrecs $db t1)
    typeset t2_cnt=$(countrecs $db t2)
    typeset t3_cnt=$(countrecs $db t3)

    # the target is the current count + the number added
    t1_target=$(( t1_cnt + tot_recs ))
    t2_target=$(( t2_cnt + tot_recs ))
    t3_target=$(( t3_cnt + tot_recs ))

    # get add executable
    typeset arch=$(myarch)
    typeset exe=./analyze_add

    # print enter-function
    [[ "$ttrc" == "1" ]] && echo >&2 "addrecs_driver $1 $2"


    # Add (cyc * n_uniq_records) to tables t1, t2, and t3 in parallel
    $exe $dbnm $cyc &

    # wait for these to finish (and print a message for every 1000 records)
    waitfull $dbnm t1 $t1_cnt $t1_target 1000
    waitfull $dbnm t2 $t2_cnt $t2_target 1000
    waitfull $dbnm t3 $t3_cnt $t3_target 1000

    echo "done"
    return
}

# Function to retrieve a single stat
function get_sqlite_stat
{
    # print debug trace
    [[ "$debug" == "1" ]] && set -x

    # args
    typeset db=$1
    typeset tbl=$2
    typeset ix=$3

    #run
    $CDB2SQL_EXE --tabs ${CDB2_OPTIONS} $db default "select stat from sqlite_stat1 where tbl='$tbl' and idx like '\$${ix}_%'" 2>&1 
    return 0
}

# Retrieve baseline stats from sqlite_stat1
function baseline_stats
{
    # print debug trace
    [[ "$debug" == "1" ]] && set -x

    # args
    typeset db=$1

    # t1 stats
    base_t1_ix_0=$(get_sqlite_stat $db t1 A)
    base_t1_ix_1=$(get_sqlite_stat $db t1 B)
    base_t1_ix_2=$(get_sqlite_stat $db t1 C)
    base_t1_ix_3=$(get_sqlite_stat $db t1 D)

    # t2 stats
    base_t2_ix_0=$(get_sqlite_stat $db t2 AB)
    base_t2_ix_1=$(get_sqlite_stat $db t2 BC)
    base_t2_ix_2=$(get_sqlite_stat $db t2 CD)

    # t3 stats
    base_t3_ix_0=$(get_sqlite_stat $db t3 ABC)
    base_t3_ix_1=$(get_sqlite_stat $db t3 BCD)

    # print t1 stats
    echo "base_t1_ix_0='$base_t1_ix_0'"
    echo "base_t1_ix_1='$base_t1_ix_1'"
    echo "base_t1_ix_2='$base_t1_ix_2'"
    echo "base_t1_ix_3='$base_t1_ix_3'"

    # print t2 stats
    echo "base_t2_ix_0='$base_t2_ix_0'"
    echo "base_t2_ix_1='$base_t2_ix_1'"
    echo "base_t2_ix_2='$base_t2_ix_2'"

    # print t3 stats
    echo "base_t3_ix_0='$base_t3_ix_0'"
    echo "base_t3_ix_1='$base_t3_ix_1'"

    return 0
}

# Get the number corresponding to this range
function get_range
{
    # print debug trace
    [[ "$debug" == "1" ]] && set -x

    # args
    typeset x=$1
    typeset pct=$2
    typeset int
    typeset rtn

    # calculate
    int=$(printf "%s\n" ".$pct * $x" | bc)
    rtn=${int%\.*}

    echo $rtn
    return 0
}

# Print a bad message if the arguments aren't within range
function compare_range
{
    # print debug trace
    [[ "$debug" == "1" ]] && set -x

    # args
    typeset x=$1
    typeset y=$2
    typeset pct=$3

    # lowest 
    typeset rng=$(get_range $x $pct)
    typeset low=$(( x - rng ))
    typeset high=$(( x + rng ))
    typeset bad=0

    # check rng
    (( y < low )) && bad=1
    (( y > high )) && bad=1

    # print
    [[ "1" == "$bad" ]] && echo "value out of range: base=$x val=$y rng=$rng"

    return 0
}

# Print a bad message if the arguments aren't identical
function compare_exact
{
    # print debug trace
    [[ "$debug" == "1" ]] && set -x

    # args
    typeset x=$1
    typeset y=$2

    # run
    [[ "$x" != "$y" ]] && echo "$x != $y"

    return 0
}

# Count the number of columns in this variable
function count_columns
{
    # print debug trace
    [[ "$debug" == "1" ]] && set -x

    # args
    typeset x=$1

    # vars
    typeset numc=0

    # count the number of columns
    for y in $x ; do
        let numc=numc+1
    done

    echo $numc
    return 0
} 

# Compare columns
function compare_columns
{
    # print debug trace
    [[ "$debug" == "1" ]] && set -x

    # args
    typeset col1="$1"
    typeset col2="$2"
    typeset rng=$3

    # vars
    typeset c1cnt=0
    typeset c2cnt=0

    # print a message immediately if either are missing
    [[ -z "$col1" ]] && echo "Empty value for arg1"
    [[ -z "$col2" ]] && echo "Empty value for arg2"

    # make sure each has the same number of columns
    c1cnt=$(count_columns "$col1")
    c2cnt=$(count_columns "$col2")

    # print a bad message on mismatch
    [[ "$c1cnt" != "$c2cnt" ]] && echo "Different column counts!"


    # loop through each column
    while [[ -n "$col1" ]] ; do

        x="${col1%% *}"
        y="${col2%% *}"

        compare_range $x $y $rng

        [[ "$col1" == "${col1#* }" ]] && col1=
        [[ "$col2" == "${col2#* }" ]] && col2=

        col1=${col1#* }
        col2=${col2#* }

    done

    return 0
}

# Compare these stats
function compare_index
{
    # print debug trace
    [[ "$debug" == "1" ]] && set -x
   
    # args
    typeset db=$1
    typeset tbl=$2
    typeset ix=$3
    typeset bval=$4
    typeset rng=$5

    # print message 
    echo "Evaluating current $ix against base"

    # run
    curval=$(get_sqlite_stat $db $tbl $ix)

    # print verbose
    if [[ "$verbose" == "1" ]]; then
        echo >&2 "idx $ix baseval='$bval' curval='$curval' rng=$rng"
    fi

    # compare against base
    compare_columns "$bval" "$curval" $rng

    return 0
}

# Compare the current stats against the baseline
function compare_stats
{
    # print debug trace
    [[ "$debug" == "1" ]] && set -x

    # args
    typeset db=$1
    typeset rng=$2

    # compare t1
    compare_index $db t1 A "$base_t1_ix_0" $rng
    compare_index $db t1 B "$base_t1_ix_1" $rng
    compare_index $db t1 C "$base_t1_ix_2" $rng
    compare_index $db t1 D "$base_t1_ix_3" $rng

    # compare t2
    compare_index $db t2 AB "$base_t2_ix_0" $rng
    compare_index $db t2 BC "$base_t2_ix_1" $rng
    compare_index $db t2 CD "$base_t2_ix_2" $rng

    # compare t3
    compare_index $db t3 ABC "$base_t3_ix_0" $rng
    compare_index $db t3 BCD "$base_t3_ix_1" $rng
}

# Analyze setting all available options
function analyze_all_opts_driver
{
    # print debug trace
    [[ "$debug" == "1" ]] && set -x

    # args
    typeset db=$1
    typeset cmp=$2
    typeset tbt=$3
    typeset cmt=$4
    typeset thr=$5

    # run 
    #was comdb2sc -c $cmp -t $tbt -T $cmt -H $thr $db analyze
    #-T compression threads
    #-t concurrent analyze threads 
#cdb2sql --tabs ${CDB2_OPTIONS} $db default "ANALYZE $cm OPTIONS THREADS $tbt, SUMMARIZE $cmt"
    $CDB2SQL_EXE --tabs ${CDB2_OPTIONS} $db default "exec procedure sys.cmd.analyze(\"\",\"$cmp\")" | grep -v "Analyze completed table"
    return $?
}


# Analyze a table setting all available options
function analyze_opts_driver
{
    # print debug trace
    [[ "$debug" == "1" ]] && set -x

    # args
    typeset db=$1
    typeset tbl=$2
    typeset cmp=$3
    typeset cmt=$4
    typeset thr=$5

    # run 
    #was comdb2sc -c $cmp -T $cmt -H $thr $db analyze $tbl
    #sql:  'analyze 3 OPTIONS threads 3, SUMMARIZE 4'
    $CDB2SQL_EXE --tabs ${CDB2_OPTIONS} $db default "exec procedure sys.cmd.analyze(\"$tbl\",\"$cmp\")" | grep -v "Analyze completed table"
    return $?
}


# Analyze all tables
function analyze_all_driver
{
    # print debug trace
    [[ "$debug" == "1" ]] && set -x

    # args
    typeset db=$1
    typeset cmp=$2

    # run 
    #comdb2sc -c $cmp $db analyze
    $CDB2SQL_EXE --tabs ${CDB2_OPTIONS} $db default "exec procedure sys.cmd.analyze(\"\",\"$cmp\")" | grep -v "Analyze completed table"
    return $?
}


# Analyze a table
function analyze_driver
{
    # print debug trace
    [[ "$debug" == "1" ]] && set -x

    # args
    typeset db=$1
    typeset tbl=$2
    typeset cmp=$3

    # run 
    #comdb2sc -c $cmp $db analyze $tbl
    $CDB2SQL_EXE --tabs ${CDB2_OPTIONS} $db default "exec procedure sys.cmd.analyze(\"$tbl\",\"$cmp\")" | grep -v "Analyze completed table"
    return $?
}


# Backout analysis for a table
function backout_driver
{
    # print debug trace
    [[ "$debug" == "1" ]] && set -x

    # args
    typeset db=$1
    typeset tbl=$2

    # run
    #comdb2sc -b $db analyze $tbl
    $CDB2SQL_EXE --tabs ${CDB2_OPTIONS} $db default "exec procedure sys.cmd.send(\"analyze backout $tbl\")" >> backout.res
    return $?
}


# Backout all for a table
function backout_all_driver
{
    # print debug trace
    [[ "$debug" == "1" ]] && set -x

    # args
    typeset db=$1

    # run
    #comdb2sc -b $db analyze
    $CDB2SQL_EXE --tabs ${CDB2_OPTIONS} $db default "exec procedure sys.cmd.send(\"analyze backout\")" >> backout.res
    return $?
}




# Sqlexplain <cnt> queries
function explain_driver
{
    # print debug trace
    [[ "$debug" == "1" ]] && set -x

    # args
    typeset db=$1
    
    # explain
    explain_query $db t1
    explain_query $db t2
    explain_query $db t3
}


# Set the compression-threshold on all machines
function threshold_set
{
    # print debug trace
    [[ "$debug" == "1" ]] && set -x

    # args
    typeset db=$1
    typeset rlst=$2
    typeset val=$3

}


# grab options from args
set - $(getopt "a:f:n:N:o:O:c:b:BesS:XhHdvt" $args)
for arg in "$@" ; do
    case "$arg" in

# driver-flags
        -a)
            todo="${todo}addrecs-$2:"
            shift 2
            ;;

        -f)
            todo="${todo}findrecs-$2:"
            shift 2
            ;;

        -n)
            tbl=${2%-*}
            samp=${2#*-}
            todo="${todo}analyze-$tbl-$samp:"
            shift 2
            ;;

        -N)
            todo="${todo}allanalyze-$2:"
            shift 2
            ;;

        -o)
            tbl=${2%%-*}        ; cur=${2#*-}
            cmp=${cur%%-*}      ; cur=${cur#*-}
            ctd=${cur%%-*}      ; cur=${cur#*-}
            trs=${cur%%-*}      ; cur=${cur#*-}
            todo="${todo}optanalyze-$tbl-$cmp-$ctd-$trs:"
            shift 2
            ;;


        -O)
            cmp=${2%%-*}        ; cur=${2#*-}
            ttd=${cur%%-*}      ; cur=${cur#*-}
            ctd=${cur%%-*}      ; cur=${cur#*-}
            trs=${cur%%-*}      ; cur=${cur#*-}

            todo="${todo}optallanalyze-$cmp-$ttd-$ctd-$trs:"
            shift 2
            ;;

        -B)
            todo="${todo}bckoall:"
            shift
            ;;


        -b)
            todo="${todo}backout-$2:"
            shift 2
            ;;

        -X)
            todo="${todo}bkostat2:"
            shift
            ;;

        -e)
            todo="${todo}explain:"
            shift
            ;;

        -s)
            todo="${todo}baseline:"
            shift
            ;;

        -S)
            rng=$2
            todo="${todo}compstats-$rng:"
            shift 2
            ;;

# other-flags
        -d)
            debug=1
            set -x
            echo "Enabled debug trace"
            shift
            ;;

        -t)
            ttrc=1
            echo "Enabled function start trace"
            shift
            ;;
        -v)
            verbose=1
            echo "Enabled verbose trace"
            shift
            ;;
        -h)
            usage
            exit
            ;;

        -H)
            full_usage
            exit
            ;;

        --)
            shift
            break
            ;;

    esac

done


# make sure we have something todo!
if [[ -z "$todo" ]]; then
    echo "${0##*/}: nothing to do!"
    usage
    exit
fi


# 'todo' contains a list of functions to run
while [[ -n "$todo" ]]; do

    action=${todo%%:*}
    todo=${todo#*:}

    case "$action" in

        addrecs*)
            cyc=${action#*-}
            echo "Adding $cyc record cycles"
            addrecs_driver $dbnm $cyc
            ;;

        findrecs*)
            cnt=${action#*-}
            echo "Count $cnt random records from each table"
            findrecs_driver $dbnm $cnt
            ;;
            
        analyze*)
            tmp=${action#analyze-}
            tbl=${tmp%-*}
            cmp=${tmp#*-}
            echo "Analyze table '$tbl' sampling $cmp % of the records"
            analyze_driver $dbnm $tbl $cmp
            ;;

        allanalyze*)
            cmp=${action#*-}
            echo "Analyze all tables sampling $cmp % of the records"
            analyze_all_driver $dbnm $cmp
            ;;

        optanalyze*)
            tmp=${action#optanalyze-}
            tbl=${tmp%%-*}      ; tmp=${tmp#*-}
            cmp=${tmp%%-*}      ; tmp=${tmp#*-}
            cmt=${tmp%%-*}      ; tmp=${tmp#*-}
            thr=${tmp%%-*}      ; tmp=${tmp#*-}
            echo "Analyze table '$tbl' sampling $cmp % of the records"
            echo "Use $cmt table-threads and a sample-threshold of $thr"
            analyze_opts_driver $dbnm $tbl $cmp $cmt $thr 
            ;;

        optallanalyze*)
            tmp=${action#optallanalyze-}
            cmp=${tmp%%-*}      ; tmp=${tmp#*-}
            tbt=${tmp%%-*}      ; tmp=${tmp#*-}
            cmt=${tmp%%-*}      ; tmp=${tmp#*-}
            thr=${tmp%%-*}      ; tmp=${tmp#*-}
            echo "Analyze all tables sampling $cmp % of the records"
            echo "Use $tbt table-threads, $cmt compression-threads"
            echo "And a sample-threshold of $thr"
            analyze_all_opts_driver $dbnm $cmp $tbt $cmt $thr 
            ;;

        baseline*)
            echo "Collect baseline stats from sqlite_stat1"
            baseline_stats $dbnm
            ;;

        compstats*)
            rng=${action#compstats-}
            echo "Compare current stats against baseline"
            echo "Complain if a current-stat is not with $rng % of the baseline"
            compare_stats $dbnm $rng
            ;;

        backout*)
            tbl=${action#*-}
            echo "Backout analysis of table $tbl"
            backout_driver $dbnm $tbl
            ;;

        bckoall*)
            echo "Backout analysis of all tables"
            backout_all_driver $dbnm
            ;;

        explain*)
            echo "Explain query-plans from selects to each table"
            explain_driver $dbnm
            ;;

    esac

done

