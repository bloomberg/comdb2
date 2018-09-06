#!/usr/bin/env bash
set -x

# overflow blobtest

# arguments

dbnm="$1"
rmt="$2"
dbex="$3"
ddir="$4"
lmch="$5"

# usage- mostly for testing the test
function usage
{
    echo "Usage: ${0##*/} <dbnm> <rmtmch> <exe> <wrkdir> <linuxmachs>"
    exit 1
}

# derive the full-path of the linux executable
function newexe
{
    typeset exe=$1
    typeset tach=$2
    typeset nach=$3

    s=${exe##*\.${tach}}
    p=${exe%${tach}*}
    n=${p}${nach}${s}

    if [[ "$s" == "$exe" ]] ; then 
        echo "$exe"
    else
        echo "$n"
    fi

    return 0
}

# delete btree on remote machine
function delbtrees
{
    typeset rmch=$1
    typeset rdir=$2

    # remove the remote btree files
    rsh $rmch "rm -f $rdir/*datas[01]"
    rsh $rmch "rm -f $rdir/*blobs[01]"
    rsh $rmch "rm -f $rdir/*dta"
    rsh $rmch "rm -f $rdir/*index"

    return 0
}

# verify btree on remote machine
function verifybtree
{
    typeset rmch=$1
    typeset rdir=$2
    typeset tool=$3

    # verify btrees- this prints error messages on error
    rsh $rmch "$tool $rdir/*datas[01]"
    rsh $rmch "$tool $rdir/*blobs[01]"
    rsh $rmch "$tool $rdir/*dta"
    rsh $rmch "$tool $rdir/*index"

    return 0
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

# archcode prefix
function archprefix
{
    u=$(uname)
    a="<unknown>"
    [[ "$u" == "SunOS" ]]   && a="sundev"
    [[ "$u" == "AIX" ]]     && a="ibm"
    [[ "$u" == "HP-UX" ]]   && a="hp"
    [[ "$u" == "Linux" ]]   && a="linx"

    echo $a
    return 0
}


# return a 1 if this is a dev machine, 0 otherwise
function isdev
{
    typeset pfx=$(archprefix)
    typeset host=$(hostname)

    if [[ "$host" = "$pfx"* ]]; then
        echo yes
    else
        echo no
    fi

    return 0
}


# return the architecture of a given dev-node
function nodearch
{
    typeset target=$1

    [[ "$target" == 0 ]] && target=$(mycpu)

    m=$(bbhost $target | awk '{print $3}')
    a="<unknown>"

    [[ "$m" = "sparc"* ]]   && a="sundev1"
    [[ "$m" = "ibm"* ]]     && a="ibm"
    [[ "$m" = "hp"* ]]      && a="hp"
    [[ "$m" = "linx"* ]]    && a="linux"
    [[ "$m" = "linux"* ]]   && a="linux"

    echo $a
    return 0
}

# return the architecture of a given dev-node
function nodearch64
{
    typeset target=$1

    [[ "$target" == 0 ]] && target=$(mycpu)

    m=$(bbhost $target | awk '{print $3}')
    a="<unknown>"

    [[ "$m" = "sparc"* ]]   && a="sundev1_64"
    [[ "$m" = "ibm"* ]]     && a="ibm_64"
    [[ "$m" = "hp"* ]]      && a="hp_64"
    [[ "$m" = "linx"* ]]    && a="linux_64"
    [[ "$m" = "linux"* ]]   && a="linux_64"

    echo $a
    return 0
}

# return a machine
function getmachine
{
    typeset list
    typeset length
    typeset count=0
    typeset mach
    typeset node
    typeset modnum
    typeset target=""

    # find appropriate candidates
    list=$(echo $lmch | tr ':' ' ')

    # length of candidates
    length=${#list[*]}

    # starting point
    modnum=$(($(mycpu) % $length))

    # ask rtcpu where to go
    while [[ $count -lt $length ]]; do 

        mach=${list[$modnum]}
        node=$(bbhost -n $mach)

        if iscpuup.sh $node ; then
            target=$mach
            break
        fi
        let count=count+1
        modnum=$((($modnum + 1) % $length))

    done

    echo $target

}

# print success message and exit
function endtest
{
    echo "Done!"
    exit 0
}

# scrub the arguments
[[ -z "$dbnm" ]] && usage
[[ -z "$rmt" ]] && usage
[[ -z "$dbex" ]] && usage
[[ -z "$ddir" ]] && usage

# Find a good target machine
tmach=$(getmachine)

# grab the master
rmt=${rmt%%:*}

# copy the database to a linux machine
if [[ "$rmt" == 0 || "$rmt" == $(mycpu) ]]; then
    isrmt=0
    mch=$(hostname)
else
    isrmt=1
    mch=$(bbhost -m $rmt)
fi

# grab the target arch
tach=$(nodearch $rmt)

# grab my arch
arch=$(myarch)

# executable which adds overflow blobs
exe=${TESTSBUILDDIR}/overflow_blobtest

# run executable with default options
$exe -d $dbnm 

# punt successfully if this is linux
[[ "$tach" == "linux" ]] && endtest

# destdb
dest=${TMPDIR}/$dbnm

# the remote lrl-file is ddir/${dbname}.lrl:  copy the database to linux.
rsh $tmach "/bb/bin/copycomdb2 $mch:$ddir/$dbnm.lrl $dest $dest"

# name of the remote comdb2
cd2=$(newexe $dbex $tach linux)
cd2_64=$(newexe $dbex $tach linux_64)

# delete the btrees on the remote side
delbtrees $tmach $dest

# restore the database on the remote side
rsh $tmach "$cd2 $dbnm -lrl $dest/$dbnm.lrl -fullrecovery >/dev/null 2>&1"
rsh $tmach "$cd2_64 $dbnm -lrl $dest/$dbnm.lrl -fullrecovery >/dev/null 2>&1"

# verify script
vfy=/bb/bin/db_verify

# verify btrees
verifybtree $tmach $dest $vfy

# end!
endtest

