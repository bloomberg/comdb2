# runstepper
function runstepper
{
    # print debug trace
    [[ "$debug" == "1" ]] && set -x

    # args
    typeset db=$1
    typeset tcs=$2
    typeset log=$3
    typeset dolog=$4

    # teststepper
    typeset stp=${TESTSBUILDDIR}/stepper

    [[ "$dolog" == "1" ]] &&  echo "> $stp $db $tcs > $log 3>&1"

    # execute
    $stp $db $tcs > $log 2>&1
}

