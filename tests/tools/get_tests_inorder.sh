#!/usr/bin/env bash
# Get list of tests in order from longest to shortest duration

SKIPLONGRUNNING=0
SHOWTIMEOUT=
LONGESTFIRST=1
ONLYGETSTRESSTESTS=0

while true ; do
  case "$1" in
  --skip-long-running) SKIPLONGRUNNING=1; shift;;
  --show-timeout) SHOWTIMEOUT=1; shift;;
  --shortest-first) LONGESTFIRST=0; shift;;
  --only-get-stress-tests) ONLYGETSTRESSTESTS=1; shift;;
  * ) break;;
  esac
done

DEFAULT_TIMEOUT=5  # default timeout for the tests is 5 minutes

shopt -s nullglob
for f in *.test/Makefile ; do
    if { [[ ${ONLYGETSTRESSTESTS} -eq 1 ]] && ! grep -q "IS_STRESS_TEST=1" "$f"; } || \
       { [[ ${ONLYGETSTRESSTESTS} -ne 1 ]] && grep -q "IS_STRESS_TEST=1" "$f"; }; then
        continue
    fi

    testdirname=$(basename "$(dirname "${f}")")
    timeout=$(grep "TEST_TIMEOUT=" "${f}" | sed 's#export TEST_TIMEOUT=##; s#/Makefile:# #; s#m$##')
    if (( timeout == 0 )) ; then
        timeout=${DEFAULT_TIMEOUT}
    fi
    times="${times}${testdirname} ${timeout}\n"

    testname=$(basename "${testdirname}" .test)
    for testopts in "${testdirname}"/*.testopts ; do
        gentestdirname="${testname}_$(basename "${testopts}" .testopts)_generated.test"
        times="${times}${gentestdirname} ${timeout}\n"
    done
done
shopt -u nullglob

IFS=$'\n'; 

F=${SHOWTIMEOUT:+,\$2}
if [ "${LONGESTFIRST}" -eq 1 ] ; then
    SORTORDER="r"
fi

echo -e ${times} | sort -k2 -t' ' -n${SORTORDER} | awk '{ if(!'${SKIPLONGRUNNING}' || $2 < 120) { print $1'${F}' } }'
