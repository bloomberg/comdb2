#!/usr/bin/env bash
# Fails a test that set COMDB2_ASAN=1 when the ASan comdb2 is not built or not
# on the cluster, in the '!<test>: failed' form that test.log consumers
# count.

asandir=${COMDB2_ASAN_BUILDDIR}
cache=${asandir}/CMakeCache.txt
cache_val() { grep -a "^$1:" "$cache" 2>/dev/null | head -1 | cut -d= -f2-; }

configure="cmake -S ${SRCHOME} -B ${asandir} -DCOMDB2_ASAN=1"
build="cmake --build ${asandir} --target comdb2"

comdb2=${asandir}/db/comdb2

if [[ -f "$comdb2" ]]; then
    reason="ASan comdb2 was not copied to the cluster"
    hint="See ${TESTDIR}/copy_asan_to_cluster.log"
elif [[ ! -f "$cache" ]]; then
    reason="requires ASan comdb2, ${comdb2} not found"
    hint="Configure and build the ASan comdb2:
    ${configure}
    ${build}"
else
    reason="requires ASan comdb2, ${comdb2} not found"
    case "$(cache_val COMDB2_ASAN)" in
        1|ON|on|YES|yes|TRUE|true|Y|y)
            hint="${asandir} is configured for ASan but not built:
    ${build}" ;;
        *)
            hint="${asandir} is not configured with -DCOMDB2_ASAN=1:
    ${configure}
    ${build}" ;;
    esac
fi

msg="!${TESTCASE}: failed (${reason})"
echo "$msg" >> "${TESTDIR}/test.log"
cat >&2 <<EOF
$msg
${hint}
To run on the regular comdb2 instead: make ${TESTCASE} COMDB2_ASAN=0
EOF
exit 1
