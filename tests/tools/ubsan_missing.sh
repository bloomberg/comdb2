#!/usr/bin/env bash
# Fails a test that set COMDB2_UBSAN=1 when the UBSan comdb2 is not built or
# not on the cluster, in the '!<test>: failed' form that test.log consumers
# count.

ubsandir=${COMDB2_UBSAN_BUILDDIR}
cache=${ubsandir}/CMakeCache.txt
cache_val() { grep -a "^$1:" "$cache" 2>/dev/null | head -1 | cut -d= -f2-; }

configure="cmake -S ${SRCHOME} -B ${ubsandir} -DCOMDB2_UBSAN=1"
build="cmake --build ${ubsandir} --target comdb2"

comdb2=${ubsandir}/db/comdb2

if [[ -f "$comdb2" ]]; then
    reason="UBSan comdb2 was not copied to the cluster"
    hint="See ${TESTDIR}/copy_ubsan_to_cluster.log"
elif [[ ! -f "$cache" ]]; then
    reason="requires UBSan comdb2, ${comdb2} not found"
    hint="Configure and build the UBSan comdb2:
    ${configure}
    ${build}"
else
    reason="requires UBSan comdb2, ${comdb2} not found"
    case "$(cache_val COMDB2_UBSAN)" in
        1|ON|on|YES|yes|TRUE|true|Y|y)
            hint="${ubsandir} is configured for UBSan but not built:
    ${build}" ;;
        *)
            hint="${ubsandir} is not configured with -DCOMDB2_UBSAN=1:
    ${configure}
    ${build}" ;;
    esac
fi

msg="!${TESTCASE}: failed (${reason})"
echo "$msg" >> "${TESTDIR}/test.log"
cat >&2 <<MSG
$msg
${hint}
To run on the regular comdb2 instead: make ${TESTCASE} COMDB2_UBSAN=0
MSG
exit 1
