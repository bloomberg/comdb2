#!/bin/bash
cd /comdb2/tests
export CLUSTER="$(cat /common/cluster)"
make master_load_commit SKIPSSL=1
rc=$?
cp -r /comdb2/tests/test_*/logs/* /dedicated/ 2>/dev/null
exit $rc
