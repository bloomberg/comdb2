SQLITE3DB=${COMDB2_ROOT}/var/lib/cdb2/pmux.sqlite
PMUXDIR=${COMDB2_ROOT}/var/lib/cdb2/pmuxdb
BINDIR=../..
PMUX=${BINDIR}/pmux
CDB2SQL=${BINDIR}/cdb2sql
COMDB2=${BINDIR}/comdb2
SVC="comdb2/replication/pmuxdb"
: ${PMUXPORT:=5101} #default port if not set in env


#START LOCAL PMUX
rm ${SQLITE3DB}
${PMUX} -l -p ${PMUXPORT}



#START PMUXDB
rm -f pmuxdb.trap
kill $(psef -q pmuxdb | awk '{print $2}')
${COMDB2} --create --dir ${PMUXDIR} pmuxdb 
${COMDB2} --lrl ${PMUXDIR}/pmuxdb.lrl pmuxdb > ${PMUXDIR}/act.log 2>&1 &
while [[ ! -f pmuxdb.trap ]] ; do
	echo waiting
	sleep 0.5
done
rm pmuxdb.trap



#BOOTSTRAP
echo exit | nc localhost ${PMUXPORT}
PORT=$(sqlite3 ${SQLITE3DB} "select port from pmux where svc='${SVC}';")
CONN="@${HOSTNAME}:port=${PORT}"
${CDB2SQL} -s pmuxdb ${CONN} - <<EOF
CREATE TABLE IF NOT EXISTS pmux {schema {
  cstring host[128]
  cstring svc[256]
  u_short port
}
keys {
  "uniq" = host + svc + port
}}\$\$
DELETE FROM pmux where host="${HOSTNAME}" and svc="${SVC}"
INSERT INTO pmux(host,svc,port) VALUES("${HOSTNAME}", "${SVC}", "${PORT}")
SELECT * from pmux;
EOF



#START PMUX TO USE PMUXDB
exec ${PMUX} -c ${CONN}
