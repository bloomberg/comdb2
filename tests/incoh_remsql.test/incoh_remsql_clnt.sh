#!/usr/bin/env bash

client=$1
iterations=$2

cmd="cdb2sql -s --cdb2cfg ${CDB2_CONFIG} ${DBNAME} default"
cmdt="cdb2sql --tabs --cdb2cfg ${CDB2_CONFIG} ${DBNAME} default"
rowfile="tmp.${client}"
clntlog="clnt.log.${client}"
failurelog="failure.${client}"

num=$(( ( RANDOM % 10 ) + 1 ))


echo "${client} runs $cmd num ${num}" > ${clntlog}
echo ${cmdt} "select n from t1 where random() % 1000 = 1 limit $num" >> ${clntlog}
${cmdt} "select n from t1 where random() % 1000 = 1 limit $num" > ${rowfile} 2> tmp.${failurelog}
if (( $? != 0 )) ; then
    exit 1
fi 
err=`cat tmp.${failurelog}`
if [[ ! -z "${err}" ]] ; then 
    echo "err" > ${failurelog}
fi

echo "Selected `wc -l  ${rowfile}` rows" >> ${clntlog}
rows=`cat ${rowfile}`
echo ${rows} >> ${clntlog}
for num in ${rows}; do
    if [[ -z "${ns}" ]] ; then
        ns="select $num as k"
    else
        ns="$ns union all select $num as k"
    fi
    echo "Filter ${num} -> ${ns}" >> ${clntlog}
done
echo "${ns}" >> ${clntlog}

q="select s.k, t1.n, t2.l1, t2.l2 from (${ns}) s left join t1 on s.k = t1.n left join ${SECONDARY_DBNAME}.t2 on t1.n = t2.n where ifnull(t2.s, 0) = 0"
echo "$q" >> ${clntlog}

for ((it=1; it<=${iterations}; it++)); do
    echo ${cmd} "$q"
    ${cmd} "$q" >clnt.log.${client}.${it} 2>tmp.${failurelog}.${it}
    err=`cat tmp.${failurelog}.${it}`
    if [[ ! -z "${err}" ]] ; then 
        echo "err" > ${failurelog}.${it}
    fi
done
