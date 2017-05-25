#!/bin/bash
set -e

while [[ $1 = -* ]]; do
    # TODO: handle options, if any, here
    shift
done

pmux -l

host=$(hostname -i)

if [[ -f /opt/bb/etc/cdb2/config/comdb2.d/hostname.lrl ]]; then
    for dbname in /opt/bb/var/cdb2/*; do 
        dbname=${dbname##*/}
        comdb2 $dbname > /opt/bb/var/log/$dbname.out 2>&1 &
    done
else 
    echo "hostname $host" > /opt/bb/etc/cdb2/config/comdb2.d/hostname.lrl

    for dbname in $*; do
        comdb2 --create $dbname >/dev/null 2>&1 &
    done
    wait

    for dbname in $*; do
        comdb2 $dbname > /opt/bb/var/log/$dbname.out 2>&1 &
    done
fi

(
echo "#!/bin/bash"
echo
echo echo comdb2_config:allow_pmux_route:true
for db in $*; do
    echo "echo $db:$host"
done 
echo echo comdb2_config:default_type:docker
) > /bin/cdb2config

chmod +x /bin/cdb2config

wait
