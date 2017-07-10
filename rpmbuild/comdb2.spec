Name:           comdb2
Version:        VVEERRSSIIOONN
Release:        1%{?dist}
Summary:        Comdb2 RDBMS

License:        Various
URL:            http://github.com/bloomberg/comdb2
Source0:        comdb2-VVEERRSSIIOONN.tar.gz

BuildRequires:  gcc gcc-c++ protobuf-c libunwind libunwind-devel protobuf-c-devel byacc flex openssl openssl-devel openssl-libs readline readline-devel sqlite sqlite-devel libuuid libuuid-devel zlib-devel zlib lz4-devel gawk tcl
Requires:       protobuf-c libunwind openssl openssl-libs readline sqlite libuuid zlib lz4

%description
Comdb2 is a distributed relational database.

%prep
%setup -q


%build
make %{?_smp_mflags}


%install
rm -rf $RPM_BUILD_ROOT
%make_install


%files
/opt/bb/bin/comdb2
/opt/bb/bin/cdb2sql
/opt/bb/bin/cdb2sockpool
/opt/bb/bin/copycomdb2
/opt/bb/bin/pmux
/opt/bb/bin/comdb2ar
/opt/bb/bin/cdb2_sqlreplay
/opt/bb/etc/cdb2/config/comdb2.d
/opt/bb/include
/opt/bb/include/cdb2api.h
/opt/bb/lib/libcdb2api.a
/opt/bb/lib/libcdb2api.so
/opt/bb/lib/libcdb2protobuf.a
/opt/bb/lib/libcdb2protobuf.so
/opt/bb/lib/systemd/system/pmux.service
/opt/bb/lib/systemd/system/cdb2sockpool.service
/opt/bb/lib/systemd/system/supervisor_cdb2.service
/opt/bb/usr/local/lib/pkgconfig/cdb2api.pc
/opt/bb/bin/comdb2admin
/opt/bb/etc/supervisord_cdb2.conf
/opt/bb/bin/comdb2dumpcsc

%doc

%post
adduser -m --system --shell /bin/bash comdb2 2> /tmp/$$.err
if [[ $? -ne 0 && $? -ne 9 ]]; then
   cat /tmp/$$.err >&2
   exit 1
fi
mkdir -p /opt/bb/var/run /opt/bb/var/cdb2/ /opt/bb/etc/cdb2 /opt/bb/var/log/cdb2 /opt/bb/var/lib/cdb2 /opt/bb/etc/cdb2/config/comdb2.d/ /opt/bb/var/log/ /opt/bb/var /opt/bb/var/log /opt/bb/var/log/cdb2_supervisor/ /opt/bb/etc/cdb2_supervisor/conf.d/
chmod 777 /opt/bb/var/run
chown comdb2:comdb2 /opt/bb/var/cdb2/ /opt/bb/etc/cdb2 /opt/bb/var/log/cdb2 /opt/bb/var/lib/cdb2 /opt/bb/etc/cdb2/config/comdb2.d/ /opt/bb/var/log/ /opt/bb/var/log/cdb2_supervisor/ /opt/bb/etc/cdb2_supervisor/conf.d/
chmod 770 /opt/bb/var/cdb2/ /opt/bb/etc/cdb2 /opt/bb/var/log/cdb2 /opt/bb/var/lib/cdb2 /opt/bb/etc/cdb2/config/comdb2.d/ /opt/bb/var/log/ /opt/bb/var/log/cdb2_supervisor/ /opt/bb/etc/cdb2_supervisor/conf.d/
chmod 755 /opt/bb/var /opt/bb/var/log
echo 'PATH=$PATH:/opt/bb/bin' >> /home/comdb2/.bashrc
chmod +x /home/comdb2/.bashrc
chown comdb2:comdb2 /home/comdb2/.bashrc
ln /opt/bb/bin/comdb2 /opt/bb/bin/cdb2_dump
ln /opt/bb/bin/comdb2 /opt/bb/bin/cdb2_printlog
ln /opt/bb/bin/comdb2 /opt/bb/bin/cdb2_stat
ln /opt/bb/bin/comdb2 /opt/bb/bin/cdb2_verify

cp /opt/bb/usr/local/lib/pkgconfig/cdb2api.pc /usr/local/lib/pkgconfig/cdb2api.pc
cp /opt/bb/lib/systemd/system/pmux.service /lib/systemd/system/
cp /opt/bb/lib/systemd/system/cdb2sockpool.service /lib/systemd/system/
cp /opt/bb/lib/systemd/system/supervisor_cdb2.service /lib/systemd/system/

systemctl daemon-reload
if [ ! -e /.dockerenv ]; then

    systemctl stop pmux
    systemctl start pmux
    systemctl enable supervisor_cdb2
    systemctl start supervisor_cdb2
    set +e
    rpm -qa | grep -q "^supervisor"
    rc=$?
    set -e
    if [ $rc -eq 0 ]; then
        systemctl enable cdb2sockpool
        systemctl start cdb2sockpool
    fi
fi

%changelog
