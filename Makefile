PACKAGE=comdb2
VERSION?=$(shell grep ^comdb2 deb/changelog  | sed 's/^comdb2 .//; s/-.*//g')
include main.mk
include libs.mk

export SRCHOME=.
export DESTDIR
export PREFIX
BASEDIR:=$(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))

# Common CFLAGS
CPPFLAGS+=-I$(SRCHOME)/dlmalloc $(OPTBBINCLUDE)
#CFLAGS+=$(OPT_CFLAGS)

# Variables that will be modified by included files
ARS:=
OBJS:=
GENC:=
GENH:=
GENMISC:=
TASKS:=

# Humble start
all:

# Rules for common low level libraries
include common.mk
include mem.mk
include sqlite/sqlite_common.defines

modules:=net comdb2rle cdb2api csc2 schemachange berkdb sqlite bdb	\
lua db tools sockpool
include $(addsuffix /module.mk,$(modules))

# The following object files make into cdb2api static (libcdb2api.a) as
# well as dynamic (libcdb2api.so) libraries and thus, need an additional
# -fPIC flag.
SPECIAL_OBJS:= cdb2api/cdb2api.o cdb2api/comdb2buf.o
ifeq ($(arch),Linux)
$(SPECIAL_OBJS): EXTRA_FLAGS := -fPIC
else
ifeq ($(arch),SunOS)
$(SPECIAL_OBJS): EXTRA_FLAGS := -xcode=pic13
endif
endif

# Generate dependencies while building object files
%.o : %.c
	$(CC) $(DEPFLAGS) $(CPPFLAGS) $(CFLAGS) $(EXTRA_FLAGS) -c -o $@ $<
	$(POSTCOMPILE)

%.o : %.cpp
	$(CXX) $(DEPFLAGS) $(CPPFLAGS) $(CXXFLAGS) -c -o $@ $<
	$(POSTCOMPILE)

%.d: ;
.PRESCIOUS: $(OBJS:.o=.d)

-include $(OBJS:.o=.d)

# Build binaries
all: comdb2

.PHONY: clean
clean:
	rm -f $(TASKS) $(ARS) $(OBJS) $(GENC) $(GENH) $(GENMISC)

# Supply our own deb builder to make packaging easier.  In case
# there's ever an official Debian package being maintained, use 'deb'
# as the debian directory (and we/they wouldn't use this target)
deb-current: clean deb-clean
	rm -fr debian
	cp -r deb debian
	rm -f ../$(PACKAGE)_$(VERSION).orig.tar.gz
	tar acf ../$(PACKAGE)_$(VERSION).orig.tar.gz bb bbinc bdb berkdb cdb2api cdb2jdbc comdb2rle contrib crc32c csc2 csc2files cson datetime db debian deb rpmbuild dfp dlmalloc linearizable libs.mk LICENSE lua main.mk Makefile net protobuf README.md schemachange sqlite sockpool tools tests docs common.mk mem.mk INTERNAL_CONTRIBUTORS.md
	dpkg-buildpackage -us -uc
	@ls -l ../$(PACKAGE)_$(VERSION)*.deb
	@rm -fr debian

rpm-current: clean rpm-clean
	-mkdir -p ${HOME}/rpmbuild/BUILD ${HOME}/rpmbuild/BUILDROOT ${HOME}/rpmbuild/RPMS ${HOME}/rpmbuild/SOURCES ${HOME}/rpmbuild/SPECS ${HOME}/rpmbuild/SRPMS
	tar --transform="s|\\./|$(PACKAGE)-$(VERSION)/|" -acf ${HOME}/rpmbuild/SOURCES/$(PACKAGE)-$(VERSION).tar.gz .
	sed s'/VVEERRSSIIOONN/$(VERSION)/' rpmbuild/comdb2.spec > ${HOME}/rpmbuild/SPECS/$(PACKAGE)-$(VERSION).spec
	rpmbuild -bb ${HOME}/rpmbuild/SPECS/$(PACKAGE)-$(VERSION).spec
	@ls -l ${HOME}/rpmbuild/RPMS/*/$(PACKAGE)-$(VERSION)*.rpm

rpm-clean:
	rm -f ${HOME}/rpmbuild/SOURCES/$(PACKAGE)_$(VERSION)*

deb-clean:
	rm -f ../$(PACKAGE)_$(VERSION)*

test: $(TASKS)
	$(MAKE) -C tests

install: all
	install -D comdb2 $(DESTDIR)$(PREFIX)/bin/comdb2
	sed "s|^PREFIX=|PREFIX=$(PREFIX)|" db/copycomdb2 > db/copycomdb2.q
	install -D db/copycomdb2.q $(DESTDIR)$(PREFIX)/bin/copycomdb2
	rm -f db/copycomdb2.q
	install -D comdb2ar $(DESTDIR)$(PREFIX)/bin/comdb2ar
	install -D comdb2sc $(DESTDIR)$(PREFIX)/bin/comdb2sc
	install -D cdb2_printlog $(DESTDIR)$(PREFIX)/bin/cdb2_printlog
	install -D cdb2_verify $(DESTDIR)$(PREFIX)/bin/cdb2_verify
	install -D cdb2_dump $(DESTDIR)$(PREFIX)/bin/cdb2_dump
	install -D cdb2_stat $(DESTDIR)$(PREFIX)/bin/cdb2_stat
	install -D cdb2sql $(DESTDIR)$(PREFIX)/bin/cdb2sql
	install -D pmux $(DESTDIR)$(PREFIX)/bin/pmux
	install -D cdb2sockpool $(DESTDIR)$(PREFIX)/bin/cdb2sockpool
	install -D tools/pmux/pmux.service $(DESTDIR)/lib/systemd/system/pmux.service
	install -D tools/cdb2sockpool/cdb2sockpool.service $(DESTDIR)/lib/systemd/system/cdb2sockpool.service
	install -D contrib/comdb2admin/supervisor_cdb2.service $(DESTDIR)/lib/systemd/system/supervisor_cdb2.service
	install -D cdb2api/cdb2api.pc $(DESTDIR)/usr/local/lib/pkgconfig/cdb2api.pc
	install -D tools/pmux/pmux.service $(DESTDIR)$(PREFIX)/lib/systemd/system/pmux.service
	install -D db/comdb2dumpcsc $(DESTDIR)$(PREFIX)/bin/comdb2dumpcsc
	mkdir -p $(DESTDIR)$(PREFIX)/var/cdb2/ $(DESTDIR)$(PREFIX)/etc/cdb2 $(DESTDIR)$(PREFIX)/var/log/cdb2 $(DESTDIR)$(PREFIX)/etc/cdb2/rtcpu $(DESTDIR)$(PREFIX)/var/lib/cdb2 $(DESTDIR)$(PREFIX)/etc/cdb2/config/comdb2.d/  $(DESTDIR)$(PREFIX)/tmp/cdb2/ $(DESTDIR)$(PREFIX)/var/log/cdb2_supervisor/conf.d $(DESTDIR)$(PREFIX)/etc/cdb2_supervisor/conf.d/ $(DESTDIR)$(PREFIX)/var/run $(DESTDIR)$(PREFIX)/var/log/cdb2_supervisor/
	[ -z "$(DESTDIR)" ] && chown $(USER):$(GROUP) $(DESTDIR)$(PREFIX)/var/cdb2/ $(DESTDIR)$(PREFIX)/etc/cdb2 $(DESTDIR)$(PREFIX)/var/log/cdb2 $(DESTDIR)$(PREFIX)/etc/cdb2/rtcpu $(DESTDIR)$(PREFIX)/var/lib/cdb2 $(DESTDIR)$(PREFIX)/etc/cdb2/config/comdb2.d/ $(DESTDIR)$(PREFIX)/etc/cdb2/config/ $(DESTDIR)$(PREFIX)/tmp/cdb2/ $(DESTDIR)$(PREFIX)/var/log/cdb2_supervisor/conf.d $(DESTDIR)$(PREFIX)/etc/cdb2_supervisor/conf.d/  $(DESTDIR)$(PREFIX)/var/run $(DESTDIR)$(PREFIX)/var/log/cdb2_supervisor/ || true  
	[ -z "$(DESTDIR)" ] && chmod 755 $(DESTDIR)$(PREFIX)/var/cdb2/ $(DESTDIR)$(PREFIX)/etc/cdb2 $(DESTDIR)$(PREFIX)/var/log/cdb2 $(DESTDIR)$(PREFIX)/etc/cdb2/rtcpu $(DESTDIR)$(PREFIX)/var/lib/cdb2 $(DESTDIR)$(PREFIX)/etc/cdb2/config/comdb2.d/ $(DESTDIR)$(PREFIX)/etc/cdb2/config/ $(DESTDIR)$(PREFIX)/tmp/cdb2/ $(DESTDIR)$(PREFIX)/var/log/cdb2_supervisor/conf.d $(DESTDIR)$(PREFIX)/etc/cdb2_supervisor/conf.d/  $(DESTDIR)$(PREFIX)/var/run $(DESTDIR)$(PREFIX)/var/log/cdb2_supervisor/ || true
	install -D cdb2api/cdb2api.h $(DESTDIR)$(PREFIX)/include/cdb2api.h
	install -D cdb2api/libcdb2api.a $(DESTDIR)$(PREFIX)/lib/libcdb2api.a
	install -D cdb2api/libcdb2api.so $(DESTDIR)$(PREFIX)/lib/libcdb2api.so
	install -D protobuf/libcdb2protobuf.a $(DESTDIR)$(PREFIX)/lib/libcdb2protobuf.a
	install -D contrib/comdb2admin/supervisord_cdb2.conf $(DESTDIR)$(PREFIX)/etc/supervisord_cdb2.conf
	install -D contrib/comdb2admin/comdb2admin $(DESTDIR)$(PREFIX)/bin/comdb2admin
	[ -z "$(DESTDIR)" ] && . db/installinfo || true

jdbc-docker-build-container:
	docker build -t jdbc-docker-builder:$(VERSION) -f docker/Dockerfile.jdbc.build docker

jdbc-docker-build: jdbc-docker-build-container
	docker run \
		--user $(shell id -u):$(shell id -g) \
		--env HOME=/tmp \
		-v $(BASEDIR):/jdbc.build \
		-v ${BASEDIR}/docker/maven.m2:/maven.m2 \
		-w /jdbc.build/docker \
		jdbc-docker-builder:$(VERSION) \
		/bin/maven/bin/mvn -f /jdbc.build/cdb2jdbc/pom.xml clean install

