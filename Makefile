PACKAGE=comdb2
VERSION?=$(shell grep ^comdb2 deb/changelog  | sed 's/^comdb2 .//; s/-.*//g')
include main.mk
include libs.mk

export SRCHOME=.
export DESTDIR
export PREFIX

# Common CFLAGS
CPPFLAGS+=-I$(SRCHOME)/dlmalloc
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
lua tools db sockpool
include $(addsuffix /module.mk,$(modules))

# The following object files make into cdb2api static
# (libcdb2api.a & libcdb2protobuf.a) as well as dynamic
# (libcdb2api.so & libcdb2protobuf.so) libraries and thus,
# need an additional -fPIC (large model) flag.
SPECIAL_OBJS:= cdb2api/cdb2api.o protobuf/%.o
ifeq ($(arch),Linux)
$(SPECIAL_OBJS): EXTRA_FLAGS := -fPIC
else
ifeq ($(arch),SunOS)
$(SPECIAL_OBJS): EXTRA_FLAGS := -kPIC
else
ifeq ($(arch),AIX)
$(SPECIAL_OBJS): EXTRA_FLAGS := -qpic
endif
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
	tar acf ../$(PACKAGE)_$(VERSION).orig.tar.gz *
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
	[ -z "$(DESTDIR)" ] && rm -f $(DESTDIR)$(PREFIX)/bin/cdb2_printlog && ln $(DESTDIR)$(PREFIX)/bin/comdb2 $(DESTDIR)$(PREFIX)/bin/cdb2_printlog || true
	[ -z "$(DESTDIR)" ] && rm -f $(DESTDIR)$(PREFIX)/bin/cdb2_verify && ln $(DESTDIR)$(PREFIX)/bin/comdb2 $(DESTDIR)$(PREFIX)/bin/cdb2_verify || true
	[ -z "$(DESTDIR)" ] && rm -f $(DESTDIR)$(PREFIX)/bin/cdb2_dump && ln $(DESTDIR)$(PREFIX)/bin/comdb2 $(DESTDIR)$(PREFIX)/bin/cdb2_dump || true
	[ -z "$(DESTDIR)" ] && rm -f $(DESTDIR)$(PREFIX)/bin/cdb2_stat && ln $(DESTDIR)$(PREFIX)/bin/comdb2 $(DESTDIR)$(PREFIX)/bin/cdb2_stat || true
	install -D cdb2_sqlreplay $(DESTDIR)$(PREFIX)/bin/cdb2_sqlreplay
	install -D cdb2sockpool $(DESTDIR)$(PREFIX)/bin/cdb2sockpool
	install -D cdb2sql $(DESTDIR)$(PREFIX)/bin/cdb2sql
	install -D comdb2ar $(DESTDIR)$(PREFIX)/bin/comdb2ar
	install -D pmux $(DESTDIR)$(PREFIX)/bin/pmux
	install -D tools/pmux/pmux.service $(DESTDIR)$(PREFIX)/lib/systemd/system/pmux.service
	install -D tools/cdb2sockpool/cdb2sockpool.service $(DESTDIR)$(PREFIX)/lib/systemd/system/cdb2sockpool.service
	install -D contrib/comdb2admin/supervisor_cdb2.service $(DESTDIR)$(PREFIX)/lib/systemd/system/supervisor_cdb2.service
	install -D cdb2api/cdb2api.pc $(DESTDIR)$(PREFIX)/usr/local/lib/pkgconfig/cdb2api.pc
	install -D db/comdb2dumpcsc $(DESTDIR)$(PREFIX)/bin/comdb2dumpcsc
	mkdir -p $(DESTDIR)$(PREFIX)/var/cdb2/ $(DESTDIR)$(PREFIX)/etc/cdb2 $(DESTDIR)$(PREFIX)/var/log/cdb2 $(DESTDIR)$(PREFIX)/etc/cdb2/rtcpu $(DESTDIR)$(PREFIX)/var/lib/cdb2 $(DESTDIR)$(PREFIX)/etc/cdb2/config/comdb2.d/  $(DESTDIR)$(PREFIX)/tmp/cdb2/ $(DESTDIR)$(PREFIX)/var/log/cdb2_supervisor/conf.d $(DESTDIR)$(PREFIX)/etc/cdb2_supervisor/conf.d/ $(DESTDIR)$(PREFIX)/var/run $(DESTDIR)$(PREFIX)/var/log/cdb2_supervisor/
	[ -z "$(DESTDIR)" ] && chown $(USER):$(GROUP) $(DESTDIR)$(PREFIX)/var/cdb2/ $(DESTDIR)$(PREFIX)/etc/cdb2 $(DESTDIR)$(PREFIX)/var/log/cdb2 $(DESTDIR)$(PREFIX)/etc/cdb2/rtcpu $(DESTDIR)$(PREFIX)/var/lib/cdb2 $(DESTDIR)$(PREFIX)/etc/cdb2/config/comdb2.d/ $(DESTDIR)$(PREFIX)/etc/cdb2/config/ $(DESTDIR)$(PREFIX)/tmp/cdb2/ $(DESTDIR)$(PREFIX)/var/log/cdb2_supervisor/conf.d $(DESTDIR)$(PREFIX)/etc/cdb2_supervisor/conf.d/  $(DESTDIR)$(PREFIX)/var/run $(DESTDIR)$(PREFIX)/var/log/cdb2_supervisor/ || true  
	[ -z "$(DESTDIR)" ] && chmod 755 $(DESTDIR)$(PREFIX)/var/cdb2/ $(DESTDIR)$(PREFIX)/etc/cdb2 $(DESTDIR)$(PREFIX)/var/log/cdb2 $(DESTDIR)$(PREFIX)/etc/cdb2/rtcpu $(DESTDIR)$(PREFIX)/var/lib/cdb2 $(DESTDIR)$(PREFIX)/etc/cdb2/config/comdb2.d/ $(DESTDIR)$(PREFIX)/etc/cdb2/config/ $(DESTDIR)$(PREFIX)/tmp/cdb2/ $(DESTDIR)$(PREFIX)/var/log/cdb2_supervisor/conf.d $(DESTDIR)$(PREFIX)/etc/cdb2_supervisor/conf.d/  $(DESTDIR)$(PREFIX)/var/run $(DESTDIR)$(PREFIX)/var/log/cdb2_supervisor/ || true
	install -D cdb2api/cdb2api.h $(DESTDIR)$(PREFIX)/include/cdb2api.h
	install -D cdb2api/libcdb2api.a $(DESTDIR)$(PREFIX)/lib/libcdb2api.a
	install -D cdb2api/libcdb2api.so $(DESTDIR)$(PREFIX)/lib/libcdb2api.so
	install -D protobuf/libcdb2protobuf.a $(DESTDIR)$(PREFIX)/lib/libcdb2protobuf.a
	install -D protobuf/libcdb2protobuf.so $(DESTDIR)$(PREFIX)/lib/libcdb2protobuf.so
	install -D contrib/comdb2admin/supervisord_cdb2.conf $(DESTDIR)$(PREFIX)/etc/supervisord_cdb2.conf
	install -D contrib/comdb2admin/comdb2admin $(DESTDIR)$(PREFIX)/bin/comdb2admin
	-[ -z "$(DESTDIR)" ] && . db/installinfo || true

# Build a container for building the database
build-build-container:
	docker build -t comdb2-build:$(VERSION) -f contrib/docker/Dockerfile.build .

docker-clean:
	rm -fr contrib/docker/build/*
	docker-compose -f $(realpath $(SRCHOME))/contrib/docker/docker-compose.yml down

docker-dev: docker-standalone
	docker build -t comdb2-dev:$(VERSION) -f contrib/docker/Dockerfile.dev .
	docker tag comdb2-dev:$(VERSION) comdb2-dev:latest

docker-standalone: docker-build
	docker build -t comdb2-standalone:$(VERSION) -f contrib/docker/Dockerfile.standalone contrib/docker

docker-cluster: docker-standalone
	mkdir -p $(realpath $(SRCHOME))/contrib/docker/volumes/node1
	mkdir -p $(realpath $(SRCHOME))/contrib/docker/volumes/node2
	mkdir -p $(realpath $(SRCHOME))/contrib/docker/volumes/node3
	mkdir -p $(realpath $(SRCHOME))/contrib/docker/volumes/node4
	mkdir -p $(realpath $(SRCHOME))/contrib/docker/volumes/node5
	docker-compose -f $(realpath $(SRCHOME))/contrib/docker/docker-compose.yml down
	docker-compose -f $(realpath $(SRCHOME))/contrib/docker/docker-compose.yml up -d

# Build the database in the build container
docker-build: build-build-container
	mkdir -p $(realpath $(SRCHOME))/contrib/docker/build
	docker run --user $(shell id -u):$(shell id -g) \
		--env HOME=/tmp \
		-v $(realpath $(SRCHOME))/contrib/docker/build:/comdb2 \
		-v $(realpath $(SRCHOME)):/comdb2.build \
		-w /comdb2.build \
		comdb2-build:$(VERSION) \
        make DESTDIR=/comdb2 -j3 install
 