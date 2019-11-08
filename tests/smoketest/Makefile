all: build-deb build-rpm build-inst

build-deb:
	-docker rm -f debtest
	docker build -t smoketest-deb -f Dockerfile.deb.build ../../
	docker run -d --name debtest smoketest-deb /comdb2/tests/smoketest/docker-dev-entrypoint.sh db1 db2 db3 db4 db5
	./testdbs debtest
	-docker kill debtest
	-docker rm debtest

build-rpm:
	-docker rm -f rpmtest
	docker build -t smoketest-rpm -f Dockerfile.rpm.build ../../
	docker run -d --name rpmtest smoketest-rpm /comdb2/tests/smoketest/docker-dev-entrypoint.sh db1 db2 db3 db4 db5
	./testdbs rpmtest
	-docker kill rpmtest
	-docker rm rpmtest

build-inst-pre:
	-docker rm -f cinsttest
	-docker rm -f jinsttest
	docker build -t smoketest-inst -f Dockerfile.inst.build ../../
	docker run -d --name cinsttest smoketest-inst /comdb2/tests/smoketest/docker-dev-entrypoint.sh db1 db2 db3 db4 db5
	-rm -rf /tmp/smoketestca
	CN="*.$(shell hostname -d)" \
	SCN='*.wrongcn' \
	CADIR=/tmp/smoketestca ../tools/keygen.sh
	docker run -v /tmp/smoketestca:/ca -d --name jinsttest smoketest-inst \
	       /comdb2/tests/smoketest/docker-jdbctest-entrypoint.sh /ca jdbctdb jdbcssltdb

build-inst-c: build-inst-pre
	./testdbs cinsttest
	-docker kill cinsttest
	-docker rm cinsttest

build-inst-java: export envSkipTests='false'
build-inst-java: export envTestDatabase='jdbctdb'
build-inst-java: export envSSLTestDatabase='jdbcssltdb'
build-inst-java: export envSSLCertPath='/ca'
build-inst-java: export envSSLCertPass='cdb2jdbctest'
build-inst-java: export envTestCluster=$(shell docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' jinsttest)
build-inst-java: build-inst-pre
	cd ../../cdb2jdbc && make jdbc-docker-build
	-docker kill jinsttest
	-docker rm jinsttest

build-inst: build-inst-c build-inst-java

build-nossl:
	-docker rm -f nossltest
	docker build -t smoketest-nossl -f Dockerfile.nossl.build ../../
	docker run -d --name nossltest smoketest-nossl /comdb2/tests/smoketest/docker-dev-entrypoint.sh db1 db2 db3 db4 db5
	./testdbs nossltest
	-docker kill nossltest
	-docker rm nossltest
