all: build-deb build-rpm build-inst

build-deb:
	-docker rm -f debtest
	docker build -t smoketest-deb -f Dockerfile.deb.build ../../
	docker run -d --name debtest smoketest-deb /build/tests/smoketest/docker-dev-entrypoint.sh db1 db2 db3 db4 db5
	./testdbs debtest
	-docker kill debtest
	-docker rm debtest

build-rpm:
	-docker rm -f rpmtest
	docker build -t smoketest-rpm -f Dockerfile.rpm.build ../../
	docker run -d --name rpmtest smoketest-rpm /build/tests/smoketest/docker-dev-entrypoint.sh db1 db2 db3 db4 db5
	./testdbs rpmtest
	-docker kill rpmtest
	-docker rm rpmtest

build-inst-pre:
	-docker rm -f insttest
	docker build -t smoketest-inst -f Dockerfile.inst.build ../../
	docker run -d --name insttest smoketest-inst /build/tests/smoketest/docker-dev-entrypoint.sh db1 db2 db3 db4 db5

build-inst-c: build-inst-pre
	./testdbs insttest

build-inst-java: export envSkipTests='false'
build-inst-java: export envTestDatabase='db1'
build-inst-java: export envTestCluster=$(shell docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' insttest)
build-inst-java: build-inst-pre
	cd ../../cdb2jdbc && make jdbc-docker-build

build-inst: build-inst-c build-inst-java

build-inst-post: build-inst-c build-inst-java
	-docker kill insttest
	-docker rm insttest
