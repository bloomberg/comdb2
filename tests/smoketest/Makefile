all: build-deb build-rpm build-inst

build-deb:
	-docker rm debtest
	docker build -t smoketest-deb -f Dockerfile.deb.build ../../
	docker run -d --name debtest -t smoketest-deb /build/tests/smoketest/docker-dev-entrypoint.sh db1 db2 db3 db4 db5
	./testdbs debtest
	-docker kill debtest
	-docker rm debtest

build-rpm:
	-docker rm rpmtest
	docker build -t smoketest-rpm -f Dockerfile.rpm.build ../../
	docker run -d --name rpmtest -t smoketest-rpm /build/tests/smoketest/docker-dev-entrypoint.sh db1 db2 db3 db4 db5
	./testdbs rpmtest
	-docker kill rpmtest
	-docker rm rpmtest

build-inst:
	-docker rm insttest
	docker build -t smoketest-inst -f Dockerfile.inst.build ../../
	docker run -d --name insttest -t smoketest-inst /build/tests/smoketest/docker-dev-entrypoint.sh db1 db2 db3 db4 db5
	./testdbs insttest
	-docker kill insttest
	-docker rm insttest

