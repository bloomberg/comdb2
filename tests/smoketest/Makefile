all: build-deb build-rpm build-inst

build-deb:
	-docker rm -f debtest || true
	docker build -t smoketest-deb -f Dockerfile.deb.build ../../
	docker run -d --name debtest smoketest-deb /build/tests/smoketest/docker-dev-entrypoint.sh db1 db2 db3 db4 db5
	./testdbs debtest
	-docker kill debtest || true
	-docker rm debtest || true

build-rpm:
	-docker rm -f rpmtest || true
	docker build -t smoketest-rpm -f Dockerfile.rpm.build ../../
	docker run -d --name rpmtest smoketest-rpm /build/tests/smoketest/docker-dev-entrypoint.sh db1 db2 db3 db4 db5
	./testdbs rpmtest
	-docker kill rpmtest || true
	-docker rm rpmtest || true

build-inst:
	-docker rm -f insttest || true
	docker build -t smoketest-inst -f Dockerfile.inst.build ../../
	docker run -d --name insttest smoketest-inst /build/tests/smoketest/docker-dev-entrypoint.sh db1 db2 db3 db4 db5
	./testdbs insttest
	-docker kill insttest || true
	-docker rm insttest || true
