# shortcut for building jdbc driver via make
BASEDIR:=$(realpath ..)
VERSION?=$(shell grep ^comdb2 ../pkg/changelog  | sed 's/^comdb2 .//; s/-.*//g')

.PHONY: jdbc
MAVEN_VER := $(shell mvn -version 2>/dev/null)

envSkipTests?='true'

jdbc:
ifdef MAVEN_VER
	+mvn package
else
	$(error "Could not find maven")
endif

clean:
ifdef MAVEN_VER
	+mvn clean
else
	$(error "Could not find maven")
endif

install: jdbc
ifdef MAVEN_VER
	+mvn install
else
	$(error "Could not find maven")
endif

jdbc-docker-build-container:
	docker build -t jdbc-docker-builder:$(VERSION) -f $(BASEDIR)/contrib/docker/Dockerfile.jdbc.build ..

jdbc-docker-build: jdbc-docker-build-container
	docker run \
        -v /tmp/smoketestca:/ca \
        --rm \
		--user $(shell id -u):$(shell id -g) \
		--env HOME=/tmp \
		-v $(BASEDIR):/jdbc.build \
		-v $(BASEDIR)/cdb2jdbc/maven.m2:/maven.m2 \
		-w /jdbc.build \
		jdbc-docker-builder:$(VERSION) \
		/bin/maven/bin/mvn -f /jdbc.build/cdb2jdbc/pom.xml \
			-Denv.skipTests=$(envSkipTests) \
			-DargLine="-Dcdb2jdbc.test.database=$(envTestDatabase) \
                       -Dcdb2jdbc.test.cluster=$(envTestCluster) \
                       -Dcdb2jdbc.test.ssldatabase=$(envSSLTestDatabase) \
                       -Dcdb2jdbc.test.sslcertpath=$(envSSLCertPath) \
                       -Dcdb2jdbc.test.sslcertpass=$(envSSLCertPass)" \
			clean install
