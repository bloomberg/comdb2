# shortcut for building jdbc driver via make

.PHONY= jdbc
MAVEN_VER := $(shell mvn -version 2>/dev/null)

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
