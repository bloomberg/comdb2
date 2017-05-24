.PHONY= codegen install 

export PREFIX?=/opt/bb
export SSHUSER?='comdb2'

gencode: codegen *.sh
	./codegen $(PREFIX) $(SSHUSER)

install: gencode
	install -d $(DESTDIR)$(PREFIX)/bin/comdb2awsinc
	install comdb2aws $(DESTDIR)$(PREFIX)/bin/
	install *.sh $(DESTDIR)$(PREFIX)/bin/comdb2awsinc
