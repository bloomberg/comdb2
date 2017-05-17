.PHONY= codegen install 

export PREFIX?=/opt/bb
export SSHUSER?='admin'

gencode: codegen *.sh
	./codegen $(PREFIX) $(SSHUSER)

install: gencode
	install -d $(DESTDIR)$(PREFIX)/bin
	install comdb2aws $(DESTDIR)$(PREFIX)/bin/
	install *.sh $(DESTDIR)$(PREFIX)/bin/
