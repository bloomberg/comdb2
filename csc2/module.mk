# Local defs
cmacc2_MEMGEN:=csc2/mem_csc2.h
cmacc2_SOURCES:=csc2/macc_so.c csc2/maccprocarr.c csc2/maccglobals.c csc2/maccwrite2.c
cmacc2_GENSOURCES:=csc2/maccparse.c csc2/macclex.c 
cmacc2_GENOBJS:=$(cmacc2_GENSOURCES:.c=.o)
cmacc2_OBJS:=$(cmacc2_SOURCES:.c=.o) $(cmacc2_GENOBJS)

# Defined in the top level makefile
ARS+=csc2/libcsc2lib.a
OBJS+=$(cmacc2_OBJS)
GENC+=$(cmacc2_GENSOURCES)
GENH+=csc2/maccparse.h $(cmacc2_MEMGEN)
GENMISC+=y.tab.c y.tab.h y.output csc2/y.output
SOURCES+=$(cmacc2_SOURCES)

csc2/libcsc2lib.a: CPPFLAGS+=-DYYMAXDEPTH=1100 -I$(SRCHOME)/bbinc -I.

csc2/libcsc2lib.a: $(cmacc2_OBJS) 
	$(AR) $(ARFLAGS) $@ $^

$(cmacc2_OBJS): $(cmacc2_GENSOURCES) $(cmacc2_MEMGEN) 
$(cmacc2_GENOBJS): csc2/maccparse.y

csc2/macclex.c: csc2/maccparse.h

csc2/maccparse.c: csc2/maccparse.h ;
csc2/maccparse.h: csc2/maccparse.y
	yacc -d -v $(YACCWARN_FLAGS) csc2/maccparse.y
	mv y.tab.c csc2/maccparse.c
	mv y.tab.h csc2/maccparse.h
	mv y.output csc2
