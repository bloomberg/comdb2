$(SRCHOME)/bb/mem_int.h:$(SRCHOME)/bb/mem.h.template
	$(SRCHOME)/bb/mem_codegen.sh

# These two are exempt from the rule below
$(SRCHOME)/bbinc/mem_override.h: ;
$(SRCHOME)/bbinc/mem_restore.h: ;

mem_%.h: $(SRCHOME)/bb/mem_int.h
	$(SRCHOME)/bb/mem_codegen.sh $(*F) $(*D)
