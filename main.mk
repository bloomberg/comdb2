PREFIX?=/opt/bb

COMDB2_ROOT?=$(PREFIX)

arch:=$(shell uname)
export $(arch)

TAGS?=ctags
CFLAGS_INC=-I$(SRCHOME)/bbinc -Ibb -I$(SRCHOME) -DCOMDB2_ROOT=$(COMDB2_ROOT)
CFLAGS_64=-DBB64BIT

ifeq ($(arch),Linux)
  #CFLAGS_STRICT=-Wall -Wno-unused-variable -Wno-unused-but-set-variable -Wno-unused-function -Wno-unused-label
  CFLAGS_STRICT=
  CFLAGS_WARNINGS=-Werror $(CFLAGS_STRICT) -Wno-parentheses
  CFLAGS_MISC=-pipe -std=c99 -fno-builtin -fno-strict-aliasing
  CFLAGS_DEFS=-D_GNU_SOURCE -D_XOPEN_SOURCE=600 -DSTDC_HEADERS -D_SYS_SYSMACROS_H
  CFLAGS_DEBUGGING=-g -ggdb -gdwarf-2
  CFLAGS_64+=-m64
  CFLAGS_ARCHFLAGS=-D_LINUX_SOURCE
  UNWINDARGS?=-DUSE_UNWIND
  UNWINDLIBS?=-lunwind
  ARCHLIBS+=$(BBSTATIC) $(UNWINDLIBS) $(BBDYN)
  CXXFLAGS=-fpermissive $(CFLAGS_DEBUGGING) $(CFLAGS_64)
  LIBREADLINE=$(BBSTATIC) -lreadline -lhistory -lncurses $(BBDYN) -ltermcap
  # Use GCC, but we already use GCC anyway.
  CXX11=$(CXX)
  CXX11FLAGS=-std=c++11 $(CXXFLAGS)
  CXX11LDFLAGS=$(LDFLAGS)
  # Flags for generating dependencies
  DEPFLAGS = -MT $@ -MMD -MP -MF $(@:.o=.Td)
  DEPFLAGS_CXX11 = -MT $@ -MMD -MP -MF $(@:.o=.Td)
  POSTCOMPILE = mv -f $(@:.o=.Td) $(@:.o=.d)
  SHARED=-shared
  OPT_CFLAGS=-O3
else
ifeq ($(arch),SunOS)
  CC=/bb/util/common/SS12_3-20131030/SUNWspro/bin/cc
  CXX=/bb/util/common/SS12_3-20131030/SUNWspro/bin/CC
  OPT_CFLAGS=-xO3 -xprefetch=auto,explicit
  CFLAGS_MISC=-xtarget=generic -fma=fused -xinline=%auto -xmemalign=8s
  CFLAGS=-mt -xtarget=generic -xc99=all -errfmt=error
  CFLAGS_DEFS=-D_POSIX_PTHREAD_SEMANTICS -D_POSIX_PTHREAD_SEMANTICS -D__FUNCTION__=__FILE__ -D_SYS_SYSMACROS_H
  CFLAGS_DEBUGGING=-g -xdebugformat=stabs
  ARCHLIBS+=-lnsl -lsocket
  LIBREADLINE=$(BBSTATIC) -lreadline -lhistory $(BBDYN)
  UNWINDLIBS?=-lunwind
  CFLAGS_ARCHFLAGS=-D_SUN_SOURCE
  CFLAGS_64+=-m64
  CXXFLAGS=$(CFLAGS_64)
  LDFLAGS+=-m64
  ARFLAGS=-r
  CXX11=/opt/swt/install/gcc-4.9.2/bin/g++
  CXX11FLAGS=-std=c++11 $(CXXFLAGS)
  CXX11LDFLAGS=$(LDFLAGS)
  SHARED=-G
else
ifeq ($(arch),AIX)
  CC=/bb/util/version12-052015/usr/vacpp/bin/cc_r
  CXX=/bb/util/version12-052015/usr/vacpp/bin/xlC_r
  OPT_FLAGS=
  CFLAGS_DEFS=-D__VACPP_MULTI__ -DMAXHOSTNAMELEN=64 -D_H_SYSMACROS
  CFLAGS_MISC=-qlanglvl=extc99 -qtls -qcpluscmt -qthreaded  -qdfp -qthreaded -qchars=signed -qro -qroconst -qkeyword=inline -qhalt=e -qxflag=dircache:71,256 -qxflag=new_pragma_comment_user -qxflag=NoKeepDebugMetaTemplateType -qfuncsect
  CFLAGS_DEBUGGING=-g
  CFLAGS_64+=-q64
  LDFLAGS+=-q64 -bmaxdata:0xb0000000/dsa -bbigtoc  -brtl -qtwolink
  CFLAGS_ARCHFLAGS=-D_IBM_SOURCE -Dibm
  ARFLAGS=-X64 -r
  ARCHLIBS+=-lthread
  CXXFLAGS=$(CFLAGS_64)
  # I give up.  Can't link these statically.
  LIBREADLINE=-blibpath:/opt/bb/lib64:/usr/lib:/lib -lreadline -lhistory
  # Use GCC on IBM for C++11 code. Also requires different options for 64 bit.
  CXX11=/opt/swt/install/gcc-4.9.2/bin/g++
  CXX11FLAGS=-std=c++11 -maix64
  CXX11LDFLAGS=-maix64
  BBLDPREFIX=-Wl,
  # Flags for generating dependencies
  DEPFLAGS = -qmakedep=gcc -MF $(@:.o=.Td)
  DEPFLAGS_CXX11 = -MT $@ -MMD -MP -MF $(@:.o=.Td)
  POSTCOMPILE = mv -f $(@:.o=.Td) $(@:.o=.d)
  SHARED=-G
endif
endif
endif

CFLAGS_DEFS+=-DBB_THREADED -D_REENTRANT -D_THREAD_SAFE
CFLAGS_DEFS+=-DPER_THREAD_MALLOC -DMONITOR_STACK
#CFLAGS_DEFS+=-DUSE_SYS_ALLOC

# COMDB2_DEBUG=1 can be used to enable asserts and other debugging code for
# specific builds.
ifeq ($(COMDB2_DEBUG),1)
  # Enable asserts
  CFLAGS_DEBUGGING+=-DSQLITE_DEBUG
  # Enable DEBUG, TODO: (NC) Disabled as it needs some cleanup.
  #CFLAGS_DEBUGGING+=-DDEBUG
endif

CPPFLAGS+=$(CFLAGS_ARCHFLAGS) $(CFLAGS_DEFS) $(CFLAGS_INC)
CFLAGS+=$(CFLAGS_DEBUGGING) $(CFLAGS_64) $(CFLAGS_MISC) $(CFLAGS_WARNINGS)
