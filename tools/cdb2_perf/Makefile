SRCHOME=../..

OBJS=cdb2perf.o
BINARY=cdb2perf

DUMBWARN=-Wno-unused-function

LIBS=-L$(SRCHOME)/cson -lcson -lz -L$(SRCHOME)/cdb2api -luuid -l:libcdb2api.a -L$(SRCHOME)/protobuf -lcdb2protobuf -lprotobuf-c -lssl -lcrypto -lpthread
CXXFLAGS=-Wall -g -I$(SRCHOME)/cson -I$(SRCHOME)/cdb2api -std=c++11 $(DUMBWARN)

$(BINARY): $(OBJS)
	g++ -o $(BINARY) $(OBJS) $(LDFLAGS) $(LIBS)

%.o: %.cpp
	g++ -c $(CXXFLAGS) $< -o $@

clean:  
	rm -f $(BINARY) $(OBJS)
