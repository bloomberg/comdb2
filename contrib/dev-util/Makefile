OBJS=testrunner.o #plhash.o pool.o
BINARY=testrunner

CFLAGS=-pthread -Wall -g -Wextra -Wno-unused-parameter

$(BINARY): $(OBJS)
	gcc -o $(BINARY) $(OBJS) $(LDFLAGS)

%.o: %.c
	gcc -c $(CFLAGS) $< -o $@

clean:  
	rm -f $(BINARY) $(OBJS)
