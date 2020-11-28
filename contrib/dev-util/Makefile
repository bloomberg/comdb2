OBJS=testrunner.o #plhash.o pool.o
BINARY=testrunner

CFLAGS=-std=c99 -pthread -Wall -g -U__STRICT_ANSI__ -Wextra -Wno-unused-parameter

$(BINARY): $(OBJS)
	gcc -o $(BINARY) $(OBJS) $(LDFLAGS)

%.o: %.c
	gcc -c $(CFLAGS) $< -o $@

clean:  
	rm -f $(BINARY) $(OBJS)
