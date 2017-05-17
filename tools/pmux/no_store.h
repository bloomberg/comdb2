#ifndef NO_STORE_H
#define NO_STORE_H

#include "pmux_store.h"

struct no_store : public pmux_store
{
    no_store() { }
    int sav_port(const char *svc, uint16_t port) { return 0; }
    int del_port(const char *svc) { return 0; }
    std::map<std::string, int> get_ports() { return std::map<std::string, int>(); }
};

#endif
