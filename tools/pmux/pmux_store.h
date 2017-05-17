#ifndef PMUX_STORE_H
#define PMUX_STORE_H

#include <cstdint>
#include <map>
#include <string>

struct pmux_store {
    virtual ~pmux_store(){};
    virtual int sav_port(const char *, uint16_t) = 0;
    virtual int del_port(const char *) = 0;
    virtual std::map<std::string, int> get_ports() = 0;
};

#endif
