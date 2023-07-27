#ifndef INCLUDED_CDB2IDENTITY_H
#define INCLUDED_CDB2IDENTITY_H

struct cdb2_identity_handle {
    void(*start)(void);
    void(*end)(int);
    void*(*get)(void);
};

#endif
