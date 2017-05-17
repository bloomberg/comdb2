
#include "riia.h"

#include <unistd.h>
#include <stdlib.h>

RIIA_fd::RIIA_fd(int fd) : m_fd(fd)
{
}

RIIA_fd::~RIIA_fd()
{
    close(m_fd);
}



RIIA_DIR::RIIA_DIR(DIR *dh) : m_dh(dh)
{
}

RIIA_DIR::~RIIA_DIR()
{
    closedir(m_dh);
}


RIIA_malloc::RIIA_malloc(void *p) : m_p(p) {}
RIIA_malloc::~RIIA_malloc() {
    free(m_p);
}
