#include "fdostream.h"
#include <unistd.h>
#include "comdb2ar.h"
#include <stdio.h>
#include <iostream>


fdoutbuf::fdoutbuf(int fd) : m_fd(fd)
{
}

fdoutbuf::~fdoutbuf()
{
    close(m_fd);
}

std::streambuf::int_type fdoutbuf::overflow(std::streambuf::int_type c)
{
    if(c != EOF) {
        char z = c;
        if(write (m_fd, &z, 1) != 1) {
            return EOF;
        }
    }
    return c;
}

// write multiple characters
std::streamsize fdoutbuf::xsputn(const char* s, std::streamsize num)
{
    return writeall(m_fd, s, num);
}

int fdoutbuf::getfd()
{
    return m_fd;
}

int fdostream::skip(unsigned long long size)
{
  int fd = buf.getfd();
  unsigned long long cur_ptr =  lseek(fd, 0, SEEK_CUR);
  unsigned long long new_ptr = lseek(fd, size, SEEK_CUR);
  if (new_ptr == cur_ptr + size)
      return 0;
  return -1;
}


fdostream::fdostream(int fd) : std::ostream(0), buf(fd)
{
    rdbuf(&buf);
}
