#include "lrlerror.h"

#include <sstream>
#include <string>

LRLError::LRLError(const std::string& lrlpath, int lineno, const char *error)
{
    std::ostringstream ss;
    ss << "Bad LRL file " << lrlpath << ":" << lineno << ": " << error;
    m_error = ss.str();
}

LRLError::LRLError(const std::string& lrlpath, const char *error)
{
    std::ostringstream ss;
    ss << "Bad LRL file " << lrlpath << ": " << error;
    m_error = ss.str();
}

LRLError::~LRLError() throw()
{
}

const char* LRLError::what() const throw()
{
    return m_error.c_str();
}
