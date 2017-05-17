#include "error.h"


Error::Error(const char *error) : m_error(error)
{
}

Error::Error(const std::string &error) : m_error(error)
{
}

Error::Error(const std::stringstream &errorss) : m_error(errorss.str())
{
}

Error::Error(const std::ostringstream &errorss) : m_error(errorss.str())
{
}

Error::~Error() throw()
{
}

const char *Error::what() const throw()
{
    return m_error.c_str();
}
