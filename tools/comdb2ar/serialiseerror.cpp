#include "serialiseerror.h"

#include <sstream>
#include <string>

SerialiseError::SerialiseError(const std::string& filename, const char *error)
{
    std::ostringstream ss;
    ss << "Error serialising " << filename << ": " << error;
    m_error = ss.str();
}

SerialiseError::SerialiseError(const std::string& filename, const std::string& error)
{
    std::ostringstream ss;
    ss << "Error serialising " << filename << ": " << error;
    m_error = ss.str();
}

SerialiseError::~SerialiseError() throw()
{
}

const char* SerialiseError::what() const throw()
{
    return m_error.c_str();
}
