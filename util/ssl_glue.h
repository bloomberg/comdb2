/*
   Copyright 2022 Bloomberg Finance L.P.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#ifndef INCLUDED_SSL_GLUE_H
#define INCLUDED_SSL_GLUE_H
#include <openssl/ssl.h>
int ssl_verify_dbname(X509 *, const char *, int);
int ssl_x509_get_attr(const X509 *, int, char *, size_t);
int ssl_verify_hostname(X509 *, int);
#endif /* INCLUDED_SSL_GLUE_H */
