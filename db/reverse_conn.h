/*
   Copyright 2023 Bloomberg Finance L.P.

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

#ifndef INCLUDED_REVERSE_CONN_H
#define INCLUDED_REVERSE_CONN_H

int start_reverse_connections_manager();
int stop_reverse_connections_manager();

int dump_reverse_connection_host_list();
int send_reversesql_request(const char *dbname, const char *host, const char *command);

#endif /* INCLUDED_REVERSE_CONN_H */
