/*
   Copyright 2015 Bloomberg Finance L.P.

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

#ifndef __bdb_net_h__
#define __bdb_net_h__


typedef struct bdb_state_tag bdb_state_type;
char *print_addr(struct sockaddr_in *addr, char *buf);
const char *get_hostname_with_crc32(bdb_state_type *bdb_state,
                                    unsigned int hash);

#endif /* __bdb_net_h__ */
