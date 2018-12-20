/*
   Copyright 2018 Bloomberg Finance L.P.

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

#include <intern_strings.h>
#include <compat.h>

static int nodenum_nop(char *host)
{
    return 0;
}
static nodenum_mapper *nodenum_impl = nodenum_nop;
void set_nodenum_mapper(nodenum_mapper *impl)
{
    nodenum_impl = impl;
}
int nodenum(char *host)
{
    return nodenum_impl(host);
}

static char *hostname_nop(int node)
{
    return NULL;
}
hostname_mapper *hostname_impl = hostname_nop;
void set_hostname_mapper(hostname_mapper *impl)
{
    hostname_impl = impl;
}
char *hostname(int node)
{
    return hostname_impl(node);
}

static char *nodenum_str_nop(char *host)
{
    return host;
}
static nodenum_str_mapper *nodenum_str_impl = nodenum_str_nop;
void set_nodenum_str_mapper(nodenum_str_mapper *impl)
{
    nodenum_str_impl = impl;
}
char *nodenum_str(char *host)
{
    return nodenum_str_impl(host);
}

static char *hostname_str_nop(char *node_str)
{
    return node_str;
}
hostname_str_mapper *hostname_str_impl = hostname_str_nop;
void set_hostname_str_mapper(hostname_str_mapper *impl)
{
    hostname_str_impl = impl;
}
char *hostname_str(char *node_str)
{
    return hostname_str_impl(node_str);
}
