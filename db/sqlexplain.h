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

#ifndef _SQLEXPLAIN_H_
#define _SQLEXPLAIN_H_

extern int all_opcodes;

struct cursor_info {
    int dbid;
    int rootpage;
    int tbl;
    int ix;
    struct schema *sc;
    int istemp;
    KeyInfo *key;
    int cols;
    int remote;
};

typedef struct {
    int *aiIndent; /* Array of indents used in MODE_Explain */
    int nIndent;   /* Size of array aiIndent[] */
} IndentInfo;

void explain_data_prepare(IndentInfo *p, Vdbe *v);
void explain_data_delete(IndentInfo *p);

int print_cursor_description(struct sqlclntstate *clnt, strbuf *out, struct cursor_info *cinfo, int append_space);
void describe_cursor(Vdbe *v, int pc, struct cursor_info *cur);

#endif /* _SQLEXPLAIN_H_ */
