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

#ifndef INCLUDED_AST_H
#define INCLUDED_AST_H

struct ast;
typedef struct ast ast_t;

enum ast_type {
    AST_TYPE_INVALID = 0,
    AST_TYPE_SELECT = 1,
    AST_TYPE_INSERT = 2,
    AST_TYPE_UNION = 3,
    AST_TYPE_IN = 4,
    AST_TYPE_DELETE = 5,
    AST_TYPE_UPDATE = 6,
    AST_TYPE_CREATE = 7,
    AST_TYPE_DROP = 8
};

struct Vdbe;
struct sqlite3;
struct Parse;
ast_t *ast_init(struct Parse *pParse, const char *caller);
int ast_push(ast_t *ast, enum ast_type op, struct Vdbe *v, void *obj);
void ast_destroy(ast_t **ast, struct sqlite3 *db);
void ast_print(ast_t *ast);

#endif
