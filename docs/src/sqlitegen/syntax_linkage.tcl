set syntax_linkage(alter-table) {table-options sql-stmt}
set syntax_linkage(alter-table-ddl) {{column-constraint expr foreign-key-def index-column-list} {}}
set syntax_linkage(analyze) {{} sql-stmt}
set syntax_linkage(begin-stmt) {{} sql-stmt}
set syntax_linkage(column-constraint) {{expr foreign-key-def signed-number} alter-table-ddl}
set syntax_linkage(comment-syntax) {{} {}}
set syntax_linkage(commit-stmt) {{} sql-stmt}
set syntax_linkage(common-table-expression) {select-stmt {compound-select-stmt factored-select-stmt select-stmt simple-select-stmt}}
set syntax_linkage(compound-operator) {{} {factored-select-stmt select-stmt}}
set syntax_linkage(compound-select-stmt) {{common-table-expression expr ordering-term select-core} {}}
set syntax_linkage(constant-section) {{} schema}
set syntax_linkage(constraint-section) {{} schema}
set syntax_linkage(create-index) {{expr index-column-list} {}}
set syntax_linkage(create-lua-func) {{} sql-stmt}
set syntax_linkage(create-proc) {{} sql-stmt}
set syntax_linkage(create-table) {table-options sql-stmt}
set syntax_linkage(create-table-ddl) {{table-constraint table-options} {}}
set syntax_linkage(create-time-part) {{} sql-stmt}
set syntax_linkage(create-trigger) {table-event {}}
set syntax_linkage(cte-table-name) {qualified-table-name {recursive-cte with-clause}}
set syntax_linkage(delete-stmt-limited) {{expr ordering-term qualified-table-name with-clause} sql-stmt}
set syntax_linkage(drop) {{} sql-stmt}
set syntax_linkage(drop-index) {{} {}}
set syntax_linkage(exec-procedure) {{} sql-stmt}
set syntax_linkage(expr) {{filter literal-value select-stmt type-name window-defn} {alter-table-ddl column-constraint compound-select-stmt create-index delete-stmt-limited factored-select-stmt filter frame-spec function-invocation insert-stmt join-constraint key-section ordering-term replace-stmt result-column select-core select-stmt simple-select-stmt table-constraint table-or-subquery update-stmt-limited upsert-clause window-defn window-function-invocation}}
set syntax_linkage(factored-select-stmt) {{common-table-expression compound-operator expr ordering-term select-core} {}}
set syntax_linkage(filter) {expr {expr window-function-invocation}}
set syntax_linkage(foreign-key-def) {index-column-list {alter-table-ddl column-constraint table-constraint}}
set syntax_linkage(frame-spec) {expr window-defn}
set syntax_linkage(function-invocation) {expr {}}
set syntax_linkage(get) {{} sql-stmt}
set syntax_linkage(grant-revoke) {{} sql-stmt}
set syntax_linkage(host-config) {{} sql-stmt}
set syntax_linkage(idxexpr-type) {{} key-section}
set syntax_linkage(index-column-list) {{} {alter-table-ddl create-index foreign-key-def table-constraint}}
set syntax_linkage(insert-stmt) {{expr select-stmt upsert-clause with-clause} sql-stmt}
set syntax_linkage(join-clause) {{join-constraint join-operator table-or-subquery} {select-core select-stmt table-or-subquery}}
set syntax_linkage(join-constraint) {expr join-clause}
set syntax_linkage(join-operator) {{} join-clause}
set syntax_linkage(key-section) {{expr idxexpr-type} schema}
set syntax_linkage(literal-value) {{} expr}
set syntax_linkage(numeric-literal) {{} {}}
set syntax_linkage(ordering-term) {expr {compound-select-stmt delete-stmt-limited factored-select-stmt select-stmt simple-select-stmt update-stmt-limited window-defn}}
set syntax_linkage(put) {{} sql-stmt}
set syntax_linkage(qualified-table-name) {{} {cte-table-name delete-stmt-limited result-column table-or-subquery update-stmt-limited}}
set syntax_linkage(rebuild) {{} sql-stmt}
set syntax_linkage(recursive-cte) {cte-table-name {}}
set syntax_linkage(replace-stmt) {{expr select-stmt with-clause} {}}
set syntax_linkage(result-column) {{expr qualified-table-name} {select-core select-stmt}}
set syntax_linkage(rollback-stmt) {{} sql-stmt}
set syntax_linkage(schema) {{constant-section constraint-section key-section schema-section} {}}
set syntax_linkage(schema-section) {{} schema}
set syntax_linkage(select-core) {{expr join-clause result-column table-or-subquery window-defn} {compound-select-stmt factored-select-stmt simple-select-stmt}}
set syntax_linkage(select-stmt) {{common-table-expression compound-operator expr join-clause ordering-term result-column table-or-subquery window-defn} {common-table-expression expr insert-stmt replace-stmt sql-stmt table-or-subquery with-clause}}
set syntax_linkage(set-stmt) {{} sql-stmt}
set syntax_linkage(signed-number) {{} {column-constraint type-name}}
set syntax_linkage(simple-select-stmt) {{common-table-expression expr ordering-term select-core} {}}
set syntax_linkage(sql-stmt) {{alter-table analyze begin-stmt commit-stmt create-lua-func create-proc create-table create-time-part delete-stmt-limited drop exec-procedure get grant-revoke host-config insert-stmt put rebuild rollback-stmt select-stmt set-stmt truncate update-stmt-limited} {}}
set syntax_linkage(table-constraint) {{expr foreign-key-def index-column-list} create-table-ddl}
set syntax_linkage(table-event) {{} create-trigger}
set syntax_linkage(table-options) {{} {alter-table create-table create-table-ddl}}
set syntax_linkage(table-or-subquery) {{expr join-clause qualified-table-name select-stmt} {join-clause select-core select-stmt}}
set syntax_linkage(truncate) {{} sql-stmt}
set syntax_linkage(type-name) {signed-number expr}
set syntax_linkage(update-stmt-limited) {{expr ordering-term qualified-table-name with-clause} sql-stmt}
set syntax_linkage(upsert-clause) {expr insert-stmt}
set syntax_linkage(window-defn) {{expr frame-spec ordering-term} {expr select-core select-stmt window-function-invocation}}
set syntax_linkage(window-function-invocation) {{expr filter window-defn} {}}
set syntax_linkage(with-clause) {{cte-table-name select-stmt} {delete-stmt-limited insert-stmt replace-stmt update-stmt-limited}}
set syntax_order {sql-stmt begin-stmt commit-stmt rollback-stmt type-name signed-number with-clause cte-table-name recursive-cte common-table-expression delete-stmt-limited expr literal-value numeric-literal insert-stmt upsert-clause replace-stmt select-stmt join-clause select-core factored-select-stmt simple-select-stmt compound-select-stmt table-or-subquery result-column join-operator join-constraint ordering-term compound-operator update-stmt-limited qualified-table-name comment-syntax filter window-defn frame-spec function-invocation window-function-invocation create-table alter-table table-options create-proc create-trigger table-event create-lua-func create-time-part drop truncate analyze grant-revoke rebuild get put set-stmt exec-procedure host-config constant-section schema schema-section key-section idxexpr-type constraint-section table-event create-table-ddl column-constraint table-constraint foreign-key-def index-column-list alter-table-ddl create-index drop-index}
