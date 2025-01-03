create procedure col_names_func version 'v1' {
local function main()
    local dbtable, rc = db:prepare("SELECT * from t;")
    dbtable:emit()
end
}$$

PUT TUNABLE return_long_column_names 1;
CREATE TABLE t(thequickbrownfoxjumpsoverthelazydogthequickbrownfoxjumpsoverthelazydogthequickbrownfoxjumpsoverthelazydog108 int)$$
INSERT INTO t VALUES(1);
-- should truncate column name to 99 chars
SELECT * FROM t;
EXEC PROCEDURE col_names_func();
