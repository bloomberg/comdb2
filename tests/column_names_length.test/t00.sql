create procedure col_names_func version 'v1' {
local function main()
    local dbtable, rc = db:prepare("SELECT 1 AS thequickbrownfoxjumpsoverthelazydog;")
    dbtable:emit()
end
}$$


SELECT "Test tunable alone";
PUT TUNABLE return_long_column_names 0;
SELECT 1 AS thequickbrownfoxjumpsoverthelazydog;
EXEC PROCEDURE col_names_func();
PUT TUNABLE return_long_column_names 1;
SELECT 1 AS thequickbrownfoxjumpsoverthelazydog;
EXEC PROCEDURE col_names_func();
PUT TUNABLE return_long_column_names 0;

SELECT "Test set stmt alone";
SET return_long_column_names OFF;
SELECT 1 AS thequickbrownfoxjumpsoverthelazydog;
EXEC PROCEDURE col_names_func();
SET return_long_column_names ON;
SELECT 1 AS thequickbrownfoxjumpsoverthelazydog;
EXEC PROCEDURE col_names_func();
SET return_long_column_names OFF;

SELECT "Test all combinations";
PUT TUNABLE return_long_column_names 1;
SET return_long_column_names OFF;
SELECT 1 AS thequickbrownfoxjumpsoverthelazydog;
EXEC PROCEDURE col_names_func();

PUT TUNABLE return_long_column_names 0;
SET return_long_column_names ON;
SELECT 1 AS thequickbrownfoxjumpsoverthelazydog;
EXEC PROCEDURE col_names_func();

PUT TUNABLE return_long_column_names 1;
SET return_long_column_names ON;
SELECT 1 AS thequickbrownfoxjumpsoverthelazydog;
EXEC PROCEDURE col_names_func();

PUT TUNABLE return_long_column_names 0;
SET return_long_column_names OFF;
SELECT 1 AS thequickbrownfoxjumpsoverthelazydog;
EXEC PROCEDURE col_names_func();
