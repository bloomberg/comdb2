# "set multiline on" must keep the trailing semicolon, which the server requires
SET MULTILINE ON
SELECT 1;
SELECT 2 ;  
SET MULTILINE OFF;
SELECT 3;
SELECT 4
