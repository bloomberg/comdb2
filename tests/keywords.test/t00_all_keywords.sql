SELECT COUNT(*) AS KEYWORDS_COUNT FROM comdb2_keywords;
SELECT COUNT(*) AS RESERVED_KW FROM comdb2_keywords WHERE reserved = 'YES';
SELECT COUNT(*) AS NONRESERVED_KW FROM comdb2_keywords WHERE reserved = 'NO';
SELECT * FROM comdb2_keywords WHERE reserved = 'YES' ORDER BY name;
SELECT * FROM comdb2_keywords WHERE reserved = 'NO' ORDER BY name;
