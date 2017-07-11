SELECT COUNT(*) AS KEYWORDS_COUNT FROM comdb2_keywords;
SELECT COUNT(*) AS RESERVED_KW FROM comdb2_keywords WHERE reserved = 'Y';
SELECT COUNT(*) AS NONRESERVED_KW FROM comdb2_keywords WHERE reserved = 'N';
SELECT * FROM comdb2_keywords WHERE reserved = 'Y' ORDER BY name;
SELECT * FROM comdb2_keywords WHERE reserved = 'N' ORDER BY name;
