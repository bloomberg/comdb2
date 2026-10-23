---
title: Schema definition language
keywords: code
sidebar: mydoc_sidebar
permalink: table_schema.html
---

## Overview

Comdb2 provides two ways to define or modify a table schema. A declarative
language (discussed here) and the standard data definition language, like
other relational database systems. The support for latter was added in version
7.0, discussed in [SQL language](sql.html) in more details.

In the first approach, Comdb2 supports a language that is declarative rather
than imperative. The entire schema per table is presented to the database at
once.  It's up to the database to figure out which indices to add/remove/modify,
which fields to grow, shrink, etc.  A table definition has a several sections:

* [Constants](#constants-section)

* [Table definition](#schema-section)

* [Keys](#key-section)

* [Constraints](#constraints-section)

Comments begin with ```//``` and go until the end of the line. Multi-line
comments enclosed in ```/* ... */``` are also supported. Lines end with a
LF character (0x0A); CR+LF pairs (0x0D 0x0A) are not supported.

## Constants section

<svg role="img" aria-label="constants" xmlns="http://www.w3.org/2000/svg" width="295" height="48" viewBox="0 0 354 57">
<title>constant-section</title><rect width="100%" height="100%" fill="white"/><path fill="none" stroke="black" stroke-width="2" d="M3,21 A3,3 0 1,0 9,21 A3,3 0 1,0 3,21 Z M56,8 A12,12 0 0,0 56,32 M101,32 A12,12 0 0,0 101,8 M56,8 L101,8 M56,32 L101,32 M148,5 A15,15 0 0,0 148,35 M152,35 A15,15 0 0,0 152,5 M148,5 L152,5 M148,35 L152,35 M114,21 L126.5,21 M199,8 A12,12 0 0,0 199,32 M305,32 A12,12 0 0,0 305,8 M199,8 L305,8 M199,32 L305,32 M168,21 L180.5,21 M169,50 L186,50 M43,50 L169,50 M191.5,50 L319,50 M26,21 L43,21 M43,21 A9,9 0 0,0 34,30 M34,30 L34,41 M34,41 A9,9 0 0,0 43,50 M319,21 A9,9 0 0,1 328,30 M328,30 L328,41 M328,41 A9,9 0 0,1 319,50 M319,21 L328,21 M8,21 L20.5,21 M345,21 A3,3 0 1,0 351,21 A3,3 0 1,0 345,21 Z M327,21 L339.5,21"/><path fill="black" d="M132,21 L122,17 L123.5,20 L123.5,22 L122,25 Z M186,21 L176,17 L177.5,20 L177.5,22 L176,25 Z M186,50 L196,54 L194.5,51 L194.5,49 L196,46 Z M26,21 L16,17 L17.5,20 L17.5,22 L16,25 Z M345,21 L335,17 L336.5,20 L336.5,22 L335,25 Z"/><g font-family="Helvetica, Arial, 'Liberation Sans', 'Nimbus Sans', sans-serif" font-weight="bold" text-anchor="middle"><text x="78" y="24.49" font-size="14" textLength="36.57" lengthAdjust="spacingAndGlyphs">name</text><text x="150" y="24.98" font-size="16" textLength="9.34" lengthAdjust="spacingAndGlyphs">=</text><text x="252" y="24.49" font-size="14" textLength="88.69" lengthAdjust="spacingAndGlyphs">integer-literal</text></g>
</svg>

Users can specify constant values in this section to use in the rest of the definition.  This is occasionally helpful when several fields should
be sized the same.  This practice is of limited utility.  The constants section consists of the keyword `constants` and a list of
constants enclosed by braces.  Example:

```
constants {
    ZERO=0,
    ONE=1,
    TWO=2,
    THREE=3,
    SEVEN=7,
    HELLO=-44,
    TWELVE=12
}
```

The constraint section is optional.

## Schema section

<svg role="img" aria-label="schema" xmlns="http://www.w3.org/2000/svg" width="1080" height="449" viewBox="0 0 1296 539">
<title>schema-section</title><rect width="100%" height="100%" fill="white"/><path fill="none" stroke="black" stroke-width="2" d="M3,18 A3,3 0 1,0 9,18 A3,3 0 1,0 3,18 Z M100,5 H164 V29 H100 Z M100,35 H144 V59 H100 Z M100,65 H140 V89 H100 Z M100,95 H122 V119 H100 Z M100,125 H174 V149 H100 Z M100,155 H178 V179 H100 Z M100,185 H197 V209 H100 Z M100,215 H183 V239 H100 Z M100,245 H205 V269 H100 Z M100,275 H191 V299 H100 Z M100,305 H189 V329 H100 Z M100,335 H189 V359 H100 Z M100,365 H199 V389 H100 Z M71,18 L93.5,18 M165,18 L265,18 M71,18 A9,9 0 0,1 80,27 M89,48 L93.5,48 M145,48 L240.5,48 M80,39 A9,9 0 0,0 89,48 M246,48 A9,9 0 0,0 255,39 M89,78 L93.5,78 M141,78 L240.5,78 M80,69 A9,9 0 0,0 89,78 M246,78 A9,9 0 0,0 255,69 M89,108 L93.5,108 M123,108 L240.5,108 M80,99 A9,9 0 0,0 89,108 M246,108 A9,9 0 0,0 255,99 M89,138 L93.5,138 M175,138 L240.5,138 M80,129 A9,9 0 0,0 89,138 M246,138 A9,9 0 0,0 255,129 M89,168 L93.5,168 M179,168 L240.5,168 M80,159 A9,9 0 0,0 89,168 M246,168 A9,9 0 0,0 255,159 M89,198 L93.5,198 M198,198 L240.5,198 M80,189 A9,9 0 0,0 89,198 M246,198 A9,9 0 0,0 255,189 M89,228 L93.5,228 M184,228 L240.5,228 M80,219 A9,9 0 0,0 89,228 M246,228 A9,9 0 0,0 255,219 M89,258 L93.5,258 M206,258 L240.5,258 M80,249 A9,9 0 0,0 89,258 M246,258 A9,9 0 0,0 255,249 M89,288 L93.5,288 M192,288 L240.5,288 M80,279 A9,9 0 0,0 89,288 M246,288 A9,9 0 0,0 255,279 M89,318 L93.5,318 M190,318 L240.5,318 M80,309 A9,9 0 0,0 89,318 M246,318 A9,9 0 0,0 255,309 M89,348 L93.5,348 M190,348 L240.5,348 M80,339 A9,9 0 0,0 89,348 M246,348 A9,9 0 0,0 255,339 M89,378 L93.5,378 M200,378 L240.5,378 M80,369 A9,9 0 0,0 89,378 M246,378 A9,9 0 0,0 255,369 M255,27 A9,9 0 0,1 264,18 M80,369 L80,27 M255,369 L255,27 M294,5 A12,12 0 0,0 294,29 M407,29 A12,12 0 0,0 407,5 M294,5 L407,5 M294,29 L407,29 M263,18 L275.5,18 M72,399 H131 V423 H72 Z M162,399 A12,12 0 0,0 162,423 M275,423 A12,12 0 0,0 275,399 M162,399 L275,399 M162,423 L275,423 M131,412 L143.5,412 M322,396 A15,15 0 0,0 322,426 M322,426 A15,15 0 0,0 322,396 M288,412 L300.5,412 M357,399 H390 V423 H357 Z M338,412 L350.5,412 M424,396 A15,15 0 0,0 424,426 M424,426 A15,15 0 0,0 424,396 M390,412 L402.5,412 M95,435 H139 V459 H95 Z M100,465 H138 V489 H100 Z M100,495 H139 V519 H100 Z M71,448 L88.5,448 M140,448 L177,448 M71,448 A9,9 0 0,1 80,457 M89,478 L93.5,478 M139,478 L152.5,478 M80,469 A9,9 0 0,0 89,478 M158,478 A9,9 0 0,0 167,469 M89,508 L93.5,508 M140,508 L152.5,508 M80,499 A9,9 0 0,0 89,508 M158,508 A9,9 0 0,0 167,499 M167,457 A9,9 0 0,1 176,448 M80,499 L80,457 M167,499 L167,457 M206,435 A12,12 0 0,0 206,459 M319,459 A12,12 0 0,0 319,435 M206,435 L319,435 M206,459 L319,459 M442,448 L443,448 M394,457 A15,15 0 0,0 394,487 M394,487 A15,15 0 0,0 394,457 M429,460 H462 V484 H429 Z M410,473 L422.5,473 M496,457 A15,15 0 0,0 496,487 M496,487 A15,15 0 0,0 496,457 M462,473 L474.5,473 M350,448 L436.5,448 M443,448 L543,448 M350,448 A9,9 0 0,1 359,457 M368,473 L372.5,473 M513,473 L518.5,473 M359,464 A9,9 0 0,0 368,473 M524,473 A9,9 0 0,0 533,464 M533,457 A9,9 0 0,1 542,448 M359,464 L359,457 M533,464 L533,457 M332,448 L344.5,448 M175,448 L187.5,448 M43,18 L65.5,18 M421,18 L578,18 M43,18 A9,9 0 0,1 52,27 M61,412 L65.5,412 M441,412 L553.5,412 M52,403 A9,9 0 0,0 61,412 M559,412 A9,9 0 0,0 568,403 M61,448 L65.5,448 M542,448 L553.5,448 M52,439 A9,9 0 0,0 61,448 M559,448 A9,9 0 0,0 568,439 M568,27 A9,9 0 0,1 577,18 M52,439 L52,27 M568,439 L568,27 M790,18 L791,18 M623,31 H691 V55 H623 Z M725,28 A15,15 0 0,0 725,58 M729,58 A15,15 0 0,0 729,28 M725,28 L729,28 M725,58 L729,58 M691,44 L703.5,44 M782,31 H904 V55 H782 Z M804,63 A12,12 0 0,0 804,87 M896,87 A12,12 0 0,0 896,63 M804,63 L896,63 M804,87 L896,87 M763,44 L781,44 M905,44 L954,44 M763,44 A9,9 0 0,1 772,53 M781,76 L785.5,76 M910,76 L929.5,76 M772,67 A9,9 0 0,0 781,76 M935,76 A9,9 0 0,0 944,67 M944,53 A9,9 0 0,1 953,44 M772,67 L772,53 M944,67 L944,53 M745,44 L757.5,44 M594,18 L784.5,18 M791,18 L994,18 M594,18 A9,9 0 0,1 603,27 M612,44 L616.5,44 M953,44 L969.5,44 M603,35 A9,9 0 0,0 612,44 M975,44 A9,9 0 0,0 984,35 M984,27 A9,9 0 0,1 993,18 M603,35 L603,27 M984,35 L984,27 M1132,18 L1133,18 M1039,31 H1070 V55 H1039 Z M1104,28 A15,15 0 0,0 1104,58 M1108,58 A15,15 0 0,0 1108,28 M1104,28 L1108,28 M1104,58 L1108,58 M1070,44 L1082.5,44 M1165,31 H1195 V55 H1165 Z M1171,61 H1194 V85 H1171 Z M1142,44 L1158.5,44 M1196,44 L1226,44 M1142,44 A9,9 0 0,1 1151,53 M1160,74 L1164.5,74 M1195,74 L1207,74 M1151,65 A9,9 0 0,0 1160,74 M1207,74 A9,9 0 0,0 1216,65 M1216,53 A9,9 0 0,1 1225,44 M1151,65 L1151,53 M1216,65 L1216,53 M1124,44 L1136.5,44 M1010,18 L1126.5,18 M1133,18 L1262,18 M1010,18 A9,9 0 0,1 1019,27 M1028,44 L1032.5,44 M1225,44 L1237.5,44 M1019,35 A9,9 0 0,0 1028,44 M1243,44 A9,9 0 0,0 1252,35 M1252,27 A9,9 0 0,1 1261,18 M1019,35 L1019,27 M1252,35 L1252,27 M992,18 L1004.5,18 M576,18 L588.5,18 M640,532 L657,532 M43,532 L640,532 M662.5,532 L1261,532 M26,18 L43,18 M43,18 A9,9 0 0,0 34,27 M34,523 L34,280.5 M34,275 L34,27 M34,523 A9,9 0 0,0 43,532 M1261,18 A9,9 0 0,1 1270,27 M1270,27 L1270,269.5 M1270,275 L1270,523 M1270,523 A9,9 0 0,1 1261,532 M1261,18 L1270,18 M8,18 L20.5,18 M1287,18 A3,3 0 1,0 1293,18 A3,3 0 1,0 1287,18 Z M1269,18 L1281.5,18"/><path fill="black" d="M99,18 L89,14 L90.5,17 L90.5,19 L89,22 Z M99,48 L89,44 L90.5,47 L90.5,49 L89,52 Z M246,48 L236,44 L237.5,47 L237.5,49 L236,52 Z M99,78 L89,74 L90.5,77 L90.5,79 L89,82 Z M246,78 L236,74 L237.5,77 L237.5,79 L236,82 Z M99,108 L89,104 L90.5,107 L90.5,109 L89,112 Z M246,108 L236,104 L237.5,107 L237.5,109 L236,112 Z M99,138 L89,134 L90.5,137 L90.5,139 L89,142 Z M246,138 L236,134 L237.5,137 L237.5,139 L236,142 Z M99,168 L89,164 L90.5,167 L90.5,169 L89,172 Z M246,168 L236,164 L237.5,167 L237.5,169 L236,172 Z M99,198 L89,194 L90.5,197 L90.5,199 L89,202 Z M246,198 L236,194 L237.5,197 L237.5,199 L236,202 Z M99,228 L89,224 L90.5,227 L90.5,229 L89,232 Z M246,228 L236,224 L237.5,227 L237.5,229 L236,232 Z M99,258 L89,254 L90.5,257 L90.5,259 L89,262 Z M246,258 L236,254 L237.5,257 L237.5,259 L236,262 Z M99,288 L89,284 L90.5,287 L90.5,289 L89,292 Z M246,288 L236,284 L237.5,287 L237.5,289 L236,292 Z M99,318 L89,314 L90.5,317 L90.5,319 L89,322 Z M246,318 L236,314 L237.5,317 L237.5,319 L236,322 Z M99,348 L89,344 L90.5,347 L90.5,349 L89,352 Z M246,348 L236,344 L237.5,347 L237.5,349 L236,352 Z M99,378 L89,374 L90.5,377 L90.5,379 L89,382 Z M246,378 L236,374 L237.5,377 L237.5,379 L236,382 Z M281,18 L271,14 L272.5,17 L272.5,19 L271,22 Z M149,412 L139,408 L140.5,411 L140.5,413 L139,416 Z M306,412 L296,408 L297.5,411 L297.5,413 L296,416 Z M356,412 L346,408 L347.5,411 L347.5,413 L346,416 Z M408,412 L398,408 L399.5,411 L399.5,413 L398,416 Z M94,448 L84,444 L85.5,447 L85.5,449 L84,452 Z M99,478 L89,474 L90.5,477 L90.5,479 L89,482 Z M158,478 L148,474 L149.5,477 L149.5,479 L148,482 Z M99,508 L89,504 L90.5,507 L90.5,509 L89,512 Z M158,508 L148,504 L149.5,507 L149.5,509 L148,512 Z M428,473 L418,469 L419.5,472 L419.5,474 L418,477 Z M480,473 L470,469 L471.5,472 L471.5,474 L470,477 Z M442,448 L432,444 L433.5,447 L433.5,449 L432,452 Z M378,473 L368,469 L369.5,472 L369.5,474 L368,477 Z M524,473 L514,469 L515.5,472 L515.5,474 L514,477 Z M350,448 L340,444 L341.5,447 L341.5,449 L340,452 Z M193,448 L183,444 L184.5,447 L184.5,449 L183,452 Z M71,18 L61,14 L62.5,17 L62.5,19 L61,22 Z M71,412 L61,408 L62.5,411 L62.5,413 L61,416 Z M559,412 L549,408 L550.5,411 L550.5,413 L549,416 Z M71,448 L61,444 L62.5,447 L62.5,449 L61,452 Z M559,448 L549,444 L550.5,447 L550.5,449 L549,452 Z M709,44 L699,40 L700.5,43 L700.5,45 L699,48 Z M791,76 L781,72 L782.5,75 L782.5,77 L781,80 Z M935,76 L925,72 L926.5,75 L926.5,77 L925,80 Z M763,44 L753,40 L754.5,43 L754.5,45 L753,48 Z M790,18 L780,14 L781.5,17 L781.5,19 L780,22 Z M622,44 L612,40 L613.5,43 L613.5,45 L612,48 Z M975,44 L965,40 L966.5,43 L966.5,45 L965,48 Z M1088,44 L1078,40 L1079.5,43 L1079.5,45 L1078,48 Z M1164,44 L1154,40 L1155.5,43 L1155.5,45 L1154,48 Z M1170,74 L1160,70 L1161.5,73 L1161.5,75 L1160,78 Z M1142,44 L1132,40 L1133.5,43 L1133.5,45 L1132,48 Z M1132,18 L1122,14 L1123.5,17 L1123.5,19 L1122,22 Z M1038,44 L1028,40 L1029.5,43 L1029.5,45 L1028,48 Z M1243,44 L1233,40 L1234.5,43 L1234.5,45 L1233,48 Z M1010,18 L1000,14 L1001.5,17 L1001.5,19 L1000,22 Z M594,18 L584,14 L585.5,17 L585.5,19 L584,22 Z M657,532 L667,536 L665.5,533 L665.5,531 L667,528 Z M34,275 L30,285 L33,283.5 L35,283.5 L38,285 Z M1270,275 L1274,265 L1271,266.5 L1269,266.5 L1266,265 Z M26,18 L16,14 L17.5,17 L17.5,19 L16,22 Z M1287,18 L1277,14 L1278.5,17 L1278.5,19 L1277,22 Z"/><g font-family="Helvetica, Arial, 'Liberation Sans', 'Nimbus Sans', sans-serif" font-weight="bold" text-anchor="middle"><text x="132" y="21.49" font-size="14" textLength="51.34" lengthAdjust="spacingAndGlyphs">u_short</text><text x="122" y="51.49" font-size="14" textLength="35" lengthAdjust="spacingAndGlyphs">short</text><text x="120" y="81.49" font-size="14" textLength="33.45" lengthAdjust="spacingAndGlyphs">u_int</text><text x="111" y="111.49" font-size="14" textLength="17.11" lengthAdjust="spacingAndGlyphs">int</text><text x="137" y="141.49" font-size="14" textLength="59.11" lengthAdjust="spacingAndGlyphs">longlong</text><text x="139" y="171.49" font-size="14" textLength="57.57" lengthAdjust="spacingAndGlyphs">datetime</text><text x="148" y="201.49" font-size="14" textLength="73.91" lengthAdjust="spacingAndGlyphs">datetimeus</text><text x="141" y="231.49" font-size="14" textLength="66.14" lengthAdjust="spacingAndGlyphs">intervalds</text><text x="152" y="261.49" font-size="14" textLength="82.47" lengthAdjust="spacingAndGlyphs">intervaldsus</text><text x="145" y="291.49" font-size="14" textLength="70.03" lengthAdjust="spacingAndGlyphs">intervalym</text><text x="144" y="321.49" font-size="14" textLength="67.7" lengthAdjust="spacingAndGlyphs">decimal32</text><text x="144" y="351.49" font-size="14" textLength="67.7" lengthAdjust="spacingAndGlyphs">decimal64</text><text x="149" y="381.49" font-size="14" textLength="75.49" lengthAdjust="spacingAndGlyphs">decimal128</text><text x="350" y="21.49" font-size="14" textLength="91.01" lengthAdjust="spacingAndGlyphs">column-name</text><text x="101" y="415.49" font-size="14" textLength="46.68" lengthAdjust="spacingAndGlyphs">cstring</text><text x="218" y="415.49" font-size="14" textLength="91.01" lengthAdjust="spacingAndGlyphs">column-name</text><text x="322" y="415.98" font-size="16" textLength="5.33" lengthAdjust="spacingAndGlyphs">[</text><text x="373" y="415.49" font-size="14" textLength="26.46" lengthAdjust="spacingAndGlyphs">size</text><text x="424" y="415.98" font-size="16" textLength="5.33" lengthAdjust="spacingAndGlyphs">]</text><text x="117" y="451.49" font-size="14" textLength="33.45" lengthAdjust="spacingAndGlyphs">vutf8</text><text x="119" y="481.49" font-size="14" textLength="29.55" lengthAdjust="spacingAndGlyphs">blob</text><text x="119" y="511.49" font-size="14" textLength="28.78" lengthAdjust="spacingAndGlyphs">byte</text><text x="262" y="451.49" font-size="14" textLength="91.01" lengthAdjust="spacingAndGlyphs">column-name</text><text x="394" y="476.98" font-size="16" textLength="5.33" lengthAdjust="spacingAndGlyphs">[</text><text x="445" y="476.49" font-size="14" textLength="26.46" lengthAdjust="spacingAndGlyphs">size</text><text x="496" y="476.98" font-size="16" textLength="5.33" lengthAdjust="spacingAndGlyphs">]</text><text x="657" y="47.49" font-size="14" textLength="51.34" lengthAdjust="spacingAndGlyphs">dbstore</text><text x="727" y="47.98" font-size="16" textLength="9.34" lengthAdjust="spacingAndGlyphs">=</text><text x="843" y="47.49" font-size="14" textLength="93.37" lengthAdjust="spacingAndGlyphs">nextsequence</text><text x="850" y="79.49" font-size="14" textLength="77.81" lengthAdjust="spacingAndGlyphs">literal-value</text><text x="1054" y="47.49" font-size="14" textLength="24.89" lengthAdjust="spacingAndGlyphs">null</text><text x="1106" y="47.98" font-size="16" textLength="9.34" lengthAdjust="spacingAndGlyphs">=</text><text x="1180" y="47.49" font-size="14" textLength="23.35" lengthAdjust="spacingAndGlyphs">yes</text><text x="1182" y="77.49" font-size="14" textLength="17.11" lengthAdjust="spacingAndGlyphs">no</text></g>
</svg>

The table definition lists the fields present in the table, along with their default values whether NULL values
are allowed.  NULLs are not allowed by default.  The section is indicated with the ```schema``` keyword, and the list
of fields enclosed by braces.

Field types are explained in the [data types](datatypes.html) section.  A field is defined by listing its type, 
name, size (if allowed/required by type), followed by optional field keywords.

An example table definition looks like this:

```
schema {
    cstring      first_name[32]                    // first name
    cstring      last_name[32]
    int          userid
    longlong     sequence    dbstore=nextsequence
    double       balance     dbstore=100.00 null=yes
    datetime     paydate     dbstore={CURRENT_TIMESTAMP}
    byte         autoid[16]  dbstore={GUID()}
    byte         permissions[12]
}

keys {
    "PK" = autoid
}

```

The field keywords are:

* ```null=<yes|no>```
  ```null=no``` is the implied value.  ```null=no``` means this column will not accept values of NULL.  
  ```null=yes``` means this column will accept values of NULL.
* ```dbstore=<numeric, string or hex literal>``` this is the value that will be stored for this column if 
  no value for the column was provided by the client. (for example, if a client added a record through a user defined tag which omitted a column).
* ```dbpad=<integer literal>```
    Only valid for byte arrays.  If a smaller byte array needs to be converted to a larger byte array it will be padded with this value.  If a larger byte array needs to be truncated then it can be truncated **provided** that the lost bytes match this value.  Value given should be an integer in the range 0..255.

The dbstore value must have the same datatype as the column they are attached to, with these exceptions:

* Blob fields cannot have dbstore values.
* For datetime fields you can specify a string such as "2017-03-08T235959.987 America/New_York" or dbstore={CURRENT_TIMESTAMP} to autofill with current database system timestamp.
* For longlong integer fields may specify a dbstore of nextsequence.  This will populate the column with a value one greater than the largest value which has ever been seen for the column.
* For byte arrays you can specify dbstore=0 to indicate that zero should be default if not specified.
* If the type is byte[16], you can also specify `dbstore={GUID()}` and that will allow the field to be autofilled by the database at the time of record insertion.

The schema section is required - it's the only required section.

## Key section

<svg role="img" aria-label="keys" xmlns="http://www.w3.org/2000/svg" width="818" height="314" viewBox="0 0 981 377">
<title>key-section</title><rect width="100%" height="100%" fill="white"/><path fill="none" stroke="black" stroke-width="2" d="M3,21 A3,3 0 1,0 9,21 A3,3 0 1,0 3,21 Z M116,21 L117,21 M100,29 H134 V53 H100 Z M71,21 L110.5,21 M117,21 L170,21 M71,21 A9,9 0 0,1 80,30 M89,42 L93.5,42 M135,42 L145.5,42 M80,33 A9,9 0 0,0 89,42 M151,42 A9,9 0 0,0 160,33 M160,30 A9,9 0 0,1 169,21 M80,33 L80,30 M160,33 L160,30 M146,62 L147,62 M100,70 H178 V94 H100 Z M71,62 L140.5,62 M147,62 L230,62 M71,62 A9,9 0 0,1 80,71 M89,83 L93.5,83 M179,83 L205.5,83 M80,74 A9,9 0 0,0 89,83 M211,83 A9,9 0 0,0 220,74 M220,71 A9,9 0 0,1 229,62 M80,74 L80,71 M220,74 L220,71 M43,21 L65.5,21 M169,21 L265,21 M43,21 A9,9 0 0,1 52,30 M61,62 L65.5,62 M229,62 L240.5,62 M52,53 A9,9 0 0,0 61,62 M246,62 A9,9 0 0,0 255,53 M255,30 A9,9 0 0,1 264,21 M52,53 L52,30 M255,53 L255,30 M514,21 L515,21 M310,29 H390 V53 H310 Z M556,42 L557,42 M452,51 A15,15 0 0,0 452,81 M452,81 A15,15 0 0,0 452,51 M504,54 H623 V78 H504 Z M562,81 A15,15 0 0,0 562,111 M562,111 A15,15 0 0,0 562,81 M503,97 L546,97 M584.5,97 L624,97 M486,67 L503,67 M503,67 A9,9 0 0,0 494,76 M494,76 L494,88 M494,88 A9,9 0 0,0 503,97 M624,67 A9,9 0 0,1 633,76 M633,76 L633,88 M633,88 A9,9 0 0,1 624,97 M624,67 L633,67 M468,67 L480.5,67 M666,51 A15,15 0 0,0 666,81 M666,81 A15,15 0 0,0 666,51 M632,67 L644.5,67 M408,42 L550.5,42 M557,42 L713,42 M408,42 A9,9 0 0,1 417,51 M426,67 L430.5,67 M683,67 L688.5,67 M417,58 A9,9 0 0,0 426,67 M694,67 A9,9 0 0,0 703,58 M703,51 A9,9 0 0,1 712,42 M417,58 L417,51 M703,58 L703,51 M390,42 L402.5,42 M281,21 L508.5,21 M515,21 L755,21 M281,21 A9,9 0 0,1 290,30 M299,42 L303.5,42 M712,42 L730.5,42 M290,33 A9,9 0 0,0 299,42 M736,42 A9,9 0 0,0 745,33 M745,30 A9,9 0 0,1 754,21 M290,33 L290,30 M745,33 L745,30 M263,21 L275.5,21 M784,8 A12,12 0 0,0 784,32 M879,32 A12,12 0 0,0 879,8 M784,8 L879,8 M784,32 L879,32 M926,5 A15,15 0 0,0 926,35 M930,35 A15,15 0 0,0 930,5 M926,5 L930,5 M926,35 L930,35 M892,21 L904.5,21 M753,21 L765.5,21 M187,165 L188,165 M140,175 A15,15 0 0,0 140,205 M240,205 A15,15 0 0,0 240,175 M140,175 L240,175 M140,205 L240,205 M96,165 L181.5,165 M188,165 L287,165 M96,165 A9,9 0 0,1 105,174 M114,191 L118.5,191 M257,191 L262.5,191 M105,182 A9,9 0 0,0 114,191 M268,191 A9,9 0 0,0 277,182 M277,174 A9,9 0 0,1 286,165 M105,182 L105,174 M277,182 L277,174 M332,152 H451 V176 H332 Z M347,184 A15,15 0 0,0 347,214 M347,214 A15,15 0 0,0 347,184 M382,187 H491 V211 H382 Z M363,200 L375.5,200 M525,184 A15,15 0 0,0 525,214 M525,214 A15,15 0 0,0 525,184 M491,200 L503.5,200 M575,184 A15,15 0 0,0 575,214 M577,214 A15,15 0 0,0 577,184 M575,184 L577,184 M575,214 L577,214 M541,200 L553.5,200 M612,187 H652 V211 H612 Z M593,200 L605.5,200 M686,184 A15,15 0 0,0 686,214 M688,214 A15,15 0 0,0 688,184 M686,184 L688,184 M686,214 L688,214 M652,200 L664.5,200 M303,165 L325.5,165 M452,165 L735,165 M303,165 A9,9 0 0,1 312,174 M321,200 L325.5,200 M705,200 L710.5,200 M312,191 A9,9 0 0,0 321,200 M716,200 A9,9 0 0,0 725,191 M725,174 A9,9 0 0,1 734,165 M312,191 L312,174 M725,191 L725,174 M285,165 L297.5,165 M412,229 A15,15 0 0,0 412,259 M416,259 A15,15 0 0,0 416,229 M412,229 L416,229 M412,259 L416,259 M96,245 L396,245 M438.5,245 L734,245 M79,165 L96,165 M96,165 A9,9 0 0,0 87,174 M87,236 L87,210.5 M87,205 L87,174 M87,236 A9,9 0 0,0 96,245 M734,165 A9,9 0 0,1 743,174 M743,174 L743,199.5 M743,205 L743,236 M743,236 A9,9 0 0,1 734,245 M734,165 L743,165 M131,302 A15,15 0 0,0 131,332 M131,332 A15,15 0 0,0 131,302 M166,305 H220 V329 H166 Z M147,318 L159.5,318 M239,305 H279 V329 H239 Z M220,318 L232.5,318 M313,302 A15,15 0 0,0 313,332 M313,332 A15,15 0 0,0 313,302 M279,318 L291.5,318 M743,165 L761,165 M761,165 A9,9 0 0,1 770,174 M770,174 L770,217.5 M770,223 L770,272 M770,272 A9,9 0 0,1 761,281 M761,281 L434.5,281 M429,281 L97,281 M97,281 A9,9 0 0,0 88,290 M88,290 L88,309 M88,309 A9,9 0 0,0 97,318 M97,318 L109.5,318 M88,309 L88,329.5 M88,335 L88,344 M88,344 A9,9 0 0,0 97,353 M330,318 A9,9 0 0,1 339,327 M339,344 A9,9 0 0,0 348,353 M97,353 L207.5,353 M213,353 L348,353 M339,327 L339,338.5 M947,21 L965,21 M965,21 A9,9 0 0,1 974,30 M974,30 L974,70.5 M974,76 L974,123 M974,123 A9,9 0 0,1 965,132 M965,132 L518.5,132 M513,132 L61,132 M61,132 A9,9 0 0,0 52,141 M52,141 L52,156 M52,156 A9,9 0 0,0 61,165 M61,165 L73.5,165 M184,370 L201,370 M43,370 L184,370 M206.5,370 L348,370 M26,21 L43,21 M43,21 A9,9 0 0,0 34,30 M34,361 L34,200.5 M34,195 L34,30 M34,361 A9,9 0 0,0 43,370 M348,353 A8.5,8.5 0 0,1 348,370 M348,353 L357,353 M8,21 L20.5,21 M374,353 A3,3 0 1,0 380,353 A3,3 0 1,0 374,353 Z M356,353 L368.5,353"/><path fill="black" d="M116,21 L106,17 L107.5,20 L107.5,22 L106,25 Z M99,42 L89,38 L90.5,41 L90.5,43 L89,46 Z M151,42 L141,38 L142.5,41 L142.5,43 L141,46 Z M146,62 L136,58 L137.5,61 L137.5,63 L136,66 Z M99,83 L89,79 L90.5,82 L90.5,84 L89,87 Z M211,83 L201,79 L202.5,82 L202.5,84 L201,87 Z M71,21 L61,17 L62.5,20 L62.5,22 L61,25 Z M71,62 L61,58 L62.5,61 L62.5,63 L61,66 Z M246,62 L236,58 L237.5,61 L237.5,63 L236,66 Z M579,97 L589,101 L587.5,98 L587.5,96 L589,93 Z M486,67 L476,63 L477.5,66 L477.5,68 L476,71 Z M650,67 L640,63 L641.5,66 L641.5,68 L640,71 Z M556,42 L546,38 L547.5,41 L547.5,43 L546,46 Z M436,67 L426,63 L427.5,66 L427.5,68 L426,71 Z M694,67 L684,63 L685.5,66 L685.5,68 L684,71 Z M408,42 L398,38 L399.5,41 L399.5,43 L398,46 Z M514,21 L504,17 L505.5,20 L505.5,22 L504,25 Z M309,42 L299,38 L300.5,41 L300.5,43 L299,46 Z M736,42 L726,38 L727.5,41 L727.5,43 L726,46 Z M281,21 L271,17 L272.5,20 L272.5,22 L271,25 Z M910,21 L900,17 L901.5,20 L901.5,22 L900,25 Z M771,21 L761,17 L762.5,20 L762.5,22 L761,25 Z M187,165 L177,161 L178.5,164 L178.5,166 L177,169 Z M124,191 L114,187 L115.5,190 L115.5,192 L114,195 Z M268,191 L258,187 L259.5,190 L259.5,192 L258,195 Z M381,200 L371,196 L372.5,199 L372.5,201 L371,204 Z M509,200 L499,196 L500.5,199 L500.5,201 L499,204 Z M559,200 L549,196 L550.5,199 L550.5,201 L549,204 Z M611,200 L601,196 L602.5,199 L602.5,201 L601,204 Z M670,200 L660,196 L661.5,199 L661.5,201 L660,204 Z M331,165 L321,161 L322.5,164 L322.5,166 L321,169 Z M331,200 L321,196 L322.5,199 L322.5,201 L321,204 Z M716,200 L706,196 L707.5,199 L707.5,201 L706,204 Z M303,165 L293,161 L294.5,164 L294.5,166 L293,169 Z M433,245 L443,249 L441.5,246 L441.5,244 L443,241 Z M87,205 L83,215 L86,213.5 L88,213.5 L91,215 Z M743,205 L747,195 L744,196.5 L742,196.5 L739,195 Z M165,318 L155,314 L156.5,317 L156.5,319 L155,322 Z M238,318 L228,314 L229.5,317 L229.5,319 L228,322 Z M297,318 L287,314 L288.5,317 L288.5,319 L287,322 Z M770,223 L774,213 L771,214.5 L769,214.5 L766,213 Z M429,281 L439,285 L437.5,282 L437.5,280 L439,277 Z M115,318 L105,314 L106.5,317 L106.5,319 L105,322 Z M88,335 L92,325 L89,326.5 L87,326.5 L84,325 Z M213,353 L203,349 L204.5,352 L204.5,354 L203,357 Z M339,344 L343,334 L340,335.5 L338,335.5 L335,334 Z M974,76 L978,66 L975,67.5 L973,67.5 L970,66 Z M513,132 L523,136 L521.5,133 L521.5,131 L523,128 Z M79,165 L69,161 L70.5,164 L70.5,166 L69,169 Z M201,370 L211,374 L209.5,371 L209.5,369 L211,366 Z M34,195 L30,205 L33,203.5 L35,203.5 L38,205 Z M26,21 L16,17 L17.5,20 L17.5,22 L16,25 Z M374,353 L364,349 L365.5,352 L365.5,354 L364,357 Z"/><g font-family="Helvetica, Arial, 'Liberation Sans', 'Nimbus Sans', sans-serif" font-weight="bold" text-anchor="middle"><text x="117" y="45.49" font-size="14" textLength="25.66" lengthAdjust="spacingAndGlyphs">dup</text><text x="139" y="86.49" font-size="14" textLength="62.23" lengthAdjust="spacingAndGlyphs">uniqnulls</text><text x="350" y="45.49" font-size="14" textLength="61.46" lengthAdjust="spacingAndGlyphs">datacopy</text><text x="452" y="70.98" font-size="16" textLength="5.33" lengthAdjust="spacingAndGlyphs">(</text><text x="563" y="70.49" font-size="14" textLength="91.01" lengthAdjust="spacingAndGlyphs">column-name</text><text x="562" y="100.98" font-size="16" textLength="4.45" lengthAdjust="spacingAndGlyphs">,</text><text x="666" y="70.98" font-size="16" textLength="5.33" lengthAdjust="spacingAndGlyphs">)</text><text x="831" y="24.49" font-size="14" textLength="80.91" lengthAdjust="spacingAndGlyphs">string-literal</text><text x="928" y="24.98" font-size="16" textLength="9.34" lengthAdjust="spacingAndGlyphs">=</text><text x="190" y="194.98" font-size="16" textLength="96.91" lengthAdjust="spacingAndGlyphs">&lt;DESCEND&gt;</text><text x="391" y="168.49" font-size="14" textLength="91.01" lengthAdjust="spacingAndGlyphs">column-name</text><text x="347" y="203.98" font-size="16" textLength="5.33" lengthAdjust="spacingAndGlyphs">(</text><text x="436" y="203.49" font-size="14" textLength="83.24" lengthAdjust="spacingAndGlyphs">idxexpr-type</text><text x="525" y="203.98" font-size="16" textLength="5.33" lengthAdjust="spacingAndGlyphs">)</text><text x="576" y="203.98" font-size="16" textLength="8.9" lengthAdjust="spacingAndGlyphs">''</text><text x="632" y="203.49" font-size="14" textLength="29.57" lengthAdjust="spacingAndGlyphs">expr</text><text x="687" y="203.98" font-size="16" textLength="8.9" lengthAdjust="spacingAndGlyphs">''</text><text x="414" y="248.98" font-size="16" textLength="9.34" lengthAdjust="spacingAndGlyphs">+</text><text x="131" y="321.98" font-size="16" textLength="6.22" lengthAdjust="spacingAndGlyphs">{</text><text x="193" y="321.49" font-size="14" textLength="40.46" lengthAdjust="spacingAndGlyphs">where</text><text x="259" y="321.49" font-size="14" textLength="29.57" lengthAdjust="spacingAndGlyphs">expr</text><text x="313" y="321.98" font-size="16" textLength="6.22" lengthAdjust="spacingAndGlyphs">}</text></g>
</svg>

Comdb2 does not require a table to have keys, and there is no requirement of a "primary key." The key section 
is optional.

Consider the previously defined record: Let's say we'd like to create 3 keys.  First one will be unique 
and have one field ```userid```.  Second will store user's *last_name* and *first_name*.  Third will 
store `paydate`.  Compound keys can be formed by simply adding fields together with a '+'.  Here's what the 
key section would look like.

```
keys
{
       "KEY_SERIAL" = userid
       "KEY_NAME" = last_name + first_name
       "KEY_DATE" = paydate
}
```

The above specification states that there are 3 **unique** keys defined for the table.

### Partial Indexes
A partial index is an index over a subset of the rows of a table. Any index that
includes the `{WHERE <expr>}` clause at the end is considered to be a partial
index. Only rows of the table for which the WHERE clause evaluates to true are
included in the index. If the WHERE clause expression evaluates to NULL or to
false for some rows of the table, then those rows are omitted from the index.

The WHERE clause must be placed within a pair of curly braces. The
[expr](sql.html#expr) could be any expressions used in SQL with some
restrictions. The `<expr>` may not contain sub-queries, references to other
tables, non-deterministic functions, or bound parameters. For more information
on allowed expressions, please read
[SQLite's Partial Indexes Documentation](https://sqlite.org/partialindex.html).

The first motivation of having partial indexes in a table is to avoid indexing
uninterested data, which can reduce the size of indexes and speed up index
searches.

Considering the following example:

Let's say we have a online shopping platform and we want to store orders info
into database. An order record could contains `id`, `email`, `total`, etc. We
might be interested in those orders that have `total` greater than $1,000 and
want to send some coupons to their emails as holiday gifts.

To filter out those orders that we are less interested in, we can have the
following schema:

```
[create table orders {
    schema {
        int     id
        cstring email[20]
        int     total
    }
    keys {
        "id"    = id
        "email" = email {where total > 1000}
    }
}] rc 0
```

The index "email" only contains records that have `total` greater than $1,000.
When we want to quickly retrieve their emails, we don't need to scan the whole
table but only those satisfy the condition.

```
(selectid=0, order=0, from=0, detail='SCAN TABLE orders USING INDEX $EMAIL_6BDD7427 (491520 rows)')
[explain query plan select email from orders where total > 1000] rc 0
```

Another motivation of having partial indexes is to enforce uniqueness across
some subset of rows in a table.

Here is an example from
[SQLite's documentation](https://sqlite.org/partialindex.html):

Suppose you have a database of the members of a large organization where each
person is assigned to a particular "team". Each team has a "leader" who is also
a member of that team.

The `team_id` field cannot be unique because there are usually
multiple people on the same team. One cannot make the combination of `team_id`
and `is_leader` unique since there are usually multiple non-leaders on each
team.

The solution to enforcing one leader per team is to create a unique index on
`team_id` but restricted to those entries for which `is_leader` is true:

```
create table person {
    schema {
        int person_id
        int team_id
        int is_leader
    }
    keys {
        "p_id" = person_id
        "t_id" = team_id {where is_leader}
    }
}
```

The "t_id" index is useful when we want to quickly locate the person who is the
leader of a specific team:

```
(selectid=0, order=0, from=0, detail='SEARCH TABLE person USING INDEX $T_ID_4CFA0F9B (team_id=?) (~1 row)')
[explain query plan select person_id from person where is_leader and team_id = 10] rc 0
```

### Indexes on Expressions
As shown in the key definition syntax diagram above, `(idxexpr-type)"<expr>"`
defines a key piece on an expression --- [expr](sql.html#expr) enclosed in
double quotes with a type surrounded by round brackets.
`idxexpr-type`s supported are as follows:

<svg role="img" aria-label="idxexpr-type" xmlns="http://www.w3.org/2000/svg" width="274" height="388" viewBox="0 0 329 466">
<title>idxexpr-type</title><rect width="100%" height="100%" fill="white"/><path fill="none" stroke="black" stroke-width="2" d="M3,16 A3,3 0 1,0 9,16 A3,3 0 1,0 3,16 Z M55,3 H119 V27 H55 Z M55,33 H99 V57 H55 Z M55,63 H95 V87 H55 Z M55,93 H77 V117 H55 Z M55,123 H129 V147 H55 Z M55,153 H133 V177 H55 Z M55,183 H152 V207 H55 Z M55,213 H138 V237 H55 Z M55,243 H160 V267 H55 Z M55,273 H146 V297 H55 Z M55,303 H144 V327 H55 Z M55,333 H144 V357 H55 Z M55,363 H154 V387 H55 Z M55,397 H94 V421 H55 Z M128,394 A15,15 0 0,0 128,424 M128,424 A15,15 0 0,0 128,394 M94,410 L106.5,410 M163,397 H196 V421 H163 Z M144,410 L156.5,410 M230,394 A15,15 0 0,0 230,424 M230,424 A15,15 0 0,0 230,394 M196,410 L208.5,410 M55,435 H114 V459 H55 Z M148,432 A15,15 0 0,0 148,462 M148,462 A15,15 0 0,0 148,432 M114,448 L126.5,448 M183,435 H216 V459 H183 Z M164,448 L176.5,448 M250,432 A15,15 0 0,0 250,462 M250,462 A15,15 0 0,0 250,432 M216,448 L228.5,448 M26,16 L48.5,16 M120,16 L304,16 M26,16 A9,9 0 0,1 35,25 M44,46 L48.5,46 M100,46 L279.5,46 M35,37 A9,9 0 0,0 44,46 M285,46 A9,9 0 0,0 294,37 M44,76 L48.5,76 M96,76 L279.5,76 M35,67 A9,9 0 0,0 44,76 M285,76 A9,9 0 0,0 294,67 M44,106 L48.5,106 M78,106 L279.5,106 M35,97 A9,9 0 0,0 44,106 M285,106 A9,9 0 0,0 294,97 M44,136 L48.5,136 M130,136 L279.5,136 M35,127 A9,9 0 0,0 44,136 M285,136 A9,9 0 0,0 294,127 M44,166 L48.5,166 M134,166 L279.5,166 M35,157 A9,9 0 0,0 44,166 M285,166 A9,9 0 0,0 294,157 M44,196 L48.5,196 M153,196 L279.5,196 M35,187 A9,9 0 0,0 44,196 M285,196 A9,9 0 0,0 294,187 M44,226 L48.5,226 M139,226 L279.5,226 M35,217 A9,9 0 0,0 44,226 M285,226 A9,9 0 0,0 294,217 M44,256 L48.5,256 M161,256 L279.5,256 M35,247 A9,9 0 0,0 44,256 M285,256 A9,9 0 0,0 294,247 M44,286 L48.5,286 M147,286 L279.5,286 M35,277 A9,9 0 0,0 44,286 M285,286 A9,9 0 0,0 294,277 M44,316 L48.5,316 M145,316 L279.5,316 M35,307 A9,9 0 0,0 44,316 M285,316 A9,9 0 0,0 294,307 M44,346 L48.5,346 M145,346 L279.5,346 M35,337 A9,9 0 0,0 44,346 M285,346 A9,9 0 0,0 294,337 M44,376 L48.5,376 M155,376 L279.5,376 M35,367 A9,9 0 0,0 44,376 M285,376 A9,9 0 0,0 294,367 M44,410 L48.5,410 M247,410 L279.5,410 M35,401 A9,9 0 0,0 44,410 M285,410 A9,9 0 0,0 294,401 M44,448 L48.5,448 M267,448 L279.5,448 M35,439 A9,9 0 0,0 44,448 M285,448 A9,9 0 0,0 294,439 M294,25 A9,9 0 0,1 303,16 M35,439 L35,25 M294,439 L294,25 M8,16 L20.5,16 M320,16 A3,3 0 1,0 326,16 A3,3 0 1,0 320,16 Z M302,16 L314.5,16"/><path fill="black" d="M112,410 L102,406 L103.5,409 L103.5,411 L102,414 Z M162,410 L152,406 L153.5,409 L153.5,411 L152,414 Z M214,410 L204,406 L205.5,409 L205.5,411 L204,414 Z M132,448 L122,444 L123.5,447 L123.5,449 L122,452 Z M182,448 L172,444 L173.5,447 L173.5,449 L172,452 Z M234,448 L224,444 L225.5,447 L225.5,449 L224,452 Z M54,16 L44,12 L45.5,15 L45.5,17 L44,20 Z M54,46 L44,42 L45.5,45 L45.5,47 L44,50 Z M285,46 L275,42 L276.5,45 L276.5,47 L275,50 Z M54,76 L44,72 L45.5,75 L45.5,77 L44,80 Z M285,76 L275,72 L276.5,75 L276.5,77 L275,80 Z M54,106 L44,102 L45.5,105 L45.5,107 L44,110 Z M285,106 L275,102 L276.5,105 L276.5,107 L275,110 Z M54,136 L44,132 L45.5,135 L45.5,137 L44,140 Z M285,136 L275,132 L276.5,135 L276.5,137 L275,140 Z M54,166 L44,162 L45.5,165 L45.5,167 L44,170 Z M285,166 L275,162 L276.5,165 L276.5,167 L275,170 Z M54,196 L44,192 L45.5,195 L45.5,197 L44,200 Z M285,196 L275,192 L276.5,195 L276.5,197 L275,200 Z M54,226 L44,222 L45.5,225 L45.5,227 L44,230 Z M285,226 L275,222 L276.5,225 L276.5,227 L275,230 Z M54,256 L44,252 L45.5,255 L45.5,257 L44,260 Z M285,256 L275,252 L276.5,255 L276.5,257 L275,260 Z M54,286 L44,282 L45.5,285 L45.5,287 L44,290 Z M285,286 L275,282 L276.5,285 L276.5,287 L275,290 Z M54,316 L44,312 L45.5,315 L45.5,317 L44,320 Z M285,316 L275,312 L276.5,315 L276.5,317 L275,320 Z M54,346 L44,342 L45.5,345 L45.5,347 L44,350 Z M285,346 L275,342 L276.5,345 L276.5,347 L275,350 Z M54,376 L44,372 L45.5,375 L45.5,377 L44,380 Z M285,376 L275,372 L276.5,375 L276.5,377 L275,380 Z M54,410 L44,406 L45.5,409 L45.5,411 L44,414 Z M285,410 L275,406 L276.5,409 L276.5,411 L275,414 Z M54,448 L44,444 L45.5,447 L45.5,449 L44,452 Z M285,448 L275,444 L276.5,447 L276.5,449 L275,452 Z M26,16 L16,12 L17.5,15 L17.5,17 L16,20 Z M320,16 L310,12 L311.5,15 L311.5,17 L310,20 Z"/><g font-family="Helvetica, Arial, 'Liberation Sans', 'Nimbus Sans', sans-serif" font-weight="bold" text-anchor="middle"><text x="87" y="19.49" font-size="14" textLength="51.34" lengthAdjust="spacingAndGlyphs">u_short</text><text x="77" y="49.49" font-size="14" textLength="35" lengthAdjust="spacingAndGlyphs">short</text><text x="75" y="79.49" font-size="14" textLength="33.45" lengthAdjust="spacingAndGlyphs">u_int</text><text x="66" y="109.49" font-size="14" textLength="17.11" lengthAdjust="spacingAndGlyphs">int</text><text x="92" y="139.49" font-size="14" textLength="59.11" lengthAdjust="spacingAndGlyphs">longlong</text><text x="94" y="169.49" font-size="14" textLength="57.57" lengthAdjust="spacingAndGlyphs">datetime</text><text x="103" y="199.49" font-size="14" textLength="73.91" lengthAdjust="spacingAndGlyphs">datetimeus</text><text x="96" y="229.49" font-size="14" textLength="66.14" lengthAdjust="spacingAndGlyphs">intervalds</text><text x="107" y="259.49" font-size="14" textLength="82.47" lengthAdjust="spacingAndGlyphs">intervaldsus</text><text x="100" y="289.49" font-size="14" textLength="70.03" lengthAdjust="spacingAndGlyphs">intervalym</text><text x="99" y="319.49" font-size="14" textLength="67.7" lengthAdjust="spacingAndGlyphs">decimal32</text><text x="99" y="349.49" font-size="14" textLength="67.7" lengthAdjust="spacingAndGlyphs">decimal64</text><text x="104" y="379.49" font-size="14" textLength="75.49" lengthAdjust="spacingAndGlyphs">decimal128</text><text x="74" y="413.49" font-size="14" textLength="28.78" lengthAdjust="spacingAndGlyphs">byte</text><text x="128" y="413.98" font-size="16" textLength="5.33" lengthAdjust="spacingAndGlyphs">[</text><text x="179" y="413.49" font-size="14" textLength="26.46" lengthAdjust="spacingAndGlyphs">size</text><text x="230" y="413.98" font-size="16" textLength="5.33" lengthAdjust="spacingAndGlyphs">]</text><text x="84" y="451.49" font-size="14" textLength="46.68" lengthAdjust="spacingAndGlyphs">cstring</text><text x="148" y="451.98" font-size="16" textLength="5.33" lengthAdjust="spacingAndGlyphs">[</text><text x="199" y="451.49" font-size="14" textLength="26.46" lengthAdjust="spacingAndGlyphs">size</text><text x="250" y="451.98" font-size="16" textLength="5.33" lengthAdjust="spacingAndGlyphs">]</text></g>
</svg>

See
[SQLite's Indexes on Expressions Documentation](http://sqlite.org/expridx.html)
for restrictions on expressions that appear in indexes.

For example, if we want to store JSON strings to database, with indexes on
expressions, we can create indexes on JSON columns using JSON functions
(expressions).

```
[create table jdemo {
    schema {
        vutf8 json[128]
    }
    keys {
        "a" = (int)"json_extract(json, '$.a')"
        "b" = (cstring[10])"json_extract(json, '$.b')"
    }
}] rc 0
(rows inserted=2)
[insert into jdemo values ('{"a":0,"b":"zero"}'), ('{"a":1,"b":"one"}')] rc 0
(rows inserted=2)
[insert into jdemo values ('{"a":2,"b":"two"}'), ('{"a":3,"b":"three"}')] rc 0
(json='{"a":3,"b":"three"}')
(json='{"a":0,"b":"zero"}')
(json='{"a":1,"b":"one"}')
(json='{"a":2,"b":"two"}')
[select * from jdemo] rc 0
```

Now, if we want all value of JSON column "a" in ascending order, index "a" can
be used:

```
(selectid=0, order=0, from=0, detail='SCAN TABLE jdemo USING INDEX $A_520C7C1B (4 rows)')
[explain query plan select json_extract(json, '$.a') as a from jdemo order by json_extract(json, '$.a')] rc 0
```

Similarly, here is a typical query that uses index "b":

```
(selectid=0, order=0, from=0, detail='SCAN TABLE jdemo USING INDEX $B_504AC242 (4 rows)')
[explain query plan select json_extract(json, '$.b') as b from jdemo order by json_extract(json, '$.b')] rc 0
```

The syntax to create indexes on expressions using standard DDL can be found in
an example [here](sql.md#create-table).


### Duplicate Keys.
For the second and third keys in the example above, there's a great possibility of having duplicate key 
entries. (>1 users can share the same paydate, and multiple users can have the same first and last names).

Preceding a key name with ```dup``` marks that key as allowing duplicate entries.

```
keys
{
       "KEY_SERIAL" = userid
  dup  "KEY_NAME" = last_name + first_name
  dup  "KEY_DATE" = paydate
}
```


### Datacopy Keys.
If the key definition is preceded by the ```datacopy``` keyword, then the backing index will maintain a copy of 
the data record in the btree used for the index.  This copy is maintained transparently by the database.  
This allows for large performance gains when reading sequential records from on a key.  The trade-off is the 
use of more disk space.

To only copy a subset of the columns (partial datacopy), follow the ```datacopy``` keyword by a comma-separated list of columns
closed by parentheses to include in the datacopy. Note that currently partial datacopies cannot be used at all
in tables that contain decimal fields.

### Unique NULL Keys.
If the key definition is preceded by the ```uniqnulls``` keyword, then the backing index will treat NULL values
as unique.

### Ascending and Descending Keys.

It is possible to make any piece of a key be sorted in DESCENDING order by using the ```<DESCEND>``` keyword (must 
be uppercase).  For example,

```"KEY_NAME" = <DESCEND>last_name + first_name``` 

In this example, last_name is stored in descending order, while first_name is stored in **ascending** order.  You 
can specify ascending key pieces with the ```<ASCEND>``` keyword, but it is not necessary since this is the default.

## Constraints Section

<svg role="img" aria-label="constraints" xmlns="http://www.w3.org/2000/svg" width="756" height="386" viewBox="0 0 907 463">
<title>constraint-section</title><rect width="100%" height="100%" fill="white"/><path fill="none" stroke="black" stroke-width="2" d="M3,6 A3,3 0 1,0 9,6 A3,3 0 1,0 3,6 Z M198,6 L199,6 M102,19 A12,12 0 0,0 102,43 M236,43 A12,12 0 0,0 236,19 M102,19 L236,19 M102,43 L236,43 M283,16 A15,15 0 0,0 283,46 M287,46 A15,15 0 0,0 287,16 M283,16 L287,16 M283,46 L287,46 M249,32 L261.5,32 M61,6 L192.5,6 M199,6 L344,6 M61,6 A9,9 0 0,1 70,15 M79,32 L83.5,32 M304,32 L319.5,32 M70,23 A9,9 0 0,0 79,32 M325,32 A9,9 0 0,0 334,23 M334,15 A9,9 0 0,1 343,6 M70,23 L70,15 M334,23 L334,15 M110,93 A12,12 0 0,0 110,117 M186,117 A12,12 0 0,0 186,93 M110,93 L186,93 M110,117 L186,117 M233,90 A15,15 0 0,0 233,120 M243,120 A15,15 0 0,0 243,90 M233,90 L243,90 M233,120 L243,120 M199,106 L211.5,106 M318,93 A12,12 0 0,0 318,117 M439,117 A12,12 0 0,0 439,93 M318,93 L439,93 M318,117 L439,117 M486,90 A15,15 0 0,0 486,120 M486,120 A15,15 0 0,0 486,90 M452,106 L464.5,106 M533,93 A12,12 0 0,0 533,117 M637,117 A12,12 0 0,0 637,93 M533,93 L637,93 M533,117 L637,117 M502,106 L514.5,106 M338,129 A15,15 0 0,0 338,159 M342,159 A15,15 0 0,0 342,129 M338,129 L342,129 M338,159 L342,159 M389,132 A12,12 0 0,0 389,156 M510,156 A12,12 0 0,0 510,132 M389,132 L510,132 M389,156 L510,156 M358,145 L370.5,145 M557,129 A15,15 0 0,0 557,159 M557,159 A15,15 0 0,0 557,129 M523,145 L535.5,145 M604,132 A12,12 0 0,0 604,156 M708,156 A12,12 0 0,0 708,132 M604,132 L708,132 M604,156 L708,156 M573,145 L585.5,145 M755,129 A15,15 0 0,0 755,159 M759,159 A15,15 0 0,0 759,129 M755,129 L759,129 M755,159 L759,159 M721,145 L733.5,145 M537,174 L554,174 M322,174 L537,174 M559.5,174 L776,174 M305,145 L322,145 M322,145 A9,9 0 0,0 313,154 M313,154 L313,165 M313,165 A9,9 0 0,0 322,174 M776,145 A9,9 0 0,1 785,154 M785,154 L785,165 M785,165 A9,9 0 0,1 776,174 M776,145 L785,145 M277,106 L299.5,106 M651,106 L820,106 M277,106 A9,9 0 0,1 286,115 M295,145 L299.5,145 M785,145 L795.5,145 M286,136 A9,9 0 0,0 295,145 M801,145 A9,9 0 0,0 810,136 M810,115 A9,9 0 0,1 819,106 M286,136 L286,115 M810,136 L810,115 M259,106 L271.5,106 M343,6 L361,6 M361,6 A9,9 0 0,1 370,15 M370,15 L370,31.5 M370,37 L370,59 M370,59 A9,9 0 0,1 361,68 M361,68 L225.5,68 M220,68 L79,68 M79,68 A9,9 0 0,0 70,77 M70,77 L70,97 M70,97 A9,9 0 0,0 79,106 M79,106 L91.5,106 M143,218 H166 V242 H143 Z M185,218 H239 V242 H185 Z M166,231 L178.5,231 M284,218 H355 V242 H284 Z M286,248 H345 V272 H286 Z M286,278 H312 V302 H286 Z M331,278 H362 V302 H331 Z M312,291 L324.5,291 M257,231 L277.5,231 M356,231 L399,231 M257,231 A9,9 0 0,1 266,240 M275,261 L279.5,261 M346,261 L374.5,261 M266,252 A9,9 0 0,0 275,261 M380,261 A9,9 0 0,0 389,252 M275,291 L279.5,291 M363,291 L374.5,291 M266,282 A9,9 0 0,0 275,291 M380,291 A9,9 0 0,0 389,282 M389,240 A9,9 0 0,1 398,231 M266,282 L266,240 M389,282 L389,240 M239,231 L251.5,231 M143,308 H166 V332 H143 Z M185,308 H247 V332 H185 Z M166,321 L178.5,321 M290,308 H361 V332 H290 Z M294,338 H353 V362 H294 Z M265,321 L283.5,321 M362,321 L403,321 M265,321 A9,9 0 0,1 274,330 M283,351 L287.5,351 M354,351 L378.5,351 M274,342 A9,9 0 0,0 283,351 M384,351 A9,9 0 0,0 393,342 M393,330 A9,9 0 0,1 402,321 M274,342 L274,330 M393,342 L393,330 M247,321 L259.5,321 M114,231 L136.5,231 M398,231 L436,231 M114,231 A9,9 0 0,1 123,240 M132,321 L136.5,321 M402,321 L411.5,321 M123,312 A9,9 0 0,0 132,321 M417,321 A9,9 0 0,0 426,312 M426,240 A9,9 0 0,1 435,231 M123,312 L123,240 M426,312 L426,240 M263,375 L280,375 M114,375 L263,375 M285.5,375 L435,375 M97,231 L114,231 M114,231 A9,9 0 0,0 105,240 M105,366 L105,308.5 M105,303 L105,240 M105,366 A9,9 0 0,0 114,375 M435,231 A9,9 0 0,1 444,240 M444,240 L444,297.5 M444,303 L444,366 M444,366 A9,9 0 0,1 435,375 M435,231 L444,231 M819,106 L837,106 M837,106 A9,9 0 0,1 846,115 M846,115 L846,146.5 M846,152 L846,189 M846,189 A9,9 0 0,1 837,198 M837,198 L463.5,198 M458,198 L79,198 M79,198 A9,9 0 0,0 70,207 M70,207 L70,222 M70,222 A9,9 0 0,0 79,231 M79,231 L91.5,231 M70,222 L70,309.5 M70,315 L70,390 M70,390 A9,9 0 0,0 79,399 M444,231 A9,9 0 0,1 453,240 M453,390 A9,9 0 0,0 462,399 M79,399 L255.5,399 M261,399 L462,399 M453,240 L453,384.5 M72,414 H122 V438 H72 Z M153,414 A12,12 0 0,0 153,438 M287,438 A12,12 0 0,0 287,414 M153,414 L287,414 M153,438 L287,438 M122,427 L134.5,427 M334,411 A15,15 0 0,0 334,441 M338,441 A15,15 0 0,0 338,411 M334,411 L338,411 M334,441 L338,441 M300,427 L312.5,427 M388,411 A15,15 0 0,0 388,441 M388,441 A15,15 0 0,0 388,411 M354,427 L366.5,427 M423,414 H477 V438 H423 Z M404,427 L416.5,427 M508,414 A12,12 0 0,0 508,438 M542,438 A12,12 0 0,0 542,414 M508,414 L542,414 M508,438 L542,438 M477,427 L489.5,427 M589,411 A15,15 0 0,0 589,441 M589,441 A15,15 0 0,0 589,411 M555,427 L567.5,427 M43,6 L61,6 M462,399 L873,399 M43,6 A9,9 0 0,1 52,15 M61,427 L65.5,427 M606,427 L848.5,427 M52,418 A9,9 0 0,0 61,427 M854,427 A9,9 0 0,0 863,418 M863,408 A9,9 0 0,1 872,399 M52,418 L52,15 M863,418 L863,408 M446,456 L463,456 M43,456 L446,456 M468.5,456 L872,456 M26,6 L43,6 M43,6 A9,9 0 0,0 34,15 M34,447 L34,236.5 M34,231 L34,15 M34,447 A9,9 0 0,0 43,456 M872,399 A9,9 0 0,1 881,408 M881,408 L881,421.5 M881,427 L881,447 M881,447 A9,9 0 0,1 872,456 M872,399 L881,399 M8,6 L20.5,6 M898,399 A3,3 0 1,0 904,399 A3,3 0 1,0 898,399 Z M880,399 L892.5,399"/><path fill="black" d="M267,32 L257,28 L258.5,31 L258.5,33 L257,36 Z M198,6 L188,2 L189.5,5 L189.5,7 L188,10 Z M89,32 L79,28 L80.5,31 L80.5,33 L79,36 Z M325,32 L315,28 L316.5,31 L316.5,33 L315,36 Z M217,106 L207,102 L208.5,105 L208.5,107 L207,110 Z M470,106 L460,102 L461.5,105 L461.5,107 L460,110 Z M520,106 L510,102 L511.5,105 L511.5,107 L510,110 Z M376,145 L366,141 L367.5,144 L367.5,146 L366,149 Z M541,145 L531,141 L532.5,144 L532.5,146 L531,149 Z M591,145 L581,141 L582.5,144 L582.5,146 L581,149 Z M739,145 L729,141 L730.5,144 L730.5,146 L729,149 Z M554,174 L564,178 L562.5,175 L562.5,173 L564,170 Z M305,106 L295,102 L296.5,105 L296.5,107 L295,110 Z M305,145 L295,141 L296.5,144 L296.5,146 L295,149 Z M801,145 L791,141 L792.5,144 L792.5,146 L791,149 Z M277,106 L267,102 L268.5,105 L268.5,107 L267,110 Z M370,37 L374,27 L371,28.5 L369,28.5 L366,27 Z M220,68 L230,72 L228.5,69 L228.5,67 L230,64 Z M97,106 L87,102 L88.5,105 L88.5,107 L87,110 Z M184,231 L174,227 L175.5,230 L175.5,232 L174,235 Z M330,291 L320,287 L321.5,290 L321.5,292 L320,295 Z M283,231 L273,227 L274.5,230 L274.5,232 L273,235 Z M285,261 L275,257 L276.5,260 L276.5,262 L275,265 Z M380,261 L370,257 L371.5,260 L371.5,262 L370,265 Z M285,291 L275,287 L276.5,290 L276.5,292 L275,295 Z M380,291 L370,287 L371.5,290 L371.5,292 L370,295 Z M257,231 L247,227 L248.5,230 L248.5,232 L247,235 Z M184,321 L174,317 L175.5,320 L175.5,322 L174,325 Z M289,321 L279,317 L280.5,320 L280.5,322 L279,325 Z M293,351 L283,347 L284.5,350 L284.5,352 L283,355 Z M384,351 L374,347 L375.5,350 L375.5,352 L374,355 Z M265,321 L255,317 L256.5,320 L256.5,322 L255,325 Z M142,231 L132,227 L133.5,230 L133.5,232 L132,235 Z M142,321 L132,317 L133.5,320 L133.5,322 L132,325 Z M417,321 L407,317 L408.5,320 L408.5,322 L407,325 Z M280,375 L290,379 L288.5,376 L288.5,374 L290,371 Z M105,303 L101,313 L104,311.5 L106,311.5 L109,313 Z M444,303 L448,293 L445,294.5 L443,294.5 L440,293 Z M846,152 L850,142 L847,143.5 L845,143.5 L842,142 Z M458,198 L468,202 L466.5,199 L466.5,197 L468,194 Z M97,231 L87,227 L88.5,230 L88.5,232 L87,235 Z M70,315 L74,305 L71,306.5 L69,306.5 L66,305 Z M261,399 L251,395 L252.5,398 L252.5,400 L251,403 Z M453,390 L457,380 L454,381.5 L452,381.5 L449,380 Z M140,427 L130,423 L131.5,426 L131.5,428 L130,431 Z M318,427 L308,423 L309.5,426 L309.5,428 L308,431 Z M372,427 L362,423 L363.5,426 L363.5,428 L362,431 Z M422,427 L412,423 L413.5,426 L413.5,428 L412,431 Z M495,427 L485,423 L486.5,426 L486.5,428 L485,431 Z M573,427 L563,423 L564.5,426 L564.5,428 L563,431 Z M71,427 L61,423 L62.5,426 L62.5,428 L61,431 Z M854,427 L844,423 L845.5,426 L845.5,428 L844,431 Z M463,456 L473,460 L471.5,457 L471.5,455 L473,452 Z M34,231 L30,241 L33,239.5 L35,239.5 L38,241 Z M881,427 L885,417 L882,418.5 L880,418.5 L877,417 Z M26,6 L16,2 L17.5,5 L17.5,7 L16,10 Z M898,399 L888,395 L889.5,398 L889.5,400 L888,403 Z"/><g font-family="Helvetica, Arial, 'Liberation Sans', 'Nimbus Sans', sans-serif" font-weight="bold" text-anchor="middle"><text x="169" y="35.49" font-size="14" textLength="108.91" lengthAdjust="spacingAndGlyphs">constraint-name</text><text x="285" y="35.98" font-size="16" textLength="9.34" lengthAdjust="spacingAndGlyphs">=</text><text x="148" y="109.49" font-size="14" textLength="59.92" lengthAdjust="spacingAndGlyphs">keyname</text><text x="238" y="109.98" font-size="16" textLength="14.67" lengthAdjust="spacingAndGlyphs">-&gt;</text><text x="378" y="109.49" font-size="14" textLength="96.46" lengthAdjust="spacingAndGlyphs">ref-table-name</text><text x="486" y="109.98" font-size="16" textLength="5.33" lengthAdjust="spacingAndGlyphs">:</text><text x="585" y="109.49" font-size="14" textLength="82.47" lengthAdjust="spacingAndGlyphs">ref-keyname</text><text x="340" y="148.98" font-size="16" textLength="9.34" lengthAdjust="spacingAndGlyphs">&lt;</text><text x="449" y="148.49" font-size="14" textLength="96.46" lengthAdjust="spacingAndGlyphs">ref-table-name</text><text x="557" y="148.98" font-size="16" textLength="5.33" lengthAdjust="spacingAndGlyphs">:</text><text x="656" y="148.49" font-size="14" textLength="82.47" lengthAdjust="spacingAndGlyphs">ref-keyname</text><text x="757" y="148.98" font-size="16" textLength="9.34" lengthAdjust="spacingAndGlyphs">&gt;</text><text x="154" y="234.49" font-size="14" textLength="17.11" lengthAdjust="spacingAndGlyphs">on</text><text x="212" y="234.49" font-size="14" textLength="40.46" lengthAdjust="spacingAndGlyphs">delete</text><text x="319" y="234.49" font-size="14" textLength="55.26" lengthAdjust="spacingAndGlyphs">cascade</text><text x="315" y="264.49" font-size="14" textLength="47.46" lengthAdjust="spacingAndGlyphs">restrict</text><text x="299" y="294.49" font-size="14" textLength="20.23" lengthAdjust="spacingAndGlyphs">set</text><text x="346" y="294.49" font-size="14" textLength="24.89" lengthAdjust="spacingAndGlyphs">null</text><text x="154" y="324.49" font-size="14" textLength="17.11" lengthAdjust="spacingAndGlyphs">on</text><text x="216" y="324.49" font-size="14" textLength="45.89" lengthAdjust="spacingAndGlyphs">update</text><text x="325" y="324.49" font-size="14" textLength="55.26" lengthAdjust="spacingAndGlyphs">cascade</text><text x="323" y="354.49" font-size="14" textLength="47.46" lengthAdjust="spacingAndGlyphs">restrict</text><text x="97" y="430.49" font-size="14" textLength="39.69" lengthAdjust="spacingAndGlyphs">check</text><text x="220" y="430.49" font-size="14" textLength="108.91" lengthAdjust="spacingAndGlyphs">constraint-name</text><text x="336" y="430.98" font-size="16" textLength="9.34" lengthAdjust="spacingAndGlyphs">=</text><text x="388" y="430.98" font-size="16" textLength="6.22" lengthAdjust="spacingAndGlyphs">{</text><text x="450" y="430.49" font-size="14" textLength="40.46" lengthAdjust="spacingAndGlyphs">where</text><text x="525" y="430.49" font-size="14" textLength="29.57" lengthAdjust="spacingAndGlyphs">expr</text><text x="589" y="430.98" font-size="16" textLength="6.22" lengthAdjust="spacingAndGlyphs">}</text></g>
</svg>

The constraints section is an optional section of the table definition used to specify foreign key constraints.
It establishes a relationship between keys in the current table and keys in other (foreign) tables.

The syntax is ```"LOCAL_KEYNAME" -> <"REFERENCED_TABLE_NAME":"REFERENCED_KEY_NAME">```.

```LOCAL_KEYNAME``` is the name of the key in the current table. ```REFERENCED_TABLE_NAME```
and ```REFERENCED_KEY_NAME``` are the names of referenced (foreign) table which contains the
key which is being pointed at, and key in the foreign table, respectively. The data types of
the columns comprising these keys need not match, though the contents must be convertible.
The local key can be a prefix of the foreign key, and vice versa.

A key can point at more than one key by specifying multiple ```<"REFERENCED_TABLE_NAME":"REFERENCED_KEY_NAME">``` 
items on the same line delimited by white-space.  A key can point to a key within its own table by specifying 
its own table name in ```REFERENCED_TABLE_NAME```.

Cascading deletes are supported on a constraint by adding the text ```on delete cascade``` (all in lower case) to 
the end of the constraint line.  Cascading updates are supported on a constraint by adding the text 
```on update cascade``` (all in lower case) to the end of the constraint line.

For some more detailed notes on the behavior of foreign key constraints, please see the 
[constraints](constraints.html) section.

### Using Partial Indexes in Constraints
Foreign key constraints ensure that a given key value in a table exists in a 
corresponding key in another table.

If a partial index is used as a local key in a constraint, the constraint is
only checked for rows included in the index.

If a partial index is used as a referenced key in a constraint, then the
referenced key might only include a subset of rows in the referenced table.
If a given local key value does not exist in the referenced key (which is a
partial index), it is considered a constraint violation even if there might
exist some excluded rows that would form a matching key if they were included
in the referenced index.

### Using Indexes on Expressions in Constraints
Indexes on expressions are allowed in key constraints. However, if the local key
of a constraint is an index on expressions, then cascading update is NOT
supported on the constraint.

