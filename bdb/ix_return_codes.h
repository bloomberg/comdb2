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

#ifndef INCLUDED_IX_RETURN_CODES_H
#define INCLUDED_IX_RETURN_CODES_H

/* compatible with comdb2 */
enum IXRCODES {
    IX_OK = 0 /*OPERATION SUCCESSFUL*/
    ,
    IX_FND = 0 /*KEY FOUND*/
    ,
    IX_FNDMORE = 1 /*KEY FOUND, ANOTHER MATCH AHEAD*/
    ,
    IX_NOTFND = 2 /*KEY NOT FOUND*/
    ,
    IX_PASTEOF = 3 /*KEY NOT FOUND, HIGHER THAN ALL KEYS*/
    ,
    IX_DUP = 2 /*DUP ON ADD*/
    ,
    IX_FNDNOCONV = 12 /*FOUND BUT COULDN'T CONVERT*/
    ,
    IX_FNDMORENOCONV = 13 /*FOUND, ANOTHER MATCH AHEAD, BUT COULDN'T CONVERT*/
    ,
    IX_NOTFNDNOCONV = 14 /*DIDN'T FIND, COULDN'T CONVERT NEXT*/
    ,
    IX_PASTEOFNOCONV = 15 /*HIGHER THAN ALL KEYS, COULDN'T CONVERT LAST*/
    ,
    IX_SCHEMACHANGED = 16 /*SCHEMA CHANGED SINCE LAST FIND*/
    ,
    IX_ACCESS = 17 /*CAN'T ACCESS TABLE */
    ,
    IX_EMPTY = 99 /*DB EMPTY*/
};

#endif
