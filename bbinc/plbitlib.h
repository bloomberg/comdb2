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

#ifndef INCLUDED_PLBITLIB
#define INCLUDED_PLBITLIB

/*
   THESE MACROS ACCEPT POINTER TO MEMORY AND BIT #.

   btst( pointer, bitnum ) - 0 if bit is false
   bset( pointer, bitnum ) - sets bit #
   bclr( pointer, bitnum ) - clears bit #
*/

#define btst(x, y) ((((const char *)(x))[(y) >> 3] & (0x80 >> ((y)&7))))
#define bset(x, y) (((char *)(x))[(y) >> 3] |= (0x80 >> ((y)&7)))
#define bclr(x, y) (((char *)(x))[(y) >> 3] &= (~(0x80 >> ((y)&7))))

#endif
