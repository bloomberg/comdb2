This is the readme.txt for the decNumber package.  It includes
instructions for compiling and testing the package; please read them.
---------------------------------------------------------------------

decNumber is distributed in three forms; as a complete package from
the IBM alphaWorks site (available under either a trial or a
commercial license), as a complete package from the International
Components for Unicode (ICU) site (under an as-is license), or as a
collection of Open Source files from the GCC source repository (under
the GPL license).

If you are using the GCC files, you can obtain the documentation, the
example files mentioned below, and this readme from:

  http://www2.hursley.ibm.com/decimal/#decNumber

(the URL for the open source files is also linked from there).


The alphaWorks and ICU packages
-------------------------------

The alphaWorks and ICU packages include the files:

  *  readme.txt (this file)

  *  alphaWorks-license-files.zip (contains the 90-day trial license, in
     multiple languages), or ICU-license.html

     Note: a commercial license for this code is also available by
     following the 'License this technology' link from the alphaWorks
     page: http://www.alphaWorks.ibm.com/tech/decnumber

  *  decNumber.pdf (documentation)

  *  The .c and .h file for each module in the package (see the
     decNumber documentation), together with other included files.

  *  The .c files for each of the examples (example1.c through
     example8.c).

The alphaWorks package is made available under the terms of the IBM
alphaWorks License Agreement (included in various languages in the
file alphaWorks-license-files.zip), unless you have agreed different
licensing terms with IBM.  Your use of that package indicates your
acceptance of the terms and conditions of that Agreement.

The ICU package is made available under the terms of the ICU License
(ICU 1.8.1 and later) included in the package as ICU-license.html.
Your use of that package indicates your acceptance of the terms and
conditions of that Agreement.



To use and check decNumber
--------------------------

  Please read the appropriate license and documentation before using
  this package.  If you are upgrading an existing use of decNumber
  (version <= 3.37) please read the Changes Appendix for later
  versions -- you may need to change the DECLITEND flag.

  1. Compile and link example1.c, decNumber.c, and decContext.c
     For instance, use:

       gcc -o example1 example1.c decNumber.c decContext.c

     Note: If your compiler does not provide stdint.h or if your C
     compiler does not handle line comments (// ...), then see the
     User's Guide section in the documentation for further information
     (including a sample minimal stdint.h).

  2. Run example1 with two numeric arguments, for example:

       example1 1.23 1.27

     this should display:

       1.23 + 1.27 => 2.50

  3. Similarly, try the other examples, at will.

     Examples 2->4 require three files to be compiled, like Example 1.

     Example 5 requires decimal64.c in addition to the core modules.

     Example 6 requires decPacked.c in addition to the core modules.

     Example 7 requires only example7.c decContext.c and decQuad.c

     Example 8 requires example8.c, decContext.c, and decQuad.c, plus
               decNumber.c, decimal128.c, and decimal64.c (the latter
               for shared tables and code)

