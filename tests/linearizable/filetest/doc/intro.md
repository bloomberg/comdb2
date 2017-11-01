# Introduction to jepsen.filetest

Jepsen/Knossos provide a lovely framework for testing database correctness.  The only
problem is that we, as database developers, aren't also particularly strong in 
clojure.  This is a very small simple wrapper around Knossos to allow writing the 
test itself in any language, and running the resulting history through the Knossos
checker.
