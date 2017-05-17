#!/usr/bin/python

print "[set hasql on] rc 0"
print "[set transaction serializable] rc 0"
print "[begin] rc 0"
print "[insert into jepsen values(1,1)] rc 0"
print "[insert into jepsen values(2,2)] rc 0"

i=0
while ( i < 2000000 ) :
    print "(a=%d)" % i
    i=i+1

print "[select * from t1 order by a] rc 0"
print "[commit] rc 0"

