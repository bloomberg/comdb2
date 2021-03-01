This test verifies that writing to comdb2_oplog will not results in a schema-lk
lock-inversion bug.  The customer who was affected was a local replicant.  The
logic relies on the bb-plugins fastseed implementation.  While this test will 
pass in the open-source branch, a copy of this test under the RM testsuite more
accurately represents what the customer was seeing.
