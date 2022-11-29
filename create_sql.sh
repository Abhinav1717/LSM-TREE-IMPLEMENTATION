#!/bin/bash
for i in {1..1000}
do
   echo "insert into t values($i,0);" >> insert_1000.sql
   echo "select pg_sleep(2);" >> insert_1000.sql
done
