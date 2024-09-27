#!/bin/bash

list=$1

spec=$2

while read line
do

if [ ${line} -gt 7000 ]; then
    runArray+=('/cache/hallc/jpsi-007/raw/'${spec}'_all_0'${line}'.dat')
else
    runArray+=('/cache/hallc/spring17/raw/'${spec}'_all_0'${line}'.dat')
fi

done < ${list}

eval jcache pin "${runArray[*]}" -D 60
