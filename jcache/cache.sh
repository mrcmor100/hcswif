
#!/bin/bash

list=$1

spec=$2

while read line
do

if [ ${line} -gt 7000 ]; then
    runArray+=('/mss/hallc/jpsi-007/raw/'${spec}'_all_0'${line}'.dat')
else
    runArray+=('/mss/hallc/spring17/raw/'${spec}'_all_0'${line}'.dat')
fi

done < ${list}

eval jcache get "${runArray[*]}"
