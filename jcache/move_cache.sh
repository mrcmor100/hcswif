#!/bin/bash

list=$1
spec=$2
dir=$3
rppfx=$4

SPEC=$(echo "$spec" | tr '[:lower:]' '[:upper:]')


while read line
do

runFile='/volatile/hallc/xem2/cmorean/FullExperiment/ROOTfiles/'${SPEC}'/'${rppfx}${line}'_-1.root'


if [ ! -f "$runFile" ]; then
    echo "Run file $runFile does not exists."
    exit 1
else
    runArray+=(${runFile}' ')
fi

done < ${list}

cacheLoc='/mss/hallc/xem2/analysis/ONLINE/REPLAYS/'${SPEC}'/'${dir}'/'

echo "jput ${runArray[*]} ${cacheLoc}"

echo "The run files have been validated."
echo "Does the comman look correct? i.e. is the final destination correct?"
echo "Enter 'yes' to continue, 'no' to abort."
read cont
if [ "$cont" = "yes" ] || [ "$cont" = "YES" ]; then 
    eval jput "${runArray[*]}" "${cacheLoc}"
else
    echo "Aborting submit!"
    exit 1
fi

exit 0
