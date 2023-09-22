#!/bin/bash

list=$1

spec=$2

while read line
do

# runArray+=('/lustre/expphy/cache/hallc/E12-10-002/f2xem/pass-3-data/'${spec}'-f2-all-data/'${spec}'_replay_production_'${line}'_-1.root')
#runArray+=('/lustre/expphy/cache/hallc/E12-10-002/f2xem/pass-3-data/'${spec}'-f2-all-reports/replay_'${spec}'_production_'${line}'_-1.report')
runArray+=('/lustre19/expphy/volatile/hallc/xem2/cmorean/ROOTfiles/SHMS/PRODUCTION/'${spec}'_replay_production_'${line}'_-1.root')

done < ${list}

#for value in "${runArray[@]}"
#do
#    echo "${runArray}"
#done
# eval cp -v "${runArray[*]}" /u/scratch/pooser/${spec}-xem-data
#eval cp -v "${runArray[*]}" /u/scratch/pooser/${spec}-xem-reports
