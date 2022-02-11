#!/bin/bash

list=$1

spec=$2

while read line
do

# runArray+=('/lustre/expphy/cache/hallc/E12-10-002/f2xem/pass-3-data/'${spec}'-f2-all-data/'${spec}'_replay_production_'${line}'_-1.root')
runArray+=('/lustre/expphy/cache/hallc/E12-10-002/f2xem/pass-3-data/'${spec}'-f2-all-reports/replay_'${spec}'_production_'${line}'_-1.report')

done < ${list}

# eval cp -v "${runArray[*]}" /u/scratch/pooser/${spec}-xem-data
eval cp -v "${runArray[*]}" /u/scratch/pooser/${spec}-xem-reports
