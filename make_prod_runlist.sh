#!/bin/bash

run_min=$1
run_max=$2
rl_name=$3

if [ -z "$rl_name" ]; then
    echo "No runlist file name given, using default runlist.dat"
    rl_name="runlist.dat"
fi

for run in $(seq $run_min $run_max); do 
    for file in /mss/hallc/c-nps/raw/nps_coin_${run}.dat.*; do
	var=`echo $file | grep -o -E "[0-9]+"`
	var2=`grep -oP "size=\K.*" $file`
	if [ $? -eq 0 ]; then
	    echo $var $var2
	fi
    done
done > $rl_name
