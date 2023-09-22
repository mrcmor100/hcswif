#!/bin/bash

cnt=1;subStr="";tmp="";
infile=$1
#/mss/hallc/spring17/raw/
mssDir=$2
#spectrometer (lowercase)
spec=$3
#must put email
pinDays=$4
grep -v '^#' < $infile | { while read line; do 
	stringarr=($line)
	#tmp="${mssDir}${spec}_all_0${stringarr[0]}.dat"
	tmp="${mssDir}${spec}_replay_production_${stringarr[0]}_-1.root"
	subStr="${subStr} $tmp"
	if (( $cnt % 20 == 0 ))
	then
	    echo $subStr
	    eval jcache get ${subStr} -D ${pinDays}
	    echo ""
	    subStr=""
	fi
	((cnt=cnt+1))
done; echo "";echo $subStr; jcache get ${subStr} -D ${pinDays};
}
