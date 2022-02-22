#!/bin/bash

### Stephen Kay --- University of Regina --- 24/09/20 ###
### Check if files from provided run number are in the cache or mss, if not, save to list ###
inputFile="$1"

if [[ "${HOSTNAME}" = *"farm"* ]]; then  
    REPLAYPATH="/group/c-pionlt/USERS/${USER}/hallc_replay_lt"
elif [[ "${HOSTNAME}" = *"qcd"* ]]; then
    REPLAYPATH="/group/c-pionlt/USERS/${USER}/hallc_replay_lt"
elif [[ "${HOSTNAME}" = *"cdaq"* ]]; then
    REPLAYPATH="/home/cdaq/hallc-online/hallc_replay_lt"
elif [[ "${HOSTNAME}" = *"phys.uregina.ca"* ]]; then
    REPLAYPATH="/home/${USER}/work/JLab/hallc_replay_lt"
fi

cd "${REPLAYPATH}/UTIL_BATCH/InputRunLists/Kaon_Data/"

if [ -f "RunsNotInCache" ]; then
    rm "RunsNotInCache"
    else touch "RunsNotInCache"
fi

while IFS='' read -r line || [[ -n "$line" ]]; do
    #Run number#
    runNum=$line
    if [[ ! -f "/cache/hallc/kaonlt/Full_Replay_Pass1/coin_replay_Full_Lumi_${runNum}_-1.root" && "/cache/hallc/kaonlt/Full_Replay_Pass1/coin_replay_Full_Lumi_${runNum}_100000.root" ]]; then
	if [[ ! -f "/cache/hallc/kaonlt/Full_Replay_Pass1/coin_replay_Full_Lumi_${runNum}_-1.root" && "/cache/hallc/kaonlt/Full_Replay_Pass1/coin_replay_Full_Lumi_${runNum}_100000.root" ]]; then
	echo "Replay for run number ${runNum} not found in /cache or /mss"
	echo "${runNum}" >> "RunsNotInCache"
	fi
    fi
done < "$inputFile"
