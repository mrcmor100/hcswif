#!/usr/bin/bash

ARGC=$#
if [[ $ARGC -ne 4 ]]; then
    echo Usage: hcswif.sh SCRIPT RUN EVENTS SEGMENT
    exit 1
fi;
script=$1
run=$2
evt=$3
seg=$4

# Setup environment
hcswif_dir=$(dirname $(readlink -f $0))
source $hcswif_dir/setup.sh

# Check environment
if ! [ $(command -v hcana) ]; then
    echo Could not find hcana! Please edit $hcswif_dir/setup.sh appropriately
    exit 1
fi

# Replay the run
runHcana="hcana -q \"$script($run,$evt,1,$seg,0)\""
#runHcana="hcana -q \"$script($run,$evt)\""

#cd $hallc_replay_dir
tar -xf nps_replay.tar.gz

echo pwd: $(pwd)
echo $runHcana
eval $runHcana