#!/usr/bin/bash
ARGC=$#
if [[ $ARGC -ne 5 ]]; then
    echo "Usage: hcswif.sh SCRIPT RUN EVENTS APPTAINER RAWDIR"
    exit 1
fi;

script=$1 # script to run
run=$2 # RUN Number
evt=$3 # Number of events in that run
apptainer=$4
rawdir=$5

# Modify as you need
#--------------------------------------------------
HALLC_REPLAY_DIR="/home/$USER/hallc_replay"   # my replay directory
DATA_DIR="${rawdir}"
ROOT_FILE="/path/to/rootfile/directory"
REPORT_OUTPUT="/path/to/REPORT_OTUPUT/directory"
APPTAINER_IMAGE="${apptainer}"
#--------------------------------------------------

cd $HALLC_REPLAY_DIR

# Check if apptainer is available
if command -v apptainer > /dev/null 2>&1; then
    echo "apptainer is already available."
else
    # Load apptainer if not available
    echo "Loading apptainer..."
   if ! eval module load apptainer; then
        echo "Failed to load apptainer. Please check if the module is installed and accessible."
        exit 1  # Exit the script with a non-zero exit code
    fi
fi

echo
echo "---------------------------------------------------------------------------------------------"
echo "REPLAY for ${runNum}. NEvent=${nEvent} using container=${APPTAINER_IMAGE}"
echo "----------------------------------------------------------------------------------------------"
echo

runStr="apptainer exec --bind ${DATA_DIR} --bind ${APPTAINER_IMAGE}--bind ${ROOT_FILE} --bind ${REPORT_OUTPUT} --bind ${HALLC_REPLAY_DIR}  ${APPTAINER_IMAGE} bash -c \"hcana -q ${script}\(${runNum},${nEvent}\)\""
eval ${runStr}