#!/usr/bin/bash

# -----------------------------------------------------------------------------
#  Change these if this if not where hallc_replay and hcana live
export hcana_dir=/group/c-xem2/software/XEM_v1.0.0/hcana
export hallc_replay_dir=/group/c-xem2/$USER/hallc_replay_XEM

# -----------------------------------------------------------------------------
#  Change if this gives you the wrong version of root, evio, etc
source /group/c-xem2/software/setup.sh

# -----------------------------------------------------------------------------
# Source setup scripts
curdir=`pwd`
cd $hcana_dir
source setup.sh
export PATH=$hcana_dir/bin:$PATH
echo Sourced $hcana_dir/setup.sh

cd $hallc_replay_dir
source setup.sh
echo Sourced $hallc_replay_dir/setup.sh

echo cd back to $curdir
cd $curdir

