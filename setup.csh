#!/usr/bin/csh

# -----------------------------------------------------------------------------
#  Change these if this if not where hallc_replay and hcana live
setenv hcana_dir "/group/c-xem2/software/XEM_v1.0.0/hcana"
setenv hallc_replay_dir "/group/c-xem2/$USER/hallc_replay_XEM"

# -----------------------------------------------------------------------------
#  Change if this gives you the wrong version of root, evio, etc
source /group/c-xem2/software/setup_v1.0.0.csh

# -----------------------------------------------------------------------------
# Source setup scripts
set curdir=`pwd`
cd $hcana_dir
source setup.csh
setenv PATH "$hcana_dir/bin:$PATH"
echo Sourced $hcana_dir/setup.csh

cd $hallc_replay_dir
source setup.csh
echo Sourced $hallc_replay_dir/setup.csh

echo cd back to $curdir
cd $curdir

