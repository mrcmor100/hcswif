#!/usr/bin/bash
#sg c-nps #Set group to c-nps, so files go into /mss/ properly (hopefully)


#source /apps/modules/5.2.0/init/profile.sh
#source /apps/modules/5.2.0/init/profile.csh
source /etc/profile
module use /group/halla/modulefiles
module use /group/nps/modulefiles
module load nps_replay/5.28.24
#setenv LD_LIBRARY_PATH /group/nps/$USER/NPSlib/BUILD/lib64:$LD_LIBRARY_PATH
#export LD_LIBRARY_PATH=/group/nps/$USER/NPSlib/BUILD/lib64:$LD_LIBRARY_PATH
# -----------------------------------------------------------------------------
#  Change these if this if not where hallc_replay and hcana live
#export REPLAYDIR=/group/nps/$USER/nps_replay
echo "Modules in use:"
module list
echo "This is the PATH variable:"
echo $PATH
echo "This is LD_LIBRARY_PATH"
echo $LD_LIBRARY_PATH
# Source setup scripts
curdir=`pwd`

cd $REPLAYDIR
#source setup.sh
#echo Sourced $REPLAYDIR/setup.sh

echo cd back to $curdir
cd $curdir

