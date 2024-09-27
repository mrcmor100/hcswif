cnt=1;subStr="";tmp="";
infile=$1
#/mss/hallc/spring17/raw/
mssDir=$2
#spectrometer (lowercase)
spec=$3
#must put email
email=$4
grep -v '^#' < $infile | { while read line; do 
	stringarr=($line)
	tmp="${mssDir}${spec}_all_0${stringarr[0]}.dat"
	subStr="${subStr} $tmp"
	if (( $cnt % 20 == 0 ))
	then
	    echo $subStr
	    jcache get ${subStr} -e $email
	    echo ""
	    subStr=""
	fi
	((cnt=cnt+1))
done; echo "";echo $subStr;jcache get ${subStr} -e $email;
}
