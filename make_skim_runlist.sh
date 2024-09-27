#!/bin/bash

# Define an associative array to store the largest NUM for each RUN
declare -A max_num

run_min=$(($1-1))
run_max=$(($2+1))
fsize=$3

if [ -z "$fsize" ]; then
    fsize=0;
fi

# Iterate through the files
for file in /mss/hallc/c-nps/raw/nps_coin_*.dat.*; do
    # Extract the RUN and NUM parts from the filename
    if [[ $file =~ ^/mss/hallc/c-nps/raw/nps_coin_([0-9]+)\.dat\.([0-9]+)$ ]]; then
        run="${BASH_REMATCH[1]}"
        num="${BASH_REMATCH[2]}"
	#echo $run $num
        # Compare the current NUM with the stored maximum for the nps_coin RUN
        if [ "$num" -ge "${max_num[$run]:-0}" ]; then
            max_num[$run]=$num
        fi
    fi
done

# Iterate through the associative array and print the filenames with the highest NUM
for run in "${!max_num[@]}"; do
    if [ $run -lt $run_max ] && [ $run -gt $run_min ]; then
	#echo "nps_coin_$run.dat.${max_num[$run]}"
	max_file="nps_coin_${run}.dat.${max_num[$run]}"
	size=$(awk -F'=' '$1=="size" { print $2+0 }' "/mss/hallc/c-nps/raw/$max_file")
	if [ "${max_num[$run]}" -eq 0 ]; then 
            if [ "$size" -gt $fsize ]; then
		echo "$run ${max_num[$run]} $size"  # Display the max_file when size is over 100MB
            fi
	else 
            echo "$run ${max_num[$run]} $size"  # Display the max_file when multiple segments.
	fi
    fi
done
