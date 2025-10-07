#!/bin/bash

# -s: survey
# -t: txt file number

# Initialize variables
survey=""
txt_file_number=""

# Parse command-line arguments
while getopts "s:t:" opt; do
  case ${opt} in
    s )
      survey=$OPTARG
      ;;
    t )
      txt_file_number=$OPTARG
      ;;
    \? )
      echo "Usage: sh $0 -s <survey_name> [-t <txt_file_number>]"
      exit 1
      ;;
  esac
done

# Validate required arguments
if [ -z "$survey" ]; then
  echo "Error: survey name not provided."
  echo "Usage: sh $0 -s <survey_name> [-t <txt_file_number>]"
  exit 1
fi

nb_jobs=100

# # Download from .txt file
# cd /gpfs/glad1/Theo/Data/Lidar/LAZ/${folder} || exit
# wget -i "OR_UpperJohnDay.txt" 

# # Continue an interrupted download with the -c flag
# cd /gpfs/glad1/Theo/Data/Lidar/LAZ/${folder} || exit
# wget -c -nc -i "${folder}${txt_file_number}.txt"

# Download files in parallel
cd /gpfs/glad1/Theo/Data/Lidar/LAZ/${survey} || exit
module load parallel/20151222
cat "${survey}${txt_file_number}.txt" | parallel -j$nb_jobs wget -c -nc --wait=5 --random-wait --limit-rate=3M --no-check-certificate --retry-connrefused {}