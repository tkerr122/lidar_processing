#!/bin/bash

# -s: survey

# Load GDAL
module load rh9/gdal

# Define i/o folders
input_folder="/gpfs/glad1/Theo/Data/Capstone/CHMs/AL_17County_CHM_P2R"
output_file="/gpfs/glad1/Theo/Data/Capstone/CHMs/AL_17County_CHM_P2R.vrt"
output_tiff="/gpfs/glad1/Theo/Data/Capstone/CHMs/AL_17County_CHM_P2R.tif"

# Create VRT
gdalbuildvrt "${output_file}" "${input_folder}"/*.tif

# Create TIFF
gdal_translate -of GTiff -co COMPRESS=LZW -co BIGTIFF=YES -co TILED=yes "$output_file" "$output_tiff"
gdaladdo -r average "$output_tiff" 2 4 8 16 32 64 128

# Clean up
rm -f "$output_file"

echo "TIFF created"
