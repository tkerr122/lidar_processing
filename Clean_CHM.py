# Theo Kerr on 3/7/2025
# For use on linux cluster "gdalenv" conda env

# Imports/env settings 
from All_clean_CHM import *
import argparse
gdal.UseExceptions()

"""I have written this script to be a command-line utility for cleaning a CHM of powerlines, water, and if
desired, slope errors, using specific values for height and NDVI thresholds. 
================================================
-s option: survey name.
-bs option: desired buffer size for powerlines, in meters. Defaults to 50.
-st option: whether or not to save the temp rasters.
-mp option: whether or not to use a manual powerline file for additional powerline masking.
-ms option: whether or not to use a manual slope errors shapefile for slope masking.
-ht option: value to use for CHM-wide slope masking using worldcover
-wcmv option: list of pixel values from worldcover image for masking
-grt option: threshold for greenred masking

Assumes the following input variables are hardcoded:
 - input_chm
 - data_folders
 - output_folder
 - crs
 - pixel_size
"""

# Create argument parser
parser = argparse.ArgumentParser(description="Script for cleaning single CHM")
parser.add_argument("-s", "--survey", type=str, help="Survey name", required=True)
parser.add_argument("-bs", "--buffer-size", type=int, default=50, help="Buffer size")
parser.add_argument("-st", "--save-temp", action="store_true", help="Save temp dir")
parser.add_argument("-mp", "--man-pwl", action="store_true", help="Buffer manual powerlines")
parser.add_argument("-ms", "--man-slp", action="store_true", help="Mask with manual slope")
parser.add_argument("-ht", "--height-threshold", type=int, help="Mask slope using height threshold")
parser.add_argument("-wcmv", "--wc-mask-values", nargs='+', type=int, default=[30, 60, 70, 100], help="List of WorldCover mask values")
parser.add_argument("-grt", "--greenred-threshold", type=int, default=135, help="Cutoff for greenred")
parser.add_argument("-bt", "--building-threshold", type=int, default=30, help="Cutoff for building mask")

# Parse arguments
args = parser.parse_args()

# Set up variables
survey = args.survey
buffer_size = args.buffer_size
save_temp = args.save_temp
man_pwl = args.man_pwl
man_slp = args.man_slp
height_threshold = args.height_threshold
wc_mask_values = args.wc_mask_values
greenred_threshold = args.greenred_threshold
building_threshold = args.building_threshold

input_chm = f"/gpfs/glad1/Theo/Data/Capstone/Raw_CHMs/{survey}.tif"
output_tiff = f"/gpfs/glad1/Theo/Data/Capstone/Cleaned_CHMs/{survey}_CHM_cleaned.tif"
data_folders = ["/gpfs/glad1/Theo/Data/Lidar/CHM_cleaning/Canopy/Canopy.shp", 
    "/gpfs/glad1/Theo/Data/Lidar/CHM_cleaning/Powerlines", 
    "/gpfs/glad1/Theo/Data/Lidar/CHM_cleaning/Manual_powerlines", 
    "/gpfs/glad1/Theo/Data/Lidar/CHM_cleaning/Water", 
    "/gpfs/glad1/Theo/Data/Lidar/CHM_cleaning/Landsat", 
    "/gpfs/glad1/Theo/Data/Lidar/CHM_cleaning/Slope_errors/Slope_errors.shp",
    "/gpfs/glad1/Theo/Data/Lidar/CHM_cleaning/WorldCover",
    "/gpfs/glad1/Theo/Data/Lidar/CHM_cleaning/Planet_tiles",
    "/gpfs/glad1/Theo/Data/Lidar/CHM_cleaning/Building_mask_2022"]
crs = "EPSG:3857"
pixel_size = 4.77731426716

# Clean the CHM
clean_chm(input_chm, output_tiff, data_folders, crs, pixel_size, buffer_size, save_temp, man_pwl, man_slp, height_threshold, wc_mask_values, greenred_threshold, building_threshold)