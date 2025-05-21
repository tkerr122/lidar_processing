# Load packages
library(raster)
library(lidR)
library(rlas)

# Set data folder and number of cores for processing
folder <- "AZ_OrganPipe"

# Set working directory
input_dir <- paste0("/gpfs/glad1/Theo/Data/Lidar/LAZ/", folder)
output_dir <- paste0("/gpfs/glad1/Theo/Data/Lidar/CHMs/", folder, "_CHM")


# Create output directory if it doesn't exist
if (!dir.exists(output_dir)) {
  dir.create(output_dir)
}

# List all the .laz files in the input directory
laz_files <- list.files(input_dir, pattern = "\\.laz$", full.names = TRUE)

# Loop for processing LAS files
for (laz_file in laz_files) {
  # Generate output file name
  output_file <- file.path(output_dir, paste0(basename(tools::file_path_sans_ext(laz_file)), "_CHM.tif"))

  # Check if the file exists
  if (file.exists(output_file)) {
    print("File exists, skipping")
    next
  } else {
    # Check if the file is valid
    header <- rlas::read.lasheader(laz_file)
    if (header[1] != "LASF") {
      print("LAS file corrupted, skipping.")
	  rm(header)
      next
    } else {
      # Read the LAS file
      las <- readLAS(laz_file, filter = "-keep_class 1 2 -drop_withheld -drop_overlap")
      crs <- projection(las)

      # Skip empty files
      if (is.empty(las)) next

      # Generate Digital Terrain Model (DTM)
      dtm <- rasterize_terrain(las, res = 4.77731426716, knnidw())

      # Normalize the point cloud
      nlas <- las - dtm
      rm(las, dtm)

      # Generate Canopy Height Model (CHM)
      chm <- rasterize_canopy(nlas, res = 4.77731426716, algorithm = p2r())
      rm(nlas)
      
      # Ensure the CHM is a RasterLayer object
      if (class(chm) != "RasterLayer") {
        chm <- raster(chm)
      }
      
      # Reproject the raster to EPSG:3857
      proj <- "+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m +nadgrids=@null +wktext +no_defs"
      chm_r <- projectRaster(chm, crs=proj, res=4.77731426716)
      rm(chm)
      
      # Stretch the raster to 8-bit depending on projection
      vunits <- str_extract(crs, "(?<=\\+vunits=)[^\\s]+")
      if (vunits %in% c("us-ft", "ft")) {
          chm_r <- chm_r * 0.3048
      } else if (vunits == "m") {
      } else {
          stop(paste("Error: vertical units are:", vunits))
      }
      chm_r[chm_r > 60] <- 60
      chm_r_stretched <- ceiling(chm_r * 4)
      rm(chm_r)
      
      # Save as .tif using LZW compression
      writeRaster(chm_r, filename = output_file, format = "GTiff", options = "COMPRESS=LZW", datatype='INT1U')
      print(output_file)
      rm(chm_r_stretched)
    }
  }
}


# List of links to relevant functions to modify
# https://github.com/r-lidar/lidR/blob/master/src/LAS.cpp
# line 1212
#
# https://github.com/r-lidar/lidR/blob/master/src/RcppFunction.cpp
# line 108
#
# https://github.com/r-lidar/lidR/blob/master/src/RcppExports.cpp
# line 246
#
# https://github.com/r-lidar/lidR/blob/master/R/rasterize_canopy.R
#
# https://github.com/r-lidar/lidR/blob/master/R/algorithm-dsm.R
# 
# https://github.com/r-lidar/lidR/blob/master/R/fasterize.R