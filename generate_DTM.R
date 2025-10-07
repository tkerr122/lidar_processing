# Load packages
suppressPackageStartupMessages({
  library(raster)
  library(lidR)
  library(foreach)
  library(doParallel)
  library(rlas)
  library(stringr)
  library(argparse)
})

# Create argument parser
parser <- ArgumentParser(description = "DTM processing script")
parser$add_argument("-s", "--survey", help = "LAZ Survey name", required=TRUE)
parser$add_argument("-c", "--cores", type = "integer", help = "Number of cores to use", required=TRUE)

# Parse arguments
args <- parser$parse_args()

# Set variables from command line arguments
folder <- args$survey
cl <- args$cores

# Set working directory
input_dir <- paste0("/gpfs/glad1/Theo/Data/Lidar/LAZ/", folder)
idw_output_dir <- paste0("/gpfs/glad1/Theo/Data/Capstone/DTMs", folder, "_DTM_IDW")
kriging_output_dir <- paste0("/gpfs/glad1/Theo/Data/Lidar/CHMs_raw/", folder, "_DTM_Kriging")

# Create output directory if it doesn't exist
if (!dir.exists(output_dir)) {
  dir.create(output_dir)
}

# List all the .laz files in the input directory
laz_files <- list.files(input_dir, pattern = "\\.laz$", full.names = TRUE)

# Set up a parallel cluster: cl = number of cores
registerDoParallel(cl)

# Parallelized loop for processing LAS files
foreach(laz_file = laz_files, .combine = "c", .errorhandling = "remove") %dopar% {
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
      vunits <- str_extract(crs, "(?<=\\+vunits=)[^\\s]+")
      if (vunits %in% c("us-ft", "ft")) {
        scale_factor <- 0.3048
        resolution <- 4.77731426716 * 3.28084
      } else if (vunits == "m") {
        scale_factor <- 1
        resolution <- 4.77731426716
      } else {
        stop(paste("Error: vertical units are:", vunits))
      }

      # Skip empty files
      if (is.empty(las)) next

      # Generate Digital Terrain Model (DTM)
      dtm_idw <- rasterize_terrain(las, res = resolution, knnidw())
      dtm_kriging <- rasterize_terrain(las, res = resolution, kriging())

      # Normalize the point cloud
      nlas1 <- las - dtm_idw
      nlas2 <- las - dtm_kriging
      rm(las)
      
      # Ensure the CHM is a RasterLayer object
      if (class(dtm_idw) != "RasterLayer") {
        dtm_idw <- raster(dtm_idw)
      }
      if (class(dtm_kriging) != "RasterLayer") {
        dtm_kriging <- raster(dtm_kriging)
      }
      
      # Reproject the raster to EPSG:3857
      proj <- "+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m +nadgrids=@null +wktext +no_defs"
      dtm_idw_r <- projectRaster(dtm_idw, crs = proj, res = 4.77731426716)
      dtm_kriging_r <- projectRaster(dtm_kriging, crs = proj, res = 4.77731426716)

      rm(dtm_idw)
      rm(dtm_idw)
      
      # Stretch the raster to 8-bit depending on projection
      chm_r <- chm_r * scale_factor
      chm_r[chm_r > 60] <- 60
      chm_r_stretched <- ceiling(chm_r * 4)
      rm(chm_r)
      
      # Save as .tif using LZW compression
      writeRaster(chm_r_stretched, filename = output_file, format = "GTiff", options = "COMPRESS=LZW", datatype = "INT1U")
      print(output_file)
      rm(chm_r_stretched)
    }
  }
}