# Load packages
suppressPackageStartupMessages({
  library(raster)
  library(lidR)
  library(foreach)
  library(doParallel)
  library(rlas)
  library(stringr)
  library(argparse)
  library(tictoc)
})

# Create argument parser
parser <- ArgumentParser(description = "DTM processing script")
parser$add_argument("-s", "--survey", help = "LAZ Survey name", required = TRUE)
parser$add_argument("-c", "--cores", type = "integer", help = "Number of cores to use", required = TRUE)
parser$add_argument("-a", "--algorithm", help = "DTM algorithm", required = TRUE)

# Parse arguments
args <- parser$parse_args()

# Set variables
folder <- args$survey
cl <- args$cores
algo <- args$algorithm
algo <- tolower(algo)
input_dir <- paste0("/gpfs/glad1/Theo/Data/Capstone/LAZ/", folder)

if (algo == "idw") {
  dtm_algorithm <- knnidw()
  output_dir <- paste0("/gpfs/glad1/Theo/Data/Capstone/DTMs/", folder, "_DTM_IDW")
} else if (algo == "kriging") {
  dtm_algorithm <- kriging()
  output_dir <- paste0("/gpfs/glad1/Theo/Data/Capstone/DTMs/", folder, "_DTM_Kriging")
} else {
  stop("You must either enter IDW or Kriging for the algorithm")
}
if (!dir.exists(output_dir)) {
  dir.create(output_dir)
}

# List all the .laz files in the input directory
laz_files <- list.files(input_dir, pattern = "\\.laz$", full.names = TRUE)

# Set up a parallel cluster: cl = number of cores
tic()
registerDoParallel(cl)

# Parallelized loop for processing LAS files into DTM using given algorithm
foreach(laz_file = laz_files, .combine = "c", .errorhandling = "remove") %dopar% {
  # Generate output file name
  output_file <- file.path(output_dir, paste0(basename(tools::file_path_sans_ext(laz_file)), "_DTM.tif"))

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
      crs <- as.character(projection(las))
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
      dtm <- rasterize_terrain(las, res = resolution, dtm_algorithm)

      # Ensure the DTM is a RasterLayer object
      if (class(dtm) != "RasterLayer") {
        dtm <- raster(dtm)
      }

      # Reproject the raster to EPSG:3857
      proj <- "+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m +nadgrids=@null +wktext +no_defs"
      dtm_r <- projectRaster(dtm, crs = proj, res = 4.77731426716)
      rm(dtm)

      # Save as .tif using LZW compression
      writeRaster(dtm_r, filename = output_file, format = "GTiff", options = "COMPRESS=LZW", datatype = "FLT4S")
      print(output_file)
      rm(dtm_r)
    }
  }
}

# Extract timing
timing <- toc(quiet = TRUE)
elapsed_time <- timing$toc - timing$tic

# Convert to hours, minutes, seconds
time_file <- "/gpfs/glad1/Theo/Data/Capstone/Logs/DTM_time.txt"
hours <- floor(elapsed_time / 3600)
mins  <- floor((elapsed_time %% 3600) / 60)
secs  <- round(elapsed_time %% 60, 2)

# Write to log
write(
  paste(Sys.time(), "-- CHM processing took", hours, "hr", mins, "min", secs, "sec for", folder, "with", algo, "using", cl, "cores"),
  file = time_file,
  append = TRUE
)
