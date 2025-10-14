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
parser <- ArgumentParser(description = "LAZ to CHM processing script")
parser$add_argument("-s", "--survey", help = "LAZ Survey name", required = TRUE)
parser$add_argument("-c", "--cores", type = "integer", help = "Number of cores to use", required = TRUE)
parser$add_argument("-a", "--algorithm", help = "CHM algorithm", required = TRUE)

# Parse arguments
args <- parser$parse_args()

# Set variables 
folder <- args$survey
cores <- args$cores
algo <- args$algorithm
algo <- tolower(algo)

input_dir <- paste0("/gpfs/glad1/Theo/Data/Capstone/LAZ/", folder)

if (algo == "p2r") {
  chm_algorithm <- p2r()
  output_dir <- paste0("/gpfs/glad1/Theo/Data/Capstone/CHMs/", folder, "_CHM_P2R")
} else if (algo == "tin") {
  chm_algorithm <- dsmtin()
  output_dir <- paste0("/gpfs/glad1/Theo/Data/Capstone/CHMs/", folder, "_CHM_TIN")
} else if (algo == "pitfree") {
  chm_algorithm <- pitfree(thresholds = c(0, 10, 20), max_edge = c(0, 1))
  output_dir <- paste0("/gpfs/glad1/Theo/Data/Capstone/CHMs/", folder, "_CHM_Pitfree")
} else {
  stop("You must either enter P2R, TIN or Pitfree for the algorithm")
}
if (!dir.exists(output_dir)) {
  dir.create(output_dir)
}

# List all the .laz files in the input directory
laz_files <- list.files(input_dir, pattern = "\\.laz$", full.names = TRUE)

# Set up a parallel cluster: cl = number of cores
tic()
cl <- makeCluster(cores)
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
      dtm <- rasterize_terrain(las, res = resolution, knnidw())

      # Normalize the point cloud
      nlas <- las - dtm
      rm(las, dtm)

      # Generate Canopy Height Model (CHM)
      chm <- rasterize_canopy(nlas, res = resolution, algorithm = chm_algorithm)
      rm(nlas)

      # Ensure the CHM is a RasterLayer object
      if (class(chm) != "RasterLayer") {
        chm <- raster(chm)
      }

      # Reproject the raster to EPSG:3857
      proj <- "+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m +nadgrids=@null +wktext +no_defs"
      chm_r <- projectRaster(chm, crs = proj, res = 4.77731426716)
      rm(chm)

      # Convert pixel values to proper vertical units
      chm_r <- chm_r * scale_factor

      # Save as .tif using LZW compression
      writeRaster(chm_r, filename = output_file, format = "GTiff", options = "COMPRESS=LZW", datatype = "INT1U")
      print(output_file)
      rm(chm_r)
    }
  }
}

# Extract timing
timing <- toc(quiet = TRUE)
elapsed_time <- timing$toc - timing$tic

# Write to output file
time_file <- "/gpfs/glad1/Theo/Data/Capstone/Logs/CHM_time.txt"
mins <- floor(elapsed_time / 60)
secs <- round(elapsed_time %% 60, 2)

write(
  paste(Sys.time(), "--CHM processing took", mins, "min", secs, "sec for", folder, "with", algo, "using", cl, "cores"),
  file = time_file,
  append = TRUE
)