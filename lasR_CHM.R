# Load packages
library(lasR)
library(foreach)
library(doParallel)
library(rlas)
library(raster)

# Set data folder and number of cores for processing
folder <- "AK_Delta"
cl <- 100

# Set working directory and subdirectories
input_dir <- paste0("/gpfs/glad1/Theo/Data/Lidar/LAZ/", folder)
output_dir <- paste0("/gpfs/glad1/Theo/Data/Lidar/CHMs_v2/", folder, "_CHMs")
max_dir <- paste0(output_dir, "/CHM_max")
median_dir <- paste0(output_dir, "/CHM_median")
dir_75 <- paste0(output_dir, "/CHM_75th")
dir_25 <- paste0(output_dir, "/CHM_25th")

for (dir in list(output_dir, max_dir, median_dir, dir_75, dir_25)) {
  if (!dir.exists(dir)) {
    dir.create(dir)
  }
}

# # Create lidar processing pipeline
# Step 1: Read the las files
read_las <- reader_las()

# Step 2: Generate DTM
dtm <- triangulate(filter = keep_ground())

# Step 3: Normalize the point cloud
nlas <- normalize()

# Step 4: Generate CHMs for max, median, 75th, and 25th percentile heights
chm_max <- rasterize(4.77731426716, "z_max")
chm_median <- rasterize(4.77731426716, "z_median")
chm_75th <- rasterize(4.77731426716, "z_p75")
chm_25th <- rasterize(4.77731426716, "z_p25")

# Step 5: Create a loop to parallelize pipeline
pipeline <- read_las + dtm + nlas + chm_max + chm_median + chm_75th + chm_25th
laz_files <- list.files(input_dir, pattern = "\\.laz$", full.names = TRUE)
registerDoParallel(cl)
foreach(laz_file = laz_files, .combine = 'c', .errorhandling = 'remove') %dopar% {
  # Create output
  output <- exec(pipeline, on = laz_file)
  
  # Reproject
  proj <- "+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m +nadgrids=@null +wktext +no_defs"
  r_chm_max <- projectRaster(output[[0]], crs=proj, res=4.77731426716)
  r_chm_median <- projectRaster(output[[1]], crs=proj, res=4.77731426716)
  r_chm_75 <- projectRaster(output[[2]], crs=proj, res=4.77731426716)
  r_chm_25 <- projectRaster(output[[3]], crs=proj, res=4.77731426716)

  # Save
  max_file <- file.path(output_dir, paste0("chm_max_", basename(tools::file_path_sans_ext(laz_file))))
  writeRaster(r_chm_max, filename = max_file, format = "GTiff", options = "COMPRESS=LZW", datatype='INT1U')

  median_file <- file.path(output_dir, paste0("chm_median_", basename(tools::file_path_sans_ext(laz_file))))
  writeRaster(r_chm_median, filename = median_file, format = "GTiff", options = "COMPRESS=LZW", datatype='INT1U')

  file_75 <- file.path(output_dir, paste0("chm_75_", basename(tools::file_path_sans_ext(laz_file))))
  writeRaster(r_chm_75, filename = file_75, format = "GTiff", options = "COMPRESS=LZW", datatype='INT1U')

  file_25 <- file.path(output_dir, paste0("chm_25_", basename(tools::file_path_sans_ext(laz_file))))
  writeRaster(r_chm_25, filename = file_25, format = "GTiff", options = "COMPRESS=LZW", datatype='INT1U')
}