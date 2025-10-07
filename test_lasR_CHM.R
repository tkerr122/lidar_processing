# Load packages
library(lasR)

# Set data folder and number of cores for processing
folder <- "AZ_OrganPipe"
# n_cores <- 1

# Set working directory and subdirectories
input_dir <- paste0("/gpfs/glad1/Theo/Data/Lidar/Testing/", folder)
output_dir <- paste0("/gpfs/glad1/Theo/Data/Lidar/CHMs_v3/", folder, "_CHMs")

if (!dir.exists(output_dir)) {
  dir.create(output_dir)
}

# # Create lidar processing pipeline
# Generate DTM
dtm <- triangulate(filter = keep_ground())

# Normalize the point cloud
nlas <- normalize()

# Generate CHMs
chm_max <- rasterize(4.77731426716, "z_max", ofile = paste0(output_dir, "/chm_*.tif"))

# Generate spatial index and execute pipeline
pipeline <- reader() + dtm + nlas + chm_max
exec(write_lax(embedded = FALSE, overwrite = TRUE), on = input_dir)
exec(pipeline, on = input_dir)