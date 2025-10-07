library(raster)
library(lidR)
library(rlas)
library(stringr)
library(future)

#!! This script doesn't work


# Set data folder and paths
folder <- "AZ_OrganPipe"
cores <- 15
input_dir <- paste0("/gpfs/glad1/Theo/Data/Lidar/LAZ/", folder)
output_dir <- paste0("/gpfs/glad1/Theo/Data/Lidar/CHMs_v3/", folder, "_CHMs")

# Create output directory if it doesn't exist
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
}

# # Compute CHM function
# compute_chm <- function(chunk) {
#   # Read the LAS file
#   las <- readLAS(chunk)
#   crs <- projection(las)
      
#   # Skip empty files
#   if (is.empty(las)) return(NULL)

#   # Generate Digital Terrain Model (DTM)
#   dtm <- rasterize_terrain(las, res = 4.77731426716, knnidw())

#   # Normalize the point cloud
#   nlas <- las - dtm
#   rm(las, dtm)

#   # Generate Canopy Height Model (CHM)
#   chm <- rasterize_canopy(nlas, res = 4.77731426716, algorithm = p2r())
#   rm(nlas)
  
#   # Ensure the CHM is a RasterLayer object
#   if (class(chm) != "RasterLayer") {
#     chm <- raster(chm)
#   }
  
#   # Reproject the raster to EPSG:3857
#   proj <- "+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m +nadgrids=@null +wktext +no_defs"
#   chm_r <- raster::projectRaster(chm, crs=proj, res=4.77731426716)
#   rm(chm)
  
#   # Stretch the raster to 8-bit depending on projection
#   vunits <- str_extract(crs, "(?<=\\+vunits=)[^\\s]+")
#   if (vunits %in% c("us-ft", "ft")) {
#       chm_r <- chm_r * 0.3048
#   } else if (vunits == "m") {
#   } else {
#       stop(paste("Error: vertical units are:", vunits))
#   }

#   chm_r[chm_r > 60] <- 60
#   chm_r_stretched <- ceiling(chm_r * 4)
#   rm(chm_r)

#   # Return the output
#   return(chm_r_stretched)
# }

# Use the function and save the output to disk
ctg <-readLAScatalog(input_dir)
crs <- projection(ctg)
print(paste0("CRS is: ",crs))

opt_output_files(ctg) <- paste0(output_dir, "/{*}_CHM")
opt_filter(ctg) <- "-keep_class 1 2 -drop_withheld -drop_overlap"

# # Create a parallel process
# plan(multisession, workers = cores)

# output <- tryCatch({
#   catalog_apply(ctg, compute_chm)
# }, error = function(e) {
#   warning(conditionMessage(e))
# })