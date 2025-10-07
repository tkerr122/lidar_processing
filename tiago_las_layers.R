#! Rscript

# Load required libraries
suppressPackageStartupMessages({
  library(lidR)
  library(future)
  library(tictoc)
  library(argparse)
})

N_CORES = parallel::detectCores() %/% 2

# Create argument parser
parser <- ArgumentParser(description="LiDAR data processing script")
parser$add_argument("-i","--input", help="Input directory path")
parser$add_argument("-o","--output", help="Output directory path")
parser$add_argument("-c", "--cores", type="integer", default=N_CORES, help="Number of CPU cores to use")
parser$add_argument("-r", "--resolution", type="double", default=1, help="Output resolution (in point cloud units)")
parser$add_argument("--trust-ground", action="store_true", help="Trust existing ground classification")
parser$add_argument("--trust-noise", action="store_true", help="Trust existing noise classification")

# Parse arguments
args <- parser$parse_args()

# Set variables from command line arguments
N_CORES <- args$cores
RES <- args$resolution
TRUST_GROUND <- args$trust_ground
TRUST_NOISE <- args$trust_noise
input <- args$input
odir <- args$output

tic()
set_lidr_threads(1)
plan(multicore, workers=N_CORES)

ctg = readLAScatalog(input, select='xyzic')

opt_chunk_size(ctg) = 1000
opt_chunk_buffer(ctg) = ifelse(RES*10 > 20, 20, RES*10)
opt_progress(ctg) = TRUE
opt_laz_compression(ctg) = TRUE
opt_merge(ctg) = TRUE
opt_stop_early(ctg) = FALSE

las_tmp = readLAS(ctg$filename[1], filter='-keep_random_fraction 0.1')
has_noise = any(las_tmp$Classification == LASNOISE)
has_ground = any(las_tmp$Classification == LASGROUND)

if( !(has_noise && TRUST_NOISE) ){
  cat("\n##-- classifying noise\n")
  opt_output_files(ctg) = file.path(odir, 'noised/tile_{YBOTTOM}_{XLEFT}')
  ctg = classify_noise(ctg, ivf())
  opt_chunk_size(ctg) = 0
}

if( !(has_ground && TRUST_GROUND) ){
  cat("\n##-- classifying ground\n")
  opt_output_files(ctg) = file.path(odir, 'ground/tile_{YBOTTOM}_{XLEFT}')
  ctg = classify_ground(ctg, csf(), FALSE)
  opt_chunk_size(ctg) = 0
}

opt_filter(ctg) = paste("-drop_class", LASNOISE)

cat("\n##-- interpolating DTM\n")
opt_output_files(ctg) = file.path(odir, 'dtm/tile_{YBOTTOM}_{XLEFT}')
dtm = rasterize_terrain(ctg, RES, knnidw())
terra::writeRaster(dtm, file.path(odir, 'dtm.tif'), overwrite=TRUE)

cat("\n##-- normalizing heights\n")
opt_output_files(ctg) = file.path(odir, 'normalized/tile_{YBOTTOM}_{XLEFT}')
ctg = normalize_height(ctg, dtm)

cat("\n##-- interpolating CHM\n")
opt_output_files(ctg) = file.path(odir, 'chm/tile_{YBOTTOM}_{XLEFT}')
chm = rasterize_canopy(ctg, p2r(), res=RES)
terra::writeRaster(chm, file.path(odir, 'chm.tif'), overwrite=TRUE)

cat("\n##-- DONE\n")
toc()
