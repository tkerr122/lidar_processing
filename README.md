# LAZ to CHM Pipeline

An R workflow for processing raw LiDAR `.laz` files into Canopy Height Models (CHMs) using lidR and raster. Both scripts run in parallel across a user-specified number of cores.

---

## Scripts

| Script | Purpose |
|---|---|
| `LAZ_to_CHM.R` | Standard LAZ → CHM processing |
| `LAZ_to_DTM_CHM.R` | LAZ → DTM + CHM processing; use when a DTM is needed for downstream slope masking |

### Which script should I use?

- **Most surveys** → `LAZ_to_CHM.R`
- **Desert or arid surveys** where you anticipate slope errors in the CHM → `LAZ_to_DTM_CHM.R`

The DTM variant saves an additional reprojected DTM raster alongside the CHM, which can later be used for slope-based masking in the CHM cleaning pipeline.

---

## Processing Overview

Both scripts follow the same core steps for each `.laz` file in the survey folder:

1. **Validate** the LAZ file header — corrupted or empty files are skipped
2. **Detect vertical units** (`m`, `ft`, or `us-ft`) and set the scale factor and resolution accordingly
3. **Generate a DTM** using k-nearest neighbor inverse distance weighting (`knnidw`)
4. **Normalize the point cloud** by subtracting the DTM
5. **Generate a CHM** from the normalized point cloud using point-to-raster (`p2r`)
6. **Reproject** to EPSG:3857 at a resolution of ~4.78 m/pixel
7. **Stretch to 8-bit** — values are capped at 60 m, scaled by 4, and saved as `INT1U`
8. **Save** as a LZW-compressed GeoTIFF

`LAZ_to_DTM_CHM.R` additionally saves the DTM as a `FLT4S` (32-bit float) GeoTIFF before proceeding to CHM generation.

---

## CLI Flags

Both scripts share the same two arguments:

```
Rscript LAZ_to_CHM.R -s <survey_name> -c <num_cores>
Rscript LAZ_to_DTM_CHM.R -s <survey_name> -c <num_cores>
```

| Flag | Long form | Required | Description |
|---|---|---|---|
| `-s` | `--survey` | Yes | Survey folder name |
| `-c` | `--cores` | Yes | Number of parallel cores to use |

---

## Input / Output Paths

Input and output paths are currently hardcoded:

| | Path |
|---|---|
| **Input LAZ files** | `/gpfs/glad1/Theo/Data/Lidar/LAZ/<survey>/` |
| **Output CHMs** | `/gpfs/glad1/Theo/Data/Lidar/CHMs_raw/<survey>_CHM/` |
| **Output DTMs** *(DTM variant only)* | `/gpfs/glad1/Theo/Data/Lidar/DTMs/<survey>_DTM/` |

Output directories are created automatically if they don't exist. Individual files are skipped if their output already exists.

---

## Output Format

| Product | Data type | Encoding | Max value |
|---|---|---|---|
| CHM | `INT1U` (8-bit unsigned) | `pixel_value / 4 = height in meters` | 60 m → pixel value 240 |
| DTM | `FLT4S` (32-bit float) | Raw meters | — |

Both outputs use LZW compression and are reprojected to EPSG:3857.

---

## Dependencies

All processing is done in R with the following packages:

- `lidR` — point cloud processing, DTM and CHM generation
- `raster` — raster manipulation and reprojection
- `rlas` — LAZ header validation
- `foreach` / `doParallel` — parallelization
- `stringr` — vertical unit extraction from CRS string
- `argparse` — command-line argument parsing

---

## Environment

Designed to run on a Linux cluster. Ensure the required R packages are installed in your environment before running.