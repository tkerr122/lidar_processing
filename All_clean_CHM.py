# Theo Kerr
# For use on linux cluster "gdalenv" conda env

# Imports/env settings 
import numpy as np
import pandas as pd
import geopandas as gpd
from osgeo import gdal, osr, ogr
from tqdm import tqdm
import os, shutil, math
gdal.UseExceptions()

# Define custom errors
class InvalidSurvey(Exception):
    pass

def get_chm_survey(chm_path):
    """Parses given CHM path for survey and state.

    Args:
        chm_path (str): path to CHM, assumed to be in format "path/to/chm/[state_abbr]_[survey]_CHM.tif".

    Returns:
        tuple: tuple containing survey name, and state.
    """
    chm_raw = os.path.splitext(os.path.basename(chm_path))[0]
    survey_name = chm_raw.rsplit("_CHM")[0]
    state = survey_name.rsplit("_")[0]
    
    return survey_name, state 

def get_chm_loc(chm):
    """Takes given raster dataset and finds which WorldCover and Planet tiles it intersects with, returns a list of the tiles. Planet_tile_list.csv is a hardcoded path.

    Args:
        chm (GDAL raster): CHM raster.

    Returns:
        tuple: tuple containing World Cover tiles, and planet tiles
    """
    # Get CHM geotransform info
    gt = chm.GetGeoTransform()
    xsize = chm.RasterXSize
    ysize = chm.RasterYSize
    
    x_left = gt[0]
    y_top = gt[3]
    
    x_right = x_left + (xsize * gt[1])
    y_bottom = y_top + (ysize * gt[5])
    
    # Find what lat lon tile it intersects with
    src_srs = osr.SpatialReference()
    src_srs.ImportFromEPSG(3857)
    dst_srs = osr.SpatialReference()
    dst_srs.ImportFromEPSG(4326)
    transform = osr.CoordinateTransformation(src_srs, dst_srs)
    lat_min, lon_min, _ = transform.TransformPoint(x_left, y_bottom)
    lat_max, lon_max, _ = transform.TransformPoint(x_right, y_top)
    
    # Extract WorldCover tile names
    def get_wc_tile_name(lat_min, lat_max, lon_min, lon_max):
        wc_names = []
        
        # Worldcover tiles have 3 degree step, starting in Southwest corner
        lat_start = math.floor(lat_min / 3) * 3 # Southmost tile
        lat_end = math.floor(lat_max / 3) * 3 # Northmost tile
        
        lon_start = math.floor(lon_min / 3) * 3 # Westmost tile
        lon_end = math.floor(lon_max / 3) * 3 # Eastmost tile
        
        for lat in range(lat_start, lat_end + 1, 3):
            for lon in range(lon_start, lon_end + 1, 3):
                lat_dir = "N" if lat > 0 else "S"
                lon_dir = "E" if lon > 0 else "W"
        
                lat_str = f"{lat_dir}{abs(lat):02d}"
                lon_str = f"{lon_dir}{abs(lon):03d}"
                
                tile_name = f"ESA_WorldCover_10m_2021_v200_{lat_str}{lon_str}_Map.tif"
                wc_names.append(tile_name)
        
        return wc_names
    
    # Extract planet tiles
    def get_planet_tile_name(lat_min, lat_max, lon_min, lon_max):
        tile_names = []
        
        # Planet tiles have 1 degree step, starting in Northwest corner
        lat_start = math.floor(lat_max) # Northmost tile
        lat_end = math.floor(lat_min) # Southmost tile
        
        lon_start = math.ceil(lon_min) # Westmost tile (because longitude in western hemisphere is negative)
        lon_end = math.ceil(lon_max) # Eastmost tile
        
        for lat in range(lat_start, lat_end - 1, -1): # Negative step to move downwards from maximum latitude
            for lon in range(lon_start, lon_end + 1, 1): # Because longitude is already negative, no need for negative step
                lat_dir = "N" if lat >= 0 else "S"
                lon_dir = "E" if lon >= 0 else "W"
        
                lat_str = f"{abs(lat):02d}{lat_dir}"
                lon_str = f"{abs(lon):03d}{lon_dir}"
                
                tile_name = f"{lon_str}_{lat_str}"
                tile_names.append(tile_name)
        
        planet_tiles = pd.read_csv("/gpfs/glad1/Theo/Data/Lidar/CHM_cleaning/Planet_tile_list/Planet_tile_list.csv")
        planet_tiles = planet_tiles[planet_tiles['TILE'].isin(tile_names)]
        
        planet_tile_names = planet_tiles['location'].tolist()
        planet_tile_names = [f"L15-{name}.tif" for name in planet_tile_names]
        
        deg_tile_names = [f"{tile}.tif" for tile in tile_names]
        
        return deg_tile_names, planet_tile_names
    
    wc_tiles = get_wc_tile_name(lat_min, lat_max, lon_min, lon_max)
    wc_tiles = sorted(set(wc_tiles))
    
    deg_tiles, planet_tiles = get_planet_tile_name(lat_min, lat_max, lon_min, lon_max)
    deg_tiles = sorted(set(deg_tiles))
    planet_tiles = sorted(set(planet_tiles))
        
    return wc_tiles, planet_tiles, deg_tiles

def get_raster_info(raster_path):
    """Opens a raster at the given path.

    Args:
        raster_path (str): path to a raster dataset.

    Returns:
        tuple: returns a GDAL raster, number of columns, number of rows, the geotransform, and the projection.
    """
    ds = gdal.Open(raster_path)
    xsize = ds.RasterXSize
    ysize = ds.RasterYSize
    transform = ds.GetGeoTransform()
    projection = ds.GetSpatialRef()
    
    print(f"Read in {os.path.basename(raster_path)}...")
    
    return ds, xsize, ysize, transform, projection

def extract_polygon(input_shp, survey, output_folder):
    """Reads in a shapefile and extracts the polygons with attribute "survey" == given survey value, creating a new output GeoJSON file.

    Args:
        input_shp (str): path to shapefile.
        survey (str): string for survey name (i.e. "AZ_BlackRock").
        output_folder (str): path to output folder for the new GeoJSON file.

    Raises:
        InvalidSurvey: custom error to make sure that the canopy shp indeed has polygons for the indicated survey, or that the survey name doesn't have a typo.

    Returns:
        str: path to the new canopy GeoJSON.
    """
    # Load in canopy shapefile and mask to survey
    shp = gpd.read_file(input_shp)
    polygon = shp[shp["Survey"] == survey]
    polygon_basename = os.path.splitext(os.path.basename(input_shp))[0]
    path = os.path.join(output_folder, f"{polygon_basename}_{survey}.geojson")
    if polygon.empty:
        print(f"\nThe survey name \"{survey}\" is incorrect or doesn't exist\n")
        raise InvalidSurvey(survey)
    
    # Write canopy geojson to file, if it hasn't already been done
    if os.path.isfile(path) == False:
        polygon.to_file(path, driver="GeoJSON")
        print(f"Created {os.path.basename(path)}...")
    else:
        print(f"\"{os.path.basename(path)}\" exists, saving path...")
    
    return path

def crop_raster(raster_path, output_folder, crs, pixel_size, cutline):
    """Uses the GDAL Warp function to reproject the given raster to given crs and pixel size, and crop to the given cutline. 

    Args:
        raster_path (str): path to a raster dataset.
        output_folder (str): folder for the output dataset.
        crs (str): string for a crs, in the format "EPSG:3857" for example.
        pixel_size (float): desired pixel size, in destination crs units.
        cutline (str): path to a GeoJSON cutline file.

    Returns:
        str: path to output warped raster.
    """
    # Set warp options
    if type(raster_path) == list and len(raster_path) != 1:
        raster_basename = os.path.splitext(os.path.basename(raster_path[0]))[0]
        dst_ds = f"{os.path.join(output_folder, raster_basename)}_cropped_merged.tif"
    elif type(raster_path) == list and len(raster_path) == 1:
        raster_basename = os.path.splitext(os.path.basename(raster_path[0]))[0]
        dst_ds = f"{os.path.join(output_folder, raster_basename)}_cropped.tif"
    else: 
        raster_basename = os.path.splitext(os.path.basename(raster_path))[0]
        dst_ds = f"{os.path.join(output_folder, raster_basename)}_cropped.tif"
    
    # Crop the raster, if it hasn't already been done
    if os.path.isfile(dst_ds) == False:
        print(f"Cropping {os.path.basename(dst_ds)}:")
        gdal.Warp(dst_ds, raster_path, format="GTiff", dstSRS=crs, xRes=pixel_size, yRes=pixel_size, cutlineDSName=cutline, cropToCutline=True, warpOptions=["COMPRESS=LZW", "BIGTIFF=YES"], callback=gdal.TermProgress_nocb)
    else:
        print(f"\"{os.path.basename(dst_ds)}\" exists, saving path...")
        
    return dst_ds

def rasterize(input_file, output_tiff, pixel_size, burn_value=None):
    """Uses the GDAL Rasterize Layer function to rasterize the given vector layer to the output tiff path, with the given pixel size. If a burn value is not specified, 
	the polygon value attribute is used.

    Args:
        input_file (str): path to vector dataset.
        output_tiff (str): path to output raster dataset.
        pixel_size (float): desired pixel size.
        burn_value (int, optional): desired output pixel value. Defaults to 1.
    """
    # Check if file exists
    if os.path.isfile(output_tiff) == False:
        # Open dataset
        dataset = ogr.Open(input_file)
        layer = dataset.GetLayer() 
        
        # Define raster properties
        x_min, x_max, y_min, y_max = layer.GetExtent()
        x_res = int((x_max - x_min) / pixel_size)
        y_res = int((y_max - y_min) / pixel_size)
        target_ds = gdal.GetDriverByName("GTiff").Create(output_tiff, x_res, y_res, 1, gdal.GDT_Byte, options=["COMPRESS=LZW", "BIGTIFF=YES"])
        target_ds.SetGeoTransform((x_min, pixel_size, 0, y_max, 0, -pixel_size))
        
        # Set projection
        srs = layer.GetSpatialRef()
        target_ds.SetProjection(srs.ExportToWkt())
        
        # Set band nodata value
        band = target_ds.GetRasterBand(1)
        band.SetNoDataValue(255)

        # Rasterize dataset
        print(f"Rasterizing {os.path.basename(input_file)}:")
        if burn_value is not None:
            gdal.RasterizeLayer(target_ds, [1], layer, burn_values=[burn_value], callback=gdal.TermProgress_nocb)
        else:
            gdal.RasterizeLayer(target_ds, [1], layer, options=["ATTRIBUTE=value"], callback=gdal.TermProgress_nocb)
      
        band = None
        target_ds = None
        dataset = None
        
    else:
        print(f"\"{os.path.basename(output_tiff)}\" exists, not rasterizing")
    
def buffer_powerlines(input_file, output_file, crs, pixel_size, buffer_size, cutline, burn_value=1):
    """Reads in given vector dataset and buffers it to given specifications.

    Args:
        input_file (str): path to input vector file.
        output_file (str): path to desired output GeoJSON.
        crs (str): string for the desired CRS, in the format "EPSG:3857".
        pixel_size (float): desired pixel size.
        buffer_size (int): desired size for the buffer.
        cutline (str): path to cutline GeoJSON for cropping.
        burn_value (int, optional): desired output pixel value. Defaults to 1.
    """
    # Check if file exists
    if os.path.exists(output_file) == False:
        # Read in powerline
        powerline = gpd.read_file(input_file)
        powerline = powerline.to_crs(crs)
        
        # Crop to cutline
        cutline_polygon = gpd.read_file(cutline)
        cutline_polygon = cutline_polygon.to_crs(crs)
        powerline_cropped = gpd.clip(powerline, cutline_polygon)

        if powerline_cropped.empty:
            print(f"The powerlines for {os.path.basename(input_file)} do not exist within the bounds of the study area, creating blank raster...")
            
            # Create blank powerlines raster over survey extent
            rasterize(cutline, output_file, pixel_size, burn_value=0)
        
        else:
            # Buffer to specified radius
            powerline_buffer = powerline_cropped.buffer(buffer_size, cap_style="square")
            powerline_geojson = f"{os.path.splitext(output_file)[0]}.geojson"
            powerline_buffer.to_file(powerline_geojson, driver="GeoJSON")
            
            # Rasterize the buffer
            rasterize(powerline_geojson, output_file, pixel_size, burn_value)
        
            # Remove temporary geojson buffer
            os.remove(powerline_geojson)
            
    else:
        print(f"\"{os.path.basename(output_file)}\" exists, not buffering")

def mask_powerlines(chm_array, powerlines_array):
    """Takes in an array for the CHM and the powerlines (must be same dimensions) and sets CHM values to 0 where there are powerlines.

    Args:
        chm_array (np.array): array for the CHM.
        powerlines_array (np.array): array for the powerlines (1 for presence).

    Returns:
        np.array: cleaned CHM array.
    """
    condition_mask = (powerlines_array == 1)
    chm_cleaned = np.where(condition_mask, 0, chm_array)
    chm_cleaned[chm_array == 255] = 255
    print("Cleaned CHM Powerlines...")
    
    return chm_cleaned

def mask_water(chm_array, water_array):
    """Takes in an array for the CHM and the water mask (must be same dimensions) and sets CHM values to 0 where there is water.

    Args:
        chm_array (np.array): array for the CHM.
        water_array (np.array): array for the water mask.

    Returns:
        np.array: cleaned CHM array.
    """
    condition_mask = (water_array == 80)
    chm_cleaned = np.where(condition_mask, 0, chm_array)
    print("Cleaned CHM Water...")
    
    return chm_cleaned

def mask_worldcover(chm_array, height_threshold, wc_array, wc_mask_values, slope_array=None):
    """Takes in an array for the CHM and worldcover, and depending on user input uses either a height threshold value to mask the entire CHM of slope errors, or uses a slope array for masking those areas, using the WorldCover mask values.

    Args:
        chm_array (np.array): array for the CHM.
        height_threshold (int): height value above which to use for masking.
        wc_array (np.array): array for the worldcover image.
        wc_mask_values (list): list of land cover types to retain as "True" for the wc mask.
        slope_array (np.array, optional): array for the manual slope errors file.

    Returns:
        np.array: cleaned CHM array
    """
    # If slope array is provided, use that for masking
    if slope_array is not None:
        # Set areas designated as ground to 0
        ground_mask = (slope_array == 0)
        chm_array = np.where(ground_mask, 0, chm_array)
        
        # Mask using worldcover
        wc_mask = np.isin(wc_array, wc_mask_values).astype(int)
        wc_mask[wc_mask == 0] = 255
        wc_mask[wc_mask == 1] = 1
        condition_mask = (wc_mask == 1) & (slope_array == 1)
        chm_cleaned = np.where(condition_mask, 0, chm_array)
        chm_cleaned[chm_array == 255] = 255
    
    else:
        # Mask CHM above height threshold using worldcover
        wc_mask = np.isin(wc_array, wc_mask_values).astype(int)
        wc_mask[wc_mask == 0] = 255
        wc_mask[wc_mask == 1] = 1
        condition_mask = (wc_mask == 1) & (chm_array > height_threshold)
        chm_cleaned = np.where(condition_mask, 0, chm_array)
        chm_cleaned[chm_array == 255] = 255

    print("Cleaned CHM slope errors using WorldCover...")
    
    return chm_cleaned

def calc_greenred(green_band, red_band, output_band, x, y, cols, rows, threshold_value, mask): 
    """Takes in green and red bands from an image raster, a desired greenred threshold value (on 8-bit scale), and the position/size of the image raster.
    Calculates greenred ratio for that position in the image, and if mask == True, thresholds it to the specified value and writes the mask array to the specified band.
    If mask == False, writes the greenred array to the output band.  

    Args:
        red_band (gdal.RasterBand): Red band from image raster.
        green_band (gdal.RasterBand): Green band from image raster.
        output_band (gdal.RasterBand): desired band of output raster to write the greenred mask to threshold_value (int): 8-bit scaled greenred threshold value.
        x (int): x position in the image raster.
        y (int): y position in the image raster.
        cols (int): x size of the image raster.
        rows (int): y size of the image raster.
        threshold_value (int): 8-bit scaled greenred threshold value.
        mask (bool): whether to create an greenred mask based on given threshold value.
    """
    # Read in bands as numpy array
    green_32 = green_band.ReadAsArray(x, y, cols, rows).astype(np.float32)
    red_32 = red_band.ReadAsArray(x, y, cols, rows).astype(np.float32)
        
    # Calculate greenred
    numerator = np.subtract(green_32, red_32)
    denominator = np.add(green_32, red_32)
    epsilon = 1e-6
    denominator[denominator == 0] = epsilon 
    result = np.divide(numerator, denominator)
    
    # Remove out of bounds areas
    result[result == -0] = 0
    
    # Scale to 8-bit and mask to threshold, if specified
    greenred = np.multiply((result + 1), (2**7 - 1))
    if mask == True: 
        greenred = np.where(greenred < threshold_value, 1, 255)
    
    # Write to raster
    output_band.WriteArray(greenred, x, y)
    
    greenred = None
        
def calc_greenred_by_block(input_image_path, output_folder, threshold_value=None, mask=True):
    """Uses array indexing to compute greenred by block for a given Planet image using the calc_greenred function.
    
    Args:
        input_image_path (str): path to landsat image raster.
        output_folder (str): path to desired output folder.
        threshold_value (int, optional): 8-bit scaled greenred threshold value. Defaults to None.
        mask (bool, optional): whether to create an greenred mask based on given threshold value. Defaults to True.

    Returns:
        str: path to output greenred raster.
    """
    # Read in landsat image
    landsat_image, xsize, ysize, geotransform, srs = get_raster_info(input_image_path)
    green = landsat_image.GetRasterBand(2)
    red = landsat_image.GetRasterBand(1)
    
    # Set block size
    x_block_size = 256
    y_block_size = 160
    
    # Create new raster
    if threshold_value is not None:
        output_path = os.path.join(output_folder, f"greenred_{threshold_value}.tif")
    else:
        output_path = os.path.join(os.path.dirname(output_folder), f"greenred.tif")
    output = gdal.GetDriverByName("GTiff").Create(output_path, xsize, ysize, 1, gdal.GDT_Byte, options=["COMPRESS=LZW", "BIGTIFF=YES"])
    output_band = output.GetRasterBand(1)
    output_band.SetNoDataValue(255)
    output.SetGeoTransform(geotransform)
    output.SetProjection(srs.ExportToWkt())
    
    # Mask greenred
    total_blocks = (xsize // x_block_size + 1) * (ysize // y_block_size + 1)
    progress_bar = tqdm(total=total_blocks, desc="Progress", unit="block")
    
    for y in range(0, ysize, y_block_size):
        rows = min(y_block_size, ysize - y)  # Handles edge case for remaining rows
        for x in range(0, xsize, x_block_size):
            cols = min(x_block_size, xsize - x)  # Handles edge case for remaining cols
            calc_greenred(green, red, output_band, x, y, cols, rows, threshold_value, mask)
            progress_bar.update(1)
     
    progress_bar.close()
    output_band = None
    output = None
    landsat_image = None
    
    return output_path

def mask_buildings(chm_array, greenred_array, building_array, building_threshold):
    """Takes in an array for the CHM and the greenred mask, and depending on user input either masks the entire CHM for buildings, or uses a building array for masking those areas, using the greenred mask.

    Args:
        chm_array (np.array): array for the CHM.
        greenred_array (np.array): array for the greenred mask.
        building_array (np.array): array for the building mask.
        building_threshold (int): threshold value for the building mask

    Returns:
        np.array: cleaned CHM array.
    """
    # Use building mask with greenred for masking
    condition_mask = (greenred_array == 1) & (building_array >= building_threshold)
    chm_cleaned = np.where(condition_mask, 0, chm_array)
    chm_cleaned[chm_array == 255] = 255
            
    print("Cleaned CHM building errors...")
    
    return chm_cleaned

def preprocess_data_layers(input_chm, temp, data_folders, crs, pixel_size, buffer_size=50, man_pwl=False, man_slp=False):
    """Creates a temporary folder for all intermediate data layers and performs preprocessing on them such as buffering, cropping, and generating file paths as specified.
    If the temp folder already exists, will just return the filenames of the previously created layers. 
    IMPORTANT: if performing multiple iterations of CHM cleaning, keep in mind that persisting the temp folder also persists the layers, so for example the powerline buffer raster will be read in as is, it will not be re-buffered. 

    Args:
        input_chm (str): path to input chm.
        temp (str): path to temp directory.
        data_folders (list): list of paths to the relevant data folders for the canopy shapefile, powerline masks, manual powerline masks, water images, landsat images, and slope errors shapefile.
        crs (str): string for the desired CRS, in the format "EPSG:3857" for example.
        pixel_size (float): desired pixel size for reprojection, in destination crs units.
        buffer_size (int, optional): desired buffer size for powerlines, in meters. Defaults to 50.
        man_pwl (bool, optional): whether or not to use a manual powerline file for additional powerline masking. Defaults to False.
        man_slp (bool, optional): whether or not to use a manual slope errors shapefile for slope masking. Defaults to False. 

    Raises:
        InvalidSurvey: depending on the condition, prints message stating how the input survey is invalid.

    Returns:
        tuple: tuple containing chm_cropped_path, powerlines_cropped_path, water_cropped_path, landsat_cropped_path, man_pwl_cropped_path, and man_slp_cropped_path.              
    """
    # Check if temp folder is already populated
    if os.path.isdir(temp) == False:
        os.makedirs(temp, exist_ok=True)
    
    # Read in CHM and get info
    chm = gdal.Open(input_chm)
    survey, state = get_chm_survey(input_chm)
    wc_tiles, planet_tiles, building_tiles = get_chm_loc(chm)

    print(f"Got CHM info: \tsurvey: {survey}\tstate: {state}")
    
    chm = None
    
    # Generate paths to corresponding mask layers and preprocess
    try:
        # Create canopy mask
        cutline = extract_polygon(data_folders[0], survey, temp)
        
        # Powerlines
        powerlines_path = os.path.join(data_folders[1], f"{state}_powerlines.geojson")
        if os.path.isfile(powerlines_path) == False:
            print(f"\nError: \"{state}_powerlines.geojson\" doesn't exist.\n")
            raise InvalidSurvey
        
        output_powerlines = os.path.join(temp, f"{state}_powerlines_buffer.tif")
        buffer_powerlines(powerlines_path, output_powerlines, crs, pixel_size, buffer_size, cutline)
        
        # Manual powerlines
        if man_pwl == True:
            man_pwl_path = os.path.join(data_folders[2], f"{state}_man_pwl.geojson")
            if os.path.isfile(man_pwl_path) == False:
                print(f"\nError: Manual powerline file \"{state}_man_pwl.geojson\" doesn't exist.\n")
                raise InvalidSurvey
            
            output_man_pwl = os.path.join(temp, f"{state}_man_pwl_buffer.tif")
            buffer_powerlines(man_pwl_path, output_man_pwl, crs, pixel_size, buffer_size, cutline)
        
        # Worldcover images
        worldcover_path = []
        for tile in wc_tiles:
            wc_path = os.path.join(data_folders[6], tile)
            
            if os.path.isfile(wc_path) == False:
                print(f"\nError: \"{tile}\" doesn't exist.\n")
                raise InvalidSurvey
            
            worldcover_path.append(wc_path)
        
        # Planet images
        planet_path = []
        for tile in planet_tiles:
            p_path = os.path.join(data_folders[7], tile)
            
            if os.path.isfile(p_path) == False:
                print(f"\nError: \"{tile}\" doesn't exist.\n")
                raise InvalidSurvey
            
            planet_path.append(p_path)
            
        # Building model tiles
        building_path = []
        for tile in building_tiles:
            b_path = os.path.join(data_folders[8], tile)

            if os.path.isfile(b_path) == False:
                print(f"\nError: \"{tile}\" doesn't exist.\n")
                raise InvalidSurvey
            
            building_path.append(b_path)
            
    except InvalidSurvey:
        shutil.rmtree(temp)
        exit()

    # If manual slope mask is specified, rasterize this as another mask layer
    if man_slp == True:
        # Extract slope errors for current survey
        slope_errors = extract_polygon(data_folders[5], survey, temp)
        
        # Rasterize the slope errors
        slope_mask_path = os.path.join(temp, f"slope_mask_{survey}.tif")
        rasterize(slope_errors, slope_mask_path, pixel_size)
        
    # Crop the rasters to the extent of the canopy mask layer
    chm_cropped_path = crop_raster(input_chm, temp, crs, pixel_size, cutline)
    powerlines_cropped_path = crop_raster(output_powerlines, temp, crs, pixel_size, cutline)
    worldcover_cropped_path = crop_raster(worldcover_path, temp, crs, pixel_size, cutline)
    planet_cropped_path = crop_raster(planet_path, temp, crs, pixel_size, cutline)
    building_cropped_path = crop_raster(building_path, temp, crs, pixel_size, cutline)
    if man_pwl == True:
        man_pwl_cropped_path = crop_raster(output_man_pwl, temp, crs, pixel_size, cutline)
    if man_slp == True: 
        man_slp_cropped_path = crop_raster(slope_mask_path, temp, crs, pixel_size, cutline)
                
    # Return the filepaths
    if "man_pwl_cropped_path" not in locals():
        man_pwl_cropped_path = None
    if "man_slp_cropped_path" not in locals():
        man_slp_cropped_path = None
        
    return chm_cropped_path, powerlines_cropped_path, man_pwl_cropped_path, man_slp_cropped_path, worldcover_cropped_path, planet_cropped_path, building_cropped_path

def clean_chm(input_chm, output_tiff, data_folders, crs, pixel_size, buffer_size=50, save_temp=False, man_pwl=False, man_slp=False, height_threshold=None, wc_mask_values=[30, 60, 70, 100], gr_threshold=135, building_threshold=30):
    """CHM cleaning workflow using all the previously defined functions. Users can specify the desired powerline buffer, whether to save the temporary output rasters, use manual powerline and slope layers for masking, desired thresholds if they do mask slope, a list of pixel values to retain for worldcover masking, and a threshold for greenred building masking.
    Steps:
    1. Gets raster information for the CHM.
    2. Creates a canopy cutline according to the survey.
    3. Crops the rasters.
    4. Sets up blank output raster.
    5. Masks powerlines, buildings, water, and slope (if speficied).
        - If man_pwl == True, will also buffer and mask the CHM to the manual powerlines file.
        - If man_slp == True, will mask slope across extent of manual slope errors shapefile using WorldCover dataset
    6. Writes the new raster (and deletes the temp files, if specified).

    Args:
        input_chm (str): path to input CHM.
        output_tiff (str): path to output raster.
        data_folders (list): list of paths to the relevant data folders for the canopy shapefile, powerline masks, water masks, and landsat images, respectively
        crs (str): string for the desired CRS, in the format "EPSG:3857" for example.
        pixel_size (float): desired pixel size for reprojection, in destination crs units.
        buffer_size (int, optional): desired buffer size for powerlines, in meters. Defaults to 50.
        save_temp (bool, optional): whether or not to save the temp rasters. Defaults to False.
        man_pwl (bool, optional): whether or not to use a manual powerline file for additional powerline masking. Defaults to False.
        man_slp (bool, optional): whether or not to use a manual slope errors shapefile for slope masking. Defaults to False. 
        height_threshold (int, optional): height threshold above which pixels will be considered for slope masking. Defaults to None.
        wc_mask_values (list, optional): list of pixel values to use for masking. Defaults to [30, 60, 70, 100]
        gr_threshold (int, optional): greenred threshold below which pixels will be masked for building masking. Defaults to 135.
        building_threshold (int, optional): probability value for the building threshold, below which pixels will be masked for building masking. Defaults to 30.        
    """
    # Start message
    print(f" PROCESSING CHM {os.path.basename(input_chm)} ".center(100, "*"))
    
    # Set up temp directory for intermediate files
    temp = os.path.join(os.path.dirname(output_tiff), "temp")
    
    # Preprocess the data layers
    chm_cropped_path, powerlines_cropped_path, man_pwl_cropped_path, man_slp_cropped_path, worldcover_cropped_path, planet_cropped_path, building_cropped_path = preprocess_data_layers(input_chm, temp, data_folders, crs, pixel_size, buffer_size, man_pwl, man_slp)
                
    # Create output blank raster
    chm_cropped, c_xsize, c_ysize, c_geotransform, c_srs = get_raster_info(chm_cropped_path)
    output = gdal.GetDriverByName("GTiff").Create(output_tiff, c_xsize, c_ysize, 1, gdal.GDT_Byte, options=["COMPRESS=LZW", "BIGTIFF=YES"])
    output_band = output.GetRasterBand(1)
    output_band.SetNoDataValue(255)
    output.SetGeoTransform(c_geotransform)
    output.SetProjection(c_srs.ExportToWkt())
    print("Created blank raster...")
    
    # Mask CHM by powerlines
    chm_8bit = chm_cropped.GetRasterBand(1).ReadAsArray(0, 0, c_xsize, c_ysize).astype(np.uint8)
    powerlines_cropped, p_xsize, p_ysize, _, _ = get_raster_info(powerlines_cropped_path)
    powerlines_8bit = powerlines_cropped.GetRasterBand(1).ReadAsArray(0, 0, p_xsize, p_ysize).astype(np.uint8)
    chm_cleaned = mask_powerlines(chm_8bit, powerlines_8bit)
    
    chm_cropped = None
    chm_8bit = None
    powerlines_cropped = None
    powerlines_8bit = None
    
    # Mask CHM by manual powerlines (if specified)
    if man_pwl == True:
        man_pwl_cropped, mp_xsize, mp_ysize, _, _ = get_raster_info(man_pwl_cropped_path)
        man_pwl_8bit = man_pwl_cropped.GetRasterBand(1).ReadAsArray(0, 0, mp_xsize, mp_ysize).astype(np.uint8)
        chm_cleaned = mask_powerlines(chm_cleaned, man_pwl_8bit)
        
        man_pwl_cropped = None
        man_pwl_8bit = None
        
    # Calculate greenred and mask buildings
    greenred_path = calc_greenred_by_block(planet_cropped_path, temp, gr_threshold)
    greenred_cropped, gr_xsize, gr_ysize, _, _ = get_raster_info(greenred_path)
    greenred_8bit = greenred_cropped.GetRasterBand(1).ReadAsArray(0, 0, gr_xsize, gr_ysize).astype(np.uint8)
    building_cropped, b_xsize, b_ysize, _, _ = get_raster_info(building_cropped_path)
    building_8bit = building_cropped.GetRasterBand(1).ReadAsArray(0, 0, b_xsize, b_ysize).astype(np.uint8)
    chm_cleaned = mask_buildings(chm_cleaned, greenred_8bit, building_8bit, building_threshold)
    
    greenred_cropped = None
    greenred_8bit = None
    building_cropped = None
    building_8bit = None
    
    # Mask CHM by water
    wc_cropped, wc_xsize, wc_ysize, _, _ = get_raster_info(worldcover_cropped_path)
    wc_8bit = wc_cropped.GetRasterBand(1).ReadAsArray(0, 0, wc_xsize, wc_ysize).astype(np.uint8)
    chm_cleaned = mask_water(chm_cleaned, wc_8bit)
    
    # Mask CHM by slope (if specified)
    if height_threshold is not None:
        # Use worldcover and height threshold to mask slope
        chm_cleaned = mask_worldcover(chm_cleaned, height_threshold, wc_8bit, wc_mask_values)
    
    elif man_slp == True:
        # Read in manual slope mask raster (if specified), use with worldcover to clean slope
        slope_cropped, s_xsize, s_ysize, _, _ = get_raster_info(man_slp_cropped_path)
        slope_8bit = slope_cropped.GetRasterBand(1).ReadAsArray(0, 0, s_xsize, s_ysize).astype(np.uint8)
                    
        chm_cleaned = mask_worldcover(chm_cleaned, height_threshold, wc_8bit, wc_mask_values, slope_8bit)
        
        slope_cropped = None
        slope_8bit = None
        
    wc_cropped = None
    wc_8bit = None
        
    # Write cleaned CHM to new raster
    output_band.WriteArray(chm_cleaned)
    print(f"Cleaned CHM written to {output_tiff}")
    print(f" FINISHED PROCESSING CHM {os.path.basename(input_chm)}".center(100, "*"))
    print("\n")
    
    chm_cleaned = None
    output_band = None
    output = None
    
    if save_temp == False:
        shutil.rmtree(temp)