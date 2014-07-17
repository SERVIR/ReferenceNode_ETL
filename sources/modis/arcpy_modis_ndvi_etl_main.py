# Developer: SpatialDev
# Company:   Spatial Development International

# --------------- Imports -------------------------------------
# standard library
from datetime import datetime, timedelta
import sys
import os

# third-party
import arcpy

# Add the ETLBaseModule directory location to the Python system path in order to import the shared ETL framework modules
sys.path.append("PATH ON DISK TO ETL BASE MODULES \\ETL\\ETLScripts\\ETLBaseModules\\")

# ETL framework 
from etl_controller import ETLController
from modis_etl_delegate import MODISETLDelegate
from arcpy_modis_etl_core import MODISLoader, MODISExtractor, MODISMetaDataTransformer, MODISExtractValidator

# ETL utils
from etl_utils import ETLDebugLogger, ETLExceptionManager
from arcpy_utils import RasterCatalog, FileGeoDatabase, AGServiceManager


# --------------- ETL ---------------------------------------------------------------------------------------------------
def createRasterCatalog(output_basepath, raster_catalog_name):
    
    # configure raster catalog object -------------------------------------
    raster_catalog = RasterCatalog(output_basepath, raster_catalog_name, {
    
        'datetime_field':'datetime',
        'datetime_field_format':'%m-%d-%Y %I:%M:%S %p',
        'datetime_sql_cast':"date",
        "archive_days": 90
    })

#    un-comment AddField_management statements when running the script for a new feature class. Re-comment after creation to speed up the initialization process.
    # custom fields -------------------------------------
#    arcpy.AddField_management(raster_catalog.fullpath, raster_catalog.options['datetime_field'], 'DATE', '', '', 25)
#    arcpy.AddField_management(raster_catalog.fullpath, 'datetime_string', 'TEXT', '', '', 25)
#    arcpy.AddField_management(raster_catalog.fullpath, 'resolution', 'TEXT', '', '', 25)
#    
#    # MODIS image specific meta-data fields -------------------------------------
#    arcpy.AddField_management(raster_catalog.fullpath, 'subset', 'TEXT', '', '', 25)
#    arcpy.AddField_management(raster_catalog.fullpath, 'date', 'TEXT', '', '', 25)
#    arcpy.AddField_management(raster_catalog.fullpath, 'satellite', 'TEXT', '', '', 25)
#    arcpy.AddField_management(raster_catalog.fullpath, 'projection', 'TEXT', '', '', 50)
#    arcpy.AddField_management(raster_catalog.fullpath, 'projection_center_lon', 'TEXT', '', '', 25)
#    arcpy.AddField_management(raster_catalog.fullpath, 'projection_center_lat', 'TEXT', '', '', 25)
#    arcpy.AddField_management(raster_catalog.fullpath, 'UL_lon', 'TEXT', '', '', 25)
#    arcpy.AddField_management(raster_catalog.fullpath, 'UL_lat', 'TEXT', '', '', 25)
#    arcpy.AddField_management(raster_catalog.fullpath, 'UR_lon', "TEXT", '', '', 25)
#    arcpy.AddField_management(raster_catalog.fullpath, 'UR_lat', 'TEXT', '', '', 25)
#    arcpy.AddField_management(raster_catalog.fullpath, 'LR_lon', 'TEXT', '', '', 25)
#    arcpy.AddField_management(raster_catalog.fullpath, 'LR_lat', 'TEXT', '', '', 25)
#    arcpy.AddField_management(raster_catalog.fullpath, 'LL_lon', 'TEXT', '', '', 25)
#    arcpy.AddField_management(raster_catalog.fullpath, 'LL_lat', 'TEXT', '', '', 25)        
#    arcpy.AddField_management(raster_catalog.fullpath, 'x_scale_factor', 'TEXT', '', '', 50)
#    arcpy.AddField_management(raster_catalog.fullpath, 'ellipsoid', 'TEXT', '', '', 10)
#    arcpy.AddField_management(raster_catalog.fullpath, 'L2_granules', 'TEXT', '', '', 500)
    
    return raster_catalog


def executeETL(raster_catalog):
    
    # initialize utility objects -------------------------------------
    debug_log_output_directory = os.path.join(sys.path[0], "MODIS_NDVI_logs")
    etl_debug_logger = ETLDebugLogger(debug_log_output_directory, "MODIS_NDVI", {                                                                                      
        
        "debug_log_archive_days":7 
    })   
    update_debug_log = etl_debug_logger.updateDebugLog  # retrieve a reference to the debug logger function
    
    etl_exception_manager = ETLExceptionManager(sys.path[0], "MODIS_NDVI_exception_reports", {
                                                                                              
         "create_immediate_exception_reports":True
    })
        
    # initialize core ETL objects -------------------------------------
    start_datetime = datetime.utcnow()
    end_datetime = start_datetime - timedelta(days=raster_catalog.options['archive_days'])
    image_extn = "tif" # target extension of MODIS images to downlaod
    
    modis_extract_validator = MODISExtractValidator({
                                                                
        "raster_catalog":raster_catalog,
        "raster_name_field":"Name",
        "start_datetime":start_datetime,
        "end_datetime":end_datetime,
        'debug_logger':update_debug_log
    })
    
    modis_extractor = MODISExtractor({
                                      
        "image_content_types":['image/tiff'], # checks the header content type for this value before downloading the images
        "text_content_types":['text/html', 'text/plain'], # checks the header content type for any of these values before downloading the meta-data
        "subset":['Bhutan', 'Nepal'], 
        "satellite":['terra','aqua'], 
        "size":['2km','1km','500m','250m'],
        "extn":image_extn,
        "subtype":['ndvi'], # list only has one item since it is the category of the raster catalog the ETL is updating
        "start_datetime":start_datetime,
        "end_datetime":end_datetime,
        'debug_logger':update_debug_log
    })
    
    modis_meta_data_transformer = MODISMetaDataTransformer({
                                                            
        'debug_logger':update_debug_log
    })
    
    modis_loader = MODISLoader({
                                           
        "raster_catalog":raster_catalog, 
        "CopyRaster_management_config":{
            'config_keyword':'#',
            'background_value':'#',
            'nodata_value':'#',
            'onebit_to_eightbit':'NONE',
            'colormap_to_RGB':'NONE',
            'pixel_type':'8_BIT_UNSIGNED'
        },
        'debug_logger':update_debug_log
    })
    
    etl_controller = ETLController(sys.path[0], "MODIS_NDVI", {
                                                               
        "remove_etl_workspace_on_finish":True
    })
    
    modis_etl_delegate = MODISETLDelegate({
          #MAY OR MAY NOT NEED TO UPDATE THIS URL                                 
        "url":'http://rapidfire.sci.gsfc.nasa.gov/subsets/?subset=', # base URL for all images
        "extn":image_extn,
        "meta_extn":"txt",
        "all_or_none_for_success":False,
        'debug_logger':update_debug_log,
        'exception_handler':etl_exception_manager.handleException
    })
    
    # set ETLDelegate object properties-------------------------------------
    modis_etl_delegate.setExtractValidator(modis_extract_validator)
    modis_etl_delegate.setExtractor(modis_extractor)
    modis_etl_delegate.setTransformer(modis_meta_data_transformer)
    modis_etl_delegate.setLoader(modis_loader)
    modis_etl_delegate.setETLController(etl_controller)

    # execute the ETL operation -------------------------------------
    successful_new_run = modis_etl_delegate.startETLProcess()
    
    # perform post-ETL operations -------------------------------------    
    raster_catalog.deleteOutdatedRows()
    etl_debug_logger.deleteOutdatedDebugLogs()
    etl_exception_manager.finalizeExceptionXMLLog()
    
    return successful_new_run

    
# --------------- ETL MAIN ---------------------------------------------------------------------------------------------------
def main(*args, **kwargs):
    
    # create the FileGeoDatabase if it does not already exist
    modis_gdb = FileGeoDatabase("PATH ON DISK TO FGDB \\Himalaya\\FileGeodatabases\\", "MODIS.gdb")
    
    # retrieve a reference to the raster catalog, create the raster catalog if it does not already exist
    raster_catalog = createRasterCatalog(modis_gdb.fullpath, "MODIS_NDVI")
    
    # execute the main ETL operation
    successful_new_run = executeETL(raster_catalog)

    if successful_new_run:
        # refresh all services to update the data
        modis_ndvi_services = ("Himalaya/BHUTAN_NDVI_AQUA", "Himalaya/BHUTAN_NDVI_TERRA", "Himalaya/NEPAL_NDVI_AQUA", "Himalaya/NEPAL_NDVI_TERRA")
        modis_ndvi_service = AGServiceManager(modis_ndvi_services, "PATH ON DISK TO ARCSOM FOR RESTART\\ReferenceNode\\ETL\\ETLTools\\AGSSOM.exe", "localhost")
        modis_ndvi_service.refreshService()

   
# method called upon module execution to start the ETL process 
main()