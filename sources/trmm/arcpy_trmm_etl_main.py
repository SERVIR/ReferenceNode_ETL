# Developer: SpatialDev
# Company:   Spatial Development International


# For more information on the TRMM data source visit:
# http://trmm.gsfc.nasa.gov/
# ftp://trmmopen.gsfc.nasa.gov/pub/merged/3B4XRT_doc.pdf


# --------------- Imports -------------------------------------
# standard-library
from datetime import datetime, timedelta
from copy import deepcopy
import os
import sys

# third-party
import arcpy

# Add the ETLBaseModule directory location to the system path in order to import the shared ETLframework modules
sys.path.append("D:\\SERVIR\\ReferenceNode\\ETL\\ETLScripts\\ETLBaseModules\\")

# ETL framework
from etl_controller import ETLController
from etl_delegate import FTPETLDelegate

# arcpy ETL framework
from arcpy_trmm_etl_core import TRMMLoader, TRMMTransformer, TRMMExtractor, TRMMMetaDataTransformer, TRMMExtractValidator

# ETL utils 
from etl_utils import ETLDebugLogger, ETLExceptionManager, ExceptionManager
from arcpy_utils import FileGeoDatabase, RasterCatalog, ArcGISServiceManager

# custom modules
from arcpy_trmm_custom_raster import TRMMCustomRasterRequest, TRMMCustomRasterCreator


def getRasterCatalog(output_basepath, spatial_projection):
    
    # configure raster catalog object -------------------------------------
    raster_catalog = RasterCatalog(output_basepath, "TRMM", {
        
        "datetime_field":'datetime',
        'datetime_sql_cast':"date",
        'datetime_field_format':'%m-%d-%Y %I:%M:%S %p',        
        "archive_days":90,
        "raster_spatial_reference":str(spatial_projection.split(";")[0]),
        "spatial_reference":spatial_projection
    })
    
#    un-comment AddField_management statements when running the script for a new feature class. Re-comment after creation to speed up the initialization process.
#    # custom fields -------------------------------------
#    arcpy.AddField_management(raster_catalog.fullpath, 'ftp_file_name', 'TEXT', '', '', 50)
#    arcpy.AddField_management(raster_catalog.fullpath, raster_catalog.options['datetime_field'], 'DATE', '', '', 25)
#    arcpy.AddField_management(raster_catalog.fullpath, 'start_datetime', 'DATE', '', '', 25)
#    arcpy.AddField_management(raster_catalog.fullpath, 'end_datetime', 'DATE', '', '', 25)
#    arcpy.AddField_management(raster_catalog.fullpath, 'datetime_string', 'TEXT', '', '', 25)
#    arcpy.AddField_management(raster_catalog.fullpath, 'start_datetime_string', 'TEXT', '', '', 25)
#    arcpy.AddField_management(raster_catalog.fullpath, 'end_datetime_string', 'TEXT', '', '', 25)
#    
#    # TRMM specific meta-data fields -------------------------------------
#    arcpy.AddField_management(raster_catalog.fullpath, 'algorithm_ID', 'TEXT', '', '', 25)
#    arcpy.AddField_management(raster_catalog.fullpath, 'algorithm_version', 'TEXT', '', '', 10)
#    arcpy.AddField_management(raster_catalog.fullpath, 'granule_ID', 'TEXT', '', '', 50)
#    arcpy.AddField_management(raster_catalog.fullpath, 'header_byte_length', 'DOUBLE', '', '', 10)
#    arcpy.AddField_management(raster_catalog.fullpath, 'file_byte_length', 'TEXT', '', '', 250)
#    arcpy.AddField_management(raster_catalog.fullpath, 'nominal_YYYYMMDD', 'TEXT', '', '', 10)
#    arcpy.AddField_management(raster_catalog.fullpath, 'nominal_HHMMSS', 'TEXT', '', '', 10)
#    arcpy.AddField_management(raster_catalog.fullpath, 'begin_YYYYMMDD', 'TEXT', '', '', 10)
#    arcpy.AddField_management(raster_catalog.fullpath, 'begin_HHMMSS', "TEXT", '', '', 10)
#    arcpy.AddField_management(raster_catalog.fullpath, 'end_YYYYMMDD', 'TEXT', '', '', 10)
#    arcpy.AddField_management(raster_catalog.fullpath, 'end_HHMMSS', 'TEXT', '', '', 10)
#    arcpy.AddField_management(raster_catalog.fullpath, 'creation_date', 'TEXT', '', '', 10)
#    arcpy.AddField_management(raster_catalog.fullpath, 'west_boundary', 'TEXT', '', '', 10)
#    arcpy.AddField_management(raster_catalog.fullpath, 'east_boundary', 'TEXT', '', '', 10)
#    arcpy.AddField_management(raster_catalog.fullpath, 'north_boundary', 'TEXT', '', '', 10)
#    arcpy.AddField_management(raster_catalog.fullpath, 'south_boundary', 'TEXT', '', '', 10)
#    arcpy.AddField_management(raster_catalog.fullpath, 'origin', 'TEXT', '', '', 25)
#    arcpy.AddField_management(raster_catalog.fullpath, 'number_of_latitude_bins', 'DOUBLE', '', '', 10)
#    arcpy.AddField_management(raster_catalog.fullpath, 'number_of_longitude_bins', 'DOUBLE', '', '', 10)
#    arcpy.AddField_management(raster_catalog.fullpath, 'grid', "TEXT", '', '', 50)
#    arcpy.AddField_management(raster_catalog.fullpath, 'first_box_center', 'TEXT', '', '', 25)
#    arcpy.AddField_management(raster_catalog.fullpath, 'second_box_center', 'TEXT', '', '', 25)
#    arcpy.AddField_management(raster_catalog.fullpath, 'last_box_center', 'TEXT', '', '', 25)        
#    arcpy.AddField_management(raster_catalog.fullpath, 'number_of_variables', 'DOUBLE', '', '', 10)
#    arcpy.AddField_management(raster_catalog.fullpath, "variable_name", 'TEXT', '', '', 250)
#    arcpy.AddField_management(raster_catalog.fullpath, 'variable_units', 'TEXT', '', '', 150)
#    arcpy.AddField_management(raster_catalog.fullpath, 'variable_scale', "TEXT", '', '', 50)
#    arcpy.AddField_management(raster_catalog.fullpath, 'variable_type', 'TEXT', '', '', 250)
#    arcpy.AddField_management(raster_catalog.fullpath, 'byte_order', "TEXT", '', '', 25)
#    arcpy.AddField_management(raster_catalog.fullpath, "flag_value", 'DOUBLE', '', '', 10)
#    arcpy.AddField_management(raster_catalog.fullpath, "flag_name", 'TEXT', '', '', 50)
#    arcpy.AddField_management(raster_catalog.fullpath, "contact_name", 'TEXT', '', '', 50)
#    arcpy.AddField_management(raster_catalog.fullpath, "contact_address", 'TEXT', '', '', 100)
#    arcpy.AddField_management(raster_catalog.fullpath, "contact_telephone", 'TEXT', '', '', 25)
#    arcpy.AddField_management(raster_catalog.fullpath, "contact_facsimile", 'TEXT', '', '', 25)
#    arcpy.AddField_management(raster_catalog.fullpath, "contact_email", 'TEXT', '', '', 50)
#    arcpy.AddField_management(raster_catalog.fullpath, "run_latency", 'TEXT', '', '', 50)

    return raster_catalog


def executeETL(raster_catalog, spatial_projection, start_datetime, end_datetime, color_map, update_debug_log):
            
    # initialize exception handler instance -------------------------------------
    etl_exception_manager = ETLExceptionManager(sys.path[0], "TRMM_Exception_Reports", {
                                                                                    
        "create_immediate_exception_reports":True,
        "delete_immediate_exception_reports_on_finish":True
    })
    
    # initialize core ETL objects -------------------------------------
    trmm_extract_validator = TRMMExtractValidator({
                                                   
        "raster_catalog":raster_catalog,
        "ftp_file_name_field":"ftp_file_name",             
        "start_datetime":start_datetime,
        "end_datetime":end_datetime,   
        'debug_logger':update_debug_log    
    })
    
    trmm_extractor = TRMMExtractor({
                                    
        "target_file_extn":"bin.gz", # extension of the bin file in the FTP directory
        "ftp_options": {          
            "ftp_host":"198.118.195.58", 
            "ftp_user":"anonymous", 
            "ftp_pswrd":"anonymous"
        },
        'debug_logger':update_debug_log                                    
    })
    
    trmm_meta_data_transformer = TRMMMetaDataTransformer({
                                                          
        'debug_logger':update_debug_log
    })
    
    trmm_transformer = TRMMTransformer({
                                        
        "raster_name_prefix":"T_",  # prefix to append to the rasters ex: "T_2012010112"
        "precip_min":1, # minimum percipitation value to write out from the bin into the CSV
        "raster_catalog":raster_catalog,
        "MakeXYEventLayer_management_config":{
            'in_x_field':'long', 
            'in_y_field':'lat', 
            'value_field':'precipitation',
            'spatial_reference':spatial_projection
        },
        "PointToRaster_conversion_config":{
            'value_field':'precipitation',
            'cell_assignment':'MOST_FREQUENT',
            'priority_field':'NONE',
            'cellsize':0.25
        },
        'debug_logger':update_debug_log
    }, decoratee=trmm_meta_data_transformer) # NOTE: the transformer is decorating the trmm_meta_data_transformer to chain along additional transform() calls
    
    trmm_loader = TRMMLoader({
                              
        "raster_catalog":raster_catalog,
        "AddColormap_management_config":{ # optional, comment out/delete entire key if no color map is needed  
            "input_CLR_file":color_map
        },
        "CopyRaster_management_config":{  
            'config_keyword':'',
            'background_value':'',
            'nodata_value':'',
            'onebit_to_eightbit':'',
            'colormap_to_RGB':'',
            'pixel_type':'16_BIT_UNSIGNED'
        },
        'debug_logger':update_debug_log
    })
    
    etl_controller = ETLController(sys.path[0], "TRMM_etl_workspace", {
                                  
        "remove_etl_workspace_on_finish":True
    })
    
    # The directory for all the bins to process is dynamically created for each year based in the given start and end datetimes
    trmm_ftp_directory = "pub/merged/mergeIRMicro/" # base directory where recent bins for the current year are stored.
    years_between = range(1, (start_datetime.year - end_datetime.year) + 1)
    years_between_list = [(start_datetime-timedelta(days=365 * x)).year for x in years_between]
    ftp_directories_to_process = [trmm_ftp_directory + str(year) for year in years_between_list]
    ftp_directories_to_process.append(trmm_ftp_directory) # append the 'recents' folder to process bins for the current year 
    
    trmm_etl_delegate = FTPETLDelegate({
                                        
        "ftp_dirs":ftp_directories_to_process,
        "all_or_none_for_success":True,
        'debug_logger':update_debug_log,
        'exception_handler':etl_exception_manager.handleException
    })
        
    # set ETLDelegate object properties-------------------------------------
    trmm_etl_delegate.setExtractValidator(trmm_extract_validator)
    trmm_etl_delegate.setExtractor(trmm_extractor)
    trmm_etl_delegate.setTransformer(trmm_transformer)
    trmm_etl_delegate.setLoader(trmm_loader)
    trmm_etl_delegate.setETLController(etl_controller)
        
    # execute the ETL operation -------------------------------------
    is_successful_new_run = trmm_etl_delegate.startETLProcess()
    
    # perform post-ETL operations -------------------------------------
    raster_catalog.deleteOutdatedRows()
    etl_exception_manager.finalizeExceptionXMLLog()
    
    return is_successful_new_run


def createTRMMComposities(raster_catalog, output_basepath, start_datetime, color_map):
    
    # initialize utility objects -------------------------------------
    debug_log_output_directory = os.path.join(sys.path[0], "TRMM_custom_raster_logs")
    custom_raster_debug_logger = ETLDebugLogger(debug_log_output_directory, "custom_raster", {
                                                                                          
        'debug_log_archive_days':7              
    })   
    
    exception_manager = ExceptionManager(sys.path[0], "Raster_Exception_Reports", {
                                                                                
        "create_immediate_exception_reports":True
    })
    
    custom_raster_debug_logger_ref = custom_raster_debug_logger.updateDebugLog
    exception_handler_ref = exception_manager.handleException
    
    
    # initialize request config objects -------------------------------------
    factory_specifications = {
                                              
        "AddColormap_management_config": { # optional, comment out/delete entire key if no color map is needed          
            "input_CLR_file":color_map
        },
        "CopyRaster_management_config":{                              
            'config_keyword':'',
            'background_value':'',
            'nodata_value':'',
            'onebit_to_eightbit':'',
            'colormap_to_RGB':'',
            'pixel_type':'16_BIT_UNSIGNED'
        }
    }
    input_raster_catalog_options = {
                                    
        'raster_catalog_fullpath':raster_catalog.fullpath,
        "raster_name_field":'Name',
        "datetime_field":raster_catalog.options['datetime_field'],
        'datetime_sql_cast':raster_catalog.options['datetime_sql_cast'],
        'datetime_field_format':raster_catalog.options['datetime_field_format'],
        'start_datetime':start_datetime
    }
    
    # TRMM1Day config --------------------------------------------------------------------------------
    factory_specifications_1day = deepcopy(factory_specifications)
    factory_specifications_1day['output_raster_fullpath'] = os.path.join(output_basepath, "TRMM1Day")
    factory_specifications_1day['AddColormap_management_config']['input_CLR_file'] = "D:\\SERVIR\\ReferenceNode\\MapServices\\trmm_1day.clr"
    input_raster_catalog_options_1day = deepcopy(input_raster_catalog_options)
    input_raster_catalog_options_1day['end_datetime'] = start_datetime - timedelta(days=1)
    trmm_1day = TRMMCustomRasterRequest({
                                         
        'factory_specifications':factory_specifications_1day, 
        'input_raster_catalog_options':input_raster_catalog_options_1day,
        'debug_logger':custom_raster_debug_logger_ref,
        'exception_handler':exception_handler_ref
    })
    
    # TRMM7Day config --------------------------------------------------------------------------------
    factory_specifications_7day = deepcopy(factory_specifications)
    factory_specifications_7day['output_raster_fullpath'] = os.path.join(output_basepath, "TRMM7Day")
    factory_specifications_7day['AddColormap_management_config']['input_CLR_file'] = "D:\\SERVIR\\ReferenceNode\\MapServices\\trmm_7day.clr"
    input_raster_catalog_options_7day = deepcopy(input_raster_catalog_options)
    input_raster_catalog_options_7day['end_datetime'] = start_datetime - timedelta(days=7)
    trmm_7day = TRMMCustomRasterRequest({
                                         
        'factory_specifications':factory_specifications_7day, 
        'input_raster_catalog_options':input_raster_catalog_options_7day,
        'debug_logger':custom_raster_debug_logger_ref,
        'exception_handler':exception_handler_ref
    })
    
    # TRMM30Day config --------------------------------------------------------------------------------
    factory_specifications_30day = deepcopy(factory_specifications)
    factory_specifications_30day['output_raster_fullpath'] = os.path.join(output_basepath, "TRMM30Day")
    factory_specifications_30day['AddColormap_management_config']['input_CLR_file'] = "D:\\SERVIR\\ReferenceNode\\MapServices\\TRMM_30Day.clr"
    input_raster_catalog_options_30day = deepcopy(input_raster_catalog_options)
    input_raster_catalog_options_30day['end_datetime'] = start_datetime - timedelta(days=30)
    trmm_30day = TRMMCustomRasterRequest({
                                          
        'factory_specifications':factory_specifications_30day, 
        'input_raster_catalog_options':input_raster_catalog_options_30day,
        'debug_logger':custom_raster_debug_logger_ref,
        'exception_handler':exception_handler_ref
    })
    
    # initialize object responsible for creating the TRMM composities
    trmm_custom_raster_factory = TRMMCustomRasterCreator({
        
        'workspace_fullpath':os.path.join(sys.path[0], "TRMMCustomRasters"),                                               
        'remove_all_rasters_on_finish':False,
        'archive_options': {
            'raster_name_prefix':"t_", # identify rasters to delete by this prefix
            'local_raster_archive_days':30, # only keep rasters local within this many days
            'raster_name_datetime_format':"t_%Y%m%d%H" # format of rasters to create a datetime object
        },
        'debug_logger':custom_raster_debug_logger_ref,
        'exception_handler':exception_handler_ref
    })
    
    trmm_custom_raster_factory.addCustomRasterReuests([trmm_1day, trmm_7day, trmm_30day])
    trmm_custom_raster_factory.createCustomRasters() # start the composite creation process
    
    custom_raster_debug_logger.deleteOutdatedDebugLogs()
    exception_manager.finalizeExceptionXMLLog()


def main(*args, **kwargs):
    
    # initialize debug logger instance -------------------------------------
    debug_log_output_directory = os.path.join(sys.path[0], "TRMM_logs")
    etl_debug_logger = ETLDebugLogger(debug_log_output_directory, "TRMM", {
                                                                                          
        "debug_log_archive_days":7          
    })
    update_debug_log = etl_debug_logger.updateDebugLog # retrieve a reference to the debug logger function
    
    # color map that is applied to each 3-hour raster as well as each N-day cumulative raster (except the 30 day)
    color_map = "PATH TO RASTER COLOR MAP\\ReferenceNode\\MapServices\\trmm_3hour.clr"

    # output location for the raster catalog and each N-day cumulative raster, creates the FileGeoDatabase if it does not exist    
    trmm_fgdb = FileGeoDatabase("PATH TO FGDB \\ReferenceNode\\FileGeodatabases\\", "TRMM.gdb")
    
    # spatial projection to apply to all rasters
    spatial_projection = "GEOGCS['GCS_WGS_1984',DATUM['D_WGS_1984',SPHEROID['WGS_1984',6378137.0,298.257223563]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]];-400 -400 1000000000;-100000 10000;-100000 10000;8.98315284119521E-09;0.001;0.001;IsHighPrecision"

    # where the output Raster Catalog is saved
    output_basepath = trmm_fgdb.fullpath
    
    # get a reference to raster catalog, create one if it does not exist
    raster_catalog = getRasterCatalog(output_basepath, spatial_projection)
    
    # used to reference all start datetimes so that they are in sync with each other 
    start_datetime = datetime.utcnow()
    end_datetime = start_datetime - timedelta(days=raster_catalog.options['archive_days'])
    
    # execute the main ETL operation
    is_successful_new_run = executeETL(raster_catalog, spatial_projection, start_datetime, end_datetime, color_map, update_debug_log)

    if is_successful_new_run:      
        
        # refresh all services to update the data    
        trmm_agsm = ArcGISServiceManager({
                                          
            'debug_logger':update_debug_log,
            'server_name':'localhost',
            'server_port':'6080',
            'username':'ARCGIS SERVICE MANAGER USER NAME',
            'password':'ARCGIS SERVICE MANAGER PASSWOWRD',
            'service_dir':'ReferenceNode',
            'services':['TRMM.MapServer', 'TRMM_1DAY.MapServer', 'TRMM_7DAY.MapServer', 'TRMM_30DAY.MapServer']
         })
        
        update_debug_log("stopping services...")
        trmm_agsm.stopServices()  
        
        createTRMMComposities(raster_catalog, output_basepath, start_datetime, color_map)
        
        update_debug_log("starting services...")
        trmm_agsm.startServices()
        
    # delete outdated debug logs
    etl_debug_logger.deleteOutdatedDebugLogs()


# method called upon module execution to start the ETL process 
main()