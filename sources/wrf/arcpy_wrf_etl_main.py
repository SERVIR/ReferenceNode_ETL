# Developer: SpatialDev
# Company:   Spatial Development International

# --------------- Imports -------------------------------------

# standard-library
from datetime import datetime, timedelta
import os
import sys

# third-party
import arcpy

# Add the ETLBaseModule directory location to the system path in order to import the shared ETL framework modules
sys.path.append(r'PATH TO BASE ETL CLASSES \ETL\base_classes')
#sys.path.append(os.path.join(sys.path[0], "base_classes"))


# ETL framework
from etl_controller import ETLController
from etl_delegate import FTPETLDelegate

# arcpy ETL framework
from arcpy_wrf_etl_core import WRFLoader, WRFTransformer, WRFMetaDataTransformer, WRFExtractor, WRFExtractValidator

# ETL utils 
from etl_utils import ETLDebugLogger, ETLExceptionManager, ExceptionManager
from arcpy_utils import FileGeoDatabase, RasterMosaicDataset, ArcGISServiceManager


def getRasterMosaicDataset(dataset_name, output_basepath, spatial_projection, archive_days):
    
    dataset_name = dataset_name.replace("-","_") 
    raster_mosaic_dataset_already_exists = arcpy.Exists(os.path.join(output_basepath, dataset_name))
    
    raster_mosaic_dataset = RasterMosaicDataset(output_basepath, dataset_name, spatial_projection, {
        
        "datetime_field":'model_runtime',
        'datetime_sql_cast':"date",
        'datetime_field_format':'%m-%d-%Y %I:%M:%S %p',        
        "archive_days":archive_days
    })
    
    if not raster_mosaic_dataset_already_exists:
    
        arcpy.AddField_management(raster_mosaic_dataset.fullpath, 'ftp_file_name', 'TEXT', '', '', 50)
        arcpy.AddField_management(raster_mosaic_dataset.fullpath, 'wrf_variable', 'TEXT', '', '', 50)
        arcpy.AddField_management(raster_mosaic_dataset.fullpath, 'wrf_domain', 'TEXT', '', '', 50)
        
        arcpy.AddField_management(raster_mosaic_dataset.fullpath, 'model_runtime', 'DATE', '', '', 25)    
        arcpy.AddField_management(raster_mosaic_dataset.fullpath, 'model_runtime_string', 'TEXT', '', '', 25)
        arcpy.AddField_management(raster_mosaic_dataset.fullpath, 'model_runtime_hour', 'SHORT', '', '', 2)
        
        arcpy.AddField_management(raster_mosaic_dataset.fullpath, 'start_datetime', 'DATE', '', '', 25)    
        arcpy.AddField_management(raster_mosaic_dataset.fullpath, 'start_datetime_string', 'TEXT', '', '', 25)
        arcpy.AddField_management(raster_mosaic_dataset.fullpath, 'start_hour', 'SHORT', '', '', 2)
    
        arcpy.AddField_management(raster_mosaic_dataset.fullpath, 'end_datetime', 'DATE', '', '', 25)
        arcpy.AddField_management(raster_mosaic_dataset.fullpath, 'end_datetime_string', 'TEXT', '', '', 25)
        arcpy.AddField_management(raster_mosaic_dataset.fullpath, 'end_hour', 'SHORT', '', '', 2)
        
        arcpy.AddField_management(raster_mosaic_dataset.fullpath, 'forecast_end_hour', 'SHORT', '', '', 2)

    return raster_mosaic_dataset


def executeETL(raster_mosaic_dataset, spatial_projection, start_datetime, end_datetime, wrf_variable, domain, fgdb):
    
    # this variable is used in the debug log and exception report file names in order to make them distinct
    domainVariable = domain + "_" + wrf_variable
    
    # datetime format of ASCII and output raster
    ascii_raster_datetime_format = '%Y%m%d%H';
    
    # initialize debug logger instance -------------------------------------
    debug_log_output_directory = os.path.join(sys.path[0], "wrf_etl_logs", domainVariable+"_Logs")
    etl_debug_logger = ETLDebugLogger(debug_log_output_directory, domainVariable+"_log", {
                                                                                          
        "debug_log_archive_days":7
    })
    update_debug_log = etl_debug_logger.updateDebugLog # retrieve a reference to the debug logger function
            
    # initialize exception handler instance -------------------------------------
    etl_exception_manager = ETLExceptionManager(os.path.join(sys.path[0], "wrf_exception_reports"), domainVariable+"_Exception_Reports", {
                                                                                    
        "create_immediate_exception_reports":True,
        "delete_immediate_exception_reports_on_finish":True
    })
    
    # initialize core ETL objects -------------------------------------
    wrf_extract_validator = WRFExtractValidator({
                                                   
        "raster_mosaic_dataset":raster_mosaic_dataset,
        'domain':domain,
        'wrf_variable':wrf_variable,
        "ftp_file_name_field":"ftp_file_name",  
        "ftp_ascii_datetime_format":ascii_raster_datetime_format,   
        'model_runtime_field':'model_runtime',
        'forecast_end_hour_field':'forecast_end_hour',
        "start_datetime":start_datetime,
        "end_datetime":end_datetime,   
        'debug_logger':update_debug_log    
    })
    
    wrf_extractor = WRFExtractor({
             #MAY NEED TO UPDATE URL                       
        "target_file_extn":"gz",
        "ftp_options": {          
            "ftp_host":"ftp.nsstc.org", 
            "ftp_user":"anonymous", 
            "ftp_pswrd":"anonymous"
        },
        'debug_logger':update_debug_log                                    
    })
        
    wrf_meta_data_transformer = WRFMetaDataTransformer({
                                                        
        'string_datetime_format':'%Y/%m/%d %I %p',                                  
        'debug_logger':update_debug_log
    })

    wrf_transformer = WRFTransformer({
                                        
        "raster_mosaic_dataset":raster_mosaic_dataset,
        'ASCIIToRaster_conversion_config': {
            'data_type':"FLOAT"
        },
        'debug_logger':update_debug_log
    }, decoratee=wrf_meta_data_transformer)
    
    wrf_loader = WRFLoader({
                              
        "raster_mosaic_dataset":raster_mosaic_dataset,
        'wrf_variable':wrf_variable, # used to create the new raster name --> <wrf_var>_2012100706 from w2012100706
        "fgdb_fullpath":fgdb.fullpath,
        "CopyRaster_management_config":{  
            'config_keyword':'',
            'background_value':'',
            'nodata_value':'',
            'onebit_to_eightbit':'',
            'colormap_to_RGB':'',
            'pixel_type':'32_BIT_SIGNED'
        },
        "AddRastersToMosaicDataset_management_config":{  
            'raster_type':'Raster Dataset',          
            'update_cellsize_ranges':'',
            'update_boundary':'',
            'update_overviews':'',
            'maximum_pyramid_levels':'',
            'maximum_cell_size':'',
            'minimum_dimension':'',
            'spatial_reference':'',
            'filter':'',
            'sub_folder':'',
            'duplicate_items_action':'OVERWRITE_DUPLICATES',
            'build_pyramids':'',
            'calculate_statistics':'',
            'build_thumbnails':'',
            'operation_description':''
        },
        'debug_logger':update_debug_log
    })
    
    etl_controller = ETLController(sys.path[0], "wrf_etl_workspace", {
                                                                      
        'debug_logger':update_debug_log,
        "remove_etl_workspace_on_finish":True
    })
    
    wrf_etl_delegate = FTPETLDelegate({
                                        
        "ftp_dirs":['/outgoing/casejl/servir/'],
        "all_or_none_for_success":True,
        'debug_logger':update_debug_log,
        'exception_handler':etl_exception_manager.handleException
    })
        
    # set ETLDelegate object properties -------------------------------------
    wrf_etl_delegate.setExtractValidator(wrf_extract_validator)
    wrf_etl_delegate.setExtractor(wrf_extractor)
    wrf_etl_delegate.setTransformer(wrf_transformer)
    wrf_etl_delegate.setLoader(wrf_loader)
    wrf_etl_delegate.setETLController(etl_controller)
        
    # execute the ETL operation -------------------------------------
    is_successful_new_run = wrf_etl_delegate.startETLProcess()
    
    # execute post-ETL operations -------------------------------------
    raster_mosaic_dataset.deleteOutdatedRows(start_datetime)
    fgdb.deleteRastersOutsideDatetimeRange(start_datetime, end_datetime, {
        'raster_name_prefix':wrf_variable, # <wrf_var>_201210070612
        'raster_name_datetime_format':wrf_variable+"_"+ascii_raster_datetime_format, # <wrf_var>_2012100706 -> <wrf_var>_%Y%m%d%H (ignore the last two chars)
        'raster_name_parser_function':lambda r:r[:-2], # return raster name minus the last two chars to convert to correct datetime object
        'raster_name_validator_function':lambda r:r.split("_")[0] == wrf_variable # determines the correct raster to check in the FGDB, <wrf_var> == <wrf_var>
    })
    etl_exception_manager.finalizeExceptionXMLLog()
    
    return is_successful_new_run


def executeWRFETL(domain, wrf_variable, start_datetime, end_datetime, archive_days, spatial_projection, output_fgdb_basepath):
    
    # output location for the raster catalog, creates the FileGeoDatabase if it does not exist 
    wrf_fgdb = FileGeoDatabase(output_fgdb_basepath, domain+".gdb")
    
    # get a reference to the raster mosaic dataset, create one if it does not exist
    raster_mosaic_dataset = getRasterMosaicDataset(wrf_variable, wrf_fgdb.fullpath, spatial_projection, archive_days)
        
    # execute the main ETL operation
    is_successful_new_run = executeETL(raster_mosaic_dataset, spatial_projection, start_datetime, end_datetime, wrf_variable, domain, wrf_fgdb)
    
    return is_successful_new_run

    
def executeWRFETLMain(domain, wrf_variable_list, start_datetime, end_datetime, archive_days, spatial_projection, output_fgdb_basepath):
    
    had_successfull_new_run_list = []
    
    # for each variable, execute the WRF ETL procedure
    for wrf_variable in wrf_variable_list:
        
        had_successfull_new_run = executeWRFETL(domain, wrf_variable, start_datetime, end_datetime, archive_days, spatial_projection, output_fgdb_basepath)
        had_successfull_new_run_list.append(had_successfull_new_run) # append a boolean flag if the raster mosaic dataset was updated with new data

    # function 'any' return True if a single True value exists within the given list
    atLeastOneDomainRasterMosaicDatasetWasUpdated = any(had_successfull_new_run_list)
    
    if atLeastOneDomainRasterMosaicDatasetWasUpdated:
        
        # initialize debug logger instance 
        debug_log_output_directory = os.path.join(sys.path[0], "wrf_map_service_logs", domain+"_Logs")
        etl_debug_logger = ETLDebugLogger(debug_log_output_directory, domain+"_log", {
                                                                                          
            "debug_log_archive_days":7        
        })
        update_debug_log = etl_debug_logger.updateDebugLog
        
        # refresh the map service associated with the given raster mosaic dataset(s)
        map_service_name = "wrf_" + domain + ".MapServer"
        agsm = ArcGISServiceManager({
                                              
            'debug_logger':update_debug_log,
            'server_name':'localhost',
            'server_port':'6080',
            'username':'',
            'password':'',
            'service_dir':'',
            'services':[map_service_name]
        })
            
        update_debug_log("Refreshing map services...")
        agsm.refreshServices()  
        update_debug_log("finished refreshing map services.")

def main():
        
    archive_days = 2
    start_datetime = datetime.utcnow()
    end_datetime = start_datetime - timedelta(days=archive_days)
    
    spatial_projection = os.path.join(sys.path[0],"wrf_Lambert.prj")
    output_fgdb_basepath = sys.path[0]
    
    wrf_variable_list = [
                         
        'apcp3h','apcp','elev'
    ]
    
    # ,'tmp2m','tsfc', 'dpt2m', 'prmsl', 'u10m', 'v10m', "ws10m", 
    # 'mhws10m', 'elev', 'soilw0-10cm', 'soilw10-40cm', 'soilw40-100cm',
    # 'soilw100-200cm', 'apcp24h', 'prate', 'refc', 'mhrefc' 
    
    executeWRFETLMain("d01", wrf_variable_list, start_datetime, end_datetime, archive_days, spatial_projection, output_fgdb_basepath)
    #executeWRFETLMain("d02", wrf_variable_list, start_datetime, end_datetime, archive_days, spatial_projection, output_fgdb_basepath)
    #executeWRFETLMain("d03", wrf_variable_list, start_datetime, end_datetime, archive_days, spatial_projection, output_fgdb_basepath)

# method called upon module execution to start the ETL process 
main()