# Developer: SpatialDev
# Company:   Spatial Development International

# for more information please visit the following link below
# Website: http://firefly.geog.umd.edu/firms/faq.htm#attributes


# --------------- Imports -------------------------------------
# standard library
import os
import sys
from datetime import datetime, timedelta

# third-party
import arcpy

# Add the ETLBaseModule directory location to the Python system path in order to import the shared base modules
sys.path.append('DRIVE PATH\\ETL\\ETLScripts\\ETLBaseModules\\')

# ETL framework
from etl_controller import ETLController
from fire_etl_delegate import FireETLDelegate
from arcpy_fire_etl_core import FireLoader, FireTransformer, FireExtractor, FireMetaDataTransformer, FireExtractValidator

# ETL utils
from arcpy_utils import FeatureClass, FileGeoDatabase, ArcGISServiceManager
from etl_utils import ETLDebugLogger, ETLExceptionManager


# --------------- ETL ---------------------------------------------------------------------------------------------------
def createFeatureClass(output_basepath, feature_class_name):
    
    # configure feature class object -------------------------------------
    feature_class = FeatureClass(output_basepath, feature_class_name, {
                                 
        "geometry_type":"POINT",
        "archive_days": 90,
        "datetime_field":"datetime",
        'datetime_sql_cast':"date",
        "datetime_field_format":"%m/%d/%Y %I:%M:%S %p"
    })
    
    #un-comment AddField_management statements when running the script for a new feature class. Re-comment after creation to speed up the initialization process.
#    # custom fields -------------------------------------
#    arcpy.AddField_management(feature_class.fullpath, feature_class.options['datetime_field'], 'DATE', '', '', 50)
#    arcpy.AddField_management(feature_class.fullpath, 'datetime_string', 'TEXT', '', '', 50)
#    arcpy.AddField_management(feature_class.fullpath, 'start_datetime', 'DATE', '', '', 50)
#    arcpy.AddField_management(feature_class.fullpath, 'start_datetime_string', 'TEXT', '', '', 50)
#    arcpy.AddField_management(feature_class.fullpath, 'end_datetime', 'DATE', '', '', 50)
#    arcpy.AddField_management(feature_class.fullpath, 'end_datetime_string', 'TEXT', '', '', 50)
#    arcpy.AddField_management(feature_class.fullpath, 'last_update_datetime', 'TEXT', '', '', 50)
#    arcpy.AddField_management(feature_class.fullpath, 'ftp_file_name', 'TEXT', '', '', 50) # used to check for duplicates 
#    
#    # fire meta-data fields -------------------------------------
#    arcpy.AddField_management(feature_class.fullpath, 'LOCALGRANULEID', 'TEXT', '', '', 50)
#    arcpy.AddField_management(feature_class.fullpath, 'PRODUCTIONDATETIME', 'TEXT', '', '', 50)
#    arcpy.AddField_management(feature_class.fullpath, 'RANGEBEGINNINGTIME', 'TEXT', '', '', 50)
#    arcpy.AddField_management(feature_class.fullpath, 'RANGEENDINGTIME', 'TEXT', '', '', 50)
#    arcpy.AddField_management(feature_class.fullpath, 'VERSIONID', 'SHORT', '', '', 50)
#    arcpy.AddField_management(feature_class.fullpath, 'RANGEBEGINNINGDATE', 'DATE', '', '', 50)
#    arcpy.AddField_management(feature_class.fullpath, 'RANGEENDINGDATE', 'DATE', '', '', 50)
#    arcpy.AddField_management(feature_class.fullpath, 'SHORTNAME', 'TEXT', '', '', 50)
#    
#    # fire CSV data fields -------------------------------------
#    arcpy.AddField_management(feature_class.fullpath, 'LATITUDE', 'DOUBLE', '', '', 25)
#    arcpy.AddField_management(feature_class.fullpath, 'LONGITUDE', 'DOUBLE', '', '', 25)
#    arcpy.AddField_management(feature_class.fullpath, 'BRIGHTNESS', 'DOUBLE', '', '', 10)
#    arcpy.AddField_management(feature_class.fullpath, 'SCAN', 'DOUBLE', '', '', 10)
#    arcpy.AddField_management(feature_class.fullpath, 'TRACK', 'DOUBLE', '', '', 10)
#    arcpy.AddField_management(feature_class.fullpath, 'DATE', 'DATE', '', '', 25)
#    arcpy.AddField_management(feature_class.fullpath, 'TIME', 'LONG', '', '', 10)
#    arcpy.AddField_management(feature_class.fullpath, 'SATELLITE', 'TEXT', '', '', 255)
#    arcpy.AddField_management(feature_class.fullpath, 'CONFIDENCE', 'LONG', '', '', 10)
#    arcpy.AddField_management(feature_class.fullpath, 'VERSION', 'LONG', '', '', 10)
#    arcpy.AddField_management(feature_class.fullpath, 'BRIGHTT31', 'DOUBLE', '', '', 10)
#    arcpy.AddField_management(feature_class.fullpath, 'FRP', 'DOUBLE', '', '', 10)
#    
#    # spatial-join fields -------------------------------------
#    arcpy.AddField_management(feature_class.fullpath, 'ADM0_NAME', 'TEXT', '', '', 100)
#    arcpy.AddField_management(feature_class.fullpath, 'ADM1_NAME', 'TEXT', '', '', 100)
#    arcpy.AddField_management(feature_class.fullpath, 'ADM2_NAME', 'TEXT', '', '', 100)
#    arcpy.AddField_management(feature_class.fullpath, 'Join_Count', 'LONG', '', '', 10)
#    arcpy.AddField_management(feature_class.fullpath, 'TARGET_FID', 'LONG', '', '', 10)
    
    return feature_class


def executeETL(feature_class, update_debug_log):
    
    # initialize utility objects -------------------------------------
    etl_exception_manager = ETLExceptionManager(sys.path[0], "Fire_exception_reports", {
                                                                                    
        "create_immediate_exception_reports":True,
        "delete_immediate_exception_reports_on_finish":True
    })
    
    # initialize core ETL objects -------------------------------------
    start_datetime = datetime.utcnow()
    end_datetime = start_datetime - timedelta(days=10) # this is how many days worth of CSVs it will retrieve from the ten-day rolling FTP
    extract_validator = FireExtractValidator({
                                                         
        "feature_class":feature_class,
        "satellite_ftp_directory_name_field":"SHORTNAME",
        "ftp_file_name_field":"ftp_file_name", # field used to determine duplicates and current feature class membership
        "start_datetime":start_datetime,
        "end_datetime":end_datetime,       
        "debug_logger":update_debug_log
    })
    
    extractor = FireExtractor({
                                          
        "target_file_extn":"txt", # extension of fire CSV
        "ftp_options": {                     
            "ftp_host":"198.118.194.32", 
            "ftp_user":"SD_FIRE", 
            "ftp_pswrd":"$d_F1R3_1",
        },
        "debug_logger":update_debug_log
    })
    
    meta_data_transformer = FireMetaDataTransformer({"debug_logger":update_debug_log})
    
    # projection used to create an XY event layer in arcpy.MakeXYEventLayer_management
    spatial_projection = "GEOGCS['GCS_WGS_1984',DATUM['D_WGS_1984',SPHEROID['WGS_1984',6378137.0,298.257223563]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]];-400 -400 1000000000;-100000 10000;-100000 10000;8.98315284119521E-09;0.001;0.001;IsHighPrecision"
        
    transformer = FireTransformer({                                
        'fire_fields':['LATITUDE','LONGITUDE','BRIGHTNESS','SCAN','TRACK','DATE','TIME','SATELLITE','CONFIDENCE','VERSION','BRIGHTT31','FRP'],
        'admin_join_table_fullpath': os.path.join(sys.path[0], 'GAUL_LEVELS.gdb', 'g2006_2'),
        'admin_fields':['ADM0_NAME', 'ADM1_NAME', 'ADM2_NAME'],
        "CreateFeatureclass_management_config":{
            'geometry_type':"POINT"
        },
        "MakeXYEventLayer_management_config":{
            'in_x_field':'LONGITUDE', 
            'in_y_field':'LATITUDE', 
            'spatial_reference':spatial_projection
        },
        "debug_logger":update_debug_log
    }, decoratee=meta_data_transformer) # NOTE: the transformer is decorating the meta_data_transformer to chain along transform() calls
    
    loader = FireLoader({
                                    
        "datetime_sql_cast":"date", # this is important when you change the underlying SQL database. PostGIS vs SQL for example.
        "feature_class":feature_class,
        "Append_management_config":{
            'schema_type':"NO_TEST"
        },
        'satellite_mapping':{ # this is used to determine which satellite the granule came from in order to update the appropriate rows
            "MYD14T":"A", # aqua
            "MOD14T":"T" # terra
        },
        "debug_logger":update_debug_log
    })
    
    etl_controller = ETLController(sys.path[0], "Fire_ETL", {
                                  
        "remove_etl_workspace_on_finish":True
    })
    
    fire_etl_delegate = FireETLDelegate({
         #DEPENDING ON NASA THIS FILE MAY OR MAY NOT NEED UPDATING                                                 
        "ftp_dirs":['/allData/1/MOD14T/Recent/', '/allData/1/MYD14T/Recent/'],# iterate through both aqua and terra FTP directories
        "ftp_file_meta_extn":"met", # extension of fire CSVs meta-data
        "all_or_none_for_success":False,
        "debug_logger":update_debug_log,
        "exception_handler":etl_exception_manager.handleException
    })
        
    # set ETLDelegate object properties-------------------------------------
    fire_etl_delegate.setExtractValidator(extract_validator)
    fire_etl_delegate.setExtractor(extractor)
    fire_etl_delegate.setTransformer(transformer)
    fire_etl_delegate.setLoader(loader)
    fire_etl_delegate.setETLController(etl_controller)

    # execute the ETL operation -------------------------------------    
    successful_new_run = fire_etl_delegate.startETLProcess()
    
    # perform post-ETL operations -------------------------------------
    feature_class.deleteOutdatedRows()
    etl_exception_manager.finalizeExceptionXMLLog()
    
    return successful_new_run


# --------------- ETL MAIN ---------------------------------------------------------------------------------------------------
def main(*args, **kwargs):
    
    debug_log_output_directory = os.path.join(sys.path[0], "Fire_logs")
    etl_debug_logger = ETLDebugLogger(debug_log_output_directory, "Fire", {
                                                                                          
        "debug_log_archive_days":7           
    }) 
    update_debug_log = etl_debug_logger.updateDebugLog # retrieve a reference to the debug logger function
    
    # create the FileGeoDatabase if it does not already exist
    fire_fgdb = FileGeoDatabase("PATH TO GEODATABASE ON DISK\\FileGeodatabases\\", "Fire.gdb", {
                                                                                              
        "compact_interval_days":7
    })
    
    feature_class_name = "global_fire"
    
    # create the main fire feature class if it does not already exist
    feature_class = createFeatureClass(fire_fgdb.fullpath, feature_class_name)
    
    # execute the main ETL operation
    is_successful_new_run = executeETL(feature_class, update_debug_log)
    
    if is_successful_new_run:
        
        fire_agsm = ArcGISServiceManager({
                                          
            'debug_logger':update_debug_log,
            'server_name':'localhost',
            'server_port':'6080',
            'username':'SERVIR ADMIN USER NAME',
            'password':'SERVIR ADMIN USER PASS',
            'service_dir':'ReferenceNode',
            'services':['MODIS_Fire.MapServer', 'MODIS_Fire_1DAY.MapServer']
         })
        fire_agsm.refreshServices()
        
        # this is ran due to the Fire.gdb having frequent updates and deletions
        fire_fgdb.compactFileGeoDatabase() 
        
    # delete outdated debug logs
    etl_debug_logger.deleteOutdatedDebugLogs()


# method called upon module execution to start the ETL process
main()