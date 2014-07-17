# Developer: SpatialDev
# Company:   Spatial Development International

# for more information please visit the following link below
# Website: https://lpdaac.usgs.gov/products/modis_products_table/land_cover/yearly_l3_global_500_m/mcd12q1


# --------------- Imports -------------------------------------
# standard library
import sys
import os

# Add the ETLBaseModule directory location to the Python system path in order to import the shared base modules
sys.path.append("SERVIR PATH TO ETL ON DISK\\ETL\\ETLScripts\\ETLBaseModules\\")

# third-party
import arcpy

# ETL framework
from etl_controller import ETLController
from land_etl_delegate import LandETLDelegate
from arcpy_land_etl_core import LandLoader, LandTransformer, LandExtractor, LandMetaDataTransformer, LandExtractValidator

# ETL utils
from etl_utils import ETLDebugLogger, ETLExceptionManager, ExceptionManager
from arcpy_utils import RasterCatalog, FileGeoDatabase

# custom modules
from arcpy_land_raster_dataset import LandCoverRasterDataset


# --------------- ETL ---------------------------------------------------------------------------------------------------
def createRasterCatalog(land_cover_year_to_process, land_cover_output_basepath):
    
    # configure raster catalog object -------------------------------------
    raster_catalog = RasterCatalog(land_cover_output_basepath, str('LandCover' + land_cover_year_to_process), {
    
        'datetime_field':'datetime',
        'datetime_field_format':'%m-%d-%Y %I:%M:%S %p',
        'datetime_sql_cast':"date"
    })
    
    # custom fields -------------------------------------
    arcpy.AddField_management(raster_catalog.fullpath, raster_catalog.options['datetime_field'], 'DATE', '', '', 25)
    arcpy.AddField_management(raster_catalog.fullpath, 'datetime_string', 'TEXT', '', '', 25)
    arcpy.AddField_management(raster_catalog.fullpath, 'land_cover_type', 'TEXT', '', '', 25)
    
    # Land Cover specific meta-data fields -------------------------------------
    arcpy.AddField_management(raster_catalog.fullpath, 'DistributedFileName', 'TEXT', '', '', 100)
    arcpy.AddField_management(raster_catalog.fullpath, 'DTDVersion', 'DOUBLE', '', '', 10)
    arcpy.AddField_management(raster_catalog.fullpath, 'DataCenterId', 'TEXT', '', '', 50)
    arcpy.AddField_management(raster_catalog.fullpath, 'GranuleUR', 'TEXT', '', '', 50)
    arcpy.AddField_management(raster_catalog.fullpath, 'DbID', 'TEXT', '', '', 25)
    arcpy.AddField_management(raster_catalog.fullpath, 'InsertTime', 'TEXT', '', '', 50)
    arcpy.AddField_management(raster_catalog.fullpath, 'LastUpdate', 'TEXT', '', '', 50)
    arcpy.AddField_management(raster_catalog.fullpath, 'ShortName', 'TEXT', '', '', 25)
    arcpy.AddField_management(raster_catalog.fullpath, 'VersionID', 'DOUBLE', '', '', 5)
    arcpy.AddField_management(raster_catalog.fullpath, 'FileSize', 'DOUBLE', '', '', 25)
    arcpy.AddField_management(raster_catalog.fullpath, 'ChecksumType', 'TEXT', '', '', 25)
    arcpy.AddField_management(raster_catalog.fullpath, 'Checksum', 'DOUBLE', '', '', 25)
    arcpy.AddField_management(raster_catalog.fullpath, 'ChecksumOrigin', 'TEXT', '', '', 50)
    arcpy.AddField_management(raster_catalog.fullpath, 'SizeMBECSDataGranule', 'DOUBLE', '', '', 25)
    arcpy.AddField_management(raster_catalog.fullpath, 'ReprocessingPlanned', 'TEXT', '', '', 250)
    arcpy.AddField_management(raster_catalog.fullpath, 'ReprocessingActual', 'TEXT', '', '', 250)
    arcpy.AddField_management(raster_catalog.fullpath, 'LocalGranuleID', 'TEXT', '', '', 100)
    arcpy.AddField_management(raster_catalog.fullpath, 'DayNightFlag', 'TEXT', '', '', 25)
    arcpy.AddField_management(raster_catalog.fullpath, 'ProductionDateTime', 'TEXT', '', '', 50)
    arcpy.AddField_management(raster_catalog.fullpath, 'LocalVersionID', "TEXT", '', '', 25)
    arcpy.AddField_management(raster_catalog.fullpath, 'PGEVersion', 'TEXT', '', '', 25)
    arcpy.AddField_management(raster_catalog.fullpath, 'RangeEndingTime', 'TEXT', '', '', 25)
    arcpy.AddField_management(raster_catalog.fullpath, 'RangeEndingDate', 'DATE', '', '', 25)
    arcpy.AddField_management(raster_catalog.fullpath, 'RangeBeginningTime', 'TEXT', '', '', 25)
    arcpy.AddField_management(raster_catalog.fullpath, "RangeBeginningDate", 'DATE', '', '', 25)
    arcpy.AddField_management(raster_catalog.fullpath, 'l1', 'TEXT', '', '', 25)
    arcpy.AddField_management(raster_catalog.fullpath, 'l2', "TEXT", '', '', 25)
    arcpy.AddField_management(raster_catalog.fullpath, 'l3', 'TEXT', '', '', 25)
    arcpy.AddField_management(raster_catalog.fullpath, 'l4', "TEXT", '', '', 25)
    arcpy.AddField_management(raster_catalog.fullpath, "ParameterName", 'TEXT', '', '', 25)
    arcpy.AddField_management(raster_catalog.fullpath, "QAPercentMissingData", 'DOUBLE', '', '', 10)
    arcpy.AddField_management(raster_catalog.fullpath, "QAPercentOutofBoundsData", 'DOUBLE', '', '', 10)
    arcpy.AddField_management(raster_catalog.fullpath, "QAPercentInterpolatedData", 'DOUBLE', '', '', 10)
    arcpy.AddField_management(raster_catalog.fullpath, "AutomaticQualityFlag", 'TEXT', '', '', 25)
    arcpy.AddField_management(raster_catalog.fullpath, "AutomaticQualityFlagExplanation", 'TEXT', '', '', 350)
    arcpy.AddField_management(raster_catalog.fullpath, "OperationalQualityFlag", 'TEXT', '', '', 25)
    arcpy.AddField_management(raster_catalog.fullpath, "OperationalQualityFlagExplanation", 'TEXT', '', '', 350)
    arcpy.AddField_management(raster_catalog.fullpath, "ScienceQualityFlag", 'TEXT', '', '', 25)
    arcpy.AddField_management(raster_catalog.fullpath, "ScienceQualityFlagExplanation", 'TEXT', '', '', 350)
    arcpy.AddField_management(raster_catalog.fullpath, "PlatformShortName", 'TEXT', '', '', 50)
    arcpy.AddField_management(raster_catalog.fullpath, "InstrumentShortName", 'TEXT', '', '', 50)
    arcpy.AddField_management(raster_catalog.fullpath, "SensorShortName", 'TEXT', '', '', 50)
    arcpy.AddField_management(raster_catalog.fullpath, "PSAName", 'TEXT', '', '', 250)
    arcpy.AddField_management(raster_catalog.fullpath, "PSAValue", 'TEXT', '', '', 250)
    arcpy.AddField_management(raster_catalog.fullpath, "InputPointer", 'TEXT', '', '', 500)
    
    return raster_catalog


def executeETL(raster_catalog, land_cover_year_to_process):
    
    # initialize utility objects -------------------------------------
    debug_log_output_directory = os.path.join(sys.path[0], "Land_logs")
    etl_debug_logger = ETLDebugLogger(debug_log_output_directory, "Land", {
                                                                                          
        "debug_log_archive_days":7          
    }) 
    update_debug_log = etl_debug_logger.updateDebugLog # retrieve a reference to the debug logger function
    
    etl_exception_manager = ETLExceptionManager(sys.path[0], "Land_exception_reports", {
                                                                                    
        "create_immediate_exception_reports":True
    })
    
    # initialize core ETL objects -------------------------------------
    arcpy_land_extract_validator = LandExtractValidator({
                                                         
        "raster_catalog":raster_catalog,
        "ftp_file_name_field":"DistributedFileName",
        "debug_logger":update_debug_log
    })
    arcpy_land_extractor = LandExtractor({
                                          
        "target_file_extn":'hdf',
        "ftp_options": {   
            #THE SOURCE OF THE HDFs COLLECTED MAY NEED TO BE UPDATED                        
            "ftp_host":'e4ftl01.cr.usgs.gov', 
            "ftp_user":'anonymous', 
            "ftp_pswrd":'anonymous'
        },
        "debug_logger":update_debug_log
    })
    
    scratch_fgdb_fullpath = FileGeoDatabase(sys.path[0], 'scratch.gdb').fullpath
    arcpy_land_transformer = LandTransformer({
                                              
        "output_file_geodatabase":scratch_fgdb_fullpath,
        "debug_logger":update_debug_log
    })
    land_meta_data_transformer = LandMetaDataTransformer(
                                                         
        {"debug_logger":update_debug_log},
        decoratee=arcpy_land_transformer
    )

    arcpy_land_loader = LandLoader({
                                    
        "raster_catalog":raster_catalog,
        "CopyRaster_management_config":{
            'config_keyword':'',
            'background_value':'',
            'nodata_value':'',
            'onebit_to_eightbit':'',
            'colormap_to_RGB':'',
            'pixel_type':''
        },
        "debug_logger":update_debug_log
    })
    
    etl_controller = ETLController(sys.path[0], "LandCover_ETL", {
                                                              
        "remove_etl_workspace_on_finish":False
    })
    
    land_etl_delegate = LandETLDelegate({
                                         
        "ftp_dirs":['/MOTA/MCD12Q1.005/'+land_cover_year_to_process+'.01.01/'],
        "ftp_file_meta_extn":'xml',
        "all_or_none_for_success":True,
        "debug_logger":update_debug_log,
        'exception_handler':etl_exception_manager.handleException
    })
        
    # set ETLDelegate object properties-------------------------------------
    land_etl_delegate.setExtractValidator(arcpy_land_extract_validator)
    land_etl_delegate.setExtractor(arcpy_land_extractor)
    land_etl_delegate.setTransformer(land_meta_data_transformer)
    land_etl_delegate.setLoader(arcpy_land_loader)
    land_etl_delegate.setETLController(etl_controller)

    # execute the ETL operation -------------------------------------    
    successful_new_run = land_etl_delegate.startETLProcess()
    
    # perform post-ETL operations -------------------------------------
    etl_exception_manager.finalizeExceptionXMLLog()
    etl_debug_logger.deleteOutdatedDebugLogs()
    
    return successful_new_run


def createLandCoverDataset(land_cover_year_to_process, land_cover_basepath, raster_catalog_fullpath):
    
    # create debug logger instance -------------------------------------
    debug_log_output_directory = os.path.join(sys.path[0], "Land_Raster_debug_logs")
    land_raster_debug_logger = ETLDebugLogger(debug_log_output_directory, "LandRaster", {
                                                                                          
        "debug_log_archive_days":7            
    }) 

    # land cover raster data set config -------------------------------------
    output_raster_dataset = str("LandCover_Type1_" + land_cover_year_to_process)    
    output_dataset_fullpath = os.path.join(land_cover_basepath, output_raster_dataset)
        
    land_raster_dataset_options = {
               
        "land_cover_type":"LC1",
        "land_cover_type_field":"land_cover_type",
        "RasterCatalogToRasterDataset_management_config": {
                                                           
            "mosaic_type":'',
            "colormap":"",
            "order_by_field":'',
            "ascending":'',
            "Pixel_type":'',
            "ColorBalancing":'',
            "matchingMethod":'',
            "ReferenceRaster":'',
            "OID":''
        },    
                                   
        # land cover classification config -------------------------------------
        "land_cover_description_field":"igbp_class",
        "land_type_value_field":"Value",
        "land_type_description_dict": {
                
                0:"Water",
                1:"Evergreen Needleleaf forest",
                2:"Evergreen Broadleaf forest",
                3:"Deciduous Needleleaf forest",
                4:"Deciduous Broadleaf forest",
                5:"Mixed forest",
                6:"Closed shrublands",
                7:"Open shrublands",
                8:"Woody savannas",
                9:"Savannas",
                10:"Grasslands",
                11:"Permanent wetlands",
                12:"Croplands",
                13:"Urban and built-up",
                14:"Cropland/Natural vegetation mosaic",
                15:"Snow and ice",
                16:"Barren or sparsely vegetated"      
        },
                                   
        # re-project raster dataset config -------------------------------------
        "gp_env_extent":arcpy.Extent(-20037507.2295943, -19971868.8804086, 20037507.2295943, 19971868.8804086),
        "out_coor_system" :'PROJCS["WGS_1984_Web_Mercator_Auxiliary_Sphere",GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137.0,298.257223563]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Mercator_Auxiliary_Sphere"],PARAMETER["False_Easting",0.0],PARAMETER["False_Northing",0.0],PARAMETER["Central_Meridian",0.0],PARAMETER["Standard_Parallel_1",0.0],PARAMETER["Auxiliary_Sphere_Type",0.0],UNIT["Meter",1.0],AUTHORITY["EPSG",3857]]',
        "reprojected_raster_dataset":os.path.join(land_cover_basepath, output_raster_dataset+"_WM"),
        "debug_logger":land_raster_debug_logger.updateDebugLog
    }
        
    # create a LandCoverRasterDataset instance, set debug logger and start the creation process -------------------------------------
    land_raster_dataset = LandCoverRasterDataset(output_dataset_fullpath, raster_catalog_fullpath, land_raster_dataset_options)
    land_raster_dataset.createLandRasterDataset()

    
# --------------- ETL MAIN ---------------------------------------------------------------------------------------------------
def main(): 
    
    # select land cover year to process, this will be relfected throughout the entire modules config
    land_cover_year_to_process = "200X"
    
    # output location for the raster catalogs and each land cover raster dataset, creates the FileGeoDatabase if it does not exist    
    land_cover_fgdb = FileGeoDatabase(sys.path[0], "LandCover.gdb")
    
    # get reference to raster catalog
    raster_catalog = createRasterCatalog(land_cover_year_to_process, land_cover_fgdb.fullpath)
    
    # execute the main ETL operation
    successful_new_run = executeETL(raster_catalog, land_cover_year_to_process)

    if successful_new_run:
        # if successful_new_run, then create the land cover type raster dataset
        createLandCoverDataset(land_cover_year_to_process, land_cover_fgdb.fullpath, raster_catalog.fullpath)


# method called upon module execution to start the ETL process
main()