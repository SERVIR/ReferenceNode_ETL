# Developer: SpatialDev
# Company:   Spatial Development International

# standard library
import os
import sys
from datetime import datetime, timedelta

# third-party
from arcpy.sa import *
import arcpy


class TRMMCustomRasterRequest:
    
    """ encapsulates a request to the TRMMCustomRasterCreator to create a custom raster from the TRMM raster catalog.
        
        request_options <dict>: contains the following additional options:
            
            factory_specifications <dict>: options for the output raster
                         
                'output_raster_fullpath' <str>: the fullpath and name of the output raster
                'clip_extent' <str>: the processing extent contained within "-180.0 -50.0 180.0 50.0" and given in the same format "xMin yMin xMax yMax"
                'clip_raster' <arcpy.Raster>: the fullpath to a raster to be used in clipping the TRMM output raster using arcpy.Clip_management
                'CopyRaster_management_config' <dict>: config for arcpy.CopyRaster_Management
                'AddColormap_management_config' <dict>: config for arcpy.AddColormap_management
                
            input_raster_catalog_options <dict>: options for the input raster catalog
            
                'raster_catalog_fullpath' <str>: fullpath to the source raster catalog
                "raster_name_field" <str>: the field in the raster catalog that contains the names of the rasters
                "datetime_field" <str>: the field in the raster catalog that contains the datetime for the rasters
                'datetime_sql_cast' <str>: the DATETIME cast expression based on the underlying SQL type ex: "date"
                'datetime_field_format' <str>: the format of the given datetime_field. ex: '%m-%d-%Y %I:%M:%S %p',
                'start_datetime' <str>: the start datetime given in the format %Y%m%d%H with 'h' being a 24-hour. ex: "2012-02-29 at 3PM == 201202291500"
                'end_datetime' <str>: the end datetime given in the format %Y%m%d%H with 'h' being a 24-hour. ex: "2012-02-29 at 3PM == 201202291500"
    """
    
    def __init__(self, request_options):
        
        self.factory_specifications = request_options['factory_specifications']       
        self.input_raster_catalog_options = request_options['input_raster_catalog_options']
        self.debug_logger = request_options.get('debug_logger',lambda*a,**kwa:None)
        self.exception_handler = request_options.get('exception_handler',lambda*a,**kwa:None)
        
    def getFactorySpecifications(self):
        
        return self.factory_specifications
    
    def getRasterCatalogFullpath(self):
        
        return self.input_raster_catalog_options['raster_catalog_fullpath']
        
    def extractRastersToWorkspace(self, path_to_extract_into):
        
        try:
        
            rasters_to_extract_list = self._getListOfRasterNamesFromRasterCatalog()
            extracted_rasters_list = self._extractRastersFromRasterCatalog(rasters_to_extract_list, path_to_extract_into)
    
            return extracted_rasters_list
    
        except Exception as e:
            
            self.debug_logger("==================== EXCEPTION (extractRastersToWorkspace) ====================")
            self.debug_logger(str(e), str(arcpy.GetMessages(2)))
            self.exception_handler(dict(exception=str(e), messages=str(arcpy.GetMessages(2))))
        
    def _getListOfRasterNamesFromRasterCatalog(self, additional_where_clause=""):
        
        self.debug_logger("_getListOfRasterNamesFromRasterCatalog()")
        
        raster_name_field = self.input_raster_catalog_options['raster_name_field']
        where_clause = self._createWhereClause()
        where_clause += self.input_raster_catalog_options.get('additional_where_clause',"")

        rows = arcpy.SearchCursor(self.input_raster_catalog_options['raster_catalog_fullpath'], where_clause, "", raster_name_field)
        try:
            return [str(row.getValue(raster_name_field)) for row in rows]
        finally:
            del rows
            
    def _createWhereClause(self):
        
        start_datetime = self.input_raster_catalog_options['start_datetime']
        end_datetime = self.input_raster_catalog_options['end_datetime']
        datetime_field = self.input_raster_catalog_options['datetime_field']
        datetime_field_format = self.input_raster_catalog_options['datetime_field_format']
        datetime_sql_cast = self.input_raster_catalog_options['datetime_sql_cast']
        
        where_clause = "%s <= %s \'%s\'" % (datetime_field, datetime_sql_cast, start_datetime.strftime(datetime_field_format))
        where_clause += " AND %s >= %s \'%s\'" % (datetime_field, datetime_sql_cast, end_datetime.strftime(datetime_field_format))
        self.debug_logger("where_clause",where_clause)
        
        return where_clause
            
    def _extractRastersFromRasterCatalog(self, rasters_to_extract_list, path_to_extract_into):

        extracted_raster_list = []
        joinPath = os.path.join
        raster_name_field = self.input_raster_catalog_options['raster_name_field']
        output_raster_catalog = self.input_raster_catalog_options['raster_catalog_fullpath']
        
        for raster_name in rasters_to_extract_list:
            
            extracted_raster_fullpath = joinPath(path_to_extract_into, raster_name)
            self.debug_logger("processing raster...",raster_name)
            
            if arcpy.Exists(extracted_raster_fullpath) and (raster_name not in extracted_raster_list):
                extracted_raster_list.append(raster_name)
                       
            else:
                where_clause = "%s = \'%s\'" % (raster_name_field, str(raster_name))
                arcpy.RasterCatalogToRasterDataset_management(output_raster_catalog, raster_name, where_clause)
                extracted_raster_list.append(raster_name)
                
        return extracted_raster_list


class TRMMCustomRasterCreator:
    
    """creates a custom raster from a given TRMMCustomRasterRequest object.
    
        raster_creator_options <dict>: config options for the TRMM raster creator.
    
            'workspace_fullpath' <str>: output workspace location for the raster creation process                                             
            'remove_all_rasters_on_finish' <bool>: cleans up all raster output on finish.
        
            'archive_options' <dict>: local extracted rasters can be kept in the workspace to allow for faster processing in future runs
            
                'raster_name_prefix' <str>: given to differentiate between extracted rasters when deleting ex: "t_"
                'local_raster_archive_days' <int>: rasters outside this number will be deleted ex: 90
                'raster_name_datetime_format' <str>: to determine if a raster is outside the archive days, 
                 each name must be in an easily convertable datetime string format. ex: "t_%Y%m%d%H"
    
            'debug_logger' <object.method>: method that will be passes variable string arguments to display current progress and values
            'exception_handler' <object.method>: method that will be variable string arguments with exception information
    """

    def __init__(self,  raster_creator_options):
        
        self.raster_creator_options = raster_creator_options
        self.workspace_fullpath = raster_creator_options['workspace_fullpath']
        self.custom_raster_requests = []
        
        if not os.path.isdir(self.workspace_fullpath):
            os.mkdir(self.workspace_fullpath)
            
        self.debug_logger = raster_creator_options.get('debug_logger',lambda*a,**kwa:None)
        self.exception_handler = raster_creator_options.get('exception_handler',lambda*a,**kwa:None)
        
    def addCustomRasterReuests(self, custom_raster_requests):
        
        self.custom_raster_requests = custom_raster_requests
        
    def createCustomRasters(self):
        self.debug_logger("Starting TRMM Custom Raster Creation Process")

        try:
            arcpy.env.extent = arcpy.Extent(-180.0, -50.0, 180.0, 50.0) # max and min extent values a given TRMM raster
            arcpy.env.workspace = self.workspace_fullpath
            arcpy.env.overwriteOutput = True
            arcpy.CheckOutExtension("spatial")

            for custom_raster in self.custom_raster_requests:
                self.debug_logger("Processing Raster")
                
                factory_specifications = custom_raster.getFactorySpecifications()
                output_raster_fullpath = factory_specifications['output_raster_fullpath']
                raster_catalog_is_not_locked = arcpy.TestSchemaLock(custom_raster.getRasterCatalogFullpath())
                
                extracted_raster_list = custom_raster.extractRastersToWorkspace(self.workspace_fullpath)
                self.debug_logger("Len(extracted_raster_list)", len(extracted_raster_list))

                if extracted_raster_list and raster_catalog_is_not_locked:

                    final_raster = self._createCumulativeRaster(extracted_raster_list, factory_specifications)
                    self._saveRaster(final_raster, output_raster_fullpath, factory_specifications)

            self._finishCustomRasterManagment()
            self.debug_logger("Finished TRMM Custom Raster Creation Process")
        
        except Exception as e:
            
            self.debug_logger("==================== EXCEPTION (createCustomRasters) ====================")
            self.debug_logger(str(e), str(arcpy.GetMessages(2)))
            self.exception_handler(dict(exception=str(e), messages=str(arcpy.GetMessages(2))))
            
        finally:
            arcpy.CheckInExtension("spatial")
            self.debug_logger("checked IN spatial extension")

    def _createCumulativeRaster(self, rasters_list, factory_specifications):
        
        self.debug_logger("Creating Cumulative Raster...")
        final_raster = sum([Con(IsNull(raster), 0, raster) for raster in rasters_list]) # for each raster in the list, set all NULL to 0 then SUM
        final_raster = Float(final_raster)
        final_raster = final_raster * 3 # multiply by 3 since each TRMM raster 3-hour period is an average not a sum
        
        if factory_specifications.get('clip_extent', None):
            
            self.debug_logger("Adding Clip Extent...")
            output_clip_raster = os.path.join(os.path.join(sys.path[0], "scratch.gdb"),"temp_clip")
            final_raster = arcpy.Clip_management(final_raster, factory_specifications['clip_extent'], output_clip_raster)

        elif factory_specifications.get('clip_raster', None):
            
            self.debug_logger("Adding Clip Raster...")
            final_raster = final_raster * Raster(factory_specifications['clip_raster']) 
        
        final_raster = SetNull(final_raster == 0, final_raster) # set 0's back to NULL after all mathematical operations are peformed
        self.debug_logger("SetNull(final_raster == 0, final_raster)")
        
        return final_raster
        
    def _saveRaster(self, raster_to_save, output_raster_fullpath, factory_specifications):
        self.debug_logger("Saving Final Raster")

        if factory_specifications.get('AddColormap_management_config', None):
            self.debug_logger("Adding Color Map...")
            
            color_map_config = factory_specifications['AddColormap_management_config']
            r = arcpy.AddColormap_management(raster_to_save, color_map_config.get('in_template_raster',''), color_map_config['input_CLR_file'])
            self.debug_logger("AddColormap_management Result", r.status)
            
        raster_name = os.path.basename(output_raster_fullpath)
        raster_to_save.save(raster_name) 
        local_raster_fullpath = os.path.join(self.workspace_fullpath, raster_name)
        self.debug_logger("local_raster_fullpath",local_raster_fullpath)
        self.debug_logger("output_raster_fullpath",output_raster_fullpath)
        
        self._removeExistingRasterIfExists(output_raster_fullpath)
        self._copyRaster(factory_specifications['CopyRaster_management_config'], local_raster_fullpath, output_raster_fullpath)        
        self._removeExistingRasterIfExists(local_raster_fullpath)
        
    def _copyRaster(self, copy_raster_managment_config, local_raster_fullpath, output_raster_fullpath):
        
        self.debug_logger("Copying Raster...", output_raster_fullpath)
        r = arcpy.CopyRaster_management(local_raster_fullpath, output_raster_fullpath,
            copy_raster_managment_config.get('config_keyword',''), copy_raster_managment_config.get('background_value',''),
            copy_raster_managment_config.get('nodata_value',''), copy_raster_managment_config.get('onebit_to_eightbit',''), 
            copy_raster_managment_config.get('colormap_to_RGB',''), copy_raster_managment_config.get('pixel_type','')
        )
        self.debug_logger("CopyRaster_management Result", r.status)
    
    def _removeExistingRasterIfExists(self, output_raster_fullpath):
        
        if arcpy.Exists(output_raster_fullpath):
            
            self.debug_logger("Deleting...", output_raster_fullpath)
            r = arcpy.Delete_management(output_raster_fullpath)
            self.debug_logger("Delete_management Result", r.status)

    def _finishCustomRasterManagment(self):
        self.debug_logger("Finishing Custom Raster Creation")
        
        archive_options = self.raster_creator_options.get('archive_options', None)
        remove_all_rasters_on_finish = self.raster_creator_options.get('remove_all_rasters_on_finish', False)
        
        if archive_options and not remove_all_rasters_on_finish:
            
            raster_name_prefix = archive_options.get('raster_name_prefix', None)
            archive_days = archive_options.get('local_raster_archive_days', None)
            raster_name_datetime_format = archive_options.get('raster_name_datetime_format', None)
            
            if (raster_name_prefix and archive_days and raster_name_datetime_format):
                
                archive_date = datetime.utcnow() - timedelta(days=archive_days)
                local_raster_list = [r for r in arcpy.ListRasters(raster_name_prefix+"*","*") if str(r[:len(raster_name_prefix)]).lower() == str(raster_name_prefix)]
                list_of_rasters_to_delete = [raster for raster in local_raster_list if datetime.strptime(str(raster), raster_name_datetime_format) < archive_date]
                self._deleteRasters(list_of_rasters_to_delete)
                    
        elif remove_all_rasters_on_finish:
            
            self.debug_logger("Removing All Rasters In Local Workspace...")
            self._deleteRasters(arcpy.ListRasters("*"))
            
    def _deleteRasters(self, list_of_rasters_to_delete):
        
        for r in list_of_rasters_to_delete:
            arcpy.Delete_management(r)