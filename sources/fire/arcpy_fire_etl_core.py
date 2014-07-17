# Developer: SpatialDev
# Company:   Spatial Development International


# standard library
from datetime import datetime
from itertools import takewhile
import os

# third-party
import arcpy

# ETL framework
from etl_utils import FTPDownloadManager


class FireExtractValidator(object):
    
    """
        Class FireExtractValidator determines which fire granules (CSVs) to process for the current ETL run.
        
        The high-level steps to accomplish this task include:
        
            1) retrieving all FTP granules and filtering them out by a given datetime range
            2) retrieving the current fire granules from the feature class from the given datetime range
            3) comparing the two lists for those granules that are not yet processed in the given feature class
            4) returning a list of only the granules to process for the current ETL run
            
        As a reference to the code below, here is an example of a fire CSV filename: 'MOD14T.A2012068.0915.005.NRT.txt'
    """
    
    def __init__(self, extract_validator_config):
        
        self.feature_class = extract_validator_config['feature_class']
        self.ftp_file_name_field = extract_validator_config['ftp_file_name_field']
        self.satellite_ftp_directory_name_field = extract_validator_config['satellite_ftp_directory_name_field']
        self.start_datetime = extract_validator_config['start_datetime']
        self.end_datetime = extract_validator_config['end_datetime']
        self.debug_logger = extract_validator_config.get('debug_logger',lambda*a,**kwa:None)
                        
    def validateExtract(self, ftp_granules_list):
        
        start_datetime = self.start_datetime
        end_datetime = self.end_datetime
        
        # parse out the satellite directory from a CSV (the CSV list should be homogenous with respect to its directory)
        satellite_ftp_directory = ftp_granules_list[0].split(".")[0]
        
        ftp_granules_list.sort(key=lambda x:x,reverse=True) # sort to filter the most recent files by filename datetime
        self.debug_logger("len(ftp_granules_list)",len(ftp_granules_list))
        
        current_fire_granules = self._getCurrentFireGranules(start_datetime, end_datetime, satellite_ftp_directory)
        current_fire_granules.sort(key=lambda x:x,reverse=True) # sort since the list was cast as a set
        self.debug_logger("len(current_fire_granules)",len(current_fire_granules))
                
        missing_fire_granules = self._getMissingCSVFiles(ftp_granules_list, current_fire_granules, start_datetime, end_datetime)
        missing_fire_granules.sort(key=lambda x:x,reverse=True) # sort and reverse to process the most recent files first
        self.debug_logger("len(missing_fire_granules)",len(missing_fire_granules))
        
        return missing_fire_granules
    
    def _getCurrentFireGranules(self, start_datetime, end_datetime, satellite_ftp_directory):
        
        self.debug_logger("start_datetime",start_datetime)
        self.debug_logger("end_datetime",end_datetime)
                
        getFeaturesFromDatetimeRange = self.feature_class.getValuesFromDatetimeRange
        additional_where_clause = " AND \"%s\" = \'%s\'" % (self.satellite_ftp_directory_name_field, satellite_ftp_directory)
        current_fire_granules = getFeaturesFromDatetimeRange(self.ftp_file_name_field, start_datetime, end_datetime, additional_where_clause)
        current_fire_granules = list(set(current_fire_granules)) # remove duplicates since there are N fire point rows per granule
                
        return current_fire_granules
    
    def _getMissingCSVFiles(self, ftp_granules_list, current_fire_granules, start_dt, end_dt):

        dtst = datetime.strptime
        csv_dt_format = 'A%Y%j%H%M' # datetime format of parsed CSV filename string 
        isWithinDatetimeRange = lambda f:dtst(f.split(".")[1]+f.split(".")[2], csv_dt_format) <= start_dt and dtst(f.split(".")[1]+f.split(".")[2], csv_dt_format) >= end_dt
        # function takewhile returns values from the given ftp_granules_list while the given input function isWithinDatetimeRange is True
        missing_csv_files = [csv_file for csv_file in takewhile(isWithinDatetimeRange, ftp_granules_list) if csv_file not in current_fire_granules]

        return missing_csv_files


class FireExtractor(FTPDownloadManager):
    
    """
        Class FireExtractor:
        
            1) retrieves a list of all fire granules (CSVs) from a given FTP directory. (This is sent to an FireExtractValidator)
            2) downloads a fire CSV and MET file from the given FTP directory
    """
        
    def __init__(self, extractor_config):
        FTPDownloadManager.__init__(self, extractor_config['ftp_options'])

        self.target_file_extn = extractor_config['target_file_extn']
        self.debug_logger = extractor_config.get('debug_logger',lambda*a,**kwa:None)
        
    def getDataToExtract(self, ftp_directory):
        
        try:
            self.openConnection()
            
            return self.getFileNamesFromDirectory(ftp_directory, self.target_file_extn)
        
        finally:
            self.closeConnection()
                    
    def extract(self, fire_data):
        
        try:
            extract_dir = fire_data.getExtractDir()
            
            fire_csv_to_download = fire_data.getDataToExtract()
            self.debug_logger("fire_csv_to_download",fire_csv_to_download)
            
            fire_meta_data_to_download = fire_data.getMetaDataToExtract()
            self.debug_logger("fire_meta_data_to_download",fire_meta_data_to_download)
            
            # re-open the connection in case it has timed-out (due to the large amount of data to process if the ETL job has not been consistently running)
            self.openConnection()
            # change the FTP working directory to the one associated with the current fire granule (CSV)
            self.changeDirectory(fire_data.getFTPDirectory()) 
            
            downloaded_fire_csv = self.downloadFileFromFTP(fire_csv_to_download, extract_dir)
            self.debug_logger("downloaded_fire_csv",downloaded_fire_csv)
            
            downloaded_fire_meta_data = self.downloadFileFromFTP(fire_meta_data_to_download, extract_dir)
            self.debug_logger("downloaded_fire_meta_data",downloaded_fire_meta_data)
            
            fire_data.setDataToTransform(downloaded_fire_csv)
            fire_data.setMetaDataToTransform(downloaded_fire_meta_data)
        
        except Exception as e:
                        
            self.debug_logger("extract Exception:",str(e),str(arcpy.GetMessages(2)))
            fire_data.handleException(exception=("extract:",str(e)),messages=arcpy.GetMessages(2))
            
        finally:
            self.closeConnection()


class FireTransformer(object):

    """
        Class FireTransformer converts a CSV into a temporary feature class to append to a main fire feature class.
        
        The high-level steps to accomplish this task include:
        
            1) re-create a CSV with fields included (The CSV files do not have any fields inside them by default)
            2) create an XY event layer from the CSV
            3) create a feature class from the XY event layer
            4) spatially join the feature class with a given administrative polygon shape file
    """
    
    def __init__(self, transformer_config, decoratee):
        
        self.create_feature_class_config = transformer_config.get('CreateFeatureclass_management_config',{})
        self.make_xy_event_layer_config = transformer_config['MakeXYEventLayer_management_config']
        self.admin_join_table_fullpath = transformer_config['admin_join_table_fullpath']
        self.admin_fields = transformer_config['admin_fields']
        self.fire_fields = transformer_config['fire_fields']
        self.debug_logger = transformer_config.get('debug_logger',lambda*a,**kwa:None)
        
        self.decoratee = decoratee # this is the FireMetaDataTransformer

    def transform(self, fire_data):
        
        try:
            fire_csv_without_fields = fire_data.getDataToTransform()
            
            transform_dir = fire_data.getTransformDir()
            fc_basename = "".join(os.path.basename(fire_csv_without_fields).split(".")[1:3])    
            temp_fc_name = fc_basename + "t"
            
            fire_xy_event_layer = self._transformCSVToXYEventLayer(fire_csv_without_fields, transform_dir)

            temp_fc_fullpath = self._createFeatureClass(transform_dir, temp_fc_name)
            self._copyFeatures(fire_xy_event_layer, temp_fc_fullpath)
            
            joined_fc_fullpath = self._createSpatialJoin(temp_fc_fullpath, os.path.join(transform_dir, str(fc_basename+"j")))
            
            fire_data.setDataToLoad(joined_fc_fullpath)
            
            self.decoratee.transform(fire_data) # call FireMetaDataTransformer.transform() to convert the MET file into a dictionary
        
        except Exception as e:
                        
            self.debug_logger("transform Exception:",str(e),str(arcpy.GetMessages(2)))
            fire_data.handleException(exception=("transform:",str(e)),messages=arcpy.GetMessages(2))
            
    def _transformCSVToXYEventLayer(self, fire_csv_without_fields, transform_dir):
        
        fire_csv_with_fields = self._addFieldsToCSV(fire_csv_without_fields, transform_dir)  
        self.debug_logger("fire_csv_with_fields",fire_csv_with_fields)
  
        fire_xy_event_layer = self._createXYEventLayer(fire_csv_with_fields, transform_dir)
        self.debug_logger("fire_xy_event_layer",str(fire_xy_event_layer))
        
        return fire_xy_event_layer
            
    def _copyFeatures(self, fire_xy_event_layer, temp_fc_fullpath):
        
        self.debug_logger("temp_fc_fullpath",temp_fc_fullpath)
        copy_features_result = arcpy.CopyFeatures_management(fire_xy_event_layer, temp_fc_fullpath)
        self.debug_logger("CopyFeatures_management result",copy_features_result.status)
                
    def _createFeatureClass(self, output_basepath, temp_fc_name):
        
        cfcc = self.create_feature_class_config
        create_fc_result = arcpy.CreateFeatureclass_management(
            output_basepath, temp_fc_name, cfcc.get('geometry_type',''), cfcc.get('template',''), cfcc.get('has_m',''), 
            cfcc.get('has_z',''), cfcc.get('spatial_reference',''), cfcc.get('config_keyword', ''), 
            cfcc.get('spatial_grid_1',''), cfcc.get('spatial_grid_2',''),cfcc.get('spatial_grid_3','')
        )            
        self.debug_logger("CreateFeatureclass_management result",create_fc_result.status)
        
        return str(create_fc_result)
            
    def _createSpatialJoin(self, temp_fc_fullpath, temp_joined_fc_fullpath):
        
        self.debug_logger("temp_joined_fc_fullpath",temp_joined_fc_fullpath)
        
        field_mappings = arcpy.FieldMappings()
        field_mappings.addTable(temp_fc_fullpath)
        admin_table = self.admin_join_table_fullpath
        admin_fields = self.admin_fields
        
        for admin_column_name in admin_fields:
            
            field_map = arcpy.FieldMap()
            field_map.addInputField(admin_table, admin_column_name)
            field_mappings.addFieldMap(field_map)    
        
        spatial_join_result = arcpy.SpatialJoin_analysis(temp_fc_fullpath, admin_table, temp_joined_fc_fullpath, "", "", field_mappings)
        self.debug_logger("SpatialJoin_analysis result",spatial_join_result.status)
        
        return str(spatial_join_result)
        
    def _createXYEventLayer(self, fire_csv_with_fields, transform_dir):
        
        arcpy.env.workspace = transform_dir
        arcpy.env.overwriteOutput = True
        fire_xy_event_layer_name = "fire_xy_layer"
        
        xyc = self.make_xy_event_layer_config
        event_layer_result = arcpy.MakeXYEventLayer_management(
            fire_csv_with_fields, xyc['in_x_field'], xyc['in_y_field'], fire_xy_event_layer_name, xyc.get('spatial_reference',''), xyc.get('in_z_field','')
        )
        self.debug_logger("MakeXYEventLayer_management result",event_layer_result.status)
        
        return fire_xy_event_layer_name

    def _addFieldsToCSV(self, fire_csv_without_fields, transform_dir):

        fire_csv_with_fields = os.path.join(transform_dir, "".join(os.path.basename(fire_csv_without_fields).split(".")[1:3])+"_temp.txt")
        
        with open(fire_csv_without_fields,'rb') as fire_no_fields:
            fire_points_string = fire_no_fields.read()
                
        with open(fire_csv_with_fields,'wb') as fire_with_fields:
            fire_with_fields.write(','.join(self.fire_fields) + "\n")
            fire_with_fields.write(fire_points_string)

        return fire_csv_with_fields


class FireMetaDataTransformer(object):
    
    """
        Class FireMetaDataTransformer:
        
            1)    converts a MET file (fire granule meta-data) into a dictionary
            2)    adds additional custom field values to the dictionary (values to fields not asscociated with the MET file)
    """
    
    def __init__(self, meta_data_transformer_config=None):
        
        if not meta_data_transformer_config:
            meta_data_transformer_config = {}
        
        self.debug_logger = meta_data_transformer_config.get('debug_logger',lambda*a,**kwa:None)
    
    def transform(self, fire_data):
        
        try:
            meta_data_dict = self._convertMetToDict(fire_data.getMetaDataToTransform())   
            self.debug_logger("len(meta_data_dict)",len(meta_data_dict))
            
            # add the FTP filename associated with the CSV to the dictionary, this is what is checked when determining duplicates in the ExtractValidator
            meta_data_dict['ftp_file_name'] = str(fire_data.getETLDataName())
            self._addCustomFields(meta_data_dict)
            
            fire_data.setMetaDataToLoad(meta_data_dict)
        
        except Exception as e:
            
            self.debug_logger("transformMetaData Exception:",str(e),str(arcpy.GetMessages(2)))
            fire_data.handleException(exception=str(e),messages=arcpy.GetMessages(2))
    
    def _convertMetToDict(self, met_file):
                
        with open(met_file,'rb') as meta_file:
            master_line = "".join([line.strip("\r") for line in meta_file])

        key_list = [kv.split("=")[1].strip() for kv in master_line.split("\n") if ("OBJECT" in kv) and ("END_OBJECT" not in kv)]
        value_list = [kv.split("=")[1].strip().replace("\"",'') for kv in master_line.split("\n") if ("VALUE" in kv) and ("END_VALUE" not in kv)]
        meta_data_dict = dict(zip(key_list, value_list))
        
        return meta_data_dict
    
    def _addCustomFields(self, meta_data_dict):
        
        strptime = datetime.strptime
                        
        # add start_datetime and datetime field values ------------------------------------------------------
        start_time_values = str(meta_data_dict['RANGEBEGINNINGTIME'])
        start_date_values = str(meta_data_dict['RANGEBEGINNINGDATE'])
                                            
        meta_data_dict["start_datetime"] = strptime("%s %s" % (start_date_values, start_time_values),"%Y-%m-%d %H:%M:%S.%f")
        meta_data_dict["start_datetime_string"] = meta_data_dict['start_datetime'].strftime("%m/%d/%Y %H:%M:%S")
        
        meta_data_dict["datetime"] = strptime("%s %s" % (start_date_values, start_time_values),"%Y-%m-%d %H:%M:%S.%f")
        meta_data_dict["datetime_string"] = meta_data_dict['start_datetime'].strftime("%m/%d/%Y %H:%M:%S")
        
        # add end_datetime field values ------------------------------------------------------
        end_time_values = str(meta_data_dict['RANGEENDINGTIME'])
        end_date_values = str(meta_data_dict['RANGEENDINGDATE'])
        meta_data_dict["end_datetime"] = strptime("%s %s" % (end_date_values, end_time_values),"%Y-%m-%d %H:%M:%S.%f")
        meta_data_dict["end_datetime_string"] = meta_data_dict['end_datetime'].strftime("%m/%d/%Y %H:%M:%S")
        
        # add last update field values ------------------------------------------------------
        meta_data_dict["last_update_datetime"] = str(datetime.utcnow())


class FireLoader(object):
        
    """
        Class FireLoader:
        
            1)    appends the rows from the temporary feature class into the main fire feature class
            2)    updates the fields asscoiated with the appended rows inside the main fire feature class
    """
    
    def __init__(self, loader_config):
        
        self.feature_class = loader_config['feature_class']
        self.append_rows_config = loader_config.get('Append_management_config',{})
        self.datetime_sql_cast = loader_config['datetime_sql_cast']
        self.satellite_mapping = loader_config['satellite_mapping']
        self.debug_logger = loader_config.get('debug_logger',lambda*a,**kwa:None)
    
    def load(self, fire_data):
        
        try:
            self._appendRowsFromTable(fire_data.getDataToLoad())
            self._updateTableFields(fire_data.getMetaDataToLoad())
            
        except Exception as e:

            self.debug_logger("load Exception:",str(e),str(arcpy.GetMessages(2)))
            fire_data.handleException(exception=("load:",str(e)),messages=arcpy.GetMessages(2))
    
    def _appendRowsFromTable(self, fire_table):
                         
        ac = self.append_rows_config
        append_result = arcpy.Append_management(
            fire_table, 
            self.feature_class.fullpath, 
            ac.get('schema_type',''), 
            ac.get('field_mapping',''), 
            ac.get('subtype','')
        )
        self.debug_logger("Append_management result",append_result.status)
        
    def _updateTableFields(self, fire_meta_dict):
        
        satellite_name = self.satellite_mapping[fire_meta_dict['SHORTNAME']]
                
        where_date_equals_date = "\"DATE\" = %s \'%s\'" % (self.datetime_sql_cast, fire_meta_dict['RANGEBEGINNINGDATE'])
        and_time_equals_time = "\"TIME\" = %s" % ("".join(fire_meta_dict['RANGEBEGINNINGTIME'].split(":")[:2]))
        and_satellite_equals_satellite = "\"SATELLITE\" = \'%s\'" % (satellite_name) # Important!, or else both aqua and terra granules will be updated 
        
        where_clause = "%s AND %s AND %s" % (where_date_equals_date, and_time_equals_time, and_satellite_equals_satellite)
        self.debug_logger("where_clause",where_clause)
        
        self.feature_class.updateFields(fire_meta_dict, {"where_clause": where_clause})