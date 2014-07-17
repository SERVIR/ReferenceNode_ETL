# Developer: SpatialDev
# Company:   Spatial Development International


# standard library
import os
from datetime import datetime, timedelta

# third-party
import arcpy

# ETL utils
from etl_utils import FTPDownloadManager, UnzipUtils


class WRFExtractValidator(object):
    
    """
        Class WRFExtractValidator determines which WRF ASCII files to process for the current ETL run.
        
        The high-level steps to accomplish this task include:
        
            1) Retrieve all ASCIIs from the given FTP for a given domain and variable.
            2) Retrieve the current rasters processed from the raster mosaic dataset within the given datetime range.
            3) Compare the ASCII files from the FTP with the current rasters processed in the raster mosaic dataset.
            4) Filter the difference from step 3 by the correct frame.
            5) Return a list containing only the new ASCII files to process for the current ETL run.
    """
    
    def __init__(self, extract_validator_config):
        
        self.raster_mosaic_dataset = extract_validator_config['raster_mosaic_dataset']
        self.ftp_file_name_field = extract_validator_config['ftp_file_name_field']
        self.model_runtime_field = extract_validator_config['model_runtime_field']
        self.forecast_end_hour_field = extract_validator_config['forecast_end_hour_field']
        self.start_datetime = extract_validator_config['start_datetime']
        self.end_datetime = extract_validator_config['end_datetime']
        self.wrf_variable = extract_validator_config['wrf_variable']
        self.domain = extract_validator_config['domain']
        self.ftp_ascii_datetime_format = extract_validator_config['ftp_ascii_datetime_format']
        self.debug_logger = extract_validator_config.get('debug_logger',lambda*a,**kwa:None)
                
    def validateExtract(self, ftp_ascii_list):
        
        # retrieve the datetime range
        start_dt = self.start_datetime
        end_dt = self.end_datetime
            
        # retrieve all ASCII files from the ftp_ascii_list for the given variable and domain
        filtered_ftp_ascii_list = self._getFilteredDomainVariableASCIIList(ftp_ascii_list)
        self.debug_logger("len(filtered_ftp_ascii_list)", len(filtered_ftp_ascii_list))
        
        # retrieve a list of the current ASCII files processed for the given datetime range
        current_asciis_processed_list = self.raster_mosaic_dataset.getValuesFromDatetimeRange(self.ftp_file_name_field, start_dt, end_dt)
        self.debug_logger("len(current_asciis_processed_list)", len(current_asciis_processed_list))
        
        # retrieve a list of all new ASCII files to process based on the difference between the filtered_ftp_ascii_list and current_asciis_processed_list
        missing_ascii_list = self._getUnprocessedASCIIsWithinDatetimeRange(filtered_ftp_ascii_list, current_asciis_processed_list, start_dt, end_dt)
        self.debug_logger("len(missing_ascii_list)", len(missing_ascii_list))
        
        # retrieve a filtered list of new ASCII files to process based on the hourly frame and archive constraints
        asciis_to_process_list = self._getCorrectHourlyFramesFromASCIIList(missing_ascii_list)
        self.debug_logger("len(asciis_to_process_list)", len(asciis_to_process_list))
        
        return asciis_to_process_list
    
    def _getFilteredDomainVariableASCIIList(self, ftp_ascii_list):
        
        # cache instance/module variables as local variables
        wrf_var = self.wrf_variable
        domain = self.domain
        
        # create helper functions
        startsWithVariable = lambda f:f.split("_")[0] == wrf_var
        endsWithDomain = lambda f:f.split(".")[0].endswith(domain)
        isTargetVariableAndDomain = lambda f:startsWithVariable(f) and endsWithDomain(f)
        
        # create the filtered_ftp_ascii_list
        filtered_ftp_ascii_list = [f for f in ftp_ascii_list if isTargetVariableAndDomain(f)]
        filtered_ftp_ascii_list = list(set(filtered_ftp_ascii_list)) # remove all possible duplicates from the list (if duplicates exists on the FTP)
        
        return filtered_ftp_ascii_list
    
    def _getUnprocessedASCIIsWithinDatetimeRange(self, filtered_ftp_ascii_list, current_asciis_processed_list, start_dt, end_dt):
        
        # create helper functions
        getDatetimeFromASCII = self._getDatetimeFromASCII # cache function as local variable
        isWithinDatetimeRange = lambda f:getDatetimeFromASCII(f) >= end_dt and getDatetimeFromASCII(f) <= start_dt

        # retrieve all ASCII files within the given datetime range
        asciis_within_datetime_range_list = [f for f in filtered_ftp_ascii_list if isWithinDatetimeRange(f)]
        self.debug_logger("len(asciis_within_datetime_range_list)", len(asciis_within_datetime_range_list))
        
        # retrieve all ASCII files that are not currenlty processed in the raster mosaic dataset
        missing_ascii_list = [ascii for ascii in asciis_within_datetime_range_list if ascii not in current_asciis_processed_list]
        sortByDateAndHour = lambda x:x.split("_")[1]+"_"+x.split("_")[2] # (apcp10h_2012082006_10_d01.asc.gz)
        missing_ascii_list.sort(key=sortByDateAndHour) # sort so that the OLDEST ASCII files are processed first

        return missing_ascii_list
    
    def _getDatetimeFromASCII(self, asciiFile):
        
        # apcp10h_2012082006_10_d01.asc.gz --> 2012-AUG-20 6:00AM
        ascii_datetime = datetime.strptime (asciiFile.split("_")[1],  self.ftp_ascii_datetime_format )
        
        return ascii_datetime
        
    def _getCorrectHourlyFramesFromASCIIList(self, missing_ascii_list):
        
        # create a datetime object precise to the day
        createWholeDayDatetime = lambda d :datetime(d.year, d.month, d.day)
        
        # retrieve the last processed ASCII and convert it to a datetime object
        lastASCIIProcessedDatetime = self.raster_mosaic_dataset.getMaxValueFromField(self.model_runtime_field)
        lastASCIIProcessedDatetime = lastASCIIProcessedDatetime if isinstance(lastASCIIProcessedDatetime, datetime) else self.start_datetime
        onlyDaylastASCIIProcessedDatetime = createWholeDayDatetime(lastASCIIProcessedDatetime)
        self.debug_logger("lastASCIIProcessedDatetime",lastASCIIProcessedDatetime)
        
        # retrieve the greatest datetime from the missing ASCII files
        greatestDatetimeFromMissingASCIIList = self._getMaxDatimeObjectFromStringList(missing_ascii_list)        
        onlyDaygreatestDatetimeFromMissingASCIIList = createWholeDayDatetime(greatestDatetimeFromMissingASCIIList)
        self.debug_logger("greatestDatetimeFromMissingASCIIList",greatestDatetimeFromMissingASCIIList)
        
        # if the datetime of the last ASCII processed is less than the greatest datetime of the missing ASCIIs, then delete frames 25-48 of the last ASCII processed. 
        greaterASCIIFileIsAvailable = onlyDaylastASCIIProcessedDatetime < onlyDaygreatestDatetimeFromMissingASCIIList
        if greaterASCIIFileIsAvailable:
        
            # remove the frame overlap from the last ASCII processed
            self._deleteLastACSIIProcessedOverlapFrames(lastASCIIProcessedDatetime)
        
        # create helper functions to filter out the correct ASCII files to download based on the frame overlap and archive constaints
        createASCIIDatetime = lambda x: createWholeDayDatetime(datetime.strptime(x.split("_")[1][:-2], self.ftp_ascii_datetime_format[:-2]))
        differenceInDaysFromLastASCIIProcess = lambda x: (lastASCIIProcessedDatetime - x).days
        isCorrectFrameForGreaterThanADay = lambda x: int(x.split("_")[2]) < 24
        withinOneDayRangePositive = lambda x: abs(differenceInDaysFromLastASCIIProcess(createASCIIDatetime(x))) < 1
        isLatestASCIIFileAvailable = lambda x : greaterASCIIFileIsAvailable and createASCIIDatetime(x) == onlyDaygreatestDatetimeFromMissingASCIIList        
        isValidGreaterThan24HourFrame = lambda x: withinOneDayRangePositive(x) or isLatestASCIIFileAvailable(x)
        isValidLessThan24HourFrame = lambda x: not withinOneDayRangePositive(x) and isCorrectFrameForGreaterThanADay(x)

        # filter the missing_ascii_list by the frame overlap contstraints        
        asciiIsValidToDownload = lambda ascii_to_check: isValidGreaterThan24HourFrame(ascii_to_check) or isValidLessThan24HourFrame(ascii_to_check)
        asciis_to_process_list = [ascii for ascii in missing_ascii_list if asciiIsValidToDownload(ascii)]

        return asciis_to_process_list
            
    def _getMaxDatimeObjectFromStringList(self, missing_ascii_list):
        
        # If there are no missing ASCII files then it is assumed that this is the first time the ETL is being run, 
        # therefore retrieve the latest ASCII files for the given start datetime.
        if len(missing_ascii_list) == 0:
            
            self.debug_logger("Returning current start_datetime")
            return self.start_datetime
        
        max_datetime_string = max(missing_ascii_list)
        max_datetime_object = datetime.strptime(max_datetime_string.split("_")[1], self.ftp_ascii_datetime_format)
        
        return max_datetime_object
                
    def _deleteLastACSIIProcessedOverlapFrames(self, lastASCIIProcessedDatetime):
        
        where_clause = "%s = date \'%s\' AND %s >= 24" % (self.model_runtime_field, lastASCIIProcessedDatetime, self.forecast_end_hour_field)
        self.debug_logger("where_clause",where_clause)
        
        self.raster_mosaic_dataset.deleteRows(where_clause)
        self.debug_logger("deleted last ASCII processed frame 24-48 overlap.")


class WRFExtractor(FTPDownloadManager):
    
    """
        Class WRFExtractor:
        
            1) Retrieves a list of all ASCII raster files from a given FTP directory. (This list is sent to a WRFExtractValidator).
            2) Downloads an ASCII raster file from the given FTP directory.
    """
    
    def __init__(self, extractor_config):
        FTPDownloadManager.__init__(self, extractor_config['ftp_options'])

        self.target_file_extn = extractor_config.get('target_file_extn', None)
        self.debug_logger = extractor_config.get('debug_logger',lambda*a,**kwa:None)
                                                    
    def getDataToExtract(self, ftp_directory):
        
        self.openConnection()

        return self.getFileNamesFromDirectory(ftp_directory, self.target_file_extn)

    def extract(self, wrf_data):
        
        try:
            ascii_to_download = wrf_data.getDataToExtract()
            
            extract_dir = wrf_data.getExtractDir()
            
            downloaded_zipped_ascii_fullpath = self.downloadFileFromFTP(ascii_to_download, extract_dir)
            self.debug_logger("downloaded_zipped_ascii_fullpath",downloaded_zipped_ascii_fullpath)
    
            unzipped_ascii_fullpath = UnzipUtils.unzipGZip(downloaded_zipped_ascii_fullpath, extract_dir)
            self.debug_logger("unzipped_ascii_fullpath",unzipped_ascii_fullpath)
            
            wrf_data.setDataToTransform(unzipped_ascii_fullpath)
            
        except Exception as e:
            
            self.debug_logger("extract Exception:",str(e),str(arcpy.GetMessages(2)))
            wrf_data.handleException(exception=("extract:",str(e)),messages=arcpy.GetMessages(2))
  

class WRFTransformer(object):
    
    """
        Class WRFTransformer converts an ASCII file into an ESRI raster grid.
    """
    
    def __init__(self, transformer_config, decoratee):
                
        self.ascii_to_raster_config = transformer_config.get('ASCIIToRaster_conversion_config', {})
        self.debug_logger = transformer_config.get('debug_logger',lambda*a,**kwa:None)
        
        self.decoratee = decoratee # this is the WRFMetaDataTransformer
                
    def transform(self, wrf_data):

        try:
            ascii_file = wrf_data.getDataToTransform()
            self.debug_logger("ascii_file",ascii_file)
            
            raster_base_name = os.path.basename(ascii_file) # apcp10h_2012082006_10_d01.asc
            raster_name_parts = raster_base_name.split("_") # [apcp10h, 2012082006, 10, d01.asc]
            raster_name = "w" + raster_name_parts[1] + raster_name_parts[2] # w201208200610
            out_raster = os.path.join(wrf_data.getTransformDir(), raster_name)
            self.debug_logger("out_raster",out_raster)
            
            arcpy.env.overwriteOutput = True
            result = arcpy.ASCIIToRaster_conversion(ascii_file, out_raster, self.ascii_to_raster_config.get('data_type', ''))
            self.debug_logger("ASCIIToRaster_conversion status: ", result.status)
            
            wrf_data.setDataToLoad(out_raster)
            self.decoratee.transform(wrf_data)
                        
        except Exception as e:
            
            self.debug_logger("transform Exception:",str(e),str(arcpy.GetMessages(2)))
            wrf_data.handleException(exception=("transform:",str(e)),messages=arcpy.GetMessages(2))
        

class WRFMetaDataTransformer(object):
    
    """
        Class WRFMetaDataTransformer calculates the raster mosaic dataset field values.
    """
    
    def __init__(self, meta_transformer_config=None):
        
        if not meta_transformer_config:
            meta_transformer_config = {}
        
        self.string_datetime_format = meta_transformer_config['string_datetime_format']
        self.debug_logger = meta_transformer_config.get('debug_logger',lambda*a,**kwa:None)
                            
    def transform(self, wrf_data):
        
        try:
            ftp_file_name = wrf_data.getDataToExtract()
            
            meta_data_dict = {}
            meta_data_dict['ftp_file_name'] = str(ftp_file_name)
            meta_data_dict['wrf_variable'] = str(ftp_file_name).split("_")[0]
            meta_data_dict['wrf_domain'] = str(ftp_file_name).split("_")[-1].split(".")[0]
            
            self._addCustomFields(meta_data_dict, ftp_file_name)
            self.debug_logger("len(meta_data_dict)",len(meta_data_dict))
            
            wrf_data.setMetaDataToLoad(meta_data_dict)
            
        except Exception as e:
            
            self.debug_logger("transformMetaData Exception:",str(e),str(arcpy.GetMessages(2)))
            wrf_data.handleException(exception=("transformMetaData:",str(e)),messages=arcpy.GetMessages(2))
            
    def _addCustomFields(self, meta_data_dict, ftp_file_name):
        
        # apcp10h_2012082006_10_d01.asc.gz
        date_value = ftp_file_name.split("_")[1][:-2] # 20120820
        self.debug_logger("date_value",date_value)
                        
        strptime = datetime.strptime
        datetime_format = '%Y%m%d'
        string_datetime_format = self.string_datetime_format
        
        model_runtime_hour = int(ftp_file_name.split("_")[1][-2:]) # 06
        meta_data_dict['model_runtime_hour'] = model_runtime_hour
        self.debug_logger("model_runtime_hour",model_runtime_hour)
        
        meta_data_dict['model_runtime'] = strptime(date_value, datetime_format) + timedelta(hours=model_runtime_hour)
        self.debug_logger("meta_data_dict['model_runtime']",meta_data_dict['model_runtime'])
        
        meta_data_dict['model_runtime_string'] = meta_data_dict['model_runtime'].strftime(string_datetime_format) 
        self.debug_logger("meta_data_dict['model_runtime_string']",meta_data_dict['model_runtime_string'])  
        
        end_hour = int(ftp_file_name.split("_")[2]) # 10
        meta_data_dict['forecast_end_hour'] = end_hour
        self.debug_logger("forecast_end_hour",end_hour)
        
        meta_data_dict['end_datetime'] = meta_data_dict['model_runtime'] + timedelta(hours=end_hour)
        self.debug_logger("meta_data_dict['end_datetime']",meta_data_dict['end_datetime'])
        
        meta_data_dict['end_hour'] =  meta_data_dict['end_datetime'].hour
        self.debug_logger("meta_data_dict['end_hour']",meta_data_dict['end_hour'])
        
        meta_data_dict['end_datetime_string'] = meta_data_dict['end_datetime'].strftime(string_datetime_format)
        self.debug_logger("meta_data_dict['end_datetime_string']",meta_data_dict['end_datetime_string'])  
        
        meta_data_dict['start_datetime'] = meta_data_dict['end_datetime'] - timedelta(hours=1)
        self.debug_logger("meta_data_dict['start_datetime']",meta_data_dict['start_datetime'])
        
        meta_data_dict['start_hour'] = meta_data_dict['start_datetime'].hour
        self.debug_logger("meta_data_dict['start_hour']",meta_data_dict['start_hour'])
        
        meta_data_dict['start_datetime_string'] = meta_data_dict['start_datetime'].strftime(string_datetime_format)
        self.debug_logger("meta_data_dict['start_datetime_string']",meta_data_dict['start_datetime_string'])  


class WRFLoader(object):
    
    """
        Class WRFLoader:
        
            1) Copies the raster created from the ASCII into the given file geodatabase
            2) Adds the raster in file geodatabasegiven to the given raster mosaic dataset.
            3) Updates the fields associated with the added raster in the given raster mosaic dataset.
    """

    def __init__(self, loader_config):
        
        self.raster_mosaic_dataset = loader_config['raster_mosaic_dataset']
        self.wrf_variable = loader_config['wrf_variable']
        self.copy_raster_config = loader_config['CopyRaster_management_config']
        self.add_raster_to_mosaic_config = loader_config['AddRastersToMosaicDataset_management_config']
        self.fgdb_fullpath = loader_config['fgdb_fullpath']
        self.debug_logger = loader_config.get('debug_logger',lambda*a,**kwa:None)
                                        
    def load(self, wrf_data):
        
        try:           
            # retrieve a reference to the raster in the transform directory 
            wrf_raster = wrf_data.getDataToLoad()
            self.debug_logger("wrf_raster",wrf_raster)
            meta_data = wrf_data.getMetaDataToLoad()
            
            # retrieve the basename from the raster
            raster_name = os.path.basename(wrf_raster)
            self.debug_logger("raster_name", raster_name)
            
            # create the output raster fullpath
            # w201208200610 --> 201208200610 --> <wrf_var>_201208200610
            fgdb_raster_name = self.wrf_variable + "_" +raster_name[1:]
            fgdb_raster_fullpath = os.path.join(self.fgdb_fullpath, fgdb_raster_name)
            self.debug_logger("fgdb_raster_fullpath", fgdb_raster_fullpath)
            
            # copy the transfrom directory raster into the file geodatabase
            self._copyRaster(wrf_raster, fgdb_raster_fullpath)
            
            # add the file geodatabase raster into the raster mosaic dataset (which is also located in the same file geodatabase)
            self._addRasterToMosaicDataset(fgdb_raster_fullpath)
            
            self.raster_mosaic_dataset.updateFieldsForInput(fgdb_raster_name, meta_data)
            self.debug_logger("updated raster mosaic dataset fields")
            
        except Exception as e:
            
            self.debug_logger("Load Exception:",str(e),str(arcpy.GetMessages(2)))
            wrf_data.handleException(exception=("Load:",str(e)),messages=arcpy.GetMessages(2))
            
    def _copyRaster(self, in_raster, out_raster):
                
        crc = self.copy_raster_config
        copy_result = arcpy.CopyRaster_management(
            in_raster, 
            out_raster, 
            crc.get('config_keyword',''), 
            crc.get('background_value',''), 
            crc.get('nodata_value',''), 
            crc.get('onebit_to_eightbit',''), 
            crc.get('colormap_to_RGB',''), 
            crc.get('pixel_type','')
        )
        self.debug_logger("CopyRaster_management status",copy_result.status)
             
    def _addRasterToMosaicDataset(self, in_raster):
        
        armc = self.add_raster_to_mosaic_config
        
        add_raster_to_dataset_result = arcpy.AddRastersToMosaicDataset_management(
                                                                                  
            self.raster_mosaic_dataset.fullpath,                                                                      
            armc['raster_type'], 
            in_raster, 
            armc.get('update_cellsize_ranges',''), 
            armc.get('update_boundary',''), 
            armc.get('update_overviews',''), 
            armc.get('maximum_pyramid_levels',''), 
            armc.get('maximum_cell_size',''), 
            armc.get('minimum_dimension',''), 
            armc.get('spatial_reference',''), 
            armc.get('filter',''), 
            armc.get('sub_folder',''), 
            armc.get('duplicate_items_action',''), 
            armc.get('build_pyramids',''), 
            armc.get('calculate_statistics',''), 
            armc.get('build_thumbnails',''), 
            armc.get('operation_description',''), 
        )
        
        self.debug_logger("AddRastersToMosaicDataset_management status",add_raster_to_dataset_result.status)