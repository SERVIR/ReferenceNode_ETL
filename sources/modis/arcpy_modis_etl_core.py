# Developer: SpatialDev
# Company:   Spatial Development International

# standard library
from datetime import datetime, timedelta
import os

# third-party
import arcpy

# ETL utils
from etl_utils import URLDownloadManager


class MODISExtractValidator(object):
    
    """
        Class MODISExtractValidator determines which MODIS imagery to process for the current ETL run.
        
        The high-level steps to accomplish this task include:
        
            1) retrieve the current MODIS images processed (rasters) from the given raster catalog and datetime range
            2) find the difference between the total MODIS images available and the current rasters in the raster catalog
            3) return a list of only the missing MODIS images to process for the current ETL run
    """
    
    def __init__(self, extract_validator_config):
        
        self.raster_catalog = extract_validator_config['raster_catalog']
        self.raster_name_field = extract_validator_config['raster_name_field']
        self.start_datetime = extract_validator_config['start_datetime']
        self.end_datetime = extract_validator_config['end_datetime']
        self.debug_logger = extract_validator_config.get('debug_logger',lambda*a,**kwa:None)
        
    def validateExtract(self, all_modis_rasters_list):
        
        current_modis_rasters = self.raster_catalog.getValuesFromDatetimeRange(self.raster_name_field, self.start_datetime, self.end_datetime)
        self.debug_logger("len(current_modis_rasters)", len(current_modis_rasters))
        
        missing_modis_rasters = list(set(all_modis_rasters_list) - set(current_modis_rasters))        
        self.debug_logger("len(missing_modis_rasters)", len(missing_modis_rasters))
        
        missing_modis_rasters.sort(key=lambda x:x,reverse=True) # re-sort and reverse since the both lists were cast as sets
        
        return missing_modis_rasters


class MODISExtractor(URLDownloadManager):
    
    """
        Class MODISExtractor:
        
            1) retrieves a list of image names given the URL component combonation. (This is sent to a MODISExtractValidator)
            2) downloads both the MODIS image and meta-data from the given URLs
    """
    
    def __init__(self, extractor_config):
        URLDownloadManager.__init__(self)
                
        self.extn = extractor_config['extn']
        self.subsets = extractor_config['subset']
        self.satellites = extractor_config['satellite']
        self.subtypes = extractor_config['subtype']
        self.sizes = extractor_config['size']
        self.image_content_types = extractor_config['image_content_types']
        self.text_content_types = extractor_config['text_content_types']
        self.start_datetime = extractor_config['start_datetime']
        self.end_datetime = extractor_config['end_datetime']
        self.debug_logger = extractor_config.get('debug_logger',lambda*a,**kwa:None)
        
    def getDataToExtract(self):
        
        correct_julian_day_year_list = self._getJulianDayListFromDateRange(self.start_datetime, self.end_datetime)
        modis_image_names_for_julian_days =  self._buildImageNameList(correct_julian_day_year_list)
        
        return modis_image_names_for_julian_days

    def _buildImageNameList(self, correct_julian_day_year_list):
                
        createImageName = lambda*r:".".join(r).replace("..",".") # replace '..' with '.'for MODIS True Color cases since subtype = ''
        extn = self.extn
        
        modis_image_name_list = [
                                  
            createImageName(subset, year_and_julian_day, satellite, subtype, size, extn)
            
            for year_and_julian_day in correct_julian_day_year_list 
            for subset in self.subsets
            for satellite in self.satellites
            for subtype in self.subtypes
            for size in self.sizes
        ]
                            
        return modis_image_name_list

    def _getJulianDayListFromDateRange(self, start_datetime, end_datetime):
                
        julian_day_year_range = int((start_datetime - end_datetime).days) # find the number of days between the start and end datetimes
        # create a list of year and day of the year items ex: ["2012345","2012346", "2012347"] 
        julian_day_year_list = [(start_datetime - timedelta(days=day)).strftime('%Y%j') for day in range(julian_day_year_range)]
        
        return julian_day_year_list
                                    
    def extract(self, modis_data):
        
        try:
            modis_image_url = modis_data.getDataToExtract()
            self.debug_logger("modis_image_url",modis_image_url)
            
            meta_data_url = modis_data.getMetaDataToExtract()
            self.debug_logger("meta_data_url",meta_data_url)
            
            file_to_download = os.path.join(modis_data.getExtractDir(), modis_data.getETLDataName())
            downloaded_image_path = self.downloadResultFromURL(modis_image_url, file_to_download, self.image_content_types)
            self.debug_logger("downloaded_image_path", downloaded_image_path)
            
            meta_data = self.getResultFromURL(meta_data_url, self.text_content_types) # returns a string of the meta-data
            
            # if a MODIS image is available
            if(downloaded_image_path):
                
                modis_data.setDataToLoad(downloaded_image_path)
                modis_data.setMetaDataToTransform(meta_data)
                
            else: # flag for removal but do not create an exception report (an unavailable image is not an exception)
                modis_data.flagForRemoval()
            
        except Exception as e:
            
            self.debug_logger("Extract Exception:",str(e),str(arcpy.GetMessages(2)))
            modis_data.handleException(exception=("Extract:",str(e)),messages=arcpy.GetMessages(2))
            

class MODISMetaDataTransformer(object):
    
    """
        Class MODISMetaDataTransformer:
        
            1)    converts the MODIS images meta-data string into a dictionary
            2)    adds additional custom field values to the dictionary (values to fields not asscociated with the MODIS image meta-data)
    """
    
    def __init__(self,meta_data_transformer_config=None):
        
        if not meta_data_transformer_config:
            meta_data_transformer_config = {}
        
        self.debug_logger = meta_data_transformer_config.get('debug_logger',lambda*a,**kwa:None)
    
    def transform(self, modis_data):
        
        try:                
            meta_data_string = modis_data.getMetaDataToTransform()     
                   
            meta_data_list = self._createMetaDataList(meta_data_string)
            meta_data_dict = dict((d.split("|")[0].replace(" ","_"), d.split("|")[1]) for d in meta_data_list if ("|" in d) and (d is not ''))
            self.debug_logger("len(meta_data_dict)",len(meta_data_dict))
            
            self._addCustomFields(meta_data_dict, modis_data.getETLDataName())

            modis_data.setMetaDataToLoad(meta_data_dict)
            
        except Exception as e:
            
            self.debug_logger("transformMetaData Exception:",str(e),str(arcpy.GetMessages(2)))
            modis_data.handleException(exception=("transformMetaData:",str(e)),messages=arcpy.GetMessages(2))
        
    def _createMetaDataList(self, meta_data_string):
        
        # the first part of the meta-data is easily split by a new line character, 
        # before splitting replace all ':' with '|' in order to split by "|" without time values causing a problem ex: "10:30" --> [10, 30]
        meta_data_list = meta_data_string[0:meta_data_string.rfind("L2 granules:")].replace(":", "|").strip().split("\n")
        
        # remove the second part of the string to re-create its key-value more explciity due to its structure
        l2_granules_string = meta_data_string[meta_data_string.rfind("L2 granules:"):] # get the raw string
        l2_granules_list = l2_granules_string.split("\n") if "\n" in l2_granules_string else [l2_granules_string] # turn it into a list of items
        l2_key_value_pair = str(l2_granules_list[0].strip(":") + "|" + str(",".join(l2_granules_list[1:])).rstrip(","))# concat the key and values together 
        meta_data_list.append(l2_key_value_pair) # append it to the list containing the rest of the key-value pairs
        
        return meta_data_list
    
    def _addCustomFields(self, meta_data_dict, raster_name):
                
        # add the value for the images resolution
        meta_data_dict['resolution'] = str(raster_name.split(".")[-2])
        
        # add the start datetime values
        raster_datetime = datetime.strptime(meta_data_dict['date'].split(" ")[1],"%Y%j")
        meta_data_dict['datetime'] = raster_datetime
        meta_data_dict['datetime_string'] = str(raster_datetime.strftime('%m-%d-%Y %I:%M:%S %p'))
        

class MODISLoader(object):
    
    """
        Class MODISLoader:
        
            1)    inserts the given MODIS image into the given raster catalog
            2)    updates the fields asscoiated with the inserted image in the raster catalog
    """
    
    def __init__(self, loader_config):
        
        self.raster_catalog = loader_config['raster_catalog']
        self.copy_raster_config = loader_config.get('CopyRaster_management_config',{})
        self.debug_logger = loader_config.get('debug_logger',lambda*a,**kwa:None)
    
    def load(self, modis_data):
        
        try:
            modis_image = modis_data.getDataToLoad()
            
            self._copyRaster(modis_image)
            self.raster_catalog.updateFieldsForInput(str(os.path.basename(modis_image)), modis_data.getMetaDataToLoad())
            
        except Exception as e:
            
            self.debug_logger("load Exception:",str(e),str(arcpy.GetMessages(2)))
            modis_data.handleException(exception=("load:",str(e)),messages=arcpy.GetMessages(2))
    
    def _copyRaster(self, modis_image):
        
        crc = self.copy_raster_config
        copy_result = arcpy.CopyRaster_management(
            modis_image, 
            self.raster_catalog.fullpath, 
            crc.get('config_keyword',''), 
            crc.get('background_value',''), 
            crc.get('nodata_value',''), 
            crc.get('onebit_to_eightbit',''), 
            crc.get('colormap_to_RGB',''), 
            crc.get('pixel_type','')
        )
        self.debug_logger("CopyRaster_management result status",copy_result.status)