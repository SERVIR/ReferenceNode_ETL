# Developer: SpatialDev
# Company:   Spatial Development International


# standard library
import os
from datetime import datetime
from itertools import takewhile

# third-party
import arcpy
import numpy as np

# ETL utils
from etl_utils import FTPDownloadManager, UnzipUtils


class TRMMExtractValidator(object):
    
    """
        Class TRMMExtractValidator determines which bin files to process for the current ETL run.
        
        The high-level steps to accomplish this task include:
        
            1) retrieve all bins from a given FTP
            2) retrieve the current bins processed from the raster catalog within a given datetime range
            3) filter the ftp list of bins by the given datetime range
            4) compare the filtered bins from the FTP with the current bins processed in the raster catalog
            5) return a list containing only the new bins to process for the current ETL run
    """
    
    def __init__(self, extract_validator_config):
        
        self.raster_catalog = extract_validator_config['raster_catalog']
        self.ftp_file_name_field = extract_validator_config['ftp_file_name_field']
        self.start_datetime = extract_validator_config['start_datetime']
        self.end_datetime = extract_validator_config['end_datetime']
        self.debug_logger = extract_validator_config.get('debug_logger',lambda*a,**kwa:None)
                
    def validateExtract(self, ftp_bins_list):
        
        start_dt = self.start_datetime
        end_dt = self.end_datetime
        
        current_bins_processed_list = self.raster_catalog.getValuesFromDatetimeRange(self.ftp_file_name_field, start_dt, end_dt)
        self.debug_logger("len(current_bins_processed_list)", len(current_bins_processed_list))
        
        missing_bins_list = self._getMissingBinFiles(ftp_bins_list, current_bins_processed_list, start_dt, end_dt)
        self.debug_logger("len(missing_bins_list)", len(missing_bins_list))

        return missing_bins_list

    def _getMissingBinFiles(self, ftp_bins_list, current_bins_processed_list, start_dt, end_dt):
        
        dtrp = datetime.strptime
        bin_datetime_format = '%Y%m%d%H' # 2012010215 --> 2012-JAN-02 3:00PM
        isWithinDatetimeRange = lambda b:dtrp(b.split(".")[1], bin_datetime_format) <= start_dt and dtrp(b.split(".")[1], bin_datetime_format) >= end_dt
        ftp_bins_list.sort(key=lambda x: x, reverse=True) # sort the list and reverse so the most recent bin files are first
        # function takewhile returns values from the given ftp_bins_list while the given input function isWithinDatetimeRange is True
        missing_bin_list = [bin for bin in takewhile(isWithinDatetimeRange, ftp_bins_list) if bin not in current_bins_processed_list]

        return missing_bin_list
    

class TRMMExtractor(FTPDownloadManager):
    
    """
        Class TRMMExtractor:
        
            1) retrieves a list of all bin files from a given FTP directory. (This list is sent to a TRMMExtractValidator).
            2) downloads a bin file from the given FTP directory.
    """
    
    def __init__(self, extractor_config):
        FTPDownloadManager.__init__(self, extractor_config['ftp_options'])

        self.target_file_extn = extractor_config.get('target_file_extn', None)
        self.debug_logger = extractor_config.get('debug_logger',lambda*a,**kwa:None)
                                                    
    def getDataToExtract(self, ftp_directory):
        
        self.openConnection()

        return self.getFileNamesFromDirectory(ftp_directory, self.target_file_extn)

    def extract(self, trmm_data):
        
        try:
            bin_to_download = trmm_data.getDataToExtract()
            
            extract_dir = trmm_data.getExtractDir()
            
            downloaded_zipped_bin_fullpath = self.downloadFileFromFTP(bin_to_download, extract_dir)
            self.debug_logger("downloaded_zipped_bin_fullpath",downloaded_zipped_bin_fullpath)
    
            unzipped_bin_fullpath = UnzipUtils.unzipGZip(downloaded_zipped_bin_fullpath, extract_dir)
            self.debug_logger("unzipped_bin_fullpath",unzipped_bin_fullpath)
            
            trmm_data.setDataToTransform(unzipped_bin_fullpath)
            
        except Exception as e:
            
            self.debug_logger("extract Exception:",str(e),str(arcpy.GetMessages(2)))
            trmm_data.handleException(exception=("extract:",str(e)),messages=arcpy.GetMessages(2))
  

class TRMMTransformer(object):
    
    """
        Class TRMMTransformer:
        
            1) converts a bin into an ESRI raster grid
            2) retrieve the bin's header (meta-data) as a string
        
        The high-level steps to accomplish task 1) includes:
        
            1) read the binary file data and write it out to a CSV using numpy
            2) create an xy event layer from the CSV
            3) create a raster from the xy event layer
    """
    
    def __init__(self, transformer_config, decoratee):
                
        self.percip_min = transformer_config.get('precip_min', 0)
        self.raster_name_prefix = transformer_config.get('raster_name_prefix', 0)
        self.make_xy_event_layer_config = transformer_config.get('MakeXYEventLayer_management_config', {})
        self.point_to_raster_config = transformer_config.get('PointToRaster_conversion_config', {})
        self.debug_logger = transformer_config.get('debug_logger',lambda*a,**kwa:None)
        
        self.decoratee = decoratee # this is the TRMMMetaDataTransformer
        
    def transform(self, trmm_data):

        try:
            bin_file = trmm_data.getDataToTransform()
            
            csv_file, header_string = self._transformBinToCSV(bin_file, trmm_data.getTransformDir())
            self.debug_logger("csv_file",csv_file)
            
            raster = self._transformCSVToRaster(csv_file, trmm_data.getLoadDir())
            self.debug_logger("raster",raster)
            
            trmm_data.setDataToLoad(raster)
            trmm_data.setMetaDataToTransform(header_string)
            
            self.decoratee.transform(trmm_data) # call the meta-transformer to transform the header into a dictonary
            
        except Exception as e:
            
            self.debug_logger("transform Exception:",str(e),str(arcpy.GetMessages(2)))
            trmm_data.handleException(exception=("transform:",str(e)),messages=arcpy.GetMessages(2))
        
    def _transformBinToCSV(self, bin_to_process, transform_dir):
        
        rows, cols, precip_scale_factor = 480, 1440, 100.0 # tuple assignment
        precip_min = self.percip_min
         
        # retrieve the header and data strings ---------
        with open(bin_to_process,'rb') as bin_file:
            
            data_string = bin_file.read() 
            # split by the last key's value to remove the meta-data, then re-concat the value
            header_string = str(data_string.split("=LAST")[0]+"=LAST") 
            
        # execute numpy operations ---------------------
        precip = np.fromstring(data_string[2880:1385280], np.int16)
        precip = precip.byteswap()
        precip = np.asarray(precip, np.float32)
        precip /= precip_scale_factor
        precip = precip.reshape(rows, cols)
        
        vstack = np.vstack
        arange = np.arange 
        
        # north-south extent
        lat = arange(59.875, -60.125, -0.25, dtype=float)
        z = arange(59.875, -60.125, -0.25, dtype=float)
        
        for i in range(1, cols): 
            lat = vstack((lat, z)) 
        lat = lat.transpose()
        
        # east-west extent
        lng = arange(0.125, 360.125, +0.25, dtype=float)
        z = arange(0.125, 360.125, +0.25, dtype=float)
        
        for i in range(1, rows): 
            lng = vstack((lng, z))     
              
        # write lat, long, and precipitation values to csv -----------------------------
        csv_name = bin_to_process.split(".")[1] + ".csv"
        csv_fullpath = os.path.join(transform_dir, csv_name)
        
        with open(csv_fullpath, "w") as out_csv:            
            out_csv.write('lat,long,precipitation\n')
            
            for i in np.ndindex(precip.shape):
                if precip[i] >= precip_min:
                    out_csv.write('%g,%g,%g\n' % (lat[i], lng[i], precip[i]))
            
        return (csv_fullpath, header_string)
        
    def _transformCSVToRaster(self, csv_file, load_dir):
        
        arcpy.env.overwriteOutput = True # set True since the xy event layer created resides in memory
        
        raster_name = os.path.basename(csv_file).split(".")[0]
        raster_name_with_prefix = self.raster_name_prefix + raster_name
        output_raster = os.path.join(load_dir, raster_name_with_prefix)
        trmm_xy_event_layer = "trmm_xy_layer"
        
        # create xy event layer
        xyc = self.make_xy_event_layer_config
        xy_result = arcpy.MakeXYEventLayer_management(
            csv_file, xyc['in_x_field'], xyc['in_y_field'],trmm_xy_event_layer, xyc.get('spatial_reference',''), xyc.get('in_z_field','')
        )
        self.debug_logger("MakeXYEventLayer_management status", xy_result.status)
        
        # create raster from xy event layer
        prc = self.point_to_raster_config
        point_to_raster_result = arcpy.PointToRaster_conversion(
            trmm_xy_event_layer, prc['value_field'], output_raster, prc.get('cell_assignment',''), prc.get('priority_field',''), prc.get('cellsize','')
        )
        self.debug_logger("PointToRaster_conversion status", point_to_raster_result.status) 
        
        return output_raster


class TRMMMetaDataTransformer(object):
    
    """
        Class TRMMMetaDataTransformer:
        
            1)    converts the header string inside a given bin file into a dictionary
            2)    adds additional custom field values to the dictionary (values to fields not associated with the bin's meta-data)
    """
    
    def __init__(self, meta_transformer_config=None):
        
        if not meta_transformer_config:
            meta_transformer_config = {}
        
        self.debug_logger = meta_transformer_config.get('debug_logger',lambda*a,**kwa:None)
                            
    def transform(self, trmm_data):
        
        try:
            header_string = trmm_data.getMetaDataToTransform()
            
            meta_data_dict = dict(d.split("=") for d in header_string.split(" ") if ("=" in d) and (d is not '')) # create the dictionary
            self.debug_logger("len(meta_data_dict)",len(meta_data_dict))
            
            # add the FTP filename associated with the bin to the dictionary, this is what is checked when determining duplicates in the TRMMExtractValidator
            meta_data_dict['ftp_file_name'] = str(trmm_data.getDataToExtract())
            self._addCustomFields(meta_data_dict)
            
            trmm_data.setMetaDataToLoad(meta_data_dict)
            
        except Exception as e:
            
            self.debug_logger("transformMetaData Exception:",str(e),str(arcpy.GetMessages(2)))
            trmm_data.handleException(exception=("transformMetaData:",str(e)),messages=arcpy.GetMessages(2))
            
    def _addCustomFields(self, meta_data_dict):
        
        strptime = datetime.strptime
        datetime_format = '%Y%m%d%H%M%S'
        string_datetime_format = '%Y/%m/%d %H:%M:%S'
        
        # add start_datetime and datetime field values ------------------------------------------------------
        timestamp_string = "%s%s" % (meta_data_dict['nominal_YYYYMMDD'], meta_data_dict['nominal_HHMMSS'])
        meta_data_dict['datetime'] = strptime(timestamp_string, datetime_format)
        
        timestamp_string = "%s%s" % (meta_data_dict['nominal_YYYYMMDD'], meta_data_dict['nominal_HHMMSS'])
        meta_data_dict['datetime_string'] = strptime(timestamp_string, datetime_format).strftime(string_datetime_format)
        
        start_time_string = "%s%s" % (meta_data_dict['begin_YYYYMMDD'], meta_data_dict['begin_HHMMSS'])
        meta_data_dict['start_datetime'] = strptime(start_time_string, datetime_format)
        
        start_time_string = "%s%s" % (meta_data_dict['begin_YYYYMMDD'], meta_data_dict['begin_HHMMSS'])
        meta_data_dict['start_datetime_string'] = strptime(start_time_string, datetime_format).strftime(string_datetime_format)
        
        # add end_datetime field values ------------------------------------------------------
        end_time_string = "%s%s" % (meta_data_dict['end_YYYYMMDD'], meta_data_dict['end_HHMMSS'])
        meta_data_dict['end_datetime'] = strptime(end_time_string, datetime_format)
        
        end_time_string = "%s%s" % (meta_data_dict['end_YYYYMMDD'], meta_data_dict['end_HHMMSS'])
        meta_data_dict['end_datetime_string'] = strptime(end_time_string, datetime_format).strftime(string_datetime_format)
            

class TRMMLoader(object):
    
    """
        Class TRMMLoader:
        
            1)    inserts the given raster into the given raster catalog
            2)    updates the fields associated with the inserted raster in the given raster catalog
    """

    def __init__(self, loader_config):
        
        self.raster_catalog = loader_config['raster_catalog']
        self.copy_raster_config = loader_config.get('CopyRaster_management_config',{})
        self.add_color_map_config = loader_config.get('AddColormap_management_config', None)
        self.debug_logger = loader_config.get('debug_logger',lambda*a,**kwa:None)
                                        
    def load(self, trmm_data):
        
        try:
            trmm_raster = trmm_data.getDataToLoad()
            meta_data = trmm_data.getMetaDataToLoad()
            
            # optionally add a color map
            if self.add_color_map_config:
                self._addColorMap(trmm_raster)
                
            self._copyRaster(trmm_raster)
            
            raster_name = os.path.basename(trmm_raster)
            self.raster_catalog.updateFieldsForInput(raster_name, meta_data)
            
        except Exception as e:
            
            self.debug_logger("Load Exception:",str(e),str(arcpy.GetMessages(2)))
            trmm_data.handleException(exception=("Load:",str(e)),messages=arcpy.GetMessages(2))
            
    def _addColorMap(self, trmm_raster):
        
        cmc = self.add_color_map_config
        color_map_result = arcpy.AddColormap_management(trmm_raster, cmc.get('in_template_raster',''), cmc['input_CLR_file'])
        self.debug_logger("AddColormap_management result", color_map_result.status)              
    
    def _copyRaster(self, trmm_raster):
        
        crc = self.copy_raster_config
        copy_result = arcpy.CopyRaster_management(
            trmm_raster, 
            self.raster_catalog.fullpath, 
            crc.get('config_keyword',''), 
            crc.get('background_value',''), 
            crc.get('nodata_value',''), 
            crc.get('onebit_to_eightbit',''), 
            crc.get('colormap_to_RGB',''), 
            crc.get('pixel_type','')
        )
        self.debug_logger("CopyRaster_management status",copy_result.status)
