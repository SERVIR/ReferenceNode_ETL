# Developer: SpatialDev
# Company:   Spatial Development International

# standard library
from datetime import datetime
import os

# third-party
import arcpy

# ETL utils
from etl_utils import ETLXMLUtils, FTPDownloadManager


class LandExtractValidator(object):
    
    """
        Class LandExtractValidator determines which HDF files to process for the current ETL run.
        
        The high-level steps to accomplish this task include:
        
            1) retrieve all HDFs from a given FTP for a given year
            2) retrieve the current HDFs processed from the raster catalog for the given year
            3) subtract the two lists to find the difference in HDFs
            4) return a list containing only the HDFs to process for the current ETL run
    """
    
    def __init__(self, extract_validator_config):
        
        self.raster_catalog = extract_validator_config['raster_catalog']
        self.ftp_file_name_field = extract_validator_config['ftp_file_name_field']
        self.debug_logger = extract_validator_config.get('debug_logger',lambda*a,**kwa:None)
                
    def validateExtract(self, ftp_hdf_list):
        
        self.debug_logger("len(ftp_dir_list)",len(set(ftp_hdf_list)))
        
        # retrieve the year from an HDF file
        start_dt = datetime.strptime(str(ftp_hdf_list[0].split(".")[1][1:5]), '%Y') 
        self.debug_logger("Current Year",str(start_dt))
        
        current_hdf_list = self.raster_catalog.getValuesFromDatetimeRange(self.ftp_file_name_field, start_dt, start_dt)
        self.debug_logger("len(current_raster_list)",len(set(current_hdf_list)))
        
        missing_hdf_list = list(set(ftp_hdf_list) - set(current_hdf_list))
        self.debug_logger("len(missing_hdf_list)",len(set(missing_hdf_list)))
        
        # re-sort and reverse since the both lists were cast as sets
        missing_hdf_list.sort(key=lambda x:x,reverse=True)
        
        return missing_hdf_list


class LandExtractor(FTPDownloadManager):
    
    """
        Class LandExtractor:
        
            1) retrieves a list of all HDF files from a given FTP directory. (This list is sent to a LandExtractValidator)
            2) downloads an HDF file and its XML meta-data from the given FTP directory
    """
    
    def __init__(self, extractor_config):
        FTPDownloadManager.__init__(self, extractor_config['ftp_options'])

        self.target_file_extn = extractor_config.get('target_file_extn', None)
        self.debug_logger = extractor_config.get('debug_logger',lambda*a,**kwa:None)
        
    def getDataToExtract(self, ftp_directory):
        
        self.openConnection()
                
        return self.getFileNamesFromDirectory(ftp_directory, self.target_file_extn)
        
    def extract(self, land_data):
        
        try:
            extract_dir = land_data.getExtractDir()
            
            hdf_name = land_data.getDataToExtract()
            self.debug_logger("hdf_name",hdf_name)
    
            xml_name = land_data.getMetaDataToExtract()
            self.debug_logger("xml_name",xml_name)
                
            downloaded_hdf = self.downloadFileFromFTP(hdf_name, extract_dir)   
            self.debug_logger("downloaded_hdf",downloaded_hdf)
            
            downloaded_xml = self.downloadFileFromFTP(xml_name, extract_dir)
            self.debug_logger("downloaded_hdf",downloaded_hdf)
            
            land_data.setDataToTransform(downloaded_hdf)
            land_data.setMetaDataToTransform(downloaded_xml)
            
        except Exception as e:
            
            self.debug_logger("extract Exception:",str(e),str(arcpy.GetMessages(2)))
            land_data.handleException(exception=("extract:",str(e)),messages=arcpy.GetMessages(2))
            

class LandTransformer:
    
    """
        Class LandTransformer extracts the rasters from a given HDF.
        
        The high-level steps includes:
        
            1) recieve both an HDF file and a list of HDF raster names
            2) iterate through the list and use each list position (index) and name as input for arcpy.ExtractSubDataset_management
            3) extract each raster from the HDF
    """
    
    def __init__(self, transformer_config):
        
        self.output_file_geodatabase = transformer_config['output_file_geodatabase']
        self.debug_logger = transformer_config.get('debug_logger',lambda*a,**kwa:None)
            
    def transform(self, land_data):
        
        try:                    
            hdf_file = land_data.getDataToTransform()
            
            raster_subset_names = land_data.getGranuleList()
            self.debug_logger("raster_subset_names",raster_subset_names)
            
            extracted_rasters_list = self._extractRastersFromHDF(hdf_file, raster_subset_names)
            self.debug_logger("extracted_rasters_list",extracted_rasters_list)
            
            land_data.setDataToLoad(extracted_rasters_list)
            
        except Exception as e:
            
            self.debug_logger("transform Exception:",str(e),str(arcpy.GetMessages(2)))
            land_data.handleException(exception=("transform:",str(e)),messages=arcpy.GetMessages(2))
            
    def _extractRastersFromHDF(self, hdf_file, raster_subset_names):

        extracted_rasters_list = []
        raster_subset_names = raster_subset_names[2:] # remove the first two items since they are not rasters in the HDF
        output_basepath = self.output_file_geodatabase
        arcpy.env.overwriteOutput = True
        
        joinPath = os.path.join
        extractSubset = arcpy.ExtractSubDataset_management
        
        for i, subset in enumerate(raster_subset_names):
            self.debug_logger("processing index and subset:", subset, i)
            
            land_cover_type = str(subset.split(".")[0])
            self.debug_logger("land-cover type", land_cover_type)
            
            raster_to_extract = joinPath(output_basepath, str("_".join(subset.split(".")[:3])))
            self.debug_logger("raster_to_extract", raster_to_extract)
            
            extract_result = extractSubset(hdf_file, raster_to_extract, i)
            self.debug_logger("ExtractSubDataset_management result", extract_result.status)
            
            raster_dict = {"extracted_raster":raster_to_extract,"land_cover_type":land_cover_type}
            extracted_rasters_list.append(raster_dict)

        return extracted_rasters_list


class LandMetaDataTransformer(object):
    
    """
        Class LandMetaDataTransformer:
        
            1) transforms the HDFs XML meta-data into a dictionary
            2) retrieves the list of raster names and positions that correspond to the contents of each HDF
            3) passes the Transform call to the given LandTransformer (decoratee)
        
        The high-level steps for task 1) include:
        
            1) recieve an HDFs meta-data XML file
            2) convert the XML into a dictionary
            3) flatten the dictionary to be one-level deep
    """
    
    def __init__(self, meta_data_transformer_config, decoratee):
        
        self.debug_logger = meta_data_transformer_config.get('debug_logger',lambda*a,**kwa:None)
        self.decoratee = decoratee # this is a LandTransformer
        
    def transform(self, land_data):
        
        try:            
            xml_file = land_data.getMetaDataToTransform()
            raw_meta_data_dict = ETLXMLUtils.getDictFromXML(xml_file)
            
            # _createLandDict flattens the nested raw_meta_data_dict into a single level dictionary
            meta_data_dict = self._createLandDict(raw_meta_data_dict) 
            self.debug_logger("len(meta_data_dict)", len(meta_data_dict))
            
            self._addCustomFields(meta_data_dict)
            
            # granule_list is used in LandTransformer to associate the name and index to each raster in an HDF
            granule_list = meta_data_dict['InputPointer'].split(",")
                        
            land_data.setGranuleList(granule_list) # set granule_list to be retrieved in LandTransformer
            land_data.setMetaDataToLoad(meta_data_dict)
            
            self.decoratee.transform(land_data) # this calls LandTransformer.transform()
            
        except Exception as e:
            
            self.debug_logger("transformMetaData Exception:",str(e),str(arcpy.GetMessages(2)))
            land_data.handleException(exception=("transformMetaData:",str(e)),messages=arcpy.GetMessages(2))
            
    def _addCustomFields(self, meta_data_dict):
        
        # add datetime field values
        meta_data_dict['datetime'] = datetime.strptime(meta_data_dict['RangeBeginningDate'],"%Y-%m-%d")
        meta_data_dict['datetime_string'] = str(datetime.strptime(meta_data_dict['RangeBeginningDate'],"%Y-%m-%d").strftime('%m-%d-%Y %I:%M:%S %p'))
        
    def _createLandDict(self, raw_meta_data_dict):
        
        DataCenterId = raw_meta_data_dict['DataCenterId']
        DTDVersion = raw_meta_data_dict['DTDVersion'] 
        GranuleUR = raw_meta_data_dict['GranuleURMetaData']['GranuleUR']
        DbID = raw_meta_data_dict['GranuleURMetaData']['DbID']
        InsertTime = raw_meta_data_dict['GranuleURMetaData']['InsertTime']
        LastUpdate = raw_meta_data_dict['GranuleURMetaData']['LastUpdate']        
        ShortName = raw_meta_data_dict['GranuleURMetaData']['CollectionMetaData']['ShortName']
        VersionID = raw_meta_data_dict['GranuleURMetaData']['CollectionMetaData']['VersionID']
        DistributedFileName = raw_meta_data_dict['GranuleURMetaData']['DataFiles']['DataFileContainer']['DistributedFileName']
        FileSize  = raw_meta_data_dict['GranuleURMetaData']['DataFiles']['DataFileContainer']['FileSize']
        ChecksumType = raw_meta_data_dict['GranuleURMetaData']['DataFiles']['DataFileContainer']['ChecksumType']
        Checksum = raw_meta_data_dict['GranuleURMetaData']['DataFiles']['DataFileContainer']['Checksum']
        ChecksumOrigin = raw_meta_data_dict['GranuleURMetaData']['DataFiles']['DataFileContainer']['ChecksumOrigin']
        SizeMBECSDataGranule = raw_meta_data_dict['GranuleURMetaData']['ECSDataGranule']['SizeMBECSDataGranule']
        ReprocessingPlanned = raw_meta_data_dict['GranuleURMetaData']['ECSDataGranule']['ReprocessingPlanned']
        ReprocessingActual = raw_meta_data_dict['GranuleURMetaData']['ECSDataGranule']['ReprocessingActual']
        LocalGranuleID = raw_meta_data_dict['GranuleURMetaData']['ECSDataGranule']['LocalGranuleID']
        DayNightFlag = raw_meta_data_dict['GranuleURMetaData']['ECSDataGranule']['DayNightFlag']
        ProductionDateTime = raw_meta_data_dict['GranuleURMetaData']['ECSDataGranule']['ProductionDateTime']
        LocalVersionID = raw_meta_data_dict['GranuleURMetaData']['ECSDataGranule']['LocalVersionID']
        PGEVersion = raw_meta_data_dict['GranuleURMetaData']['PGEVersionClass']['PGEVersion']
        RangeEndingTime = raw_meta_data_dict['GranuleURMetaData']['RangeDateTime']['RangeEndingTime']
        RangeEndingDate = raw_meta_data_dict['GranuleURMetaData']['RangeDateTime']['RangeEndingDate']
        RangeBeginningTime = raw_meta_data_dict['GranuleURMetaData']['RangeDateTime']['RangeBeginningTime']
        RangeBeginningDate = raw_meta_data_dict['GranuleURMetaData']['RangeDateTime']['RangeBeginningDate']            
        ParameterName = raw_meta_data_dict['GranuleURMetaData']['MeasuredParameter']['MeasuredParameterContainer']['ParameterName']
        QAPercentMissingData = raw_meta_data_dict['GranuleURMetaData']['MeasuredParameter']['MeasuredParameterContainer']['QAStats'].get('QAPercentMissingData','')
        QAPercentOutofBoundsData = raw_meta_data_dict['GranuleURMetaData']['MeasuredParameter']['MeasuredParameterContainer']['QAStats'].get('QAPercentOutofBoundsData','')
        QAPercentInterpolatedData = raw_meta_data_dict['GranuleURMetaData']['MeasuredParameter']['MeasuredParameterContainer']['QAStats'].get('QAPercentInterpolatedData','')
        AutomaticQualityFlag = raw_meta_data_dict['GranuleURMetaData']['MeasuredParameter']['MeasuredParameterContainer']['QAFlags'].get('AutomaticQualityFlag','')
        AutomaticQualityFlagExplanation = raw_meta_data_dict['GranuleURMetaData']['MeasuredParameter']['MeasuredParameterContainer']['QAFlags'].get('AutomaticQualityFlagExplanation','')
        OperationalQualityFlag = raw_meta_data_dict['GranuleURMetaData']['MeasuredParameter']['MeasuredParameterContainer']['QAFlags'].get('OperationalQualityFlag','')
        OperationalQualityFlagExplanation = raw_meta_data_dict['GranuleURMetaData']['MeasuredParameter']['MeasuredParameterContainer']['QAFlags'].get('OperationalQualityFlagExplanation','')
        ScienceQualityFlag = raw_meta_data_dict['GranuleURMetaData']['MeasuredParameter']['MeasuredParameterContainer']['QAFlags'].get('ScienceQualityFlag','')
        ScienceQualityFlagExplanation = raw_meta_data_dict['GranuleURMetaData']['MeasuredParameter']['MeasuredParameterContainer']['QAFlags'].get('ScienceQualityFlagExplanation','')
        
        Point = raw_meta_data_dict['GranuleURMetaData']['SpatialDomainContainer']['HorizontalSpatialDomainContainer']['GPolygon']['Boundary']['Point']
        l1 = Point[0]['PointLongitude']+","+Point[0]['PointLatitude']
        l2 = Point[1]['PointLongitude']+","+Point[1]['PointLatitude']
        l3 = Point[2]['PointLongitude']+","+Point[2]['PointLatitude']
        l4 = Point[3]['PointLongitude']+","+Point[3]['PointLatitude']
                
        PlatformShortName = ""
        SensorShortName = ""
        InstrumentShortName = ""
        for p in raw_meta_data_dict['GranuleURMetaData']['Platform']:
            PlatformShortName += p['PlatformShortName']+","
            SensorShortName += p['Instrument']['Sensor']['SensorShortName']+","
            InstrumentShortName += p['Instrument']['InstrumentShortName']+","
        PlatformShortName = PlatformShortName.rstrip(",")
        SensorShortName = SensorShortName.rstrip(",")
        InstrumentShortName = InstrumentShortName.rstrip(",")
                
        PSAName = ""
        PSAValue = ""
        for psa_pair in raw_meta_data_dict['GranuleURMetaData']['PSAs']['PSA']:
            PSAName += psa_pair['PSAName']+","
            PSAValue += psa_pair['PSAValue']+","
        PSAName = PSAName.rstrip(",")
        PSAValue = PSAValue.rstrip(",")
                
        inputPointers = ""
        for ip in raw_meta_data_dict['GranuleURMetaData']['InputGranule']['InputPointer']:
            inputPointers += (ip + ",")
        inputPointers = inputPointers.rstrip(",")
                
        return {
                        
            'DTDVersion':DTDVersion,'DataCenterId':DataCenterId,'GranuleUR':GranuleUR,'DbID':DbID,'InsertTime':InsertTime,'LastUpdate':LastUpdate,
            'ShortName':ShortName,'VersionID':VersionID, 'DistributedFileName':DistributedFileName,'FileSize':FileSize, 'ChecksumType':ChecksumType,
            'Checksum':Checksum,'ChecksumOrigin':ChecksumOrigin,'SizeMBECSDataGranule':SizeMBECSDataGranule,'ReprocessingPlanned':ReprocessingPlanned,
            'ReprocessingActual':ReprocessingActual,'LocalGranuleID':LocalGranuleID,'DayNightFlag':DayNightFlag,'ProductionDateTime':ProductionDateTime,
            'LocalVersionID':LocalVersionID,'PGEVersion':PGEVersion,'RangeEndingTime':RangeEndingTime,'RangeEndingDate':RangeEndingDate,'l1':l1,'l2':l2,
            'l3':l3, 'l4':l4,'RangeBeginningTime':RangeBeginningTime,'RangeBeginningDate':RangeBeginningDate,'ParameterName':ParameterName,'InputPointer':inputPointers,
            'QAPercentMissingData':QAPercentMissingData,'QAPercentOutofBoundsData':QAPercentOutofBoundsData,'QAPercentInterpolatedData':QAPercentInterpolatedData,                       
            'AutomaticQualityFlag':AutomaticQualityFlag,'AutomaticQualityFlagExplanation':AutomaticQualityFlagExplanation,'OperationalQualityFlag':OperationalQualityFlag,
            'OperationalQualityFlagExplanation':OperationalQualityFlagExplanation,'ScienceQualityFlag':ScienceQualityFlag,'ScienceQualityFlagExplanation':ScienceQualityFlagExplanation,
            'PlatformShortName':PlatformShortName,'SensorShortName':SensorShortName,'InstrumentShortName':InstrumentShortName,'PSAValue':PSAValue,'PSAName':PSAName
        }


class LandLoader(object):
    
    """
        Class LandLoader:
        
            1)    inserts each extracted raster from an HDF into the given raster catalog
            2)    updates the fields asscoiated with each inserted raster in the given raster catalog
    """
    
    def __init__(self, loader_config):
        
        self.raster_catalog = loader_config['raster_catalog']
        self.copy_raster_config = loader_config.get('CopyRaster_management_config',{})
        self.debug_logger = loader_config.get('debug_logger',lambda*a,**kwa:None)
                        
    def load(self, land_data):
        
        try:
            extracted_rasters_list = land_data.getDataToLoad()
            self.debug_logger("len(extracted_rasters_list)",len(extracted_rasters_list))
            
            meta_data_dict = land_data.getMetaDataToLoad()
            self.debug_logger("len(meta_data_dict)",len(meta_data_dict))
            
            crc = self.copy_raster_config
            raster_catalog = self.raster_catalog
            
            # loop through the extracted rasters for a single HDF and insert them into the raster catalog
            for raster_dict in extracted_rasters_list:
                
                extracted_raster_fullpath = raster_dict['extracted_raster']
                extracted_raster_name = os.path.basename(extracted_raster_fullpath)
                land_cover_type = raster_dict['land_cover_type']
                self.debug_logger("loading raster", extracted_raster_name)
                                
                result = arcpy.CopyRaster_management(
                    extracted_raster_fullpath, 
                    raster_catalog.fullpath, 
                    crc['config_keyword'], 
                    crc['background_value'], 
                    crc['nodata_value'], 
                    crc['onebit_to_eightbit'], 
                    crc['colormap_to_RGB'], 
                    crc['pixel_type']
                ) 
                self.debug_logger("CopyRaster_management status result", result.status)
                
                # explicity set the type into the meta-dict before it is loaded
                meta_data_dict['land_cover_type'] = land_cover_type
                self.debug_logger("Land-Cover Type", land_cover_type)
                
                # only update the fields associated with the extracted raster with the given meta-data
                where_clause = "Name = \'%s\'" % (extracted_raster_name)
                
                raster_catalog.updateFields(meta_data_dict, {'where_clause': where_clause})
                
        except Exception as e:
             
            self.debug_logger("load Exception:",str(e),str(arcpy.GetMessages(2)))
            land_data.handleException(exception=("load:",str(e)),messages=arcpy.GetMessages(2))