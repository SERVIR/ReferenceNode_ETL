# Developer: SpatialDev
# Company:   Spatial Development International


class ETLData(object):
    
    """
        The responsibility of an ETLData object is to encapsulates all references to files and/or objects associated 
        with an ETL sources single defined unit of data. An ETLData is the only object passed amongst the framework components.
        
        For different data sources, a single defined unit of data may be a raster, CSV, or rows from a database table. 
        Properties and behaviors can be added to an ETLData through sub-classing to account for specific data source constraints and requirements.
    """
    
    def __init__(self):
        
        self.etl_data_name = ""
         
        self.data_to_extract = None
        self.data_to_transform = None
        self.data_to_load = None
        
        self.meta_data_to_extract = ""
        self.meta_data_to_transform = ""
        self.meta_data_to_load = {}
        
        self.extract_dir = ""
        self.transform_dir = ""
        self.load_dir = ""
        
        self.is_flagged_for_removal = False
        self.has_encountered_an_exception = False        
        self.exception_report = {}
        
    def setETLDataName(self, etl_data_name):
        self.etl_data_name = etl_data_name
        
    def getETLDataName(self):
        return self.etl_data_name
    
    def setDataToExtract(self, data_to_extract):
        self.data_to_extract = data_to_extract
    
    def getDataToExtract(self):
        return self.data_to_extract
    
    def setDataToTransform(self, data_to_transform):
        self.data_to_transform = data_to_transform
    
    def getDataToTransform(self):
        return self.data_to_transform
    
    def setDataToLoad(self, data_to_load):
        self.data_to_load = data_to_load
    
    def getDataToLoad(self):
        return self.data_to_load
    
    def setMetaDataToExtract(self, meta_data_to_extract):
        self.meta_data_to_extract = meta_data_to_extract
        
    def getMetaDataToExtract(self):
        return self.meta_data_to_extract
    
    def setMetaDataToTransform(self, meta_data_to_transform):
        self.meta_data_to_transform = meta_data_to_transform
        
    def getMetaDataToTransform(self):
        return self.meta_data_to_transform
    
    def setMetaDataToLoad(self, meta_data_to_load):
        self.meta_data_to_load = meta_data_to_load
    
    def getMetaDataToLoad(self):
        return self.meta_data_to_load
          
    def setExtractDir(self, extract_dir):
        self.extract_dir = extract_dir
    
    def getExtractDir(self):
        return self.extract_dir
    
    def setTransformDir(self, transform_dir):
        self.transform_dir = transform_dir
    
    def getTransformDir(self):
        return self.transform_dir
    
    def setLoadDir(self, load_dir):
        self.load_dir = load_dir
    
    def getLoadDir(self):
        return self.load_dir
    
    def flagForRemoval(self):
        self.is_flagged_for_removal = True
        
    def hasEncounteredAnException(self):
        return self.has_encountered_an_exception
            
    def isFlaggedForRemoval(self):
        return self.is_flagged_for_removal
    
    def handleException(self, **exception_info_dict):
        
        self.has_encountered_an_exception = True
        self.flagForRemoval()
        self._createExceptionReport(exception_info_dict)
        
    def _createExceptionReport(self, exception_info_dict):
        
        self.exception_report['exception'] = exception_info_dict.get('exception','')
        self.exception_report['messages'] = exception_info_dict.get('messages','')
        self.exception_report['etl_data_name'] = self.etl_data_name
        self.exception_report['etl_data_properties'] = self.getETLDataProperties()
                    
    def getExceptionReport(self):
        return self.exception_report
            
    def getETLDataProperties(self):
        
        return  dict(etl_data_name=str(self.getETLDataName()),
                     extract_data=str(self.getDataToExtract()),
                     transform_data=str(self.getDataToTransform()),
                     load_data=str(self.getDataToLoad()),
                     meta_data_to_extract=str(self.getMetaDataToExtract()),
                     meta_data_to_transform=str(self.getMetaDataToTransform()),
                     meta_data_to_load=str(self.getMetaDataToLoad()))
            
    def __str__(self):
        return str(self.getETLDataName())


class FTPETLData(ETLData):
    
    def __init__(self):
        ETLData.__init__(self)
        
        self.ftp_directory = ""

    def setFTPDirectory(self, ftp_directory):
        self.ftp_directory = ftp_directory

    def getFTPDirectory(self):
        return self.ftp_directory