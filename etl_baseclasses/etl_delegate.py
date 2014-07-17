# Developer: SpatialDev
# Company:   Spatial Development International


# ETL framework
from etl_core import Extractor, Transformer, Loader, ExtractorValidator
from etl_data import ETLData, FTPETLData


class ETLDelegate(object):
    
    """
        An ETLDelegate is composed of various objects that contain the logic and tools required to process each ETLData. 
        It is responsible for wrapping the correct Object.method so that any call to the wrapped function will execute 
        the procedures associated with its abstraction.
        
        Key Responsibilites:
        
            -    The ETL process is initiated by calling ETLDelegate.startETLProcess().
            -    An ETLDelegate instantiates ETLData for the entire system by calling its method getETLDataToProcess().
            -    An ETLDelegate is primarily composed of instances from classes inside the developer defined modules and 
                 an ETLController which manages the processing of ETLData.
            -    In summary, an ETLDelegate asks its composite objects for the data to process, then passes that list as well 
                 as the functions to map to each ETLData in that list to an ETLController which iterates and executes over each 
                 core operation (abstraction).
    """
    
    def __init__(self, etl_config):
        
        self.etl_config = etl_config
        self.etl_controller = None
        
        self.extract_validator = ExtractorValidator()
        self.extractor = Extractor()
        self.transformer = Transformer()
        self.loader = Loader()
                
        self.has_flagged_etl_data = False
        self.no_new_etl_data_was_processed = True
        self.all_or_none_for_success = etl_config.get('all_or_none_for_success', False)
        
        self.exception_handler = etl_config.get('exception_handler',lambda*args,**kwargs:None)
        self.debug_logger = etl_config.get('debug_logger',lambda*args,**kwargs:None)
        
    def setExtractor(self, extractor):
        self.extractor = extractor
        
    def setExtractValidator(self, extract_validator):
        self.extract_validator = extract_validator
        
    def setTransformer(self, transformer):
        self.transformer = transformer
        
    def setLoader(self, loader):
        self.loader = loader
        
    def setETLController(self, etl_controller):
        self.etl_controller = etl_controller
                
    def startETLProcess(self):
                
        self.debug_logger("==================== starting ETL process ====================")
        self.etl_controller.startETLProcess()
        self._manageETLProcess()        
        self.etl_controller.finishETLProcess()
        
        is_successful_new_run = False if (self.has_flagged_etl_data and self.all_or_none_for_success) or self.no_new_etl_data_was_processed else True
        self.debug_logger("is_successful_new_run",is_successful_new_run)
        self.debug_logger("==================== finished ETL process ====================")
        
        return is_successful_new_run
    
    def _manageETLProcess(self):
        
        etl_data_to_process = self._getETLDataToProcess()
        etl_functions_dict = {'extract':self.extract,'transform':self.transform,'load':self.load}
        self._processETLData(etl_data_to_process, etl_functions_dict)
    
    def _getETLDataToProcess(self):
        
        all_data_to_process_list =  self.extractor.getDataToExtract()            
        validated_data_to_process_list = self.extract_validator.validateExtract(all_data_to_process_list)
        etl_data_to_process_list = self._createETLDataToProcessList(validated_data_to_process_list)
                
        return etl_data_to_process_list
    
    def _createETLDataToProcessList(self, validated_files_list):
                
        etl_data_to_process = []        
        for file_to_process in validated_files_list:
                      
            etl_data = ETLData()
            etl_data.setETLDataName(file_to_process)    
            etl_data.setDataToExtract(file_to_process)
            etl_data_to_process.append(etl_data)
            
        return etl_data_to_process
    
    def _processETLData(self, etl_data_to_process, etl_functions_dict):
        
        number_of_etl_data_to_process = len(etl_data_to_process)
        
        if number_of_etl_data_to_process == 0:
            self.debug_logger("No New ETLData To Process")
            
        elif number_of_etl_data_to_process > 0: 
            self.debug_logger("number_of_etl_data_to_process",number_of_etl_data_to_process)
            
            self.no_new_etl_data_was_processed = False
            self.etl_controller.processETLData(etl_data_to_process, etl_functions_dict)        
        
    def extract(self, etl_data):
        
        self._execute(self.extractor.extract, etl_data, "EXTRACT")
        return None if self._ETLDataIsFlagged(etl_data) else etl_data
                             
    def transform(self, etl_data):
        
        self._execute(self.transformer.transform, etl_data, "TRANSFORM")
        return None if self._ETLDataIsFlagged(etl_data) else etl_data
                 
    def load(self, etl_data):
        
        self._execute(self.loader.load, etl_data, "LOAD")
        return None if self._ETLDataIsFlagged(etl_data) else etl_data

    def _execute(self, etl_function, etl_data, etl_operation_name=""):
        
        self.debug_logger("-------------------- "+etl_operation_name+" --------------------")
        if not self._ETLDataIsFlagged(etl_data):
            etl_function(etl_data)
    
    def _ETLDataIsFlagged(self, etl_data):

        if etl_data.isFlaggedForRemoval():
            self._handleFlaggedETLData(etl_data)
            return True
        return False
    
    def _handleFlaggedETLData(self, etl_data):
        
        self.has_flagged_etl_data = True
        if etl_data.hasEncounteredAnException():
            self.exception_handler(etl_data.getExceptionReport())

      
class FTPETLDelegate(ETLDelegate):
    
    
    def __init__(self, etl_config):
        ETLDelegate.__init__(self, etl_config)
    
    def _manageETLProcess(self):
                                
        etl_functions_dict = {'extract':self.extract,'transform':self.transform,'load':self.load}
        
        for ftp_directory in self.etl_config['ftp_dirs']:
            self.debug_logger("processing FTP directory",ftp_directory)
            
            etl_data_to_process = self._getETLDataToProcess(ftp_directory)
            self._processETLData(etl_data_to_process, etl_functions_dict)
            
    def _getETLDataToProcess(self, ftp_directory):
        
        all_data_to_process_list =  self.extractor.getDataToExtract(ftp_directory)            
        validated_data_to_process_list = self.extract_validator.validateExtract(all_data_to_process_list)
        ftp_etl_data_to_process_list = self._createFTPETLDataToProcessList(validated_data_to_process_list, ftp_directory)
                
        return ftp_etl_data_to_process_list
    
    def _createFTPETLDataToProcessList(self, validated_files_list, ftp_directory):
                
        ftp_etl_data_to_process = []        
        for file_to_process in validated_files_list:
                      
            ftp_etl_data = FTPETLData()
            ftp_etl_data.setFTPDirectory(ftp_directory)
            ftp_etl_data.setETLDataName(file_to_process)    
            ftp_etl_data.setDataToExtract(file_to_process)
            ftp_etl_data_to_process.append(ftp_etl_data)
                        
        return ftp_etl_data_to_process