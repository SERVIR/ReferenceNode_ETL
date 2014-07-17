# Developer: SpatialDev
# Company:   Spatial Development International

# standard library
from os import path, makedirs
from shutil import rmtree


class ETLController(object):

    def __init__(self, etl_project_basepath, etl_project_name, etl_controller_config):
        
        """
            An ETLController is responsible for the movement and execution of ETLData. 
            It is passed a list of functions and a list of ETLData objects to map to each function by an ETLDelegate. 
            The primary advantage of this implementation is to separate the movement and execution of ETLData from any 
            single procedural implementation and to allow for various ways to process the list of ETLData (single, batch, ect).
            
            Aside from executing and iterating over ETLData, an ETLController also creates and manages the output directory on 
            disk (staging areas) for any files downloaded and/or created from each ETLData instance. Before an ETLData object is processed, 
            the ETLController sets its output directory properties. These properties are then referenced in the Extractor, Transformer, Loader 
            classes via the ETLData object.            
        """
        
        self.etl_project_fullpath = path.join(etl_project_basepath, etl_project_name)
        self.extract_dir = etl_controller_config.get('extract_dir', path.join(self.etl_project_fullpath, "extract"))
        self.transform_dir = etl_controller_config.get('transform_dir', path.join(self.etl_project_fullpath, "transform"))
        self.load_dir = etl_controller_config.get('load_dir', path.join(self.etl_project_fullpath, "load"))
                
        self.remove_etl_workspace_on_finish = etl_controller_config.get('remove_etl_workspace_on_finish', True)
        self.remove_extract_workspace_on_finish = etl_controller_config.get('remove_extract_workspace_on_finish', False)
        self.remove_transform_workspace_on_finish = etl_controller_config.get('remove_transform_workspace_on_finish', False)
        self.remove_load_workspace_on_finish = etl_controller_config.get('remove_load_workspace_on_finish', False)
        
        self.debug_logger = etl_controller_config.get('debug_logger',lambda*args,**kwargs:None)
        
    def startETLProcess(self):
        self._createWorkspaceDirectory()
                                             
    def _createWorkspaceDirectory(self):
        
        self._createDirectory(self.etl_project_fullpath)
        self._createDirectory(self.extract_dir)
        self._createDirectory(self.transform_dir)
        self._createDirectory(self.load_dir)

    def processETLData(self, etl_data_to_process, etl_functions_dict):
        
        number_of_etl_data_remaining = len(etl_data_to_process)
        for etl_data in etl_data_to_process:
            self._setETLDataProperties(etl_data)
            for etl_function in [etl_functions_dict['extract'], etl_functions_dict['transform'], etl_functions_dict['load']]:
                etl_data = etl_function(etl_data)
                if etl_data == None:
                    break
            number_of_etl_data_remaining -=1
            self.debug_logger("number_of_etl_data_remaining: ", number_of_etl_data_remaining)
            
    def _setETLDataProperties(self, etl_data):
        
        etl_data.setExtractDir(self.extract_dir)
        etl_data.setTransformDir(self.transform_dir)
        etl_data.setLoadDir(self.load_dir)
        
    def finishETLProcess(self):
        self._removeWorkspaceDirectory()
        
    def _removeWorkspaceDirectory(self):
        
        if self.remove_etl_workspace_on_finish:
            self._removeDirectory(self.etl_project_fullpath)
        else:
            if self.remove_extract_workspace_on_finish:
                self._removeDirectory(self.extract_dir)
            if self.remove_transform_workspace_on_finish:
                self._removeDirectory(self.transform_dir)                
            if self.remove_load_workspace_on_finish:
                self._removeDirectory(self.load_dir)
                
    def _createDirectory(self, dir_to_create):
        if not path.isdir(dir_to_create):
            makedirs(dir_to_create)
    
    def _removeDirectory(self, dir_to_remove):        
        if path.exists(dir_to_remove):
            rmtree(dir_to_remove)
            

class BatchETLController(ETLController):
    
    def __init__(self,  etl_project_basepath, etl_project_name, etl_controller_config):
        ETLController.__init__(self, etl_project_basepath, etl_project_name, etl_controller_config)
        
    def processETLData(self, etl_data_to_process, etl_functions_dict):
                
        for etl_data in etl_data_to_process:
            self._setETLDataProperties(etl_data)
            
        for etl_data in etl_data_to_process:            
            etl_functions_dict['extract'](etl_data)
                
        for etl_data in etl_data_to_process:
            etl_functions_dict['transform'](etl_data)
            
        for etl_data in etl_data_to_process:
            etl_functions_dict['load'](etl_data)
   
         
class BatchExtractController(ETLController):
    
    def __init__(self,  etl_project_basepath, etl_project_name, etl_controller_config):
        ETLController.__init__(self, etl_project_basepath, etl_project_name, etl_controller_config)
        
    def processETLData(self, etl_data_to_process, etl_functions_dict):
        
        for etl_data in etl_data_to_process:  
            self._setETLDataProperties(etl_data)
            etl_functions_dict['extract'](etl_data)
                    
        for etl_data in etl_data_to_process:
            for etl_function in [etl_functions_dict['transform'], etl_functions_dict['load']]:
                etl_data = etl_function(etl_data)
                if etl_data == None:
                    break


class BatchLoadController(ETLController):
    
    def __init__(self,  etl_project_basepath, etl_project_name, etl_controller_config):
        ETLController.__init__(self, etl_project_basepath, etl_project_name, etl_controller_config)
        
    def processETLData(self, etl_data_to_process, etl_functions_dict):
        
        for etl_data in etl_data_to_process:
            self._setETLDataProperties(etl_data)
            for etl_function in [etl_functions_dict['extract'], etl_functions_dict['transform']]:
                etl_data = etl_function(etl_data)
                if etl_data == None:
                    break
                
        for etl_data in etl_data_to_process:
            etl_functions_dict['load'](etl_data)