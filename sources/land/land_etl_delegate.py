# Developer: SpatialDev
# Company:   Spatial Development International

# ETL framework
from etl_delegate import FTPETLDelegate
from land_etl_data import LandETLData


class LandETLDelegate(FTPETLDelegate):
    
    """
        Class LandETLDelegate overrides FTPETLDelegate._createFTPETLDataToProcessList 
        in order to set the meta-data reference for each LandETLData
        
        Note: since the current implementation only procsses a single year.
        the FTP directory property of each object is NOT set.  
    """
                
    def __init__(self, etl_config):
        FTPETLDelegate.__init__(self, etl_config)
        
    def _createFTPETLDataToProcessList(self, validated_hdf_list, ftp_directory):
        
        land_etl_data_to_process_list = []
        meta_extn = self.etl_config['ftp_file_meta_extn']
        
        for hdf_file in validated_hdf_list:  

            land_data = LandETLData()
            land_data.setETLDataName(hdf_file)            
            land_data.setDataToExtract(hdf_file)  
            land_data.setMetaDataToExtract("%s.%s" % (hdf_file, meta_extn))
            land_etl_data_to_process_list.append(land_data)
            
        return land_etl_data_to_process_list
