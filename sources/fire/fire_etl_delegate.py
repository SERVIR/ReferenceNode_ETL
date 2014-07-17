# Developer: SpatialDev
# Company:   Spatial Development International

# ETL framework
from etl_delegate import FTPETLDelegate
from etl_data import FTPETLData


class FireETLDelegate(FTPETLDelegate):
    
    """
        Class FireETLDelegate overrides FTPETLDelegate._createFTPETLDataToProcessList 
        in order to set the meta-data reference for each FTPETLData.
    """
                
    def __init__(self, etl_config):
        FTPETLDelegate.__init__(self, etl_config)
        
    def _createFTPETLDataToProcessList(self, validated_fire_granules_list, ftp_directory):
        
        fire_etl_data_to_process = []
        meta_extn = self.etl_config['ftp_file_meta_extn']
        
        for fire_points_csv in validated_fire_granules_list:  

            fire_data = FTPETLData()
            fire_data.setETLDataName(fire_points_csv)            
            fire_data.setFTPDirectory(ftp_directory) # Note: The FTP directory is explicitly set.
            fire_data.setDataToExtract(fire_points_csv)     
            fire_data.setMetaDataToExtract("%s.%s" % (fire_points_csv, meta_extn))
            fire_etl_data_to_process.append(fire_data)
            
        return fire_etl_data_to_process