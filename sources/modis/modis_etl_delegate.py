# Developer: SpatialDev
# Company:   Spatial Development International

# ETL framework
from etl_delegate import ETLDelegate
from etl_data import ETLData


class MODISETLDelegate(ETLDelegate):
        
    """
        Class MODISETLDelegate overrides ETLDelegate._createETLDataToProcessList 
        in order to set the meta-data reference for each ETLData.
    """
                
    def __init__(self, etl_config):
        ETLDelegate.__init__(self, etl_config)
        
    def _createETLDataToProcessList(self, missing_modis_images):
        
        modis_etl_data_to_process = []
        base_url = self.etl_config['url']
        extn = self.etl_config['extn']
        meta_extn = self.etl_config['meta_extn']
        
        for modis_image in missing_modis_images:
                
            modis_data = ETLData()
            modis_data.setETLDataName(modis_image)            
            modis_data.setDataToExtract(base_url + modis_image)
            modis_data.setMetaDataToExtract(base_url + modis_image.replace(extn, meta_extn))
            modis_etl_data_to_process.append(modis_data)
                
        return modis_etl_data_to_process