# Developer: SpatialDev
# Company:   Spatial Development International


# ETL framework
from etl_data import FTPETLData


class LandETLData(FTPETLData):
    
    """
        LandETLData overrides ETLData so that the additional property granule_list can be
        shared between Transformers. This property is retrieved from the meta-data and 
        is used to determine the position and name of each raster in the HDF.
    """

    def __init__(self):
        FTPETLData.__init__(self)
        
        self.granule_list = []
    
    def setGranuleList(self, granule_list):
        self.granule_list = granule_list
    
    def getGranuleList(self):
        return self.granule_list
