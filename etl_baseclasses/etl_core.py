# Developer: SpatialDev
# Company:   Spatial Development International

# This module contains the abstract classes and their methods (interface) for the SERVIR ETL framework.


class ExtractorValidator(object):
    """
        An ExtractValidator is responsible for determining what data to retrieve from the source 
        based on the current ETL run requirements and constraints.
    """
    def validateExtract(self): pass


class Extractor(object):
    """
        An Extractor is responsible for retrieving the data from the source.
    """
    def getDataToExtract(self): return []
    def extract(self, etl_data): pass


class Transformer(object):
    
    """
        A Transformer is responsible for conforming the data into a pre-defined standard format, 
        or type that meets the technical and business requirements for an ETL data source.
    """
    def transform(self, etl_data): pass


class Loader(object):
    """
        A Loader is responsible for the transferring of data and meta-data to a location where it will ultimately be persisted or saved.
    """
    def load(self, etl_data): pass