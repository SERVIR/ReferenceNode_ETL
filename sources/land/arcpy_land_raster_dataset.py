# Developer: SpatialDev
# Company:   Spatial Development International

# third-party
import arcpy


class LandCoverRasterDataset:

    
    def __init__(self, output_dataset_fullpath, input_raster_catalog, options):
        
        self.output_dataset_fullpath = output_dataset_fullpath
        self.input_raster_catalog = input_raster_catalog
        self.options = options
        self.debug_logger = options.get('debug_logger', lambda*args,**kwargs:None)
                
    def createLandRasterDataset(self):
        
        try:
            self._createRasterDatasetFromRasterCatalog()
            self._addAndUpdateClassDescriptionFields()
            self._reprojectRasterDataset()
            
        except Exception as e:
            self.debug_logger("EXCEPTION:",e,arcpy.GetMessages(2))

    def _createRasterDatasetFromRasterCatalog(self):
        
        where_clause = str("\""+self.options['land_cover_type_field']+"\" = '"+self.options['land_cover_type']+"'")
        rcrd_config = self.options['RasterCatalogToRasterDataset_management_config']
        
        result = arcpy.RasterCatalogToRasterDataset_management(self.input_raster_catalog, self.output_dataset_fullpath, where_clause,
            rcrd_config.get('mosaic_type',''),
            rcrd_config.get('colormap',''),
            rcrd_config.get('order_by_field',''),
            rcrd_config.get('ascending',''),
            rcrd_config.get('Pixel_type',''),
            rcrd_config.get('ColorBalancing',''),
            rcrd_config.get('matchingMethod',''),
            rcrd_config.get('ReferenceRaster',''),
            rcrd_config.get('OID','')
        )
        self.debug_logger("RasterCatalogToRasterDataset_management status", result.status)

    def _addAndUpdateClassDescriptionFields(self):
        
        land_type_description_dict = self.options['land_type_description_dict']
        land_type_description_field = self.options['land_cover_description_field']
        land_type_value_field = self.options['land_type_value_field']
        
        try:
            result = arcpy.BuildRasterAttributeTable_management(self.output_dataset_fullpath)
            self.debug_logger("BuildRasterAttributeTable_management status", result.status)
            
            result = arcpy.AddField_management(self.output_dataset_fullpath, land_type_description_field, 'TEXT', '', '', 40)
            self.debug_logger("AddField_management status", result.status)
            
            self.debug_logger("updating rows...")
            rows = arcpy.UpdateCursor(self.output_dataset_fullpath)
            for row in rows:
                land_type_value = row.getValue(land_type_value_field)
                land_type_description = land_type_description_dict[land_type_value]                
                row.setValue(land_type_description_field, land_type_description)
                rows.updateRow(row)
                self.debug_logger("updated row ---->", land_type_value, land_type_description)
        finally:
            del rows
            
    def _reprojectRasterDataset(self):
        
        arcpy.env.extent = self.options['gp_env_extent']
        result = arcpy.ProjectRaster_management(self.output_dataset_fullpath, self.options['reprojected_raster_dataset'], self.options['out_coor_system'])
        self.debug_logger("ProjectRaster_management status", result.status)
