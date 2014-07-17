# Developer: SpatialDev
# Company:   Spatial Development International

# standard library
from datetime import datetime, timedelta
import os
import httplib
import urllib
import json

# third-party
import arcpy


class FileGeoDatabase(object):
    
    """
        Wrapper class for a file geodatabase.
    """
    
    def __init__(self, basepath, name, options=None):
        
        if not options:
            options = {}
                 
        self.name = name
        self.basepath = basepath
        self.version = options.get('version', "CURRENT")
        self.options = options
        self.fullpath = os.path.join(basepath, name)
        
        if not arcpy.Exists(self.fullpath):
            arcpy.CreateFileGDB_management(self.basepath, self.name, self.version)
            
    def compactFileGeoDatabase(self):
        
        compact_interval_days = self.options.get('compact_interval_days', None)
        compact_ready = compact_interval_days and datetime.utcnow().day % compact_interval_days == 0
        
        if compact_ready:
            arcpy.Compact_management(self.fullpath)
            
    def deleteRastersOutsideDatetimeRange(self, start_datetime, end_datetime, options):
        
        arcpy.env.workspace = self.fullpath
        raster_name_prefix = options['raster_name_prefix']
        raster_name_datetime_format = options['raster_name_datetime_format']
        raster_name_parser_function = options['raster_name_parser_function']
        raster_name_validator_function = options['raster_name_validator_function']
                
        local_raster_list = [r for r in arcpy.ListRasters(raster_name_prefix+"*","*") if raster_name_validator_function(r)]
        convertRasterToDatetimeObject = lambda r:datetime.strptime(raster_name_parser_function(str(r)), raster_name_datetime_format)
        rasterWithinDatetimeRange = lambda r: convertRasterToDatetimeObject(r) >= start_datetime or convertRasterToDatetimeObject(r) <= end_datetime
        list_of_rasters_to_delete = [r for r in local_raster_list if rasterWithinDatetimeRange(r)]
        
        for raster in list_of_rasters_to_delete:
            print "deleteing raster from FGDB:",raster
            arcpy.Delete_management(raster)

class ArcTableUtils(object):
    
    """
        Utility object that contains common arcpy.<cursor_type> operations and patterns.
    """
                    
    def updateFields(self, table_fullpath, fields_dict, options):
        
        """
            This method updates the fields for the given table_fullpath.
            
            arguments:
            
                table_fullpath <str>: fullpath to a given table object (raster catalog, feature class, table)
                fields_dict <dict>: contains key-value (field_name, field_value) pairs that will be used to udpate the given table_fullpath
                options <dict>: options for arcpy.UpdateCursor
        """
        rows = None
        try: 
            rows = arcpy.UpdateCursor(table_fullpath, options.get('where_clause',''), options.get('spatial_reference',''),options.get('fields',''), options.get('sort_fields',''))
            for row in rows:
                for field_name in fields_dict:
                    print "updating field...", str(field_name), fields_dict[field_name]
                    row.setValue(str(field_name), fields_dict[field_name])
                rows.updateRow(row)
        finally:
            del rows

    def deleteOutdatedRows(self, table_fullpath, archive_limit, date_column_name, datetime_field_format, datetime_sql_cast, start_datetime):
                
        """
            This method deletes all rows from the given table_fullpath that are outside of the given archive_limit.
            
            arguments:
            
                table_fullpath <str>: fullpath to a given table object (raster catalog, feature class, table)
                archive_limit <int>: the number of days minus todays date to delete from the given table_fullpath
                date_column_name <str>: the name of the column that contains the date values for the table_fullpath.
                datetime_field_format <str>: the format (ex: %Y%m%d%H') of the datetime values in the given date_column_name.
                datetime_sql_cast <str>: the datetime CAST operator associated with the underlying SQL type to use to create a datetime object 
                in the SQL WHERE clause.
        """
        
        archive_limit_date = (start_datetime - timedelta(days=int(archive_limit))).strftime(datetime_field_format)
        where_clause = "%s <= %s \'%s\'" % (date_column_name, datetime_sql_cast, archive_limit_date)
        where_clause += " OR %s >= %s \'%s\'" % (date_column_name, datetime_sql_cast, start_datetime.strftime(datetime_field_format))
        print "deleteOutdatedRows(): where_clause: ",where_clause
        
        self.deleteRows(table_fullpath, where_clause, date_column_name)
            
    def deleteRows(self, table_fullpath, where_clause, columns=""):
        
        rows = None
        try:
            rows = arcpy.UpdateCursor(table_fullpath, where_clause,"", columns)
            for row in rows:
                print "deleting row..."
                rows.deleteRow(row)
        finally:
            del rows
            
    def getRowsFromFields(self, table_fullpath, where_clause, field):
        
        ''' This method returns a rows object from the given fields '''
        rows = None
        try:
            rows = arcpy.SearchCursor(table_fullpath, where_clause, "", field, "")
            return rows
        finally:
            del rows
                    
    def getValuesFromField(self, table_fullpath, where_clause, field): 
        
        """
            This method returns the values from a given field.
            
            arguments:
            
                table_fullpath <str>: fullpath to a given table object (raster catalog, feature class, table)
                where_clause <str>: the conditions of the query
                field <str>: the column name in the table_fullpath to retireve the values from
        """
        
        rows = None
        try:
            rows = self.getRowsFromFields(table_fullpath, where_clause, field)
            return [str(row.getValue(field)) for row in rows]
        finally:
            del rows


class ArcTable(object):
    
    """
        Utility object that contains common operations and patterns performed on feature classes, raster catalogs, tables.
    """
    
    def __init__(self, out_path, out_name, options):
        
        self.name = out_name
        self.basepath = out_path
        self.fullpath = os.path.join(out_path, out_name)
        self.options = options
        self.arc_table_utils = ArcTableUtils()
        
    def delete(self):
        if arcpy.Exists(self.fullpath):
            arcpy.Delete_management(self.fullpath)
    
    def listFields(self):
        return arcpy.ListFields(self.fullpath)
                
    def deleteOutdatedRows(self, start_datetime=None):
        
        if not start_datetime:
            start_datetime = datetime.utcnow()
            
        self.arc_table_utils.deleteOutdatedRows(
            self.fullpath, self.options['archive_days'], self.options['datetime_field'], 
            self.options['datetime_field_format'], self.options['datetime_sql_cast'],start_datetime
        )
        
    def deleteRows(self, where_clause):
        self.arc_table_utils.deleteRows(self.fullpath, where_clause)
        
    def getValuesFromDatetimeRange(self, data_field, start_datetime, end_datetime, additional_where_clause=""): 
        
        """
            This method retrieves values from the given datetime range from the 'fullpath' associated with the object that is inheriting ArcTable.
            
            arguments:
            
                data_field <str>: the name of the field to retireve the values from 
                start_datetime <datetime.datetime>: retrieve all values before this date
                end_datetime <datetime.datetime>: retrieve all values after this date
                additional_where_clause <str>: optional conditions for the WHERE clause
        """

        start_datetime = start_datetime.strftime(self.options['datetime_field_format'])
        end_datetime = end_datetime.strftime(self.options['datetime_field_format'])
        datetime_sql_cast = self.options['datetime_sql_cast'] # this is important if the underlying SQL type changes
        datetime_field = self.options['datetime_field']
        
        where_clause = "%s <= %s \'%s\'" % (datetime_field, datetime_sql_cast, start_datetime)
        where_clause += " AND %s >= %s \'%s\'" % (datetime_field, datetime_sql_cast, end_datetime)
        where_clause += additional_where_clause # this is optional. It is available for specific queries that do not only contain a datetime range.
        print "where_clause",where_clause
        
        return self.arc_table_utils.getValuesFromField(self.fullpath, where_clause, data_field)
    
    def getRowsFromFields(self, fields, where_clause=""):
        
        return self.arc_table_utils.getRowsFromFields(self.fullpath, where_clause, fields)
    
    def getMaxValueFromField(self, field, where_clause=""):
        
        rows = None
        try:
            rows = self.arc_table_utils.getRowsFromFields(self.fullpath, where_clause, field)
            rows_list = [r.getValue(field) for r in rows]
            
            return max(rows_list) if (len(rows_list) > 0 and None not in rows_list) else None

        finally:
            del rows
        
    def updateFields(self, fields_dict, options):
        self.arc_table_utils.updateFields(self.fullpath, fields_dict, options)
        
    def updateFieldsForInput(self, input_name, fields_dict, input_field_name="Name"):
        
        """
            This method updates the fields for the given input_name associated with the given input_field_name (column name in table). 
            For example, if input_name was a raster then it will update the fields associated with the given raster with the given fields_dict.
        """
        
        where_clause = "%s = \'%s\'" % (input_field_name, input_name)
        self.arc_table_utils.updateFields(self.fullpath, fields_dict, {'where_clause':where_clause})
        

class FeatureClass(ArcTable):
    
    """
        Wrapper class for a feature class.
    """
    
    def __init__(self, out_path, out_name, options): 
        ArcTable.__init__(self, out_path, out_name, options)
        
        if not arcpy.Exists(self.fullpath):
            
            arcpy.CreateFeatureclass_management(out_path, out_name, 
                self.options.get('geometry_type',''), self.options.get('template',''),self.options.get('has_m',''), 
                self.options.get('has_z',''), self.options.get('spatial_reference',''), self.options.get('config_keyword', ''), 
                self.options.get('spatial_grid_1',''), self.options.get('spatial_grid_2',''), self.options.get('spatial_grid_3','')
            )

class RasterCatalog(ArcTable):
    
    """
        Wrapper class for a raster catalog.
    """
    
    def __init__(self, raster_catalog_basepath, raster_catalog_name, options):
        ArcTable.__init__(self, raster_catalog_basepath, raster_catalog_name, options)
        
        if not arcpy.Exists(self.fullpath):

            arcpy.CreateRasterCatalog_management(raster_catalog_basepath, raster_catalog_name, 
                options.get('raster_spatial_reference',''), options.get('spatial_reference',''), options.get('config_keyword',''), options.get('spatial_grid_1',''), 
                options.get('spatial_grid_2',''), options.get('spatial_grid_3',''), options.get('raster_management_type',''), options.get('template_raster_catalog','')
            )
            
            
class RasterMosaicDataset(ArcTable):
    
    """
        Wrapper class for a raster mosaic dataset
    """
    
    def __init__(self, output_basepath, dataset_name, coordinate_system, options):
        ArcTable.__init__(self, output_basepath, dataset_name, options)
        
        if not arcpy.Exists(self.fullpath):
            arcpy.CreateMosaicDataset_management(output_basepath, dataset_name, coordinate_system, options.get('num_bands'), options.get('pixel_type'))

           
class ArcGISServiceManager(object):
    
    """
        Class ArcGISServiceManager manages the starting and stopping of ArcGIS services
        
        public interface:
        
            stopServices()
            startServices()
            refreshServices()
    """

    def __init__(self, service_options_dict):
        
        self._server_name = service_options_dict['server_name']
        self._server_port = service_options_dict['server_port']
        self._username = service_options_dict['username']
        self._password = service_options_dict['password']        
        self._service_dir = service_options_dict['service_dir']
        self._services = service_options_dict['services']
        self.debug_logger = service_options_dict.get('debug_logger',lambda*a,**kwa:None)
        self._header = {"Content-type":"application/x-www-form-urlencoded", "Accept":"text/plain"}
        
    def _executeServiceAction(self, action):
        self.debug_logger("_executeServiceAction()")
        
        token = self._getToken()
        if not token:
            return
        
        try:
            http_conn = httplib.HTTPConnection(self._server_name, self._server_port)
            params = urllib.urlencode({'token':token,'f':'json'})
            
            for serivce_name in self._services:
                self.debug_logger("processing service", serivce_name)
                
                service_action_url = "/arcgis/admin/services/" + self._service_dir + "/" + serivce_name + "/" + action
                self.debug_logger("service_action_url",service_action_url)
                http_conn.request("POST", service_action_url, params, self._header)
                response = http_conn.getresponse()
                
                self._assertResponseSuccess(response.status, response.read(), 'success')
        
        except Exception as e:
            self.debug_logger("Exception: ",str(e))
            
        finally:
            http_conn.close()
            
    def _getToken(self):
        self.debug_logger("_getToken()")

        try: 
            params = urllib.urlencode({'username':self._username, 'password':self._password, 'client':'requestip', 'f':'json'})
            http_conn = httplib.HTTPConnection(self._server_name, self._server_port)
            http_conn.request("POST", "/arcgis/admin/generateToken", params, self._header)
            response = http_conn.getresponse()
            response_data = response.read()
            
            if self._assertResponseSuccess(response.status, response_data, 'token'):
                
                self.debug_logger("successfully retrieved a token")
                return json.loads(response_data)['token']
            
            else:
                self.debug_logger("failure retrieving a token")                                              
                                
        except Exception as e:
            self.debug_logger("Exception: ",str(e))
            
        finally:
            http_conn.close()
                        
    def _assertResponseSuccess(self, status, response_data, target_key):
                
        if (status == 200) and (target_key in response_data):
            self.debug_logger("SUCCESS: ",status, response_data)
            return True
        
        if (status == 200) and ('warning' in response_data):
            self.debug_logger("JSON WARNING: ",response_data)
            return True
        
        if status != 200:
            self.debug_logger("HTTP FAILURE: recieved HTTP status: ",status)
            return False
        
        if response_data.strip() == "":
            self.debug_logger("JSON FAILURE: response data is empty")
            return False
        
        if target_key not in response_data:
            self.debug_logger("JSON FAILURE: error: ",response_data)
            return False
            
    def stopServices(self): 
        self._executeServiceAction("STOP")
     
    def startServices(self):        
        self._executeServiceAction("START")

    def refreshServices(self):
                
        self.stopServices()
        self.startServices()