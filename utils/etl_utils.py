# Developer: SpatialDev
# Company:   Spatial Development International

# standard library
import os
from shutil import rmtree
from datetime import datetime
import ftplib
import urllib2
import gzip, zipfile
import logging
from logging.handlers import RotatingFileHandler
import xml.dom.minidom
from xml.dom.minidom import Node, Document


class URLDownloadManager(object):

    """
        Class URLDownloadManager manages URL downloading operations.
        
        public interface:
        
            getResultFromURL(url, content_types=[]) <object>:  This method returns the response from the given URL address. If a content_types list is supplied, it will
            first check the response header to deteremine if the content types match before downloading.
            
            downloadResultFromURL(url, downloaded_file_path, content_types=[]) <str>: This method downloads the object returned from the given URL into the given downloaded_file_path.
    """
               
    def getResultFromURL(self, url, content_types=[]):

        response = urllib2.urlopen(url)
        try:
            if content_types:
                return response.read() if (response.info()['Content-Type'] in content_types) else None
            else:
                return response.read()
        finally:
            response.close()

    def downloadResultFromURL(self, url, downloaded_file_path, content_types=[]):
        
        file_to_download = self.getResultFromURL(url, content_types)
            
        if file_to_download: 
                        
            with open(downloaded_file_path, "wb") as downloaded_image:
                downloaded_image.write(file_to_download)
            
            return downloaded_file_path

   
class FTPDownloadManager(object):
    
    """
        Class FTPDownloadManager manages common FTP operations and patterns.
        
        constructor arguments:
        
            ftp_options <dict>:
        
                ftp_host <str>: host address to the target FTP
                user <str>: user name for the given host
                password <str>: password for the given host
        
        fields:
        
            ftp_connection <ftplib.FTP>: FTP connection object to call core FTP operations
            ftp_host: see above
            user: see above
            password <str>: see above
            
        public interface:
        
            openConnection() <void>: connects to the given FTP host
            closeConnection() <void>: closes the connection to the given FTP host
            changeDirectory(ftp_dir) <void>: change the working directory of the FTP connection
            getDirectoryListing(ftp_dir) <list>: retrieve a list of all contents from the given directory
            getFileNamesFromDirectory(ftp_dir, target_file_extn=None) <list>: retrieve a list of all filenames from the given FTP directory
            downloadFileFromFTP(file_to_download, download_directory) <str>: downloads the given file_to_download from the current working FTP directory
    """
    
    def __init__(self, ftp_options):
        
        self.ftp_connection = None
        self.ftp_host = ftp_options['ftp_host']
        self.user = ftp_options['ftp_user']
        self.password = ftp_options['ftp_pswrd']

    def openConnection(self):
        self.ftp_connection = ftplib.FTP(self.ftp_host, self.user, self.password) 
        
    def closeConnection(self):
        self.ftp_connection.close()
        
    def changeDirectory(self, ftp_dir):
        self.ftp_connection.cwd(ftp_dir)
            
    def getDirectoryListing(self, ftp_dir):

        self.changeDirectory(ftp_dir)
        all_dir_files = []   
        self.ftp_connection.dir(all_dir_files.append)
        
        return all_dir_files
    
    def getFileNamesFromDirectory(self, ftp_dir, target_file_extn=None):
        
        """
            This method returns all filenames from the given FTP directory. If a target_file_extn
            if given then it will only return files that end with the given target_file_extn.
        """
        
        ftp_dir_list = []
        if target_file_extn:
            ftp_dir_list = [f.split(" ")[-1] for f in self.getDirectoryListing(ftp_dir) if f.endswith(target_file_extn)]
        else:
            ftp_dir_list = [f.split(" ")[-1] for f in self.getDirectoryListing(ftp_dir)]
        
        return ftp_dir_list
    
    def downloadFileFromFTP(self, file_to_download, download_directory):
        
        """
            This method downloads the given file_to_download from the file_to_download and 
            saves it to the given download_directory.
        """
        
        downloaded_file_fullpath = os.path.join(download_directory, file_to_download)        

        with open(downloaded_file_fullpath, 'wb') as f:
            self.ftp_connection.retrbinary('RETR %s' % file_to_download, f.write)
            
        return downloaded_file_fullpath
            

class UnzipUtils(object):
    
    """
        Class UnzipUtils wraps functions that unzip various compressed file formats.
        
        public interface:
        
            unzipGZip(gzip_file_fullpath, directory_to_unzip_into) <str>: This method unzips the given GZIP compressed gzip_file_fullpath into the given directory_to_unzip_into.
            unzipZip(zip_file_fullpath, directory_to_unzip_into) <str>: This method unzips the given compressed zip_file_fullpath into the given directory_to_unzip_into.
    """
    
    @staticmethod
    def unzipGZip(gzip_file_fullpath, directory_to_unzip_into):

        if gzip_file_fullpath.endswith("gz"):
            unzipped_filename = os.path.basename(gzip_file_fullpath.strip(".gz"))
            unzipped_file_fullpath = os.path.join(directory_to_unzip_into, unzipped_filename)
            
            with open(unzipped_file_fullpath, 'wb') as zip:
                zip.write(gzip.open(gzip_file_fullpath).read())
        
            return unzipped_file_fullpath

    @staticmethod
    def unzipZip(zip_file_fullpath, directory_to_unzip_into):

        if zip_file_fullpath.endswith("zip"):
            zip = zipfile.ZipFile(os.path.join(directory_to_unzip_into, zip_file_fullpath))
            zip.extractall(os.path.join(directory_to_unzip_into, zip_file_fullpath.strip(".zip")))
            
        return os.path.join(directory_to_unzip_into, zip_file_fullpath.strip(".zip"))
    
    
class ExceptionManager(object):
    
    """
        Class ExceptionManager creates an XML document with nodes that represent custom defined attributes asscoiated with exceptions. This document
        is created once the method finalizeExceptionXMLLog is called. To defined these custom nodes create a subclass of ExceptionManager and override 
        the _addETLDataExceptionNode method.
        
        An ExceptionManager also manages the output directory for the exception XML files as well as options to configure how and 
        when an exception report is created.
        
        constructor arguments:
        
            exception_log_dir_basepath <str>: output directory for all exception reports
            exception_dir_name <str>: the name of the above output directory
            options <dict>: 
            
                'create_immediate_exception_reports' <bool>: create an XML exception report as soon as a call to ExceptionManager.handleException() is made
                'delete_immediate_exception_reports_on_finish' <bool>: delete all immediate exception reports once a call to ExceptionManager.finalizeExceptionXMLLog is made
        
        fields:
        
            exception_xml_dict: the dictionary to contain all nodes added from calls made to ExceptionManager.handleException()
            exception_xml_order_node_list: the order to write out each exception node in the final report
            exception_count: the number of calls made to ExceptionManager.handleException()
            exception_directory <str>: the joined path from the exception_log_dir_basepath and  exception_dir_name
            immediate_exception_directory: the output directory contained inside the exception_directory that will hold any immediate exception logs
            create_immediate_exception_reports <object>: see above
            delete_immediate_exception_reports_on_finish <object>: see above
            
        public interface:
        
            handleException(exception_dict) <void>: This method is called and passed exception information contained in the given exception_dict. These key-value pairs are
            customized by the calling object and implemented in the ExceptionManager via its ExceptionManager._addETLDataExceptionNode().
            finalizeExceptionXMLLog() <void>: This method is called to finalize all ExceptionManager tasks which include: creating the final XML report if there were any calls to
            ExceptionManager.handleException(), creating the output directory, and cleaning up any immediate XML reports if delete_immediate_exception_reports_on_finish was set to True.
            
        private methods:
        
            _createImmediateExceptionXMLog(exception_dict) <void>: This method writes out a single XML report from the most recent exception_dict passed to ExceptionManager.handleException().
            _addETLDataExceptionNode(new_exception_node, exception_dict) <void>: Override this method to defined the node key and values assoicated with the key-value pairs from each exception_dict.
            _createExceptionXMLLog(exception_xml) <void>: This method calls ETLXMLUtils.writeDictToXML to create an XML document from the given exception_xml, exception_xml_dict, and xml_options.
            _addXMLNode(node_name, node_value, node_index) <void>: This method adds a new node to the XML document with the given parameters.
    """
    
    def __init__(self, exception_log_dir_basepath, exception_dir_name, options={}):
                
        self.exception_xml_dict = {}
        self.exception_xml_order_node_list = []
        self.exception_count = 0  
        
        self.exception_directory = os.path.join(exception_log_dir_basepath, exception_dir_name)
        self.immediate_exception_directory = os.path.join(self.exception_directory, "ImmediateExceptionLogs")
        self.create_immediate_exception_reports = options.get('create_immediate_exception_reports', False)
        self.delete_immediate_exception_reports_on_finish = options.get('delete_immediate_exception_reports_on_finish', True)
        
    def handleException(self,  exception_dict):
        
        self.exception_count +=1
        
        new_exception_node = str('exception_'+str(self.exception_count))
        self.exception_xml_order_node_list.append(new_exception_node)        
        self._addETLDataExceptionNode(new_exception_node, exception_dict)
                
        if self.create_immediate_exception_reports:
            self._createImmediateExceptionXMLog(self.exception_xml_dict[new_exception_node])
                    
    def _createImmediateExceptionXMLog(self, exception_dict):
        
            self._createDirectory(self.exception_directory)
            self._createDirectory(self.immediate_exception_directory)
                            
            exception_xml = os.path.join(self.immediate_exception_directory, str("ExceptionLog_"+str(datetime.strftime(datetime.utcnow(),"%Y-%m-%d_%H-%M-%S")))+".xml")
            ETLXMLUtils.writeDictToXML(exception_xml, exception_dict)
            
    def _addETLDataExceptionNode(self, new_exception_node, exception_dict):
        
        self.exception_xml_dict[new_exception_node] = {}
        self.exception_xml_dict[new_exception_node]['exception'] = exception_dict.get('exception','')
        self.exception_xml_dict[new_exception_node]['messages'] = exception_dict.get('messages','')
        
    def _createExceptionXMLLog(self, exception_xml):
        
        xml_options = {"parent_element":"Exceptions","node_list":self.exception_xml_order_node_list}
        ETLXMLUtils.writeDictToXML(exception_xml, self.exception_xml_dict, xml_options)
                        
    def finalizeExceptionXMLLog(self):
        
        if self.exception_count > 0:
            
            self._createDirectory(self.exception_directory)
                                    
            self._addXMLNode(node_name="total_exceptions", node_value=self.exception_count, node_index=0)
            exception_xml = os.path.join(self.exception_directory, str("ExceptionReport_"+str(datetime.strftime(datetime.utcnow(),"%Y-%m-%d_%H-%M-%S")))+".xml")

            self._createExceptionXMLLog(exception_xml)
            
            if self.delete_immediate_exception_reports_on_finish:
                rmtree(self.immediate_exception_directory)
                
    def _addXMLNode(self, node_name, node_value, node_index):
        
        self.exception_xml_order_node_list.insert(node_index, node_name)
        self.exception_xml_dict[node_name] = node_value
        
    def _createDirectory(self, dir):
        if not os.path.isdir(dir):
            os.makedirs(dir)


class ETLExceptionManager(ExceptionManager):
    
    """
        Class ETLExceptionManager subclasses ExceptionManager in order to override ExceptionManager._addETLDataExceptionNode() for the SERVIR ETL Framework implementation.
    """
    
    def __init__(self, exception_log_dir_basepath, exception_dir_name, options={}):
        ExceptionManager.__init__(self, exception_log_dir_basepath, exception_dir_name, options)
                                    
    def _addETLDataExceptionNode(self, new_exception_node, exception_dict):

        self.exception_xml_dict[new_exception_node] = {}
        self.exception_xml_dict[new_exception_node]['exception'] = exception_dict.get('exception','')
        self.exception_xml_dict[new_exception_node]['messages'] = exception_dict.get('messages','')
                
        etl_data_properties = exception_dict.get('etl_data_properties',{})
        self.exception_xml_dict[new_exception_node]['etl_data_name'] =  exception_dict.get('etl_data_name')
        
        self.exception_xml_dict[new_exception_node]['etl_data_properties'] = {}
        self.exception_xml_dict[new_exception_node]['etl_data_properties']['extract_data'] = etl_data_properties.get('extract_data','')
        self.exception_xml_dict[new_exception_node]['etl_data_properties']['transform_data'] = etl_data_properties.get('transform_data','')
        self.exception_xml_dict[new_exception_node]['etl_data_properties']['load_data'] = etl_data_properties.get('load_data','')
            
            
class ETLDebugLogger(object):
    
    """
        constructor arguments:
        
            debug_log_basename <str>: name of the debug log
            debug_log_dir <str>: output directory for debug logs
            debug_log_options <dict>: 
            
                'log_datetime_format' <str>: datetime format for debug logs
                'log_file_extn' <str>: extension of debug logs
                'debug_log_archive_days' <int>: number of days to keep debug logs
        
        fields:
        
            log_datetime_format: see above
            log_file_extn: see above
            debug_log_archive_days: see above
            debug_log_name <str>: the full name of the debug logs
            debug_log_dir: see above
            debug_logger <object>: logging object reference
            
        public interface:
        
            updateDebugLog(*args) <void>: accepts variable arguments and both prints to the screen and logs them to a file
            deleteOutdatedDebugLogs() <void>: deletes all debug logs outside a given archive datetime range
            
        private methods:
        
            _getDebugLogger(logger_name) <logger>: retrieves or creates a logging object from the given logger_name
    """

    def __init__(self, debug_log_dir, debug_log_basename, debug_log_options):
        
        self.log_datetime_format = debug_log_options.get('log_datetime_format','%Y-%m-%d')
        self.log_file_extn = debug_log_options.get('log_file_extn','log')
        self.debug_log_archive_days = debug_log_options.get('debug_log_archive_days', 0)
                
        log_datetime_string = datetime.strftime(datetime.now(), self.log_datetime_format)
        self.debug_log_name =  "%s_%s.%s" % (debug_log_basename, log_datetime_string, self.log_file_extn)
        self.debug_log_dir = debug_log_dir
        
        if not os.path.isdir(debug_log_dir):
            os.makedirs(debug_log_dir)
            
        self.debug_logger = self._getDebugLogger(debug_log_basename)
                
    def _getDebugLogger(self, logger_name):
        
        txt_handler = RotatingFileHandler(os.path.join(self.debug_log_dir, self.debug_log_name))
        txt_handler.setFormatter(logging.Formatter("%(asctime)s: %(message)s"))
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.DEBUG)
        logger.addHandler(txt_handler)
                    
        return logger
    
    def updateDebugLog(self, *args):
        
        print args
        self.debug_logger.debug(str(args))

    def deleteOutdatedDebugLogs(self):
        
        debug_log_archive_days = self.debug_log_archive_days
        current_datetime = datetime.now()
        isOutsideArchiveRange = lambda d:(current_datetime - d).days > int(debug_log_archive_days)
        getDebugLogDatetime = lambda d:datetime.strptime(os.path.basename(d).split("_")[-1].split(".")[0], self.log_datetime_format)
        hasArchiveRangeAndDirectory = debug_log_archive_days > 0 and os.path.isdir(self.debug_log_dir)
        
        if hasArchiveRangeAndDirectory:
            current_debug_logs = [d for d in os.listdir(self.debug_log_dir) if d.endswith(self.log_file_extn)]
            for debug_log in current_debug_logs:
                if isOutsideArchiveRange(getDebugLogDatetime(debug_log)):
                    os.remove(os.path.join(self.debug_log_dir, debug_log))
            

class ETLXMLUtils(object):
    
    """
        Class ETLXMLUtils
        
        public interface:
        
            getDictFromXML(xml_file) <dict>: This method contains the procedure to parse an XML document into a dictionary.
            writeDictToXML(xml_file, data_dict, options={}) <void>: This method contains the procedure to create an XML document from a given nested dictionary.
            
        private methods:
        
            _getStringFromXML(xml_file) <str>: This method returns a string representation of the given xml_file.
            _convertXMLStringToDict(xml_string) <dict>: This method parses the given string from an XML file into a dictionary.
            _elementToDict(parent) <dict>: This method creates a dictionary from an XML node element.
            _removeWhiteSpaceNodes(node, unlink=True) <void>: This method removes white space nodes from an XML document.
            _createElementNode(key, value, doc, parent_element) <void>: This method creates a new XML node. If the given value is a dictionary, then it will
            recursivly call itself until all levels of the given dictionary are represented and created as nested XML nodes.
    """
        
    @staticmethod
    def getDictFromXML(xml_file): 
        
        xml_string = ETLXMLUtils._getStringFromXML(xml_file)
        xml_to_dict = ETLXMLUtils._convertXMLStringToDict(xml_string)
        
        return xml_to_dict
    
    @staticmethod
    def writeDictToXML(xml_file, data_dict, options={}):
              
        doc = Document()
        parent_element = doc.createElement(options.get('parent_element', str(os.path.basename(xml_file).split(".")[0])))
        doc.appendChild(parent_element)  
        
        for key in options.get('node_list', sorted(data_dict.keys())):
            ETLXMLUtils._createElementNode(key, data_dict[key], doc, parent_element)
            
        with open(xml_file, "wb") as xml:
            xml.write(doc.toprettyxml(indent=" "))
    
    @staticmethod
    def _getStringFromXML(xml_file):
        
        with open(xml_file,'rb') as xml:
            return xml.read()
    
    @staticmethod
    def _convertXMLStringToDict(xml_string):
        
        doc = xml.dom.minidom.parseString(xml_string)
        ETLXMLUtils._removeWhiteSpaceNodes(doc.documentElement)
        xml_dict = ETLXMLUtils._elementToDict(doc.documentElement)
        
        return xml_dict
    
    @staticmethod
    def _elementToDict(parent):
        
        child = parent.firstChild
        if (not child):
            return None
        elif (child.nodeType == xml.dom.minidom.Node.TEXT_NODE):
            return child.nodeValue
        d={}        
        while child is not None:
            if child.nodeType == Node.ELEMENT_NODE:
                try:
                    d[child.tagName]                
                except KeyError:
                    d[child.tagName] = ETLXMLUtils._elementToDict(child)
                else:
                    if type(d[child.tagName]) != list:   
                        d[child.tagName] = [d[child.tagName]]
                    d[child.tagName].append(ETLXMLUtils._elementToDict(child))    
            child = child.nextSibling
        return d
    
    @staticmethod
    def _removeWhiteSpaceNodes(node, unlink=True):
        
        remove_list = []
        for child in node.childNodes:
            if child.nodeType == xml.dom.Node.TEXT_NODE and not child.data.strip():
                remove_list.append(child)                
            elif child.hasChildNodes():
                ETLXMLUtils._removeWhiteSpaceNodes(child, unlink)
        for node in remove_list:
            node.parentNode.removeChild(node)
            if unlink:
                node.unlink()
                
    @staticmethod
    def _createElementNode(key, value, doc, parent_element):
        
        parent = doc.createElement(key)
        if isinstance(value, dict):
            for k in value.keys():
                ETLXMLUtils._createElementNode(k, value[k], doc, parent)
        else:
            data = doc.createTextNode(str(value))
            parent.appendChild(data)
            
        parent_element.appendChild(parent)