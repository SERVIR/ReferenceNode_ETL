ReferenceNode_ETL
=================

Scripts to access and compile near real time NASA satellite data into ArcGIS Server time-enabled map services. These ETL's we're built to demonstrate streaming data from NASA directly into ArcGIS Server and maintaining a time enabled 90 day archive of map services. These currently include Fire, Modis Imagery from Aqua and Terra, and TRMM rainfall. 

Scripts we're also created to compile MODIS classified landcover data into yearly layers.

Fire ETL:
The Fire ETL grabs the 5minute updates directy from the MODIS FTP site maintained by NASA. The script builds a 90day archive of these in a Esri formatted file geodatabase and subesequently updates a target map service on ArcGIS Server.
