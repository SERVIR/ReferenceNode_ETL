ReferenceNode_ETL
=================

Scripts to access and compile near real time NASA satellite data into ArcGIS Server time-enabled map services. These ETL's we're built to demonstrate streaming data from NASA directly into ArcGIS Server and maintaining a time enabled 90 day archive of map services. These currently include Fire, Modis Imagery from Aqua and Terra, and TRMM rainfall. 

**Landcover ETL:**
Scripts we're also created to compile MODIS classified landcover data into yearly layers. The arcpy scripts extract the Landcover data from the HDF's and resort them into FGDB for easy access for Esri users.

**Fire ETL:**
The Fire ETL grabs the 5minute updates directy from the MODIS FTP site maintained by NASA. The script builds a 90day archive of these in a Esri formatted file geodatabase and subesequently updates a target map service on ArcGIS Server.

**TRMM Rainfall ETL:**
The TRMM ETL does a number of things including downloading the 3-hour source CSVs from the NASA FTP sites. Build the raster from that data, apply the color map and import into an Esri FGDB format. It also optionally builds composites of n time periods (currently 24 and 168hrs respectively).

The final output is a time enabled map service with a rolling 90 day window of global rainfall.
