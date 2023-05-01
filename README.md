# Weather Data Collection Demo

This is a demonstration of a collection of a certain NOAA dataset. 

The scrape itself is written in Python. The script ghcnyearlydownload.py takes multiple arguments to direct its function. It will make a call to the website (URL not included out of respect for NOAA's bandwidth), collect the appropriate files based on argument values, download the files, and further process them according to argument values.

The database portion is created for a PostgreSQL server. Table creation is defined in create.sql. This creates the raw and processed data tables as well as multiple lookup tables that correspond to NOAA metadata. Small lookup tables are populated in the creation script, but larger tables are handled by Python scripts that parse the larger metadata files (not included). 

A stored procedure (in PostgreSQL: a function) is defined in stp_daily.sql. It filters out duplicate records, preps the data, and inserts the data into the final data table. 

The file work.sql contains various queries used to view different aspects of the data that were used to develop and test the collection/transformation process.

Ancillary visualization of the data using pandas and matplotlib is defined in visualize.py. This script will display the plots of daily high temperatures of a certain day and month over multiple years for a couple Colorado weather stations along with the current high temperature for said day and month. This requires the database structure and data from the above, and optionally a URI to a popular weather site (not included out of respect for their bandwidth) to obtain the current daily high temperature.