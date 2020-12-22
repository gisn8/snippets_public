# SCRIPT GOALS
# To download the most recent specified OGRIP LBRS data for available Ohio counties for use within a geopackage database
# and/or shapefiles reprojected into a common projection.
# A windowed interface is not provided with the intent this is to be run in the background on a schedule.

# Source website: http://gis3.oit.ohio.gov/geodatadownload/lbrs.aspx
# Source link format: http://gis3.oit.ohio.gov/LBRS/_downloads/HAR_CL.zip

# OVERVIEW
# First checks if projection files or original raw files only are requested. If so, only downloads those with no further
# action taken.
# Checks for existing local geopackage(gpkg) database where this data is stored. If one does not exist, creates a new
# one.
# If needed, download and reproject a county boundaries layer into the gpkg from ODOT and create a table listing all 
# counties and the archive date of the downloaded LBRS data. 
# Download a list of layer types from a list of counties from available OGRIP LBRS layers, reproject to a common
# projection, and put them in a statewide geopackage with the option to also have a subset of reprojected shapefiles.

# PREREQUISITES
# GDAL
# Python 3.6+
# Windows users: It is recommended to download OSGeo4W network installer from https://trac.osgeo.org/osgeo4w/.
# Using the installer, you can get GDAL which also comes with Python 3.
# You will then need to launch this using an environment setting script. A py3_env.bat script should have been available
# along with this file.




#################
#    IMPORTS    #
#################

import os
import sys
import subprocess
import shutil
import time
import ogr
import pathlib

# From what I can find, requests is a spin of urllib.request or maybe just urllib. It's a little unclear to me, 
# but the language between the two do not seem to be shared and I haven't the time right now to dig out the equities
# to reduce it down to just one library.  So we're using both until then. :/
import requests
from urllib import request

from datetime import datetime
from zipfile import ZipFile
from io import BytesIO

# For error catching
from inspect import currentframe, getframeinfo
frameinfo = getframeinfo(currentframe())




###################################
#    SCRIPT BEHAVIOR VARIABLES    #
###################################

# 0 = No
# 1 = Yes

# Default is 0.
# If set to 1, downloads only the projection files for each county and layer type selected and takes no further actions
# 	to update the GeoPackage database. This feature remains from earlier development in trying to determine how to fix
#	SRS	errors that some counties experience.
# If set to 0, then evaluates the raw_files_only variable (below) before continuing.
# WARNING: If this varible is not set to 0, all other updates will not be made.
prj_only = 0


# Default is 0.
# If set to 1, downloads only the original zip files and takes no further actions to update the GeoPackage database.
#	Keep in mind that the raw files come in a variety of projections, and some are incorrectly assigned from the source.
# If set to 0, continues on to update the GeoPackage database. Raw files will then only be downloaded in the event that
#	layers are found that misalign from their intended projection; then can be used to evaluate what changes need to be
#	made in SRS assignments.
# WARNING: If prj_only is not set to 0, this setting will be ignored.
# WARNING: If this variable is not set to 0, all other updates will not be made.
raw_files_only = 0


# Default is 1. 
# If set to 1, will delete existing pertainent workspace files and re-create as needed.
# If set to 0, will utilize existing files found in the workspace. Setting to 0 is is useful to update only certain
# 	layers while retaining the existing layers in the workspace database typically for testing purposes.
clean_workspace = 1


# Default is 0. 
# If set to 1 will force download/overwrite of existing layers. After running this script the first time, subsequent
# 	runs compare archive dates to see if the local file is the most recent. Setting to 1 will download the file
# 	regardless. Useful for tests or in the event new code needs to be applied to the data.
# If set to 0, will compare archive dates and only download files where web archive is more recent.
force_import = 0


# Default is 1. 
# If set to 1, communicates with the archive location (db_arch_loc) to search for an existing GeoPackage database to
# 	copy to the workspace (db_ws_loc) for updating. Will archive the GPKG back to the archive location and delete the
# 	workspace database. 
# If set to 0, changes will only remain in the database workspace location. If operating only in the workspace
# 	environment is desired, keep at 0 and in the Local File Variables section, set db_arch_loc = db_ws_loc.
use_arch_db = 1



# If set to a value greater than 0, except for the ODOT counties layer, will limit the number of features loaded from
# each layer to the value entered.
limit_features = 0




##############################
#    LOCAL FILE VARIABLES    #
##############################

# As GeoPackage does not handle large operations well over network, this uses the speed of a local (SSD or flash
# ideally) drive workspace location (db_ws_loc) for the initial download and operations before copying over to a network
# location or HDD as the destination. If db_ws_loc and db_arch_loc are assigned the same location, the copy command will be
# skipped.
home = str(pathlib.Path.home())
db_ws_loc = f"{home}/Downloads/LBRS"


# Change to desired location or if operating only in the workspace environment is desired, set db_arch_loc = db_ws_loc.
db_arch_loc = f"{home}/Drives/S/GIS Data/OGRIP"


# GeoPackage name
db = r'OGRIP_LBRS.gpkg'

# Directs the Python script to operate within the workspace location
os.chdir(db_ws_loc)

driver = ogr.GetDriverByName("GPKG")

# Leftover from earlier development, but may come back to later.
# Making a read-only version to limit open connections. It appears that a process that just needs to read will sometimes
# get blocked by an another process left open? I suspect there may be a better solution to this issue.
# file = driver.Open(db_ws_loc + db, 1) # 1=writable
# rfile = driver.Open(db_ws_loc + db, 0)




###############################
#    SOURCE FILE VARIABLES    #
###############################

# All Ohio counties. Modifying this variable is not advised.
all_counties = ['ADA', 'ALL', 'ASD', 'ATB', 'ATH', 'AUG', 'BEL', 'BRO', 'BUT', 'CAR', 'CHP', 'CLA', 'CLE', 'CLI', 'COL', 'COS', 'CRA', 'CUY', 'DAR', 'DEF', 'DEL', 'ERI', 'FAI', 'FAY', 'FRA', 'FUL', 'GAL', 'GEA', 'GRE', 'GUE', 'HAM', 'HAN', 'HAR', 'HAS', 'HEN', 'HIG', 'HOC', 'HOL', 'HUR', 'JAC', 'JEF', 'KNO', 'LAK', 'LAW', 'LIC', 'LOG', 'LOR', 'LUC', 'MAD', 'MAH', 'MAR', 'MED', 'MEG', 'MER', 'MIA', 'MOE', 'MOT', 'MRG', 'MRW', 'MUS', 'NOB', 'OTT', 'PAU', 'PER', 'PIC', 'PIK', 'POR', 'PRE', 'PUT', 'RIC', 'ROS', 'SAN', 'SCI', 'SEN', 'SHE', 'STA', 'SUM', 'TRU', 'TUS', 'UNI', 'VAN', 'VIN', 'WAR', 'WAS', 'WAY', 'WIL', 'WOO', 'WYA']

# Modify line below in the listing style as used above to narrow to only counties of interest.
# county_list = all_counties
county_list = all_counties

# List the counties you want projection-converted shapefiles for. Typically your own and/or surrounding counties.
shp_counties = ['HAR', 'ALL', 'AUG', 'HAN', 'LOG', 'MAR', 'UNI', 'WYA']

# All available layer types
layer_types = ['ADDS', 'CL', 'INTRSCTS', 'LNDMRKS', 'RLXING']
# Overwrites above. Most commonly requested, but modify as desired or preface line below with # for all layer types:
layer_types = ['ADDS', 'CL']




##################################################
#    SPATIAL REFERENCE SYSTEM (SRS) VARIABLES    #
##################################################

# Target Spatial Reference System. Desired projection in EPSG coordinate reference system. THIS IS THE ONLY SRS VARIABLE
# YOU'LL NEED TO MODIFY IF NEEDED.
t_srs = '3734'


# VARIABLES BELOW SHOULD NOT NEED TO BE MODIFIED.

# Default CRS is EPSG:32123. However, exceptions should be listed here to be converted.
crs3734 = ['AUG_CL','CAR_CL', 'CUY_CL', 'DEL_CL','FUL_CL', 'HAS_CL', 'HEN_CL', 'HOL_CL', 'KNO_CL', 'MAH_CL', 'RIC_CL', 'TUS_CL']
crs3735 = ['BUT_CL', 'CLA_CL', 'CLE_CL', 'CLI_CL', 'FAI_CL', 'FRA_CL', 'GRE_CL', 'HIG_CL','LAW_CL','LIC_CL','MAD_CL','MOT_CL','MUS_CL']
# I think these two have an error that they are actually supposed to have been 32123 N half, but they were converted to
# 32122 S half and assigned 32123 by mistake.
crs32122 = ['HAR_CL', 'POR_CL']




#########################
#    REFERENCE LISTS    #
#########################

# The following omissions are anticipated: BEL, GEA, HAM, MED, UNI, WAR. If download failures happen beyond these,
# additional messages are shown at the end.
anticipated_omissions = ['BEL_ADDS', 'BEL_CL', 'GEA_ADDS', 'GEA_CL', 'HAM_ADDS', 'HAM_CL', 'MED_ADDS', 'MED_CL', 'UNI_ADDS', 'UNI_CL', 'WAR_ADDS', 'WAR_CL']

# Python note: To check if all items in a list of elements are present in a master list: all(item in mlist for item in elist)
omission_list = []
updated_list = []
empty_tables_list = []
geom_mismatch_list = []
missing_src_list = []




###################
#    FUNCTIONS    #
###################

# Produces message with error code
def errorcatch(e, lineno='0'):
	template = "Exception: {0}\nLine: {lineno}\nArguments: {1!r}"
	message = template.format(type(e).__name__, e.args)
	# print(message)
	print(f"Exception: {type(e).__name__}\nLine: {lineno}\nArguments: {e.args}")


# May run first if just looking for prj files, but otherwise will download raw files as needed.
def get_src_data(c_list=county_list, t_list=layer_types):
	for county in c_list:
		for layer_type in t_list:
			layer_name  = f'{county}_{layer_type}'
			
			url = f"http://gis3.oit.ohio.gov/LBRS/_downloads/{layer_name}.zip"
			url = url_check(url)
			
			if url is not None:
				if prj_only == 1:
					if not os.path.exists(f'{db_ws_loc}/PRJs'):
						os.mkdir(f'{db_ws_loc}/PRJs')
					
					print(f'Importing {layer_name}.prj - {datetime.now().strftime("%T")}')
					
					# Extract prj file from online.zip
					with request.urlopen(url) as zipurl:
						with ZipFile(BytesIO(zipurl.read())) as zfile:
							for fileName in zfile.namelist():
							   if fileName.endswith(f'{layer_name}.prj'):
								   zfile.extract(fileName, f'{db_ws_loc}/PRJs/')
				else:
					print('Downloading raw data.')
					if not os.path.exists(f'{db_ws_loc}/raw'):
						os.mkdir(f'{db_ws_loc}/raw')
					urllib.request.urlretrieve(url, f'{db_ws_loc}/raw/{layer_name}.zip') 

			else:
				print(f"Source for {layer_name} not available.")
				missing_src_list.append(f'{layer_name}')


# Prepares the workspace. Cleans it if clean_workspace variable indicates.
def prep_workspace():
	if clean_workspace == 1:
		clean_workspace()
	else:
		print('Preparing workspace.')
		
	# If there are matching surrounding counties are in the county list, then make the SHP folder.
	if len(set(shp_counties).intersection(county_list)) > 0 and not os.path.exists(f'{db_ws_loc}/SHPs'):
		os.mkdir(f'{db_ws_loc}/SHPs')

	# To bring in the existing archived database so we can compare dates and update only as necessary.
	if use_arch_db == 1 and os.path.exists(f'{db_arch_loc}/{db}'):
	   xfer_data(src=f'{db_arch_loc}/{db}', dest=f'{db_ws_loc}/{db}')
	elif not os.path.exists(f'{db_ws_loc}/{db}'):
   		create_new_db()


# Removes working files and directories if present
def clean_workspace():
		print('Cleaning workspace.')
		
		if os.path.exists(f'{db_ws_loc}/SHPs'):
			shutil.rmtree(f'{db_ws_loc}/SHPs')

		if os.path.exists(f'{db_ws_loc}/PRJs'):
			shutil.rmtree(f'{db_ws_loc}/PRJs')

		if os.path.exists(f'{db_ws_loc}/Raw'):
			shutil.rmtree(f'{db_ws_loc}/Raw')

		if os.path.exists(f'{db_ws_loc}/{db}'):
			os.remove(f'{db_ws_loc}/{db}')


# Creates new db.gpkg template from on-hand empty.gpkg or downloads a new one. 
# Then calls get_odot_counties_layer and creates another table tracking data timestamp changes.
def create_new_db():
	if os.path.exists(f'{db_ws_loc}/empty.gpkg'):
		print('empty.gpkg template found.')
		# os.system(r'cd %s && cp empty.gpkg %s' % (db_ws_loc, db))
		shutil.copy2('empty.gpkg', db)
		if os.path.exists(f'{db_ws_loc}/{db}'):
			print(f'Fresh {db} created from existing template.')
	else:
		print('No template found. Downloading fresh template.')
		# os.system(r'cd %s && wget -N "https://github.com/opengeospatial/ets-gpkg10/raw/master/src/test/resources/gpkg/empty.gpkg" && cp empty.gpkg %s' % (db_ws_loc, db))
		# url = "https://github.com/opengeospatial/ets-gpkg10/raw/master/src/test/resources/gpkg/empty.gpkg"
		url = "http://www.geopackage.org/data/empty.gpkg"
		empty_gpkg = requests.get(url)
		open(f'{db_ws_loc}/empty.gpkg', 'wb').write(empty_gpkg.content)
		os.rename(f'{db_ws_loc}/empty.gpkg', f'{db_ws_loc}/{db}')
		if os.path.exists(f'{db_ws_loc}/{db}'):
			print(f'Fresh {db} created from new template.')

	print('')

	get_odot_counties_layer()

	# As the file first called on has been replaced, re-pointing to the new file.
	sql = "CREATE TABLE IF NOT EXISTS shp_dates (id integer primary key, \"COUNTY_CD\" text);"
	run_sql(getframeinfo(currentframe()).lineno, sql=sql)[0]
		
	# Using the values from the county layer to populate the shp_dates table. This also prevents an accidental out of order from the list.
	sql = 'INSERT INTO shp_dates ("COUNTY_CD") SELECT "COUNTY_CD" FROM county ORDER BY "COUNTY_CD"'
	run_sql(getframeinfo(currentframe()).lineno, sql=sql)
	
	# Add a column for each layer type's package date
	for layer_type in layer_types:
		sql = f"ALTER TABLE shp_dates ADD COLUMN \"{layer_type}_shp_date\" text DEFAULT '0';"
		run_sql(getframeinfo(currentframe()).lineno,sql=sql)[0]


# Reprojects and downloads ODOT counties layer to the db.
def get_odot_counties_layer():
	# In case you're comparing to the odot download file, the geom restriction isn't necessary because we're only dealing with 88 features.
	cmd = f'ogr2ogr -f "GPKG" -update -append -skipfailures -gt 20000 -ds_transaction -unsetFieldWidth -nln "county" -preserve_fid -geomfield "geom" -t_srs EPSG:3734 "{db_ws_loc}/{db}" "https://gis.dot.state.oh.us/arcgis/rest/services/TIMS/Boundaries/MapServer/2/query?where=1%3D1&text=&objectIds=&time=&geometry=&geometryType=esriGeometryPolygon&inSR=4326&spatialRel=esriSpatialRelIntersects&relationParam=&outFields=*&returnGeometry=true&returnTrueCurves=true&maxAllowableOffset=&geometryPrecision=&outSR=&returnIdsOnly=false&returnCountOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&gdbVersion=&returnDistinctValues=false&resultOffset=&resultRecordCount=&queryByDistance=&returnExtentsOnly=false&datumTransformation=&parameterValues=&rangeValues=&f=pjson" OGRGeoJSON && echo "County file import complete."'
	
	print(f'Importing ODOT county layer - {datetime.now().strftime("%T")}')
	try:
		subprocess.Popen(cmd, shell=True).wait()
	except Exception as e:
		print(f'ODOT Counties layer dowload failed.')
		omission_list.append(f'odot.county')
		errorcatch(e, {getframeinfo(currentframe()).lineno})


# Cycles through the county_list and layer_types and manages the downloads and file manipulations.
def get_data():
	for county in county_list:
		print('')
		print(county)
		for layer_type in layer_types:
			layer_name = f'{county}_{layer_type}'
			
			url = f'http://gis3.oit.ohio.gov/LBRS/_downloads/{layer_name}.zip'
			url = url_check(url)

			if url is None:
				print(f"Source missing for {layer_name}. Line No.: {getframeinfo(currentframe()).lineno}")
				omission_list.append(layer_name)
				missing_src_list.append(layer_name)
			else:
				print(f'Importing {layer_name} - {datetime.now().strftime("%T")}')
				lyr_dat_dict = get_url_date(url, layer_name)
				shp_date=lyr_dat_dict[f'{layer_name}.shp']
				
				proceed = True
				if force_import == 0:
					proceed = check_date(county, layer_type, shp_date)
				else:
					print('Forced import is activated.')
					check_date(county, layer_type, shp_date)
					proceed = True
				
				if proceed == True and layer_name not in omission_list:
					cmd = format_cmd(layer_name)
					subprocess.Popen(f'cd {db_ws_loc} && {cmd}', shell=True).wait()

					try:
						sql = f"select count(*) from \"{layer_name}\""
						fc = run_sql(getframeinfo(currentframe()).lineno, sql=sql, layer_name=layer_name)[0]
						print(f'{layer_name} feature count: {fc}')
						if fc < 1:
							print(f'{layer_name} table is empty.')
							empty_tables_list.append(layer_name)
						else:
							spatial_check(county, layer_type, layer_name)
					except Exception as e:
						print(f"Import for {layer_name} failed.")
						omission_list.append(layer_name)
						errorcatch(e, {getframeinfo(currentframe()).lineno})


					if (county in shp_counties) * (layer_name not in empty_tables_list) * (layer_name not in geom_mismatch_list) == 1:
						dest = f'{db_ws_loc}/SHPs/{layer_name}'

						if os.path.exists(dest):
							shutil.rmtree(dest)                  
						
						# Extracting SHPs with corrected SRS
						print(f'Extracting {layer_name} from {db}.')
						cmd = f'ogr2ogr -f "Esri shapefile" "{dest}" "{db_ws_loc}/{db}" "{layer_name}"'
						print(cmd)
						subprocess.Popen(f'{cmd}', shell=True).wait()
						
						print('Collecting resulting filenames.')
						filelist = []
						for (dirpath, dirnames, filenames) in os.walk(dest):
							print('filenames: ' + str(filenames))
							filelist.extend(filenames)
							print('filelist: ' + str(filelist))
							break

						# update timestamp on file to reflect the original then pass into zipfile.
						print('Updating timestamps on files and sending to zip.')
						with ZipFile(f'{dest}.zip', 'w') as zipObj:
							for file in filelist:
								update_timestamp(lyr_dat_dict[f'{file}'], f'{dest}/{file}')
								zipObj.write(f'{dest}/{file}', file)	# The second argument being the name to save it under.
						zipObj.close()
						print(shp_date)
						print(f'{dest}.zip')
						
						update_timestamp(shp_date, f'{dest}.zip')
						shutil.rmtree(dest)
				
			print('-----')


# Checks the validity of a url. Early catch for unavailable URLs. Returns None if invalid.
def url_check(url):
	request = requests.get(url)
	if request.status_code == 200:
		return url
	else:
		return None


# Retrieves, stores the publishing date of each file in the web.zip file and returns them in a python dictionary.
def get_url_date(url, layer_name):
	# Get dates from top-level files contained in online zip file.
	
	open_url = request.urlopen(url)
	print(url)

	lyr_dat_dict = {}
	try:
		with open_url as zipurl:
			with ZipFile(BytesIO(zipurl.read())) as zfile:
				for info in zfile.infolist():
					if info.filename.startswith(layer_name) or info.filename.startswith('ALL_ADD'):
						print(info.filename)
						url_date = datetime(*info.date_time)
						lyr_dat_dict[info.filename] = url_date
						if info.filename.startswith('ALL_ADD') and not info.filename.startswith('ALL_ADDS'):
							# slicing off the .extension from the ALL_ADD* as we know it's been miss named
							lyr_dat_dict[f'{layer_name}{info.filename[7:]}'] = lyr_dat_dict[info.filename]
							del lyr_dat_dict[info.filename]
	except Exception as e:
		print(f'Error getting url dates.')
		errorcatch(e, {getframeinfo(currentframe()).lineno})
	return lyr_dat_dict


# Checks the shp_date table to see if the file on-hand is current with the one on the web.
def check_date(county,layer_type, shp_date):
	# 1. TRY Pull url date (keeping in mind some counties are unavailable)
	layer_name = f'{county}_{layer_type}'
	try:
		### 'Tue, 10 Mar 2020 17:08:37 GMT'
		### If 'Last-Modified' no longer works, print() in script or command in interface below to check the available headers.
		### url.info().keys()   # It's a dictionary, so .keys() and .values() work here.
		### ['Content-Type', 'Last-Modified', 'Accept-Ranges', 'ETag', 'Server', 'X-Powered-By', 'Date', 'Connection', 'Content-Length']

		# 2. Pull stored date for layer and assign py variable
		
		sql = f"select \"{layer_type}_shp_date\" from shp_dates where \"COUNTY_CD\" = '{county}';"
		# print(sql)
		val = run_sql(getframeinfo(currentframe()).lineno, sql=sql)[1]
		archive_date = val
		print(f'Web.shp Date: {shp_date}')
		print(f"Archive Date: {archive_date}")

		# 3. Compare
		if str(archive_date) == str(shp_date):
			# 3a. If the same, 
			# 3a1. Move on to the next county
			if force_import == 0:
				print(f'{layer_name} up to date.')
			return False
		else:
			# 3b. If different, 
			# 3b1. update the _package_date table 
			print('Updating archive date.')
			sql = f"UPDATE shp_dates SET \"{layer_type}_shp_date\" = '{shp_date}' WHERE \"COUNTY_CD\"='{county}';"
			run_sql(getframeinfo(currentframe()).lineno, sql=sql)[0]
			# 3b2. update the layer
			print(f'Updating {layer_name}')
			return True
	
	except Exception as e:
		print(f"Import for {county}_{layer_type} failed.")
		omission_list.append(county + '_' + layer_type)
		errorcatch(e, {getframeinfo(currentframe()).lineno})
		return False


# Formats the ogr2ogr command to:
#	Modify and convert the spatial references of all assigned layers for consistency.
#	Download the assigned layer and store it in the db.gpkg
def format_cmd(layer_name, f='GPKG', dest=db):
	if layer_name in crs3734:
		s_srs = '3734'
		t_code = ''
	elif layer_name in crs3735:
		s_srs = '3735'
		t_code = f'-s_srs EPSG:{s_srs} -t_srs EPSG:{t_srs} '
	elif layer_name in crs32122:
		# There's an error in these where they were misassigned. This accomodates for that
		s_srs = '32123'
		t_code = f'-s_srs EPSG:{s_srs} -t_srs EPSG:{t_srs} '
	else:
		s_srs = '32123'
		t_code = f'-t_srs EPSG:{t_srs} '

	if limit_features > 0:
		lmt = f'-limit {limit_features} '
	else:
		lmt = ''

	cmd = r'ogr2ogr -f "' + f +'" -update -append -gt 20000 -skipfailures -unsetFieldWidth -nln "' + layer_name + r'" -preserve_fid -geomfield "geom" ' + lmt + t_code + dest + r' "/vsizip/vsicurl/http://gis3.oit.ohio.gov/LBRS/_downloads/' + layer_name + r'.zip"'
	print(cmd)
	return cmd


# Runs SQL commands through the db.gdb throughout the script.
# 	1. To create the timestamp table. 
#	2. Update the timestamp table as needed. 
# 	3. Run counts on layers to see if data downloaded. 
#	4. Runs the spatial_check sql cmd.
# With both os.system calls mixed in with ogr.py calls, the code is funky and redundant because it seems like the gpkg 
# trips over itself sometimes on both reads and writes. Somehow, the weird redundancy seems to manage it okay, but it 
# looks a little icky. Possibly should be broken into separate functions.
def run_sql(lineno, sql=None, layer_name=None):
	fc = 0
	val = 0
	print('')
	print(f'Called from line {lineno}.')
	print(f'SQL: {sql}')
	print(f'Layer_name: {layer_name}')

	filename = f'{db_ws_loc}/{db}'
	if layer_name == None:
		try:    
			cmd = f"""ogrinfo {filename} -sql "{sql}" """
			print(cmd)
			# os.system(cmd)
			subprocess.Popen(cmd, shell=True).wait()
			file = driver.Open(filename, 0)
			output = file.ExecuteSQL(sql)
			feat = output.GetNextFeature()
			val = feat.GetField(0)
		except AttributeError:
			pass
		except Exception as e:
			print(f'Error running SQL.')
			errorcatch(e, {getframeinfo(currentframe()).lineno})
			print('Retrying')
			run_sql(lineno, sql=sql)

	if layer_name != None:
		try:
			file = driver.Open(filename, 0)
			output = file.ExecuteSQL(f'SELECT count(*) FROM {layer_name}')
			feat = output.GetNextFeature()
			val = feat.GetField(0)
		except Exception as e:
			print(f'SQL output has no returns.')
			errorcatch(e, {getframeinfo(currentframe()).lineno})
	
	try:
		file = driver.Open(filename, 0)
		lyr = file.GetLayerByName(layer_name)
		if lyr != None:
			fc = lyr.GetFeatureCount()
	except Exception as e:
		print("Feature count not found, but is not always applicable.")
		errorcatch(e, {getframeinfo(currentframe()).lineno})
		pass
		

	# fc = output.GetFeatureCount() or 0
	print(f'layer_name: {layer_name}; val: {val}')
	return fc, val


# Puts together the SQL to verfiy that random features from the layer downloaded aligns with the county layer. 
# If not, adds the layer to the geom_mismatch_list and tries to pull the raw web.zip.
def spatial_check(county, layer_type, layer_name):
	# Compare a few random intersecting features from each county layer to the ODOT county layer to make sure that odot.county.COUNTY_CD = coalesce(ogrip.county.L_COUNTY, ogrip.county.COUNTY), otherwise, add the layer_name to geom_mismatch_list
	sql = f"select count(*) from \"{layer_name}\" lyr, county c where st_intersects(lyr.geom,c.geom) and c.\"COUNTY_CD\" = '{county}' order by random() limit 100;"
	val = run_sql(getframeinfo(currentframe()).lineno, sql=sql)[1]
	print(f'SQL: {sql}')
	print(f'SQL return: {val}')
	if val == 0:
		print(f'{layer_name} does not align.')
		geom_mismatch_list.append(layer_name)
		get_src_data(c_list=[county], t_list=[layer_type])


# Assigns the create and modified datetime to the destination file
def update_timestamp(srcfile_datetime, destfile):
	# Change the date
	print('Syncronizing timestamps')
	# print(srcfile_datetime)
	# print(srcfile_datetime.timestamp())
	os.utime(destfile, (srcfile_datetime.timestamp(), srcfile_datetime.timestamp())) # os.utime(file, (atime, mtime)) # atime=access time, mtime = modified time
	
	# Verify it worked
	destfile_datetime = datetime.fromtimestamp(os.path.getmtime(destfile))
	
	print(srcfile_datetime)
	print(destfile_datetime)

	if str(srcfile_datetime) == str(destfile_datetime):
		print('Timestamp updated!')
	else:
		print('ERROR: Timestamp unable to update!')


# Sends a list of files to a zip file
def zip_files(filelist, dest):
	with ZipFile(f'{dest}.zip', 'w') as zipObj:
		for filename in filelist:
			# print(filename)
			zipObj.write(f'{dest}/{filename}')
	zipObj.close()


# Transfers files
def xfer_data(src, dest):
	print(f'Transferring from {src} to {dest}.')
	if src == dest:
		pass
	elif not os.path.exists(src):
		print('Source does not exists. Skipping transfer')
	elif os.path.isdir(src):
		#shutil.copytree(src, dest)	# Doesn't work if directory already exists.
		mergefolders(src,dest)
		print('Folder contents transferred.')
	elif os.path.exists(dest) and datetime.fromtimestamp(os.path.getmtime(src)) == datetime.fromtimestamp(os.path.getmtime(dest)):
		print('No changes made between files. Transfer not needed.')
	else:
		print("Transferring files to %s - %s" % (db_arch_loc, datetime.now().strftime("%T")))
		if os.stat(src).st_size < (200 * 1000 * 1000):
			# 200 MB (round)
			shutil.copy2(src, dest)
		else:
			copy_large_file(src, dest)
		update_timestamp(datetime.fromtimestamp(os.path.getmtime(src)), dest)
		print('File transfer completed')


# From https://lukelogbook.tech/2018/01/25/merging-two-folders-in-python/, but using copy2 to retain metadata instead.
# As copytree will not replace an existing folder, this merges one folder into another including subfolders
def mergefolders(root_src_dir, root_dst_dir):
    for src_dir, dirs, files in os.walk(root_src_dir):
        dst_dir = src_dir.replace(root_src_dir, root_dst_dir, 1)
        if not os.path.exists(dst_dir):
            os.makedirs(dst_dir)
        for file_ in files:
            src_file = os.path.join(src_dir, file_)
            dst_file = os.path.join(dst_dir, file_)
            if os.path.exists(dst_file):
                os.remove(dst_file)
            shutil.copy2(src_file, dst_dir)


# From https://gist.github.com/jlinoff/0f7b290dc4e1f58ad803
# Modified to have a minimum chunk size of 10MB
# Prior to Python3.8 (running 3.6.9 here at the moment), shutil transfer packets are 16kb which is pretty disagreeable 
# moving back and forth on a Windows system like our archive destination. It appears this could have been what led to
# a failure after downloading everything and moving over.
def copy_large_file(src, dst):
    '''
    Copy a large file showing progress.
    '''
    print('copying "{}" --> "{}"'.format(src, dst))
    if os.path.exists(src) is False:
        print('ERROR: file does not exist: "{}"'.format(src))
        sys.exit(1)
    if os.path.exists(dst) is True:
        os.remove(dst)
    if os.path.exists(dst) is True:
        print('ERROR: file exists, cannot overwrite it: "{}"'.format(dst))
        sys.exit(1)

    # Start the timer and get the size.
    start = time.time()
    size = os.stat(src).st_size
    print('{} bytes'.format(size))

    # Adjust the chunk size to the input size.
    divisor = 10000  # .1%
    # Setting to round as chunk needs to be an integer.
    chunk_size = round(size / divisor)
    # But if file size is too small, the chunk size may be too small to run efficiently. Setting chunk to 10MB min.
    if chunk_size < (10 * 1000 * 1000):
    	chunk_size = (10 * 1000 * 1000)
    while chunk_size == 0 and divisor > 0:
        divisor /= 10
        chunk_size = size / divisor
    print('chunk size is ~{}MB'.format(round(chunk_size/(1000*1000),2)))

    # Copy.
    try:
        with open(src, 'rb') as ifp:
            with open(dst, 'wb') as ofp:
                copied = 0  # bytes
                chunk = ifp.read(chunk_size)
                while chunk:
                    # Write and calculate how much has been written so far.
                    ofp.write(chunk)
                    copied += len(chunk)
                    per = 100. * float(copied) / float(size)

                    # Calculate the estimated time remaining.
                    elapsed = time.time() - start  # elapsed so far
                    avg_time_per_byte = elapsed / float(copied)
                    remaining = size - copied
                    est = remaining * avg_time_per_byte
                    est1 = size * avg_time_per_byte
                    eststr = 'rem={:>.1f}s, tot={:>.1f}s'.format(est, est1)

                    # Write out the status.
                    sys.stdout.write('\r\033[K{:>6.1f}%  {}  {} --> {} '.format(per, eststr, src, dst))
                    sys.stdout.flush()

                    # Read in the next chunk.
                    chunk = ifp.read(chunk_size)

    except IOError as obj:
        print('\nERROR: {}'.format(obj))
        sys.exit(1)

    sys.stdout.write('\r\033[K')  # clear to EOL
    elapsed = time.time() - start
    print('copied "{}" --> "{}" in {:>.1f}s"'.format(src, dst, elapsed))


###############
#    START    #
###############

print("Begin OGRIP LBRS data download - " + datetime.now().strftime("%F %T"))
print('')

if prj_only > 0 or raw_files_only > 0:
	get_src_data()

else:
	prep_workspace()
	
	get_data()

	# To check if all items in a list of elements are present in a master list: all(item in mlist for item in elist)
	if use_arch_db > 0 and len(geom_mismatch_list) <1 and len(empty_tables_list) < 1 and (all(item in anticipated_omissions for item in omission_list) or len(omission_list) < 1):
		xfer_data(src=f'{db_ws_loc}/{db}', dest=f'{db_arch_loc}/{db}')
		xfer_data(src=f'{db_ws_loc}/SHPs', dest=f'{db_arch_loc}/SHPs')
		clean_workspace()
	elif use_arch_db > 0:
		print('Update incomplete. Transfer to archive halted.')

print('')

if len(omission_list) > 0:
	print('The following layers failed: %s' % omission_list)

if len(empty_tables_list) > 0:
	print('The following layers returned empty: %s' % empty_tables_list)

if len(geom_mismatch_list) > 0:
	print('The following layers do not align properly: %s' % geom_mismatch_list)
	print('Evaluate the files in the raw folder to consider SRS reassignment.')

if len(missing_src_list) > 0:
	print('The following sources were not available: %s' % missing_src_list)

print('')
print("Download completed - " + datetime.now().strftime("%F %T"))

# Note:
"""
Source: https://stackoverflow.com/questions/123198/how-do-i-copy-a-file-in-python
 __________________________________________________________________________________
│     Function     │ Copies │   Copies  │Can use│   source       │   destination  │
│                  │metadata│permissions│buffer │may be directory│may be directory│
 ----------------------------------------------------------------------------------
│shutil.copy       │  No    │    Yes    │  No   │      No        │      Yes       │
│shutil.copyfile   │  No    │    No     │  No   │      No        │      No        │
│shutil.copy2      │  Yes   │    Yes    │  No   │      No        │      Yes       │
│shutil.copyfileobj│  No    │    No     │  Yes  │      No        │      No        │
|shutil.copytree   |  Yes   |    Yes    |  unk  |      Yes       |      Yes       |
 ----------------------------------------------------------------------------------

"""
