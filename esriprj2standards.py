#! /usr/bin/env python
# Slightly modified for Python3 and cleaner output from https://gis.stackexchange.com/a/7615

import sys
from osgeo import osr

def esriprj2standards(shapeprj_path):
   prj_file = open(shapeprj_path, 'r')
   prj_txt = prj_file.read()
   srs = osr.SpatialReference()
   srs.ImportFromESRI([prj_txt])
   print('')
   print('Shape prj is:')
   print(prj_txt)
   print('')
   print('WKT is:')
   print(srs.ExportToWkt())
   print('')
   print('Proj4 is:')
   print(srs.ExportToProj4())
   print('')
   srs.AutoIdentifyEPSG()
   print('EPSG is:')
   print(srs.GetAuthorityCode(None))

esriprj2standards(sys.argv[1])

# Then in cmd/terminal:
# python3 esriprj2standards.py target.prj