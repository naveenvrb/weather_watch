#!/usr/bin/env python3

import os
import re
import sys
import ssl
import gzip
import glob
import html
import getopt
import shutil
import datetime

from urllib import request
from multiprocessing.dummy import Pool as ThreadPool 

# ############################################################ #
# #                           MAIN                           # #
# ############################################################ #
def main(argv):
  
  year = 0
  concurrency = 4
  station_id = None
  
  # Arguments
  try:
    opts, args = getopt.getopt(sys.argv[1:], 'y:c:s:', [ 'year=', 'concurrency=', 'station=' ])
  except getopt.GetoptError:
    sys.exit(2)
  
  for opt, arg in opts:
    
    if opt in ('-y', '--year'):
      try:
        year = int(arg)
        if year < 1901 or year > datetime.datetime.now().year:
          help()
      except Exception as e:
        print(str(e))
        help()
        
    elif opt in ('-c', '--concurrency'):
      concurrency = arg
      try:
        concurrency = int(arg)
      except Exception as e:
        print(str(e))
        help()
        
    elif opt in ('-s', '--station'):
      station_id = arg
      
    else:
      sys.exit(2)
  
  if not station_id:
    help()
  
  # Need this for HTTPS
  ssl._create_default_https_context = ssl._create_unverified_context
  
  isd_url = 'https://www1.ncdc.noaa.gov/pub/data/noaa/isd-lite'
  temp_dir = './data/temp'
  file_list = []
  arg_list = []
  
  if year > 0:
    final_file = './data/{}_{}.txt'.format(station_id, year)
  else:
    final_file = './data/{}.txt'.format(station_id)
  
  print('NOAA ISD Lite Download Utility')
  print('Download data for station_id: {}'.format(station_id))
  
  # (Re)create temp dir
  if not os.path.exists(temp_dir):
    os.makedirs(temp_dir)
  else:
    shutil.rmtree(temp_dir)
    os.makedirs(temp_dir)
  
  # Delete final file if already exists
  if os.path.exists(final_file):
    os.remove(final_file)
  
  # Scan all years for station specific files
  print('Scanning root directory for all year directories')
  response = request.urlopen(isd_url)
  dir_regex = re.compile('<img src="/icons/folder\.gif" alt="\[DIR\]"></td><td><a href="([0-9]{4})/">[0-9]{4}/</a>')
  file_regex = re.compile('<a href="({0}.+?\.gz)">{0}.+?\.gz</a>'.format(station_id))
  dir_list = dir_regex.findall(html.unescape(response.read().decode('utf-8')))
  
  # Restrict to years greater than provided year, if given
  for d in dir_list:
    if int(d) >= year:
      arg_list.append(('{}/{}'.format(isd_url, d), file_list, station_id, temp_dir, file_regex))
  
  # Populate file_list of all valid station files across all years
  threaded_job(concurrency, scan_directory, arg_list)
  
  print('Found {} files for {}'.format(len(file_list), station_id))
  
  # Download discovered files
  threaded_job(concurrency, dl_file, file_list)
  
  # Concatenate all downloaded files into final file
  with open(final_file, 'wb') as final_file_handle:
    for filename in glob.glob('{}/*'.format(temp_dir)):
      with open(filename, 'rb') as part_file:
        shutil.copyfileobj(part_file, final_file_handle)
  
  # Clear out temp
  shutil.rmtree(temp_dir)
  
  print('Generated: {}'.format(final_file))
  
  return

# ############################################################ #
# #                        THREADPOOL                        # #
# ############################################################ #
def threaded_job(concurrency, task, arg_list):
  print('Initializing thread pool with concurrency: {}'.format(concurrency))
  pool = ThreadPool(concurrency)
  pool.map(task, arg_list)
  pool.close()
  pool.join()
  return

# ############################################################ #
# #                      DIRECTORY SCAN                      # #
# ############################################################ #
def scan_directory(argv):
  year_url = argv[0]
  file_list = argv[1]
  station_id = argv[2]
  temp_dir = argv[3]
  file_regex = argv[4]
  
  print('Scanning: {}'.format(year_url))
  
  response = request.urlopen(year_url)
  results = file_regex.findall(html.unescape(response.read().decode('utf-8')))
  for f in results:
    file_list.append(('{}/{}'.format(year_url, f), temp_dir))
  
  return

# ########################################################### #
# #                       FILE DOWNLOAD                      # #
# ########################################################### #
def dl_file(argv):
  file_url = argv[0]
  temp_dir = argv[1]
  filename = file_url.rsplit('/', 1)[-1]
  
  gzip_file = '{}/{}'.format(temp_dir, filename)
  final_file = '{}/{}'.format(temp_dir, filename.split('.')[0])
  
  print('Downloading: {}'.format(filename))
  response = request.urlopen(file_url)
  
  with open(gzip_file, 'wb') as gz_file_handle:
    gz_file_handle.write(response.read())
  with open(final_file, 'wb') as final_file_handle:
    with gzip.open(gzip_file, 'rb') as unzipped_handle:
      final_file_handle.write(unzipped_handle.read())
  os.remove(gzip_file)
  
  return

# ############################################################ #
# #                            HELP                          # #
# ############################################################ #
def help():
  print('Usage: ./noaa_dl.py -y YYYY -c N -s XXXXXX-XXXXX')
  sys.exit(99)

if __name__ == '__main__':
  main(sys.argv)