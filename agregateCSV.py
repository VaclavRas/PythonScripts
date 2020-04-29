###########################
# created by:  Vaclav Ras #
# created at:  25.04.2020 #
# last mod:             - #
# version:          1.0.0 #
###########################

import getopt, sys, os, errno, re, time
import pandas as pd
import numpy as np

######################################################
# Global variables                                   #
######################################################
gdStartTime = time.time()
gbDebugOn = False
gsOutputFolder = 'AgregatedResults'
gsShortOptions = 'hd:o:g'
gaLongOptions = ['help', 'dataset=', 'outputdir=', 'debugmode']
gsHelp = """
------------------------ H E L P ------------------------
long argument   short argument  mandatory  description
---------------------------------------------------------
--help          -h              no          
--dataset       -d              yes        dataset path in csv format (f.e. 'data.csv')
--outputdir     -o              no         name of output folder (f.e. 'AgregatedResults')
--debugmode     -g              no         to turn on debug mode (default is debug off)
---------------------------------------------------------
"""

######################################################
# Functions                                          #
######################################################

def debug(iContent):
  if gbDebugOn:
    print(iContent)

# File has to be ".csv" and must exists
def isCSVFile(isCheckedFile):
  lbResult = False
  if re.search('\.csv$', isCheckedFile):
    try:
      f = open(isCheckedFile) 
      lbResult = True
      f.close()
    except IOError:
      print('File not accessible')      
  else:
    print('File not in ".csv" format')

  return lbResult

# Function save multiple files to folder by distinct first column value in input dataframe
# Each file is named by first column value
def saveToCSVByDistinctColumnValue(idfData):
  # Creates folder if not exists yet
  try:
    os.makedirs(gsOutputFolder, exist_ok=True)
  except OSError as e:
    if e.errno != errno.EEXIST:
      raise
  # Saves dataframe to files by first column values
  lnCounter = 0
  for lsFilename in idfData[idfData.keys()[0]].unique():
    lnCounter = lnCounter + 1 
    debug(lsFilename)
    # Each file saving
    idfData.query(idfData.keys()[0] + ' == "'+lsFilename+'"')\
           .to_csv(gsOutputFolder + '\\' + lsFilename + '.csv', encoding ='utf-8', index = False)
  
  print('Data were aggregated into ' + str(lnCounter) + ' files in "' + gsOutputFolder + '" folder.\n'+\
        'Execution time: ' + str(round(time.time() - gdStartTime, 2)) + 's')

def agregate(isDatasetPath):
  if not isCSVFile(isDatasetPath):
    sys.exit()

  # Load dataset from csv file (only used columns for better performence)
  ldfData = pd.read_csv(filepath_or_buffer = isDatasetPath, \
                        usecols = ['ProtocolName', 'Timestamp', 'Destination.IP',\
                                   'Total.Length.of.Bwd.Packets', 'Total.Backward.Packets',\
                                   'Total.Length.of.Fwd.Packets', 'Total.Fwd.Packets'],\
                        index_col = 'ProtocolName')

  # Normalize Timestamp to DateHour format (yyyymmdd hh)
  ldfData['DateHour'] = pd.to_datetime(ldfData['Timestamp'], format = '%d/%m/%Y%H:%M:%S').dt.strftime('%Y%m%d %H (%A)')
  ldfData['TotalPackets'] = ldfData['Total.Fwd.Packets'] + ldfData['Total.Backward.Packets']
  ldfData['TotalBytes'] = ldfData['Total.Length.of.Fwd.Packets'] + ldfData['Total.Length.of.Bwd.Packets']
  ldfData.drop(columns = ['Timestamp', 'Total.Fwd.Packets', 'Total.Backward.Packets', 'Total.Length.of.Fwd.Packets', 'Total.Length.of.Bwd.Packets'], axis = 1, inplace = True)
  ldfData.rename(columns = {'Destination.IP': 'DestinationIP'}, inplace = True)
  
  # Agregate
  ldfAgregated = ldfData.reset_index()\
                        .groupby(['DateHour', 'ProtocolName', 'DestinationIP'], as_index=False)\
                        .sum()
  # Save dataframe to files by DateHour
  saveToCSVByDistinctColumnValue(ldfAgregated)


######################################################
# MAIN                                               #
######################################################

if (len(sys.argv) == 1):
  print('Please use arguments to run the script properly or print "--help" to discover more informations.')
  sys.exit()

# input params error catch
try:
  arguments, values = getopt.getopt(sys.argv[1:], gsShortOptions, gaLongOptions)
except getopt.error as err:
  # Output error, and return with an error code
  print(str(err))
  print('Please use arguments to run the script properly or print "--help" to discover more informations.')
  sys.exit()

# Evaluate given options
for current_argument, current_value in arguments:
  if current_argument in ('-d', '--dataset'):
    lsDataset = current_value
  elif current_argument in ('-o', '--outputdir'):
    gsOutputFolder = current_value
  elif current_argument in ('-g', '--debugmode'):
    gbDebugOn = True
  elif current_argument in ('-h', '--help'):
    print(gsHelp)

if 'lsDataset' in locals():
  agregate(lsDataset)