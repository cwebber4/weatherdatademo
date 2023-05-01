#Copyright 2023 Chris Webber

import argparse
from lxml import html
import glob
import os
import psycopg
from pprint import pprint
import shutil
import urllib.request

import downloadstatus


#ghcnyearlydownload.py -k -p -d .\tmp 2010 USW00003017,USW00023066,USC00051401,USC00053005
#ghcnyearlydownload.py -k -d .\tmp 1900 USW00003017


BASE_URL = ""
DEFAULT_TMP_PATH = "./tmp"
connectionString = ""

elementsCollected = ('SNOW', 'PRCP', 'TMIN', 'TMAX')

def parseArguments():
    optparse = argparse.ArgumentParser(prog="ghcnyearlydownload")
    optparse.add_argument('years', help="Comma separated years to be downloaded. Matches based on the starting numbers, so \"190\" will match 1900 through 1909")
    optparse.add_argument('stations', help="Comma separated stations to collect data for")
    optparse.add_argument('-d', '--dir', dest="downloadDir", default=DEFAULT_TMP_PATH, help="The directory to download files to")
    optparse.add_argument('-k', '--keep', dest="keepFiles", action="store_true", help="Keep the downloaded files after processing")
    optparse.add_argument('-o', '--download-only', dest="downloadOnly", action="store_true", help="Keep the downloaded files after processing")
    optparse.add_argument('-p', '--process-only', dest="processOnly", action="store_true", help="Process the existing files for the given years in the directory specified by -d")

    args = optparse.parse_args()

    return args


def getFileLinks(years):
    req = urllib.request.Request(url=BASE_URL)
    con = urllib.request.urlopen(req)
    page = con.read()
    tree = html.fromstring(page)
    linkElems = tree.xpath("//table/tr/td/a")

    fileLinks = []
    for year in years:
        found = False
        for linkElem in linkElems:
            href = linkElem.get('href').strip()
            #check startswith to allow for easy bulk collections.
            if href.startswith(year):
                fileLinks.append("/".join([BASE_URL, href]))
                found = True
        if not found:
            print("Unable to find files starting with {0}".format(year))
    
    return fileLinks


def setupDownloadDir(downloadDir):
    #TODO: check os.path.isfile(downloadDir)
    if not os.path.isdir(downloadDir):
        print("Creating download directory {0}".format(downloadDir))
        os.makedirs(downloadDir)
    else:
        print("Using download directory {0}".format(downloadDir))


def cleanupDownloadDir(downloadDir):
    print()
    print("Deleting download directory {0}".format(downloadDir))
    shutil.rmtree(downloadDir)
    
    
def deleteFile(filePath):
    print()
    print("Deleting file {0}".format(filePath))
    os.remove(filePath)


def downloadFile(fileLink, downloadDir):
    downloadPath = os.path.join(downloadDir, fileLink.split("/")[-1])
    print()
    print("Downloading file {0} to {1}".format(fileLink, downloadPath))
    print()
    urllib.request.urlretrieve(fileLink, filename=downloadPath, reporthook=downloadstatus.reportStatus)
    print()
    
    return downloadPath


def getExistingFiles(years, downloadDir):
    existingFiles = [f for f in os.scandir(downloadDir)]
    filePaths = set()
    for year in years:
        files = glob.glob(os.path.join(downloadDir, year + "*.gz"))
        if not files:
            print("Unable to find files starting with {0}".format(year))
        else:
            for file in files:
                filePaths.add(file)
    
    return list(filePaths)


def execInsert(cur, valueData):
    queryBase = "INSERT INTO ghcn.daily_raw (StationID, Date, Element, Value, MFlag, QFlag, SFlag, OBSTime) VALUES "
    values = ",".join([cur.mogrify("(%s, %s, %s, %s, %s, %s, %s, %s)", row) for row in valueData])
    query = (queryBase + values + ";")
    cur.execute(query)


def execStoredProc(cur):
    query = "select ghcn.stp_daily();"
    cur.execute(query)


def insertData(data):
    print()
    print("Inserting data...")
    if data:
        with psycopg.connect(connectionString, cursor_factory=psycopg.ClientCursor) as conn:
            with conn.cursor() as cur:
                rowData = []
                rowCount = 0
                for datum in data:
                    row = [datum["StationID"], datum["Date"], datum["Element"], datum["Value"], datum["MFlag"], 
                        datum["QFlag"], datum["SFlag"], datum["OBSTime"]]
                        
                    rowData.append(row)
                    rowCount += 1
                
                    if len(rowData) % 100 == 0:
                        execInsert(cur, rowData)
                        rowCount = 0
                        rowData = []
                    
                if rowData:
                    execInsert(cur, rowData)
                    
                execStoredProc(cur)
    else:
        print("No data to insert.")


def processFile(filePath, tmpDir, stations):
    unzipCmdTmpl = "7z e -o{0} {1}"
    unzipCmd = unzipCmdTmpl.format(tmpDir, filePath)
    
    print("Unzipping file {0}...".format(filePath))
    os.system(unzipCmd)

    zippedFileName = filePath.split(os.path.sep)[-1]
    unzippedFile = zippedFileName.replace(".gz", "")
    unzippedFilePath = os.path.join(tmpDir, unzippedFile)
    
    # print(unzippedFilePath)
    print()
    print("Parsing file...")
    stationsList = stations.split(",")
    data = []
    with open(unzippedFilePath, "r") as file:
        # count = 1
        for line in file:
            parts = line.split(",")
            parts = [part.strip() for part in parts]
            id = parts[0]
            element = parts[2]
            if id in stationsList and element in elementsCollected:
                row = {
                    "StationID" : id,
                    "Date" : parts[1],
                    "Element" : element,
                    "Value" : parts[3],
                    "MFlag" : parts[4] if parts[4] else None,
                    "QFlag" : parts[5] if parts[5] else None,
                    "SFlag" : parts[6] if parts[6] else None,
                    "OBSTime" : parts[7] if parts[7] else None
                }
                data.append(row)
            
                # count += 1
                # if count > 5:
                    # break
    
    # print("#" * 80)
    # pprint(data)
    # print("#" * 80)
    
    print("{0} rows collected.".format(len(data)))
    
    os.remove(unzippedFilePath)
    
    insertData(data)


if __name__ == "__main__":
    args = parseArguments()
    years = [year for year in args.years.split(",") if year.strip() != ""]
    setupDownloadDir(args.downloadDir)
    print()
    if args.processOnly:
        filePaths = getExistingFiles(years, args.downloadDir)
        for filePath in filePaths:
            processFile(filePath, args.downloadDir, args.stations)
            if not args.keepFiles:
                deleteFile(filePath)
    else:
        fileLinks = getFileLinks(years)
        for fileLink in fileLinks:
            filePath = downloadFile(fileLink, args.downloadDir)
        
            if not args.downloadOnly:
                processFile(filePath, args.downloadDir, args.stations)
                if not args.keepFiles:
                    deleteFile(filePath)
    
    print()
    
    if not args.keepFiles:
        print()
        cleanupDownloadDir(args.downloadDir)