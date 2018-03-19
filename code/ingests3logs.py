import json
import os
import time
import datetime
import configparser
import traceback
from dateutil.parser import parse as iso8601dateparser

systemLogFile='createsubsetfroms3logs.py'

Config = configparser.ConfigParser()

Config.read(r"..\config\extract.cfg")
extractPath = Config.get('FilePaths', 'extractPath')

Config.read(r"..\config\subset.cfg")
subsetPath= Config.get('FilePaths', 'subsetPath')
subsetDonePath= Config.get('FilePaths', 'subsetDonePath')
s3LogFileDateFormat= Config.get('DateFormats', 's3LogFileDateFormat')
subsetLogLineDateFormat= Config.get('DateFormats', 'subsetLogLineDateFormat')
subsetLogFileDateFormat= Config.get('DateFormats', 'subsetLogFileDateFormat')
logReadySuffix= Config.get('FileReady', 'logReadySuffix')
minDate=Config.get('TimeInterval', 'minDate')
maxDate=Config.get('TimeInterval', 'maxDate')


Config.read(r"..\config\log.cfg")
sysLogDateFormat= Config.get('DateFormats', 'sysLogDateFormat')

print('starting...' + time.strftime(sysLogDateFormat))

if not os.path.exists(subsetPath):
    print("Making subset directory")
    os.mkdir(subsetPath)
if not os.path.exists(subsetPath):
    print("Making subset done directory")
    os.mkdir(subsetPath)

requestStartDate=''
requestDuration=''
url=''
methodName=''
methodType=''

jsondata = {}

if os.path.isfile('../data.txt'):
    os.remove('../data.txt')

while True:

    logFiles = [f for f in sorted(os.listdir(extractPath)) if os.path.isfile(os.path.join(extractPath, f))]

    subsetFilesDict=dict()

    for logFile in logFiles:

        try:
            print('start for', logFile)

            fileTime = datetime.datetime.strptime(logFile, s3LogFileDateFormat)

            subsetFiles = [sF for sF in sorted(os.listdir(subsetPath)) if os.path.isfile(os.path.join(subsetPath, sF)) and sF[-2:]!=logReadySuffix]

            print('subsetFiles', subsetFiles)

            for subsetFileKey in list(subsetFiles):
                subsetFilesDict[subsetFileKey] = open(subsetPath + subsetFileKey, 'a')

            print('subsetFilesDict', subsetFilesDict)

            #preparing subset files to append subset log lines for an -2 and +2 minutes of interval
            for i in range(-2,3):

                subsetFileKey=(fileTime+datetime.timedelta(0,i*60)).strftime(subsetLogLineDateFormat)

                if subsetFileKey not in subsetFilesDict.keys():
                    # if os.path.isfile(subsetPath + subsetFileKey):
                    #     os.remove(subsetPath + subsetFileKey)
                    #
                    # if os.path.isfile(subsetPath + subsetFileKey + logReadySuffix):
                    #     os.remove(subsetPath + subsetFileKey + logReadySuffix)

                    subsetFilesDict[subsetFileKey]=open(subsetPath + subsetFileKey, 'a')

            print('subsetFilesDict', subsetFilesDict)

            for subsetFileName in list(subsetFiles):

                subsetFileDate=datetime.datetime.strptime(subsetFileName,subsetLogFileDateFormat)

                if fileTime-subsetFileDate >datetime.timedelta(0, 3*60) :
                    if subsetFileName in subsetFilesDict.keys():
                        del subsetFilesDict[subsetFileName]
                    os.rename(subsetPath + subsetFileName, subsetPath + subsetFileName + logReadySuffix)

                if fileTime-subsetFileDate  >  datetime.timedelta(0, 24*60*60):

                    if subsetFileName in subsetFilesDict.keys():
                        subsetFilesDict[subsetFileName].close()
                    os.remove(subsetPath + subsetFileName + logReadySuffix)
                    del subsetFilesDict[subsetFileName]

            with open(extractPath+logFile, 'r',encoding="utf8") as fp:
                print('json parse start for', fp.name, 'at', time.strftime(sysLogDateFormat))
                cnt=0
                keys = []
                for line in fp:
                    jsonLogLine = json.loads(line)

                    exception = 0
                    if jsonLogLine.get("Arguments"):
                        exception=1
                    if exception != 1:
                        eventType = jsonLogLine.get("EventType",'nan')
                        if eventType == "BusinessServiceCall" or eventType == "BusinessServiceException":

                            requestStartDate = iso8601dateparser(jsonLogLine.get("Request", {}).get("Start", {}).get("Date")).strftime(subsetLogLineDateFormat)
                            methodName = jsonLogLine.get("Method", {}).get("Name", "nan")
                            methodType = jsonLogLine.get("Method", {}).get("Type", "nan")
                            requestDuration = jsonLogLine.get("Request", {}).get("Duration")

                            if "Http" in jsonLogLine:
                                if "Url" in jsonLogLine["Http"]:
                                    if "$ctx" in jsonLogLine["Http"]["Url"]:
                                        url = jsonLogLine["Http"]["Url"]["$ctx"][0]["$v"]
                            if eventType == "BusinessServiceCall":
                                line = '"' + str(cnt) + '","nrml","' + str(requestStartDate) + '","' +str(url) + '","' + str(
                                    requestDuration) + '","' + str(methodName) + '","' + str(methodType) + '"'

                            if eventType == "BusinessServiceException":
                                line = '"' + str(cnt) + '","err","' + str(requestStartDate) + '","' +str(url) + '","' + str(
                                    requestDuration) + '","' + str(methodName) + '","' + str(methodType) + '"'

                            subsetFilesDict[requestStartDate].write(str(line)+"\n")
                    cnt += 1

            os.remove(extractPath + logFile)
            print('json parse end for', fp.name,'at', time.strftime(sysLogDateFormat))
        except:
            print('file', logFile,'is being copied')
            traceback.print_exc()
            time.sleep(2)
            #traceback.print_exc()