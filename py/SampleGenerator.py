import random as rd
import datetime as dt
import argparse
import os
import boto3
from boto3.s3.transfer import TransferConfig
import time

parser = argparse.ArgumentParser()
parser.add_argument("recRange", help="Number of Records in each file Ex: 100:200", type=str )
parser.add_argument("numOfFile", help="Number of Files", type=int)
args = parser.parse_args()



currencies = ['CAD','HKD','ISK','PHP','DKK','HUF','CZK','GBP','RON','SEK','IDR','INR','BRL','RUB',
 'HRK','JPY','THB','CHF','EUR','MYR','BGN','TRY','CNY','NOK','NZD','ZAR','USD','MXN',
 'SGD','AUD','ILS','KRW','PLN']

# rec_range = int(input("Enter Number of Records: "))


# cntNumberOfFile = 0
# numberofFile = 5
# recRange = 2
# noOfFiles = 2

fileNameList = []

############ File Generator Function
def fileGenerator(recRange):
    global fileNameList
    recRangeBounds = (recRange).split(':')
    recRangeLbound = int(recRangeBounds[0])
    recRangeUbound = int(recRangeBounds[1])
    recRandomUbound = int(rd.randrange(recRangeLbound, recRangeUbound))

    curtime = dt.datetime.now()
    fileID = curtime.strftime("%Y%m%d%H%M%S")
    fileName = "SampleFile_{0}.csv".format(fileID)
    savePath = "../samplefiles/"
    completeFileName = os.path.join(savePath,fileName)
    fileNameList.append(completeFileName)
    print(f'========= Generating File: {fileName} =========================')
    with open(completeFileName, "w") as fwriter:
            for counter in range(1, recRandomUbound):
                line = ("{base};{cur_1} {cur_2} {cur_3} {cur_4} {cur_5}".format(base=currencies[rd.randrange(1,33)], 
                            cur_1=currencies[rd.randrange(1,33)], 
                            cur_2=currencies[rd.randrange(1,33)], 
                            cur_3=currencies[rd.randrange(1,33)], 
                            cur_4=currencies[rd.randrange(1,33)],
                            cur_5=currencies[rd.randrange(1,33)]))
                fwriter.write(line)

                if(counter != recRandomUbound - 1):
                    fwriter.write("\n")
                #print(line)
############### File Uploader Function
def fileUploader(fileName):
    fileStats = os.stat(fileName)
    fileSize = int(fileStats.st_size /( 1024 * 1024))
    fileBaseName = os.path.basename(fileName)
    s3 = boto3.client('s3')
    

    if fileSize >= 5:
        print(f'========= Mulitpart Upload =======================================================')
        MB = 1024 ** 2
        config = TransferConfig(multipart_threshold=5*MB, max_concurrency=10)
        s3.upload_file(
            Filename = fileName,
            Bucket = 'team-bdengg-rj',
            Key = fileBaseName,
            Config = config
        )
    else:
        print(f'========= Regular Upload =========================================================')
        s3.upload_file(
            Filename = fileName,
            Bucket =  'team-bdengg-rj',
            Key = fileBaseName
        )

counter = 0

print("==================================================================================")
while counter < args.numOfFile:
    #Generating files
    fileGenerator(args.recRange)
    print(f'========= File generation complete. ==============================================')
    print(f'========= Uploading file: {fileNameList[counter]} ===========')
    #Uploading files
    fileUploader(fileNameList[counter])
    print(f'========= Uploaded successfully ==================================================')
    #Sleep 
    sleepInterval = rd.randrange(60,300)
    print(f'========= Sleep Interval: {sleepInterval: <3}s ===================================================')
    time.sleep(rd.randrange(60,300))
    counter += 1
print("==================================================================================")

