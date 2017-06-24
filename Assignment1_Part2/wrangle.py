
# coding: utf-8

# # Data Wrangling
#     
# 
# 

# ## 1.Importing all the packages, modules

# In[65]:

import pandas as pd
import numpy as np
import datetime, time
import operator
import boto
import boto.s3
from boto.s3.key import Key
from boto.s3.cors import CORSConfiguration
import glob
import boto3
import botocore
import csv
import re
import json
import os
import sys
from scipy import interpolate
flag = 0
tempFullDataList =[]
tempSODDataList =[]


# In[66]:

#fetching the timestamp
ts = time.time()
st = datetime.datetime.fromtimestamp(ts).strftime('%d%m%y%M%S')
st1 = datetime.datetime.fromtimestamp(ts).strftime('%d%m%y')


# In[67]:

# Create logfile.
logfile = open(st+".txt", "a")
def log_entry(s):
    #print('Date now: %s' % datetime.datetime.now())

    timestamp = '[%s] : ' % datetime.datetime.now()
    log_line = timestamp + s + '\n'
    logfile.write(log_line)
    logfile.flush()
log_entry("Import Done")


# In[50]:

with open('configWrangle.json') as data_file:    
    configdata = json.load(data_file)
log_entry("Raw Data Link from config file: "+configdata["rawData"])
log_entry("Clean Data Link from config file: "+configdata["cleanData"])
raw_link=configdata["rawData"]
clean_link=configdata["cleanData"]


# In[51]:

# connect to AWS
AWS_ACCESS_KEY_ID = configdata["AWSAccess"]
print(AWS_ACCESS_KEY_ID)
AWS_SECRET_ACCESS_KEY = configdata["AWSSecret"]
print(AWS_SECRET_ACCESS_KEY)
TeamNumber=configdata["team"]
conn = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
s3 = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)


# In[52]:

#check for raw data file  on local 
status= os.path.exists(raw_link)
print status  


# In[53]:

#if file exists get file form the local
if status==True:
    file=raw_link
    print("File found on Local")
    log_entry("File found on Local")
elif status==False:
    file=raw_link
    bucket_name = str(TeamNumber) + configdata["state"].lower() + 'assignment1'
    my_bucket = conn.get_bucket(bucket_name, validate=False)
    print my_bucket
    k = Key(my_bucket)
    exists = False

    try:
        s3.Object(bucket_name, file).load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            exists = False
        else:
            raise
    else:
        exists = True
    
    if exists==False:
        print("File not found.")
        log_entry("File not found.")
    elif exists==True:
        print("File exists.")
        log_entry("File exists.")
        cors_cfg = CORSConfiguration()
        cors_cfg.add_rule(['PUT', 'POST', 'DELETE'], 'https://www.example.com', allowed_header='*', max_age_seconds=3000, expose_header='x-amz-server-side-encryption')
        cors_cfg.add_rule('GET', '*')
        k = my_bucket.get_key(file, validate=False)
        my_bucket.set_cors(cors_cfg)
        k.get_contents_to_filename(raw_link)
        print k.last_modified
        log_entry(file+" has been uploaded to "+bucket_name)
        print("File Downloaded.")
        log_entry("File downloaded from S3 bucket.")
        file=raw_link


# ## 2.Loading CSV File from the Config file Link 

# In[54]:

#file= "https://www.ncei.noaa.gov/orders/cdo/996279.csv"
#file1='998702.csv'
a=pd.read_csv(file,sep=',', error_bad_lines=False, index_col=False, dtype='unicode')
for i,x in a.iterrows():
         if(x['REPORTTPYE'] != 'SOD'):
                tempFullDataList.append(x)
         elif(x['REPORTTPYE'] =='SOD'):
                tempSODDataList.append(x)
                
print("Step 2 Loading CSV File Done")              
log_entry("Step 2 Loading CSV File Done")


    


# ## 3.Missing Value Analysis

# ## Filling the SkyCondition Data
# <h4>A. We are filling the missing value with the value present in the previous hour , as it is the most approximate sky condition </h4>

# In[55]:

def missingSkyConditionData():
	for i in range(len(tempFullDataList)):
		try:
			a = int(tempFullDataList[i][7])
			prev = tempFullDataList[i-1][7].split()
			if 'CLR:00' not in prev:
				prevStr = prev[0]+ ' '+ tempFullDataList[i][7]
			else:
				prevStr = tempFullDataList[i][7]

			tempFullDataList[i][7] = prevStr
            
		except:
			continue

temp = missingSkyConditionData()

print("Sky Condition")
log_entry("Sky Condition")       
    



# <h4> B. Removing Alphabets present at the end of the Numbers present in the Columns </h4>

# <h4>Defining the Functions </h4>

# In[56]:

def removeAlphabetsAndInterpolate(specificColumn):
    non = re.compile(r'[^\d.+-]+')
    tempp=[] 
    for i in specificColumn:
        tempp.append(str(non.sub('',str(i))))
    g =pd.to_numeric(tempp)
    visi = pd.DataFrame(g)
#     visi=visi.interpolate(method='spline', order=2).bfill()
    return visi[0]

def removeAlphabetsOnly(specificColumn):
    non = re.compile(r'[^\d.+-]')
    temp=[]
    for i in specificColumn:
        temp.append(str(non.sub('',str(i))))
    g =pd.to_numeric(temp)
    output= pd.DataFrame(g)
    return output

print("Functions Defined")
log_entry("Functions Defined")


# ## Invoking the declared functions from above. 
# ### Notice we have chose interpolation function - Spline of degree 2 from the Scipy package to make up the missing values as smooth as possible on the assumption
# <p>Hourly Precipitation with Tag "T"  denotes <0.01 , so we have assumed 0.005 to make the column all numeric values</p>
# <p>Wind Gust Speed is assumed 0 miles/hr where ever there are NAN in the column</p>
# 

# ## Moving Hourly Data seperately to analyse

# In[57]:

def removeJunk():
    dataf = pd.DataFrame(tempFullDataList)
    dataf["HOURLYVISIBILITY"]=dataf["HOURLYVISIBILITY"].astype(str).map(lambda x:x.strip('V'))
    dataf["HOURLYVISIBILITY"]=dataf["HOURLYVISIBILITY"].interpolate(method='spline', order=2)
    dataf["HOURLYDRYBULBTEMPF"]=dataf["HOURLYDRYBULBTEMPF"].astype(str).map(lambda x:x.strip('s'))
    dataf["HOURLYDRYBULBTEMPC"]=dataf["HOURLYDRYBULBTEMPC"].astype(str).map(lambda x:x.strip('s'))
    dataf["HOURLYWETBULBTEMPF"]=dataf["HOURLYWETBULBTEMPF"].astype(str).map(lambda x:x.strip('s'))
    dataf["HOURLYWETBULBTEMPC"]=dataf["HOURLYWETBULBTEMPC"].astype(str).map(lambda x:x.strip('s'))
    dataf["HOURLYDewPointTempF"]=dataf["HOURLYDewPointTempF"].astype(str).map(lambda x:x.strip('s'))
    dataf["HOURLYDewPointTempC"]=dataf["HOURLYDewPointTempC"].astype(str).map(lambda x:x.strip('s'))
    dataf["HOURLYRelativeHumidity"]=dataf["HOURLYRelativeHumidity"].astype(str).map(lambda x:x.strip('s'))
    dataf["HOURLYWindSpeed"]=dataf["HOURLYWindSpeed"].astype(str).map(lambda x:x.strip('s'))
    dataf["HOURLYWindDirection"]=dataf["HOURLYWindDirection"].astype(str).map(lambda x:x.strip('s'))
    dataf["HOURLYWindGustSpeed"].fillna(0) 
    dataf["HOURLYStationPressure"]=dataf["HOURLYStationPressure"].astype(str).map(lambda x:x.strip('s'))
    dataf["HOURLYPressureTendency"]=dataf["HOURLYPressureTendency"].astype(str).map(lambda x:x.strip('s'))
    dataf["HOURLYPressureChange"]=dataf["HOURLYPressureChange"].astype(str).map(lambda x:x.strip('s'))
    dataf["HOURLYSeaLevelPressure"]=dataf["HOURLYSeaLevelPressure"].astype(str).map(lambda x:x.strip('s'))
    dataf["HOURLYPrecip"]=dataf["HOURLYPrecip"].astype(str).map(lambda x:x.strip('s'))
    dataf["HOURLYAltimeterSetting"]=dataf["HOURLYAltimeterSetting"].astype(str).map(lambda x:x.strip('s'))
    dataf["DAILYPrecip"]=dataf["DAILYPrecip"].astype(str).map(lambda x:x.strip('s'))
    dataf["HOURLYVISIBILITY"]=dataf["HOURLYVISIBILITY"].astype(str).map(lambda x:x.strip('s'))
#     print(dataf["HOURLYVISIBILITY"].unique())
    return dataf
    
    
    
dataHours = removeJunk()
#important
# removeAlphabetsAndInterpolate(dath["HOURLYVISIBILITY"]).interpolate(method='spline', order=2).bfill()
dataHours
    


# In[58]:


# def interpolateMissingData():
#     dataf = pd.DataFrame(tempFullDataList)
# #     non = re.compile(r'[^\d.+-]+')
# #     for i in dataf["HOURLYVISIBILITY"]:
# #         print i
# #     non.sub('',dataf["HOURLYVISIBILITY"])
   
# #     dataf["HOURLYVISIBILITY"].str.replace("1","77777")
# #     dataf["HOURLYVISIBILITY"].fillna("-99",inplace=True)

#     variable = removeAlphabetsAndInterpolate(dataf["HOURLYVISIBILITY"])
#     removeAlphabetsAndInterpolate(dataf["HOURLYDRYBULBTEMPF"])
#     removeAlphabetsAndInterpolate(dataf["HOURLYDRYBULBTEMPC"])
#     removeAlphabetsAndInterpolate(dataf["HOURLYWETBULBTEMPF"])
#     removeAlphabetsAndInterpolate(dataf["HOURLYWETBULBTEMPC"])
#     removeAlphabetsAndInterpolate(dataf["HOURLYDewPointTempF"])
#     removeAlphabetsAndInterpolate(dataf["HOURLYDewPointTempC"])
#     removeAlphabetsAndInterpolate(dataf["HOURLYRelativeHumidity"])
#     removeAlphabetsAndInterpolate(dataf["HOURLYWindSpeed"])
#     removeAlphabetsAndInterpolate(dataf["HOURLYWindDirection"])
#     dataf["HOURLYWindGustSpeed"].fillna(0) 
#     removeAlphabetsAndInterpolate(dataf["HOURLYStationPressure"])
#     removeAlphabetsOnly(dataf["HOURLYPressureTendency"])
#     removeAlphabetsOnly(dataf["HOURLYPressureChange"])
#     removeAlphabetsAndInterpolate(dataf["HOURLYSeaLevelPressure"])
#     removeAlphabetsAndInterpolate(dataf["HOURLYPrecip"]).fillna(0.005)
#     removeAlphabetsAndInterpolate(dataf["HOURLYAltimeterSetting"])
#     removeAlphabetsAndInterpolate(dataf["DAILYPrecip"])
#     return dataf["HOURLYVISIBILITY"]

# # dataf = pd.DataFrame(tempFullDataList)
# # jack1 = pd.DataFrame(dataf["HOURLYVISIBILITY"])
# # jack1.to_csv("jack1.csv")

# # temp=[]
# # for i in dataf["HOURLYVISIBILITY"]:
# #         temp.append(str(i).replace(r'V','').replace(r's',''))
        
# # print(temp)
# # jack = pd.DataFrame(dataf["HOURLYVISIBILITY"])
# # jack.to_csv("jack.csv")
# # dataf
# gal = interpolateMissingData()
# g = pd.DataFrame(gal)
# g.to_csv("Extrat.csv")
# print(gal)

# print("Interpolation of data done")


# <h4>Moving SOD Data  to seperate DataFrame to analyse</H4>

# In[59]:

def interpolateMissingSODData():
    dataf = pd.DataFrame(tempSODDataList)
    dataf["DAILYMaximumDryBulbTemp"]       = dataf["DAILYMaximumDryBulbTemp"].astype(str).map(lambda x:x.strip('s'))
    dataf["DAILYMinimumDryBulbTemp"]     = dataf["DAILYMinimumDryBulbTemp"].astype(str).map(lambda x:x.strip('s'))
    dataf["DAILYAverageDryBulbTemp"]     = dataf["DAILYAverageDryBulbTemp"].astype(str).map(lambda x:x.strip('s'))
    dataf["DAILYDeptFromNormalAverageTemp"]     = dataf["DAILYDeptFromNormalAverageTemp"].astype(str).map(lambda x:x.strip('s'))
    dataf["DAILYHeatingDegreeDays"]     = dataf["DAILYHeatingDegreeDays"].astype(str).map(lambda x:x.strip('s'))
    dataf["DAILYCoolingDegreeDays"]     = dataf["DAILYCoolingDegreeDays"].astype(str).map(lambda x:x.strip('s'))
    dataf["DAILYPrecip"]     = dataf["DAILYPrecip"].astype(str).map(lambda x:x.strip('s'))
    dataf["DAILYAverageStationPressure"]     = dataf["DAILYAverageStationPressure"].astype(str).map(lambda x:x.strip('s'))
    dataf["DAILYAverageWindSpeed"]     = dataf["DAILYAverageWindSpeed"].astype(str).map(lambda x:x.strip('s'))
    dataf["DAILYPeakWindSpeed"]     = dataf["DAILYPeakWindSpeed"].astype(str).map(lambda x:x.strip('s'))
    dataf["PeakWindDirection"]     = dataf["PeakWindDirection"].astype(str).map(lambda x:x.strip('s'))
    dataf["DAILYSustainedWindSpeed"]     = dataf["DAILYSustainedWindSpeed"].astype(str).map(lambda x:x.strip('s'))
    dataf["DAILYSustainedWindDirection"]     = dataf["DAILYSustainedWindDirection"].astype(str).map(lambda x:x.strip('s'))
    return dataf

DailySOD = interpolateMissingSODData()
print("Interpolation of data done")
log_entry("Interpolation of data done")


# ## Extracting the Monthly Data Seperately to analyse

# In[60]:

temp = a["DATE"]
from calendar import monthrange
import calendar
import datetime


tempSODMonthlyDataList =[]

for m in tempSODDataList:
    datee = datetime.datetime.strptime((m["DATE"].split()[0]), "%Y-%m-%d")
    if(calendar.monthrange(datee.year,datee.month)[1]==datee.day):
        tempSODMonthlyDataList.append(m)

monthData =pd.DataFrame(tempSODMonthlyDataList)
# monthData.to_csv("xxx.csv")


# ### Interpolating the missing data : Removing the junks (alphabets) present at the end of the number  

# In[61]:

def interpolateMissingSODMonthlyData():
    mdataf = pd.DataFrame(tempSODMonthlyDataList)
    mdataf["MonthlyMaximumTemp"]       = mdataf["MonthlyMaximumTemp"].astype(str).map(lambda x:x.strip('V'))
    mdataf["MonthlyMinimumTemp"]     = mdataf["MonthlyMinimumTemp"].astype(str).map(lambda x:x.strip('s'))
    mdataf["MonthlyMeanTemp"]     = mdataf["MonthlyMeanTemp"].astype(str).map(lambda x:x.strip('s'))
    mdataf["MonthlyStationPressure"]     = mdataf["MonthlyStationPressure"].astype(str).map(lambda x:x.strip('s'))
    mdataf["MonthlySeaLevelPressure"]     = mdataf["MonthlySeaLevelPressure"].astype(str).map(lambda x:x.strip('s'))
    mdataf["MonthlyDeptFromNormalMaximumTemp"]     = mdataf["MonthlyDeptFromNormalMaximumTemp"].astype(str).map(lambda x:x.strip('s'))
    mdataf["MonthlyDeptFromNormalMinimumTemp"]     = mdataf["MonthlyDeptFromNormalMinimumTemp"].astype(str).map(lambda x:x.strip('s'))
    mdataf["MonthlyDeptFromNormalAverageTemp"]     = mdataf["MonthlyDeptFromNormalAverageTemp"].astype(str).map(lambda x:x.strip('s'))
    mdataf["MonthlyDeptFromNormalPrecip"]     = mdataf["MonthlyDeptFromNormalPrecip"].astype(str).map(lambda x:x.strip('s'))
    mdataf["MonthlyTotalLiquidPrecip"]     = mdataf["MonthlyTotalLiquidPrecip"].astype(str).map(lambda x:x.strip('s'))
    mdataf["MonthlyTotalHeatingDegreeDays"]     = mdataf["MonthlyTotalHeatingDegreeDays"].astype(str).map(lambda x:x.strip('s'))
    mdataf["MonthlyTotalCoolingDegreeDays"]     = mdataf["MonthlyTotalCoolingDegreeDays"].astype(str).map(lambda x:x.strip('s'))
    mdataf["MonthlyDeptFromNormalHeatingDD"]     = mdataf["MonthlyDeptFromNormalHeatingDD"].astype(str).map(lambda x:x.strip('s'))
    mdataf["MonthlyDeptFromNormalCoolingDD"]     = mdataf["MonthlyDeptFromNormalCoolingDD"].astype(str).map(lambda x:x.strip('s'))
    return mdataf

monthWiseData = interpolateMissingSODMonthlyData()
# monthWiseData.to_csv("yyy.csv")
# monthWiseData["MonthlyMaximumTemp"]


# ## Dividing Clouds Formation Data in various Category 

# In[62]:

def divideInTypes():
	types = {'SCT':{},'BKN':{},'OVC':{},'VV':{},'10':{},'FEW':{},'CLR':{}}
	for i in tempFullDataList:
		row = str(i[7])
		if(len(row) > 3):
			x = i[7].split()
			if 'CLR:00' not in x:
				for j in range(0,len(x),2):
					a = x[j].split(':')
					if a[0]+' '+x[j+1] in types[a[0]]:
						types[a[0]][a[0]+' '+x[j+1]] += 1
					else:
						types[a[0]][a[0]+' '+x[j+1]] = 1
	return types





# ## Dividing the Data into Sub Slots of 
# <p>00:00AM - 5:59AM - Slot1</p>
# <p>6:00AM  - 11:59AM-</p>
#  

# In[63]:

def getByTimeSlots():
    tempDic = {'slot1':[], 'slot2':[],'slot3':[],'slot4':[]}
    
    
    for x,i in dataHours.iterrows():
        time = i['DATE'].split()[1]
        hour = time.split(':')
        if(int(hour[0]) >= 0 and int(hour[0]) < 6):
            if(int(hour[1]) >= 0 and int(hour[1]) <= 59):
                tempDic['slot1'].append(i)
        elif(int(hour[0]) >= 6 and int(hour[0]) < 12):
            if(int(hour[1]) >= 0 and int(hour[1]) <= 59):
                tempDic['slot2'].append(i)
        elif(int(hour[0]) >= 12 and int(hour[0]) < 18):
            if(int(hour[1]) >= 0 and int(hour[1]) <= 59):
                tempDic['slot3'].append(i)
        elif(int(hour[0]) >= 18 and int(hour[0]) < 24):
            if(int(hour[1]) >= 0 and int(hour[1]) <= 59):
                tempDic['slot4'].append(i)
    return tempDic            

 

        
def seasonsExploration(tempDic):
    seasons ={'Springslot1':[],'Summerslot1':[],'Fallslot1':[],'Springslot2':[],
              'Summerslot2':[],'Fallslot2':[],'Springslot3':[],'Summerslot3':[],
              'Fallslot3':[],'Springslot4':[],'Summerslot4':[],'Fallslot4':[]}
    for t,v in tempDic.items():
            print(t)
            for r in v:        
                year = r[5].split()[0]
                month = year.split('-')
                if(int(month[1]) >=1 and int(month[1]) <= 4):
                    seasons['Spring'+t].append(r)
                elif(int(month[1]) >=5 and int(month[1]) <= 8):
                    seasons['Summer'+t].append(r)
                elif(int(month[1]) >=9 and int(month[1]) <= 12):
                    seasons['Fall'+t].append(r)

                    
seasonsExploration(getByTimeSlots())
#     for t,v in seasons.items():
#         dataf = pd.DataFrame(v)
#         dataf.to_csv(t+".csv")
#   


# In[64]:

# uploading Clean data to s3 bucket 
bucket_name = str(TeamNumber) + configdata["state"].lower() + 'assignment1_clean'
print bucket_name
log_entry("S3 bucket has been successfully created.")
conn = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
print conn
bucket = conn.create_bucket(bucket_name, location=boto.s3.connection.Location.DEFAULT)
filename_clean_data=configdata["state"]+"_"+st1+"_"+configdata["StationId"]+"_"+"clean"
print filename_clean_data
dataHours.to_csv(filename_clean_data+".csv")

s3 = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
file = filename_clean_data+".csv"
exists = False

try:
    s3.Object(bucket_name, file).load()
except botocore.exceptions.ClientError as e:
    if e.response['Error']['Code'] == "404":
        exists = False
    else:
        raise
else:
    exists = True

if exists==False:
    print ('Uploading %s to Amazon S3 bucket %s' % (file, bucket_name))
    def percent_cb(complete, total):
        sys.stdout.write('.')
        sys.stdout.flush()
    k = Key(bucket)
    k.key = file
    k.set_contents_from_filename(file, cb=percent_cb, num_cb=10)
    log_entry(file+" has been uploaded to "+bucket_name)
    print("File uploaded.")
    
elif exists==True:
    print("File already exists.")
    log_entry("File already exists.")


# In[ ]:



