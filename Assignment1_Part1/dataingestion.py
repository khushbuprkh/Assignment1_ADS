
# coding: utf-8

# In[58]:

import pandas as pd
import json
import datetime, time
import boto
import boto.s3
import sys
from boto.s3.key import Key
import glob
import boto3
import botocore
import csv
import os


# In[59]:

ts = time.time()
st = datetime.datetime.fromtimestamp(ts).strftime('%d%m%y%M%S')
st1 = datetime.datetime.fromtimestamp(ts).strftime('%d%m%y')


# In[60]:

# Create logfile.
logfile = open(st+".txt", "a")
def log_entry(s):
    #print('Date now: %s' % datetime.datetime.now())

    timestamp = '[%s] : ' % datetime.datetime.now()
    log_line = timestamp + s + '\n'
    logfile.write(log_line)
    logfile.flush()
  


# In[61]:

with open('config.json') as data_file:    
    configdata = json.load(data_file)
log_entry("Link from config file: "+configdata["link"])


# In[63]:

AWS_ACCESS_KEY_ID = configdata["AWSAccess"]
print(AWS_ACCESS_KEY_ID)
AWS_SECRET_ACCESS_KEY = configdata["AWSSecret"]
print(AWS_SECRET_ACCESS_KEY)
TeamNumber=configdata["team"]

bucket_name = str(TeamNumber) + configdata["state"].lower() + 'assignment1'
print bucket_name
log_entry("S3 bucket has been successfully created.")
conn = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
print conn
bucket = conn.create_bucket(bucket_name, location=boto.s3.connection.Location.DEFAULT)

filename_base_data=configdata["state"]+"_"+configdata["StationId"]
s3 = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
file = filename_base_data+".csv"
exists = False

try:
    s3.Object(bucket_name, file).load()
except botocore.exceptions.ClientError as e:
    if e.response['Error']['Code'] == "404":
        exists = False
        print exists
    else:
        raise
else:
    exists = True
    print exists

if exists==False:
    Listlinks=configdata["basedata_links"]
    length=len(Listlinks)
    for counter in range(0,length):
        link= Listlinks[counter]
        datadf=pd.read_csv(link, sep=',', error_bad_lines=False, index_col=False, dtype='unicode')
        datadf.to_csv("temp"+str(counter)+".csv",index=False)
    interesting_files = glob.glob("*.csv") 
        
    status= os.path.exists(filename_base_data+".csv")
    print status
    if status==False:     
        header_saved = False

        with open(filename_base_data+".csv",'wb') as fout:
            for filename in interesting_files:
                sz=os.path.getsize(filename)
                if sz is not 0:
                    with open(filename) as fin:
                        header = next(fin)
                        if not header_saved:
                            fout.write(header)
                            header_saved = True
                        for line in fin:
                            fout.write(line) 
        print ('Uploading %s to Amazon S3 bucket %s' % (file, bucket_name))
        def percent_cb(complete, total):
            sys.stdout.write('.')
            sys.stdout.flush()
        k = Key(bucket)
        k.key = file
        k.set_contents_from_filename(file, cb=percent_cb, num_cb=10)
        log_entry(file+" has been uploaded to "+bucket_name)
        print("File uploaded.")  
        
    elif status==True:
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
                
if exists==True: 
    print "Base file exists in the bucket"
    log_entry("Base file exists in the bucket"+bucket_name)   


# In[67]:

bucket = conn.lookup(bucket_name)
cnt=0
for counter in bucket.list():
    cnt=cnt+1

if cnt is not 0:
    l = [(k.last_modified, k) for k in bucket]
    key_to_download = sorted(l, cmp=lambda x,y: cmp(x[0], y[0]))[-1][1]
    key_to_download.get_contents_to_filename("LatestDataOnBucket.csv")
    df = pd.read_csv("LatestDataOnBucket.csv", sep=',', error_bad_lines=False, index_col=False, dtype='unicode')
    df['DATE'] = pd.to_datetime(df['DATE'])
    recent_date = df['DATE'].max()        
    print  recent_date
    datadf=pd.read_csv(configdata["link"], sep=',', error_bad_lines=False, index_col=False, dtype='unicode')
    df1 = datadf[datadf['DATE'] > str(recent_date)]
    df1.to_csv("NewData.csv",index=False)
    filename_data=configdata["state"]+"_"+st1+"_"+configdata["StationId"]
    merge_files = ["LatestDataOnBucket.csv",'NewData.csv'] 
    file=filename_data+".csv" 
   
    status= os.path.exists(filename_data+".csv")
    print status
    if status==False:     
        header_saved = False

        with open(filename_data+".csv",'wb') as fout:
            for filename in merge_files:
                    with open(filename) as fin:
                        header = next(fin)
                        if not header_saved:
                            fout.write(header)
                            header_saved = True
                        for line in fin:
                            fout.write(line) 
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
   
    elif status==True: 
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
        
elif cnt==0:
    
    datadf=pd.read_csv(configdata["link"], sep=',', error_bad_lines=False, index_col=False, dtype='unicode')
    filename=configdata["state"]+"_"+st1+"_"+configdata["StationId"]
    file = filename+".csv"
    datadf.to_csv(filename+".csv",index=False)
    log_entry(filename+".csv has been created.")   


# In[68]:

s3 = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
file = filename_data+".csv"
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




# In[ ]:




# In[ ]:




# In[ ]:


    


# In[ ]:



