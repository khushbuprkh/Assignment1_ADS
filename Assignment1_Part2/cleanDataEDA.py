
# coding: utf-8

# # EDA on Raw Data 
#     
# 
# 

# ## 1.Importing all the packages, modules

# In[32]:

import pandas as pd
import numpy as np
import operator
import csv
import re
from scipy import interpolate
flag = 0
tempFullDataList =[]
tempSODDataList =[]

print("Step1 -Import Done ")


# In[35]:

file1='998702.csv'
a=pd.read_csv(file1,sep=',', error_bad_lines=False, index_col=False, dtype='unicode',parse_dates=["DATE"])
for i,x in a.iterrows():
         if(x['REPORTTPYE'] != 'SOD'):
                tempFullDataList.append(x)
         elif(x['REPORTTPYE'] =='SOD'):
                tempSODDataList.append(x)
                
print("Step 2 Loading CSV File Done")


# ## EAD 

# ## Getting the Latest Date for One Day Analysis 

# In[36]:

v = a.tail(2)['DATE'][:-1]
for i in v:
    latestDate=str(i).split()[0].split('-')[2]
print("LatestDate "+latestDate)
dataHours =a


# In[72]:

dataframeHours = pd.DataFrame(dataHours.tail(48)[:-1])
dateATemp = {'Temperature':[],'Time':[]}
for x,i in dataframeHours.iterrows():
# Moving to Seperate Dictionary to Analyse it 
      if(str(i['DATE']).split()[0].split('-')[2]==latestDate):
                dateATemp['Time'].append(str(i['DATE']).split()[1].split(':')[0])
                dateATemp['Temperature'].append(str(i['HOURLYDRYBULBTEMPC']))


# In[70]:

## Relationship between Temperature and DEw Point 


# In[41]:

from bokeh.plotting  import figure,output_file, show
p=figure(width=2500,height=500,x_axis_type="datetime")
p.line(a["DATE"],a["HOURLYDRYBULBTEMPC"],color="Orange",alpha=0.5)
p.line(a["DATE"],a["HOURLYDewPointTempC"],color="Green",alpha=0.5)
p.xaxis.axis_label="TimeLine"
p.yaxis.axis_label="TEmperature in C"
show(p)


 


# In[42]:

## Latest DAy Temperature Analysis 


# In[73]:

from bokeh.io import output_file, show
from bokeh.charts import Scatter
import bokeh.plotting as bk
from bokeh.plotting import figure 
bk.output_notebook()
plot = figure(plot_width=600,plot_height=200,tools ='pan,box_zoom')
 
p = Scatter(dateATemp,x="Time",y="Temperature",xlabel="Hours",ylabel="Temperature in C ")

bk.show(p)


# In[44]:

## Relationship between HOURLYSeaLevelPressure, HOURLYAltimeterSetting and HOURLYStationPressure


# In[45]:

from bokeh.plotting  import figure,output_file, show
p=figure(width=2500,height=500,x_axis_type="datetime")
p.line(a["DATE"],a["HOURLYSeaLevelPressure"],color="Orange",alpha=0.5)
p.line(a["DATE"],a["HOURLYAltimeterSetting"],color="Green",alpha=0.5)
p.line(a["DATE"],a["HOURLYStationPressure"],color="yellow",alpha=0.5)
p.xaxis.axis_label="TimeLine"
p.yaxis.axis_label="TEmperature in C"
show(p)


# In[46]:

## Plotting of relative Humidity 


# In[47]:

from bokeh.plotting  import figure,output_file, show
p=figure(width=2500,height=500,x_axis_type="datetime")
p.line(a["DATE"],a["HOURLYRelativeHumidity"],color="indigo",alpha=0.5)
p.xaxis.axis_label="TimeLine"
p.yaxis.axis_label="TEmperature in C"
show(p)


# In[48]:

## Wind Speed along the years 


# In[50]:

from bokeh.plotting  import figure,output_file, show
p=figure(width=2500,height=500,x_axis_type="datetime")
p.line(a["DATE"],a["HOURLYWindSpeed"],color="dodgerblue",alpha=0.5)
p.xaxis.axis_label="TimeLine"
p.yaxis.axis_label="TEmperature in C"
show(p)

