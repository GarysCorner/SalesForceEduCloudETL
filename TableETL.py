#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import sqlalchemy as sa
from datetime import datetime

from collections import defaultdict
from json import JSONEncoder

from simple_salesforce import Salesforce
from os import environ
from dotenv import load_dotenv
load_dotenv()


# In[ ]:





# In[2]:


logfile = open("TableETL.log","w")
def lprint(val):
    outstr = "[%s] %s" % (datetime.now(), val)
    print(outstr)
    logfile.write(outstr)
    logfile.write("\n")
    logfile.flush()
    
lprint("Started...")


# In[3]:


sfusername = environ.get('sfusername')
sfpassword = environ.get('sfpassword')
sfsecret = environ.get("sfsecret")
sfinstanceurl = environ.get("sfinstanceurl")
#connstr = environ.get("connstr")
connstr = environ.get("KNOS_Datawarehouse")


# In[4]:


lprint("Creating engine")
engine = sa.create_engine(connstr, fast_executemany=True)


# In[5]:


desiredTables = [
    'School_Program__c',
    'ConstituentRole',
    'ContactProfile',
    'Grade_Level__c',
    'Address',
    'AcademicTerm',
    'AcademicYear',
    'Display_School__c',
    'ContactContactRelation',
    'AccountContactRelation',
    'AccountContactRole',
    'AcademicTermEnrollment',
    'Account',
    'Contact'
]
#desiredTables = ['Contact']


# In[6]:


soqlFilters = defaultdict(lambda: "where IsDeleted = false", {
    'AcademicTermEnrollment':'where Active__c = true and IsDeleted = false'
})


# In[7]:


lprint("Creating sessions...")
sf = Salesforce(username=sfusername, password=sfpassword, security_token=sfsecret, instance_url=sfinstanceurl, version='57.0')
lprint("Session created!")


# In[8]:


metaData = {}

for tbl in desiredTables:
    fieldDescs = {}
    
    lprint("Getting metadata for %s" % tbl)
    
    tblDesc = getattr(sf, tbl).describe()
    
    for field in tblDesc['fields']:
        fieldDescs[field['name']] = {
                                'type':field['type'],
                                'length':field['length']
        }
        
    
    metaData[tbl] = fieldDescs
    


# In[9]:


outputData = {}

for tbl in desiredTables:
    lprint("Querying data for %s" % tbl)
    
    feilds = ", ".join(metaData[tbl].keys())
    
    soql = "select %s from %s %s" % (feilds, tbl, soqlFilters[tbl])
    
    
    lprint("Starting query:  %s" % soql)
    resp = sf.query_all(soql)
    
    outputData[tbl] = resp
    
    lprint("Finished %s" % tbl)
    


# In[10]:


for tbl in outputData.keys():
    lprint("%s totalRecords %d" % (tbl, outputData[tbl]['totalSize']))


# In[11]:


lprint("Turning output data into dataframes")
dataFrames = {tbl:pd.DataFrame.from_dict(outputData[tbl]['records']) for tbl in desiredTables}

for key in dataFrames.keys():
    
    
    if 'attributes' in dataFrames[key].columns:
        
        lprint("Dropping attributes from %s" % tbl)
        dataFrames[key].drop(columns=['attributes'], inplace=True)
    
    lprint("%s shape %s" % (key, dataFrames[key].shape))


# In[12]:


for key in dataFrames.keys():
    
    lprint("Converting datetimes for %s..." % key)
    
    for col in dataFrames[key].columns:
        if metaData[key][col]['type'] in ['date', 'datetime']:
            
            lprint("Converting %s to datetime..." % col)
            
            dataFrames[key][col] = pd.to_datetime(dataFrames[key][col])
            
    
    


# In[13]:


jcoder = JSONEncoder()


# In[14]:


for tbl in dataFrames.keys():
    for col in dataFrames[tbl].columns:
        
        if np.count_nonzero(dataFrames[tbl][col].map(lambda a: isinstance(a, dict ) or isinstance(a, list ))) > 0:
            lprint("Ordered dict found in %s column %s converting..." % (tbl, col)) 
            
            dataFrames[tbl][col] = dataFrames[tbl][col].map(lambda v: jcoder.encode(v) if not pd.isna(v) else None)
            
            lprint("Updating type for %s column %s to JSON..." % (tbl, col)) 
            metaData[tbl][col]['type'] = 'JSON'
            
            fieldLen = int(dataFrames[tbl][col].str.len().max())
            
            lprint("Setting fieldlen to %d for %s" % (fieldLen, col))            
            metaData[tbl][col]['length'] = int(dataFrames[tbl][col].str.len().max())
            
            
            


# In[18]:


staticFields = {
                 'boolean':sa.Boolean,
                 'date':sa.DATE,
                 'datetime':sa.DATETIME,
                 'double': sa.FLOAT,
                 #'email',
                 #'id',
                 'int':sa.INT,
                 #'multipicklist',
                 #'picklist',
                 #'reference',
                 #'string',
                 'textarea':sa.TEXT,
                 #'JSON':sa.JSON
}

#this is a mess
def getSQLTypes(tbl):
    
    sqlTypes = {}
    
    lprint("Getting SQLTypes for %s" % tbl)
    
    curMeta = metaData[tbl]
        
    for field in curMeta.keys():
        
        if curMeta[field]['type'] in staticFields.keys():
            sqlTypes[field] = staticFields[curMeta[field]['type']]()
        
        
        else:
            fieldLen = curMeta[field]['length']
            
            if fieldLen <= 255:
                sqlTypes[field] = sa.NVARCHAR(fieldLen)
            
            #this is a fix they set some of the custom field max values to weird stuff
            elif np.count_nonzero(~pd.isna(dataFrames[tbl][field])) > 0                             and ( fieldLen := int(dataFrames[tbl][field].str.len().max())) <= 255:
                sqlTypes[field] = sa.NVARCHAR(fieldLen)                
                
            else:
                sqlTypes[field] = sa.TEXT()
            
    return sqlTypes
            
#getSQLTypes('Contact')


# In[16]:


for tbl in desiredTables:
    csvTblName = "SalesForceEduCloud_%s.csv" % tbl
    lprint("Saving %s to %s" % (tbl, csvTblName))

    dataFrames[tbl].to_csv(csvTblName, index=False)

    lprint("Finished saving %s!" % tbl)


# In[20]:


with engine.connect() as conn:

    for tbl in desiredTables:
        
        if dataFrames[tbl].shape[0] == 0:
            lprint("Skipping %s no data!" % tbl)
            continue
        
        sqlTypes = getSQLTypes(tbl)
        
        
        sqlTblName = "SalesForceEduCloud_%s" % tbl
        lprint("Uploading table %s to %s" % (tbl,  sqlTblName))
        
        dataFrames[tbl].to_sql(sqlTblName, conn, schema='etl', if_exists='replace', index=False, dtype=sqlTypes, chunksize=1)
        
        lprint("Finished uploading %s!" % tbl)


# In[ ]:


lprint("=============DONE!===================")


# In[ ]:


logfile.close()


# In[ ]:


print("Log file closed")


# In[ ]:




