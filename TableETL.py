#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import sqlalchemy as sa
from datetime import datetime

from collections import defaultdict, OrderedDict

from simple_salesforce import Salesforce
from os import environ
from dotenv import load_dotenv
load_dotenv()


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
connstr = environ.get("connstr")
schema = environ.get("schema")


# In[4]:


if not schema:
    lprint("Schema not set setting to [dbo]")
    schema = 'dbo'


# In[5]:


lprint("Creating engine for %s" % connstr)
engine = sa.create_engine(connstr)


# In[6]:


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


# In[7]:


soqlFilters = defaultdict(lambda: "where IsDeleted = false", {
    'AcademicTermEnrollment':'where Active__c = true and IsDeleted = false'
})


# In[8]:


lprint("Creating sessions...")
sf = Salesforce(username=sfusername, password=sfpassword, security_token=sfsecret, instance_url=sfinstanceurl, version='57.0')
lprint("Session created!")


# In[9]:


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
    


# In[10]:


outputData = {}

for tbl in desiredTables:
    lprint("Querying data for %s" % tbl)
    
    feilds = ", ".join(metaData[tbl].keys())
    
    soql = "select %s from %s %s" % (feilds, tbl, soqlFilters[tbl])
    
    
    lprint("Starting query:  %s" % soql)
    resp = sf.query_all(soql)
    
    outputData[tbl] = resp
    
    lprint("Finished %s" % tbl)
    


# In[11]:


for tbl in outputData.keys():
    lprint("%s totalRecords %d" % (tbl, outputData[tbl]['totalSize']))


# In[12]:


lprint("Turning output data into dataframes")
dataFrames = {tbl:pd.DataFrame.from_dict(outputData[tbl]['records']) for tbl in desiredTables}

for key in dataFrames.keys():
    
    
    if 'attributes' in dataFrames[key].columns:
        
        lprint("Dropping attributes from %s" % tbl)
        dataFrames[key].drop(columns=['attributes'], inplace=True)
    
    lprint("%s shape %s" % (key, dataFrames[key].shape))


# In[13]:


for key in dataFrames.keys():
    
    lprint("Converting datetimes for %s..." % key)
    
    for col in dataFrames[key].columns:
        if metaData[key][col]['type'] in ['date', 'datetime']:
            
            lprint("Converting %s to datetime..." % col)
            
            dataFrames[key][col] = pd.to_datetime(dataFrames[key][col])
            
    
    


# In[14]:


for tbl in dataFrames.keys():
    for col in dataFrames[tbl].columns:
        
        if np.count_nonzero(dataFrames[tbl][col].map(type) == OrderedDict) > 0:
            lprint("Ordered dict found in %s column %s converting..." % (tbl, col)) 
            dataFrames[tbl][col] = dataFrames[tbl][col].astype(str)
            
            maxLen = dataFrames[tbl][col].str.len().max()
            
            lprint("Updating length for %s column %s to %d..." % (tbl, col, maxLen)) 
            
            metaData[tbl][col]['length'] = maxLen


# In[15]:


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
                 'textarea':sa.TEXT
}

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
            elif np.count_nonzero(~pd.isna(dataFrames[tbl][field])) > 0 \
                            and ( fieldLen := int(dataFrames[tbl][field].str.len().max())) <= 255:
                sqlTypes[field] = sa.NVARCHAR(fieldLen)                
                
            else:
                sqlTypes[field] = sa.TEXT()
            
    return sqlTypes
            
#getSQLTypes('AcademicTermEnrollment')


# In[16]:


for tbl in desiredTables:
    csvTblName = "SalesForceEduCloud_%s.csv" % tbl
    lprint("Saving %s to %s" % (tbl, csvTblName))

    dataFrames[tbl].to_csv(csvTblName, index=False)

    lprint("Finished uploading %s!" % tbl)


# In[23]:


with engine.connect() as conn:

    for tbl in desiredTables:
        
        if dataFrames[tbl].shape[0] == 0:
            lprint("Skipping %s no data!" % tbl)
            continue
        
        sqlTypes = getSQLTypes(tbl)
        
        
        sqlTblName = "SalesForceEduCloud_%s" % tbl
        lprint("Uploading table %s to %s/%s" % (tbl, engine.url, sqlTblName))
        
        dataFrames[tbl].to_sql(sqlTblName, conn, schema=schema, if_exists='replace', index=False, dtype=sqlTypes)
        
        lprint("Finished uploading %s!" % tbl)


# In[ ]:


lprint("=============DONE!===================")


# In[ ]:


logfile.close()


# In[ ]:


print("Log file closed")


# In[ ]:




