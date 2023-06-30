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


# In[ ]:


logfile = open("TableETL.log","w")
def lprint(val):
    outstr = "[%s] %s" % (datetime.now(), val)
    print(outstr)
    logfile.write(outstr)
    logfile.write("\n")
    logfile.flush()
    
lprint("Started...")


# In[ ]:


sfusername = environ.get('sfusername')
sfpassword = environ.get('sfpassword')
sfsecret = environ.get("sfsecret")
sfinstanceurl = environ.get("sfinstanceurl")
connstr = environ.get("connstr")


# In[ ]:


lprint("Creating engine for %s" % connstr)
engine = sa.create_engine(connstr)


# In[3]:


desiredTables = [
    'AcademicTermEnrollment',
    'Account',
    'Contact'
]


# In[4]:


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


for tbl in dataFrames.keys():
    for col in dataFrames[tbl].columns:
        
        if np.count_nonzero(dataFrames[tbl][col].map(type) == OrderedDict) > 0:
            lprint("Ordered dict found in %s column %s converting..." % (tbl, col)) 
            dataFrames[tbl][col] = dataFrames[tbl][col].astype(str)
            
            maxLen = dataFrames[tbl][col].str.len().max()
            
            lprint("Updating length for %s column %s to %d..." % (tbl, col, maxLen)) 
            
            metaData[tbl][col]['length'] = maxLen


# In[14]:


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
                
            else:
                sqlTypes[field] = sa.TEXT()
            
    return sqlTypes
            
#etSQLTypes('AcademicTermEnrollment')


# In[15]:


for tbl in desiredTables:
    csvTblName = "SalesForceEduCloud_%s.csv" % tbl
    lprint("Saving %s to %s" % (tbl, csvTblName))

    dataFrames[tbl].to_csv(csvTblName, index=False)

    lprint("Finished uploading %s!" % tbl)


# In[ ]:


with engine.connect() as conn:

    for tbl in desiredTables:
        
        sqlTypes = getSQLTypes(tbl)
        
        
        sqlTblName = "SalesForceEduCloud_%s" % tbl
        lprint("Uploading table %s to %s/%s" % (tbl, engine.url, sqlTblName))
        
        dataFrames[tbl].to_sql(sqlTblName, conn, schema='dbo', if_exists='replace', index=False, dtype=sqlTypes)
        
        lprint("Finished uploading %s!" % tbl)


# In[ ]:


lprint("=============DONE!===================")


# In[ ]:


logfile.close()


# In[ ]:


print("Log file closed")


# In[ ]:



