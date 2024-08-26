#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import sqlalchemy as sa
from datetime import datetime, timedelta

from collections import defaultdict
from json import JSONEncoder,JSONDecoder
import requests

import threading
from queue import Queue

from time import sleep

from simple_salesforce import Salesforce
from os import environ, sys
from dotenv import load_dotenv
_ = load_dotenv()


# In[2]:


transformThreadCount = 3
logFileName = "TableETL_KippFoundation.log"
sqlSchema = 'etl'
saveFileBaseName = "SalesForceEduCloud_KippFound_%s.csv"
outputTableBaseName = "SalesForceEduCloud_KippFound_%s"
jsonConfigFileName = "TableETL-KippFoundation.json"


# In[3]:


logQueue = Queue()


def lprint(val):
    outstr = f"[{datetime.now()}] ({threading.current_thread().name}) {val}"
    print(outstr)
    logQueue.put(outstr)
    
    
lprint("Started...")


# In[4]:


handleThreadError_Super = threading.excepthook

def handleThreadError(args):
    lprint(f"ERROR!!! {args.exc_type} \"{args.exc_value}\" in thread {args.thread}  ERROR!!!")
    handleThreadError_Super(args)
    
threading.excepthook = handleThreadError


# In[5]:


def lprintDaemon():
    with open(logFileName,"w") as logfile:
        while True:
            logfile.write(logQueue.get())
            logfile.write("\n")
            logfile.flush()
            logQueue.task_done()

lprintDaemonThread = threading.Thread(target=lprintDaemon)
lprintDaemonThread.daemon = True
lprintDaemonThread.name = "lprintDaemon"
lprintDaemonThread.start()


# In[6]:


#sfusername = environ.get('KFsfusername')
#sfpassword = environ.get('KFsfpassword')
#sfsecret = environ.get("KFsfsecret")
sfclientid = environ.get("KFsfclientid")
sfclientsecret = environ.get("KFsfclientsecret")
sfinstanceurl = environ.get("KFsfinstanceurl")
connstr = environ.get("KNOS_Datawarehouse")


# In[7]:


lprint("Creating engine")
engine = sa.create_engine(connstr, fast_executemany=True, isolation_level="READ UNCOMMITTED")


# In[8]:


jsonDecoder = JSONDecoder()


# In[9]:


lprint(f"Reading config file {jsonConfigFileName}")
with open(jsonConfigFileName) as f:

    jsonConfig = jsonDecoder.decode( f.read() )

lprint(jsonConfig)


# In[10]:


lprint("Setting destired tables")
desiredTables = list(jsonConfig['Tables'].keys())
lprint(desiredTables)


# In[11]:


#soqlFilters = defaultdict(lambda: "where IsDeleted = false LIMIT 1000", {})
soqlFilters = defaultdict(lambda: "where IsDeleted = false", {})


# In[12]:


lprint(f"Getting access token for {sfclientid} from instance {sfinstanceurl}")
sfVersion='58.0'
payload = {
    'grant_type':'client_credentials',
    'client_id':sfclientid,
    'client_secret':sfclientsecret,
    #'username':sfusername,
    #'password':sfpassword+sfsecret
}

authURL = f"{sfinstanceurl}services/oauth2/token"
#authURL = "http://127.0.0.1:55555/services/oauth2/token"
lprint(f"Getting Auth token from {authURL}")
session = requests.Session()
authResp = session.post(authURL,\
                    data=payload,)

authRespData = jsonDecoder.decode(authResp.text)


# In[13]:


authRespDataPublic = authRespData.copy()

if 'access_token' in authRespDataPublic.keys():
    authRespDataPublic['access_token'] = '*' * len(authRespDataPublic['access_token'])

lprint(authRespDataPublic)


# In[14]:


lprint("Creating Simple Salesforce Instance using sessionID...")
sf = Salesforce(instance_url=authRespData['instance_url'], session_id=authRespData['access_token'], version='58.0')
lprint("Session created!")


# In[15]:


metaData = {}
metaDataLock = threading.Lock()

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
    


# In[16]:


daemonStatusLock = threading.Lock()
daemonStatus = {}
daemonCurTaskQueue = Queue()


# In[17]:


outputDataQueue = Queue()
extractDaemonStats = {}

def extractDaemon():
    daemonName = 'extractDaemon'
    while True:
        with daemonStatusLock:
            daemonStatus[daemonName] = f"{daemonName} waiting for job"
        
        tbl = tableQueue.get()
        daemonCurTaskQueue.put(daemonName)
        
        lprint(f"Querying data for {tbl}")
        startTime = datetime.now()
        
        with daemonStatusLock:
            daemonStatus[daemonName] = f"{daemonName} Querying data for {tbl}"

        #with metaDataLock:
        #    feilds = ", ".join(metaData[tbl].keys())

        feilds = ", ".join(jsonConfig['Tables'][tbl]['Columns'])
        
        soql = "select %s from %s %s" % (feilds, tbl, soqlFilters[tbl])

        
        lprint("Starting query:  %s" % soql)
        resp = sf.query_all(soql)
    
        lprint(f"Adding {tbl} to output queue")
        
        lprint("Finished %s" % tbl)
    
        lprint("%s totalRecords %d" % (tbl, resp['totalSize']))
        
        outputDataQueue.put( (tbl, resp) )

        extractDaemonStats[tbl] = datetime.now() - startTime
        tableQueue.task_done()
        daemonCurTaskQueue.get()
        daemonCurTaskQueue.task_done()
        
lprint("Creating extractDaemon Thread")
extractDaemonThread = threading.Thread(target=extractDaemon)
extractDaemonThread.daemon=True
extractDaemonThread.name = 'extractDaemon'


# In[18]:


dataFramesQueue = Queue()
transformDaemonStats = {}
transformDaemonStatsLock = threading.Lock()
transformDaemonThreads = []

def transformDaemon(num):
    daemonName = f"transformDaemon[{num}]"
    while True:
        with daemonStatusLock:
            daemonStatus[daemonName] = f"{daemonName} waiting for job"
            
        (tbl, resp) = outputDataQueue.get()
        daemonCurTaskQueue.put(daemonName)
        
        lprint(f"Turning {tbl} into a dataframe")
        startTime = datetime.now()
        
        with daemonStatusLock:
            daemonStatus[daemonName] = f"{daemonName} Turning {tbl} into a dataframe"
        
        respDf = pd.DataFrame.from_dict(resp['records'])
    
        if 'attributes' in respDf.columns:
            
            lprint("Dropping attributes from %s" % tbl)
            respDf.drop(columns=['attributes'], inplace=True)
        
        lprint("%s shape %s" % (tbl, respDf.shape))


        csvTblName = saveFileBaseName % tbl
        lprint(f"Saving {tbl} to {csvTblName}")

        with daemonStatusLock:
            daemonStatus[daemonName] = f"{daemonName} Saving {tbl} to {csvTblName}"

        respDf.to_csv(csvTblName, index=False)

        lprint("Finished saving %s!" % tbl)
        
        lprint(f"transforming columns for {tbl}...")

        for col in respDf.columns:
            with metaDataLock:
                theType = metaData[tbl][col]['type']

            with daemonStatusLock:
                daemonStatus[daemonName] = f"{daemonName} processing {tbl}->{col}..."
            
            if theType in ['date', 'datetime']:
                lprint(f"Converting {tbl}->{col} to datetime...")
                respDf[col] = pd.to_datetime(respDf[col], errors='coerce')
            
    
            for col in respDf.columns:
        
                if np.count_nonzero(respDf[col].map(lambda a: isinstance(a, dict ) or isinstance(a, list ))) > 0:
                    lprint("Ordered dict found in %s column %s converting..." % (tbl, col)) 
                    
                    respDf[col] = respDf[col].map(lambda v: jcoder.encode(v) if not pd.isna(v) else None)
                    
                    lprint("Updating type for %s column %s to JSON..." % (tbl, col)) 
                    with metaDataLock:
                        metaData[tbl][col]['type'] = 'JSON'
                    
                    fieldLen = int(respDf[col].str.len().max())
                    
                    lprint("Setting fieldlen to %d for %s" % (fieldLen, col))            
                    with metaDataLock:
                        metaData[tbl][col]['length'] = int(respDf[col].str.len().max())
            

        lprint(f"Adding response for {tbl} to dataFrame queue")

        dataFramesQueue.put( (tbl, respDf) )
        
        with transformDaemonStatsLock:
            transformDaemonStats[tbl] = datetime.now() - startTime
            
        outputDataQueue.task_done()
        daemonCurTaskQueue.get()
        daemonCurTaskQueue.task_done()

for i in range(transformThreadCount):
    lprint(f"Creating dataFrameDaemon[{i}] thread")
    threadObj = threading.Thread(target=transformDaemon,args=(i,))
    threadObj.daemon=True
    threadObj.name = f"transformDaemon[{i}]"
    transformDaemonThreads.append(threadObj)
    


# In[19]:


jcoder = JSONEncoder()


# In[20]:


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
def getSQLTypes(tbl, respDf):
    with metaDataLock:
        sqlTypes = {}
        
        lprint("Getting SQLTypes for %s" % tbl)
        
        curMeta = metaData[tbl]
            
        for field in jsonConfig['Tables'][tbl]['Columns']:

            
            
            if curMeta[field]['type'] in staticFields.keys():
                sqlTypes[field] = staticFields[curMeta[field]['type']]()
            
            
            else:
                fieldLen = curMeta[field]['length']
                
                if fieldLen <= 255:
                    sqlTypes[field] = sa.NVARCHAR(fieldLen)
                
                #this is a fix they set some of the custom field max values to weird stuff
                elif np.count_nonzero(~pd.isna(respDf[field])) > 0 \
                                and ( fieldLen := int(respDf[field].str.len().max())) <= 255:
                    sqlTypes[field] = sa.NVARCHAR(fieldLen)                
                    
                else:
                    sqlTypes[field] = sa.TEXT()
                
    return sqlTypes


# In[21]:


loadDaemonStats = {}
def loadDaemon():
    daemonName = 'loadDaemon'
    while True:
        with daemonStatusLock:
            daemonStatus[daemonName] = f"{daemonName} waiting for job"
        
        (tbl, respDf) = dataFramesQueue.get()
        daemonCurTaskQueue.put(daemonName)
        
        startTime = datetime.now()
        
        if respDf.shape[0] == 0:
            lprint("Skipping %s no data!" % tbl)
            loadDaemonStats[tbl] = "Skipped, no data!"
            dataFramesQueue.task_done()
            daemonCurTaskQueue.get()
            daemonCurTaskQueue.task_done()
            continue

        with daemonStatusLock:
            daemonStatus[daemonName] = f"{daemonName} getting SQL types for {tbl}"
            
        sqlTypes = getSQLTypes(tbl, respDf)
        
        
        sqlTblName = outputTableBaseName % tbl
        lprint("Uploading table %s to etl.%s" % (tbl,  sqlTblName))

        with daemonStatusLock:
            daemonStatus[daemonName] = f"{daemonName} uploading {tbl}"
            
        with engine.begin() as conn:
            respDf.to_sql(sqlTblName, conn, schema=sqlSchema, if_exists='replace', index=False, dtype=sqlTypes, chunksize=1)
        
        lprint("Finished uploading %s!" % tbl)
        loadDaemonStats[tbl] = datetime.now() - startTime

        dataFramesQueue.task_done()
        daemonCurTaskQueue.get()
        daemonCurTaskQueue.task_done()

lprint("Creating loadDaemon thread")
loadDaemonThread = threading.Thread(target=loadDaemon)
loadDaemonThread.daemon=True
loadDaemonThread.name = "loadDaemon"


# In[22]:


tableQueue = Queue()
for tbl in desiredTables:
    lprint(f"Queuing {tbl}")
    tableQueue.put(tbl)


# In[23]:


lprint("Starting loadDaemon thread")
loadDaemonThread.start()


# In[24]:


lprint("Starting dataFrameDaemon threads")
for i,thread in enumerate(transformDaemonThreads):
    lprint(f"Starting dataFrameDaemon[{i}]")
    thread.start()


# In[25]:


lprint("Starting query Thread")
extractDaemonThread.start()


# In[26]:


lprint("All daemons started!")


# In[ ]:


comboSize = 1
while comboSize > 0:
    tqSize = tableQueue.qsize()
    odqSize = outputDataQueue.qsize()
    dfqSize = dataFramesQueue.qsize()
    runSize = daemonCurTaskQueue.qsize()
    comboSize = tqSize + odqSize + dfqSize + runSize
    
    with daemonStatusLock:
        lprint("Daemon Status:\n" + ("\n".join(daemonStatus.values())))

    lprint(f"Extract queue({tqSize})\tTransform queue({odqSize})\tLoad queue({dfqSize})\tRunning({runSize})\tTotal({comboSize})")
        
    sleep(30)

lprint("All queues are empty but uploading is most likely still continueing")
    
    


# In[ ]:


tableQueue.join()
lprint("All table data extracted")
outputDataQueue.join()
lprint("All data transformed!")
dataFramesQueue.join()
lprint(f"All data loaded to {engine}")


# In[ ]:


daemonCurTaskQueue.join()
lprint("All running daemons have completed!")


# In[ ]:


for key in sorted(extractDaemonStats.keys()):
    lprint(f"extractDaemon processed {key} in {extractDaemonStats[key]}")


# In[ ]:


for key in sorted(transformDaemonStats.keys()):
    lprint(f"transformDaemon processed {key} in {transformDaemonStats[key]}")


# In[ ]:


for key in sorted(loadDaemonStats.keys()):
    lprint(f"loadDaemon processed {key} in {loadDaemonStats[key]}")


# In[ ]:


statLists = [extractDaemonStats, transformDaemonStats, loadDaemonStats]
for key in sorted(extractDaemonStats.keys()):
    if np.all([isinstance(statList[key], timedelta) for statList in statLists]):
        comboTime = np.sum([statList[key] for statList in statLists])
        lprint(f"{key} completed in {comboTime}")
    else:
        lprint(F"{key} incomplete {[str(l[key]) for l in statLists]}")


# In[ ]:


lprint("=============DONE!===================")


# In[ ]:


sleep(5)


# In[ ]:


#jupyter nbconvert .\TableETL-KippFoundation.ipynb --to python


# In[ ]:




