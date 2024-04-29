import grpc

from openagents_grpc_proto import rpc_pb2_grpc
from openagents_grpc_proto import rpc_pb2
import time
import os
import traceback
import json
import asyncio
import pickle
import queue
import base64
import concurrent
from threading import Condition
import requests



class BlobWriter : 
    def __init__(self,writeQueue,res ):
        self.writeQueue = writeQueue
        self.res = res

    async def write(self, data):
        self.writeQueue.put_nowait(data)

    async def writeInt(self, data):
        self.writeQueue.put_nowait(data.to_bytes(4, byteorder='big'))
    
    async def end(self):
        self.writeQueue.put_nowait(None)
        
    async def close(self):
        self.writeQueue.put_nowait(None)
        res= await self.res
        return res.success

class BlobReader: 
    def __init__(self, chunksQueue , req):
        self.chunksQueue = chunksQueue
        self.buffer = bytearray()
        self.req = req


    async def read(self, n = 1):
        while len(self.buffer) < n:
            v = await self.chunksQueue.get()
            if v is None: break
            self.buffer.extend(v)
        result, self.buffer = self.buffer[:n], self.buffer[n:]
        return result

    async def readInt(self):
        return int.from_bytes(await self.read(4), byteorder='big')
 
        
    async def close(self):
        self.chunksQueue.task_done()
        return await self.req


class BlobStorage:
    def __init__(self, id, url, node):
        self.id = id
        self.url = url
        self.node = node
        self.closed = False
    
    async def list(self, prefix="/"):
        client = self.node.getClient()
        files = await client.diskListFiles(rpc_pb2.RpcDiskListFilesRequest(diskId=self.id, path=prefix))
        return files.files
    
    async def delete(self, path):
        client = self.node.getClient()
        res = await client.diskDeleteFile(rpc_pb2.RpcDiskDeleteFileRequest(diskId=self.id, path=path))
        return res.success

    async def writeBytes(self, path, dataBytes):
        client = self.node.getClient()
        CHUNK_SIZE = 1024*1024*15
        def write_data():
            for j in range(0, len(dataBytes), CHUNK_SIZE):
                chunk = bytes(dataBytes[j:min(j+CHUNK_SIZE, len(dataBytes))])                   
                request = rpc_pb2.RpcDiskWriteFileRequest(diskId=str(self.id), path=path, data=chunk)
                yield request                              
        res=await client.diskWriteFile(write_data())
        return res.success


    async def openWriteStream(self, path):
        client = self.node.getClient()
        writeQueue = asyncio.Queue()
        CHUNK_SIZE = 1024*1024*15
       
      
        async def write_data():
            while True:
                dataBytes = await writeQueue.get()
                if dataBytes is None:  # End of stream
                    break
                for j in range(0, len(dataBytes), CHUNK_SIZE):
                    chunk = bytes(dataBytes[j:min(j+CHUNK_SIZE, len(dataBytes))])                   
                    request = rpc_pb2.RpcDiskWriteFileRequest(diskId=str(self.id), path=path, data=chunk)
                    yield request
                writeQueue.task_done()

        res=client.diskWriteFile(write_data())

        return BlobWriter(writeQueue, res)

        
    async def openReadStream(self, path):
        client = self.node.getClient()
        readQueue = asyncio.Queue()

        async def read_data():
            async for chunk in client.diskReadFile(rpc_pb2.RpcDiskReadFileRequest(diskId=self.id, path=path)):
                readQueue.put_nowait(chunk.data)
        r = asyncio.create_task(read_data())
        return BlobReader(readQueue, r)

    async def readBytes(self, path):
        client = self.node.getClient()
        bytesOut = bytearray()
        async for chunk in client.diskReadFile(rpc_pb2.RpcDiskReadFileRequest(diskId=self.id, path=path)):
            bytesOut.extend(chunk.data)
        return bytesOut

    async def writeUTF8(self, path, data):
        return await self.writeBytes(path, data.encode('utf-8'))

    async def readUTF8(self, path):
        return (await self.readBytes(path)).decode('utf-8')

    async def close(self):
        if self.closed: return
        client = self.node.getClient()
        await client.closeDisk(rpc_pb2.RpcCloseDiskRequest(diskId=self.id))
        self.closed=True

    def getUrl(self):
        return self.url

class JobRunner:

    def __init__(self, filters, meta, template, sockets):
        self._filters = None
        self._node = None
        self._job = None
        self._disksByUrl = {}
        self._disksById = {}
        self._diskByName = {}
        self._template = None
        self._meta = None
        self._sockets = None
        self._nextAnnouncementTimestamp = 0
        self.cachePath = None
        self.logger = None
        
        self.logger = Logger("JobRunner", "0", self, False)
        self._filters = filters
        
        if not isinstance(meta, str):
            self._meta = json.dumps(meta)
        else:
            self._meta = meta
        if not isinstance(template, str):
            self._template = json.dumps(template)
        else:
            self._template = template
        if not isinstance(sockets, str):
            self._sockets = json.dumps(sockets)
        else:
            self._sockets = sockets
        
        self.cachePath = os.getenv('CACHE_PATH', os.path.join(os.path.dirname(__file__), "cache"))
        if not os.path.exists(self.cachePath):
            os.makedirs(self.cachePath)

    
    def getLogger(self,name=None):   
        return self.logger

    async def cacheSet(self, path, value, version=0, expireAt=0, local=False):
        try:
            dataBytes = pickle.dumps(value)
            if local:
                fullPath = os.path.join(self.cachePath, path)
                with open(fullPath, "wb") as f:
                    f.write(dataBytes)
                with open(fullPath+".meta.json", "w") as f:
                    f.write(json.dumps({"version":version, "expireAt":expireAt}))
            else:
                client = self._node.getClient()
                CHUNK_SIZE = 1024*1024*15
                def write_data():
                    for j in range(0, len(dataBytes), CHUNK_SIZE):
                        chunk = bytes(dataBytes[j:min(j+CHUNK_SIZE, len(dataBytes))])                   
                        request = rpc_pb2.RpcCacheSetRequest(
                            key=path, 
                            data=chunk,
                            expireAt=expireAt,
                            version=version
                        )
                        yield request                              
                res=await client.cacheSet(write_data())
                return res.success
        except Exception as e:
            self.getLogger().error("Error setting cache "+str(e))
            return False
        

    async def cacheGet(self, path, lastVersion = 0, local=False):
        try:
            if local:
                fullPath = os.path.join(self.cachePath, path)
                if not os.path.exists(fullPath) or not os.path.exists(fullPath+".meta.json"):
                    return None
                with open(fullPath+".meta.json", "r") as f:
                    meta = json.loads(f.read())
                if lastVersion > 0 and meta["version"] != lastVersion:
                    return None
                if meta["expireAt"] > 0 and time.time()*1000 > meta["expireAt"]:
                    return None
                with open(fullPath, "rb") as f:
                    return pickle.load(f)
            else:
                client = self._node.getClient()
                bytesOut = bytearray()
                stream = client.cacheGet(rpc_pb2.RpcCacheGetRequest(key=path, lastVersion = lastVersion))
                async for chunk in stream:
                    if not chunk.exists:
                        return None
                    bytesOut.extend(chunk.data)
                return pickle.loads(bytesOut)
        except Exception as e:
            self.getLogger().error("Error getting cache "+str(e))
            return None

    def _setNode(self, node):
        self._node = node

    def _setJob(self, job):
        self._job = job

    def log(self, message):
        if self._job: message+=" for job "+self._job.id
        if self._node: 
            self._node.log(message, self._job.id if self._job else None)
        

    async def openStorage(self, url):
        if url in self._disksByUrl:
            return self._disksByUrl[url]
        client = self._node.getClient()
        diskId =(await client.openDisk(rpc_pb2.RpcOpenDiskRequest(url=url))).diskId
        disk =  BlobStorage(id=diskId, url=url, node=self._node)
        self._disksByUrl[url] = disk
        self._disksById[diskId] = disk
        return disk

    async def createStorage(self,name=None,encryptionKey=None,includeEncryptionKeyInUrl=None):
        if name in self._diskByName:
            return self._diskByName[name]
        
        client = self._node.getClient()
        url = (await client.createDisk(rpc_pb2.RpcCreateDiskRequest(
            name=name,
            encryptionKey=encryptionKey,
            includeEncryptionKeyInUrl=includeEncryptionKeyInUrl
        ))).url
        diskId =( await client.openDisk(rpc_pb2.RpcOpenDiskRequest(url=url))).diskId
        disk = BlobStorage(id=diskId, url=url, node=self._node)
        self._disksByUrl[url] = disk
        self._disksById[diskId] = disk
        self._diskByName[name] = disk
        return disk

    async def postRun(self):
        for disk in self._disksById.values():
            await disk.close()
        for disk in self._disksByUrl.values():
            await disk.close()
        for disk in self._diskByName.values():
            await disk.close()
        self._disksById = {}
        self._disksByUrl = {}
        self._diskByName = {}

    async def canRun(self,job):
        return True
        
    async def preRun(self):
        pass

    async def loop(self):
        pass

    async def run(self, job):
        pass

class OpenAgentsNode:
  
    def __init__(self, nameOrMeta=None, icon=None, description=None):
        self.nextNodeAnnounce = 0
        self.nodeName = ""
        self.nodeIcon = ""
        self.nodeDescription = ""
        self.channel = None
        self.rpcClient = None
        self.runners=[]
        self.poolAddress = None
        self.poolPort = None
        self.failedJobsTracker = []
        self.isLooping = False
        self.logger = None
        
        name = ""
        icon = ""
        description = ""
        version = "0.0.1"

        if isinstance(nameOrMeta, str):
            name = nameOrMeta
        else :
            name = nameOrMeta["name"] if "name" in nameOrMeta else None
            icon = nameOrMeta["picture"] if "picture" in nameOrMeta else None
            description = nameOrMeta["about"] if "about" in nameOrMeta else None
            version = nameOrMeta["version"] if "version" in nameOrMeta else None

        self.nodeName = name or os.getenv('NODE_NAME', "OpenAgentsNode")
        self.nodeIcon = icon or os.getenv('NODE_ICON', "")
        self.nodeVersion = version or os.getenv('NODE_VERSION', "0.0.1")
        self.nodeDescription = description or  os.getenv('NODE_DESCRIPTION', "")

        self.channel = None
        self.rpcClient = None
        self.logger = Logger(self.nodeName,self.nodeVersion)

    def registerRunner(self, runner):
        runner.logger=Logger(self.nodeName+"."+runner.__class__.__name__,self.nodeVersion,runner)
        self.runners.append(runner)

    def getLogger(self):
        return self.logger        

    def getClient(self): 
        if self.channel is None or self.channel._channel.check_connectivity_state(True)  == grpc.ChannelConnectivity.SHUTDOWN:
            if self.channel is not None:
                try:
                    self.getLogger().info("Closing channel")
                    self.channel.close()
                except Exception as e:
                    self.getLogger().error("Error closing channel "+str(e))
            self.getLogger().info("Connect to "+self.poolAddress+":"+str(self.poolPort)+" with ssl "+str(self.poolSsl))
            
            options=[
                # 20 MB
                ('grpc.max_send_message_length', 1024*1024*20),
                ('grpc.max_receive_message_length', 1024*1024*20)
            ]

            if self.poolSsl:
                self.channel = grpc.aio.secure_channel(self.poolAddress+":"+str(self.poolPort), grpc.ssl_channel_credentials(),options)
            else:
                self.channel = grpc.aio.insecure_channel(self.poolAddress+":"+str(self.poolPort),options)
            self.rpcClient = rpc_pb2_grpc.PoolConnectorStub(self.channel)
        return self.rpcClient

    async def _logToJob(self, message, jobId=None):
        try:
            await self.getClient().logForJob(rpc_pb2.RpcJobLog(jobId=jobId, log=message)) 
        except Exception as e:
            print("Error logging to job "+str(e))

    def log(self,message, jobId=None):
        if jobId: 
            asyncio.create_task(self._logToJob(message, jobId))
    
    async def _acceptJob(self, jobId):
        await self.getClient().acceptJob(rpc_pb2.RpcAcceptJob(jobId=jobId))

    async def executePendingJobForRunner(self , runner):
        if runner not in self.runners:
            del self.runnerTasks[runner]
            return
        try:
            client = self.getClient()
            #for runner in self.runners:
            jobs=[]
            for filter in runner._filters:
                jobs.extend((await client.getPendingJobs(rpc_pb2.RpcGetPendingJobs(
                    filterByRunOn =  filter["filterByRunOn"] if "filterByRunOn" in filter else None,
                    filterByCustomer = filter["filterByCustomer"] if "filterByCustomer" in filter else None,
                    filterByDescription = filter["filterByDescription"] if "filterByDescription" in filter else None,
                    filterById = filter["filterById"] if "filterById" in filter else None,
                    filterByKind  = filter["filterByKind"] if "filterByKind" in filter else None,
                    wait=60000,
                    # exclude failed jobs
                    excludeId = [x[0] for x in self.failedJobsTracker if time.time()-x[1] < 60]
                ))).jobs)    
            
            for job in jobs:           
                if len(jobs)>0 : runner.getLogger().log(str(len(jobs))+" pending jobs")
                else : runner.getLogger().log("No pending jobs")
                wasAccepted=False
                t=time.time()   
                try:
                    client = self.getClient() # Reconnect client for each job
                    if not await runner.canRun(job):
                        continue
                    await self._acceptJob(job.id)
                    wasAccepted = True
                    runner.getLogger().info("Job started on node "+self.nodeName)  
                    runner._setNode(self)
                    runner._setJob(job)
                    await runner.preRun()
                    async def task():
                        try:
                            output=await runner.run(job)    
                            await runner.postRun()                            
                            runner.getLogger().info("Job completed in "+str(time.time()-t)+" seconds on node "+self.nodeName, job.id)                
                            await client.completeJob(rpc_pb2.RpcJobOutput(jobId=job.id, output=output))
                        except Exception as e:
                            self.failedJobsTracker.append([job.id, time.time()])
                            runner.getLogger().error("Job failed in "+str(time.time()-t)+" seconds on node "+self.nodeName+" with error "+str(e), job.id)
                            if wasAccepted:
                                await client.cancelJob(rpc_pb2.RpcCancelJob(jobId=job.id, reason=str(e)))
                            traceback.print_exc()
                    asyncio.create_task(task())
                except Exception as e:
                    self.failedJobsTracker.append([job.id, time.time()])
                    runner.getLogger().error("Job failed in "+str(time.time()-t)+" seconds on node "+self.nodeName+" with error "+str(e), job.id)
                    if wasAccepted:
                        await client.cancelJob(rpc_pb2.RpcCancelJob(jobId=job.id, reason=str(e)))
                    traceback.print_exc()
        except Exception as e:
            traceback.print_exc()
            runner.getLogger().error("Error executing runner "+str(e))
            await asyncio.sleep(5000.0/1000.0)
        self.runnerTasks[runner]=asyncio.create_task(self.executePendingJobForRunner(runner))

 
    runnerTasks={}
    async def executePendingJob(self ):
        for runner in self.runners:
            try:
                if not runner in self.runnerTasks:
                    self.runnerTasks[runner]=asyncio.create_task(self.executePendingJobForRunner(runner))
            except Exception as e:
                self.getLogger().log("Error executing pending job "+str(e), None)


    async def reannounce(self):    
        # Announce node
        try:
            time_ms=int(time.time()*1000)
            if time_ms >= self.nextNodeAnnounce:
                try:
                    client = self.getClient()
                    res=await client.announceNode(rpc_pb2.RpcAnnounceNodeRequest(
                        iconUrl = self.nodeIcon,
                        name = self.nodeName,
                        description = self.nodeDescription,
                    ))
                    self.nextNodeAnnounce = int(time.time()*1000) + res.refreshInterval
                    self.getLogger().log("Node announced, next announcement in "+str(res.refreshInterval)+" ms")
                except Exception as e:
                    self.getLogger().error("Error announcing node "+ str(e), None)
                    self.nextNodeAnnounce = int(time.time()*1000) + 5000

            for runner in self.runners:
                try:
                    if time_ms >= runner._nextAnnouncementTimestamp:
                        client = self.getClient()
                        res = await client.announceEventTemplate(rpc_pb2.RpcAnnounceTemplateRequest(
                            meta=runner._meta,
                            template=runner._template,
                            sockets=runner._sockets
                        ))
                        runner._nextAnnouncementTimestamp = int(time.time()*1000) + res.refreshInterval
                        self.getLogger().log("Template announced, next announcement in "+str(res.refreshInterval)+" ms")
                except Exception as e:
                    self.getLogger().error("Error announcing template "+ str(e), None)
                    runner._nextAnnouncementTimestamp = int(time.time()*1000) + 5000
        except Exception as e:
            self.getLogger().error("Error reannouncing "+str(e), None)
        await asyncio.sleep(5000.0/1000.0)
        asyncio.create_task(self.reannounce())
  
    async def loop(self):
        promises = [runner.loop() for runner in self.runners]
        await asyncio.gather(*promises)
        self.isLooping = False
        await asyncio.sleep(100.0/1000.0)
        asyncio.create_task(self.loop())
        

        
    def start(self, poolAddress=None, poolPort=None):
        asyncio.run(self.run(poolAddress, poolPort))

    async def run(self, poolAddress=None, poolPort=None, poolSsl=False):
        await asyncio.sleep(5000.0/1000.0)
        self.poolAddress = poolAddress or os.getenv('POOL_ADDRESS', "127.0.0.1")
        self.poolPort = poolPort or int(os.getenv('POOL_PORT', "5000"))
        self.poolSsl = poolSsl or os.getenv('POOL_SSL', "false")== "true"
        await self.loop()
        await self.reannounce()
        while True:
            await self.executePendingJob()
            await asyncio.sleep(1000.0/1000.0)
        
class OpenObserveLogger:

    def __init__(self, options):
        self.options = options        
        self.batchSize= self.options["batchSize"]
        self.flushInterval = self.options["flushInterval"]
        if not self.flushInterval:
            self.flushInterval = 5000
        if not self.batchSize:
            self.batchSize = 21        
        self.buffer = queue.Queue()
        self.wait = Condition()
        self.flushThread = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        self.flushThread.submit(self.flushLoop)

    def log(self, level, message, timestamp=None):
        log_entry = {
            'level': level,
            '_timestamp': timestamp or int(time.time()*1000),
            'message': message
        }
        meta=self.options["meta"] if "meta" in self.options else {}
        for key in meta:
            log_entry[key]=meta[key]

        self.buffer.put(log_entry)
        if self.buffer.qsize() >= self.batchSize:
            with self.wait:
                self.wait.notify_all()
        
 
    def flushLoop(self):
        while True:
            with self.wait:
                self.wait.wait(self.flushInterval/1000)

            batch = []
            while not self.buffer.empty():
                batch.append(self.buffer.get())
         
            try:
                url = self.options["baseUrl"]+"/api/"+self.options["org"]+"/"+self.options["stream"]+"/_json"   

                basicAuth = self.options["auth"]
                if not isinstance(basicAuth, str):
                    if "username" in basicAuth and "password" in basicAuth:
                        basicAuth = basicAuth["username"]+":"+basicAuth["password"]
                        basicAuth = base64.b64encode(basicAuth.encode()).decode()

                headers = {
                    'Content-Type': 'application/json',
                    "Authorization": "Basic "+basicAuth if basicAuth else None
                }

                res = requests.post(url, headers=headers, json=batch)
                if res.status_code != 200:
                    print("Error flushing log "+str(res.status_code))

            except Exception as e:
                print("Error flushing log "+str(e))



    
class Logger :
    def __init__(self, name, version, runner=None, enableOobs=True):
        self.name=name or "main"
        self.runner=runner
        self.logger=None
        self.logLevel=None
        self.oobsLogger=None
        self.version=version
        self.logLevel = os.getenv('LOG_LEVEL', "debug")
        oobsEndPoint = os.getenv('OPENOBSERVE_ENDPOINT', None)
        self.oobsLogLevel= os.getenv('OPENOBSERVE_LOGLEVEL', self.logLevel)
        if enableOobs and oobsEndPoint:
            self.oobsLogger = OpenObserveLogger({
                "baseUrl": oobsEndPoint,
                "org": os.getenv('OPENOBSERVE_ORG', "default"),
                "stream": os.getenv('OPENOBSERVE_STREAM', "default"),
                "auth": os.getenv('OPENOBSERVE_BASICAUTH', None) or {
                    "username": os.getenv('OPENOBSERVE_USERNAME', None),
                    "password": os.getenv('OPENOBSERVE_PASSWORD', None)
                },
                "batchSize": int(os.getenv('OPENOBSERVE_BATCHSIZE', 21)),
                "flushInterval": int(os.getenv('OPENOBSERVE_FLUSHINTERVAL', 0)),
                "meta":{
                    "appName": self.name,
                    "appVersion": self.version
                }
                
            })

    def levelToValue(self, level):
        if level == "error": return 8
        if level == "warn": return 7
        if level == "info": return 6
        if level == "verbose": return 5
        if level == "debug": return 4
        if level == "fine": return 3
        if level == "finer": return 2
        if level == "finest": return 1
        return level

    def _log(self, level, message):
        levelV=self.levelToValue(level)
        if levelV >=self.levelToValue(self.logLevel): 
            date = time.strftime("%Y-%m-%d %H:%M:%S")
            print(date+" ["+self.name+":"+self.version+"] : "+level+" : "+message)
        if self.oobsLogger and  self.levelToValue(self.oobsLogLevel) >= levelV:
            self.oobsLogger.log(level, message)
        
        if self.runner and levelV >= self.levelToValue("info"):
            self.runner.log(message)


    def log(self, *args):
        self._log("debug", " ".join([str(x) for x in args]))
    
    def info(self, *args):
        self._log("info", " ".join([str(x) for x in args]))
    
    def warn(self, *args):
        self._log("warn", " ".join([str(x) for x in args]))
    
    def error(self, *args):
        traceback.print_exc()
        self._log("error", " ".join([str(x) for x in args]))

    def debug(self, *args):
        self._log("debug", " ".join([str(x) for x in args]))
    
    def fine(self, *args):
        self._log("fine", " ".join([str(x) for x in args]))
    
    def finer(self, *args):
        self._log("finer", " ".join([str(x) for x in args]))

    def finest(self, *args):
        self._log("finest", " ".join([str(x) for x in args]))

       