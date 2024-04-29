from OpenAgentsNode import OpenAgentsNode
from OpenAgentsNode import JobRunner

import config as NodeConfig
from events import retrieve as EventTemplate
import json
import hashlib
import os
import pickle
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.request import Request, urlopen
import asyncio

from loaders.PDFLoader import PDFLoader
from loaders.HTMLLoader import HTMLLoader
from loaders.SitemapLoader import SitemapLoader


class Runner (JobRunner):
    executor=None
    loaders = []



    def __init__(self, filters, meta, template, sockets):
        super().__init__(filters, meta, template, sockets)
        self.cachePath = os.getenv('CACHE_PATH', os.path.join(os.path.dirname(__file__), "cache"))
        maxWorkers = int(os.getenv('MAX_WORKERS', "32"))
        os.makedirs(self.cachePath, exist_ok=True)
        self.executor = ThreadPoolExecutor(max_workers=maxWorkers)

        # Register all loaders
        self.registerLoader(SitemapLoader(self))
        self.registerLoader(PDFLoader(self))
        self.registerLoader(HTMLLoader(self))

    def registerLoader(self, loader):
        self.loaders.append(loader)


    def _fetch_content(self, url):
        try:
            output = None
            nextUpdate = int(time.time()*1000 + 30*24*60*60*1000)
            for loader in self.loaders:
                print("Loader found",loader)
                try:
                    output,nextT =  loader.load(url)
                    if not output:
                        continue
                except Exception as e:
                    print(e)
                    continue
                if nextT < nextUpdate:  nextUpdate = nextT
                break
            return output,nextUpdate
        except Exception as e:
            print(e)
            return ""
        


    async def fetch_content(self, url):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self._fetch_content, url)


    async def run(self,job):
        outputFormat = job.outputFormat
        cacheId = str(outputFormat)+"-"+"-".join([jin.data for jin in job.input])
        cacheId = hashlib.sha256(cacheId.encode()).hexdigest()

        def getParamValue(key,default=None):
            param = [x for x in job.param if x.key == key]
            return param[0].value[0] if len(param) > 0 else default

        ignoreCache = getParamValue("no-cache", "false") == "true"
        cacheDurationHint = int(getParamValue("cache-duration-hint", "0")) # in seconds
        cacheExpirationHint = (time.time() + cacheDurationHint)*1000 if cacheDurationHint > 0 else -1

        output = await self.cacheGet(cacheId) if not ignoreCache else None
        meta = await self.cacheGet(cacheId+".meta") if not ignoreCache else None
        try:
            if output and meta and (meta["nextUpdate"] > int(time.time()*1000) or meta["nextUpdate"] < 0):
                print("Cache hit")
                return output
        except Exception as e:
            print(e)
            
            
        outputContent = []
        nextUpdate = int(time.time()*1000 + 30*24*60*60*1000)
        for jin in job.input:
            try:
                content,nextT = await self.fetch_content(jin.data)
                if nextT < nextUpdate:
                    nextUpdate = nextT
                if type(content) == list:
                    outputContent.extend(content)
                else:
                    outputContent.append(content)                
            except Exception as e:
                print(e)
                self.log("Error: Can't fetch "+jin.data+" "+str(e))
        
        if nextUpdate < cacheExpirationHint or cacheExpirationHint < 0:
            nextUpdate = cacheExpirationHint
        
        outputContent = ["\n".join(outputContent)]
        output = ""
        if outputFormat == "application/hyperdrive+bundle":
            blobDisk = await self.createStorage()
            for i in range(len(outputContent)):
                await blobDisk.writeUTF8(str(i)+".md",outputContent[i])
            output = blobDisk.getUrl()
            await blobDisk.close()
        else:
            output = json.dumps(outputContent)
       
        await self.cacheSet(cacheId, output)
        await self.cacheSet(cacheId+".meta", {"nextUpdate":nextUpdate})
        return output


        
runner  = Runner(filters=EventTemplate.filters,sockets=EventTemplate.sockets,meta=EventTemplate.meta,template=EventTemplate.template)
node = OpenAgentsNode(NodeConfig.meta)
node.registerRunner(runner)
node.start()
