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
        os.makedirs(self.cachePath, exist_ok=True)
        self.executor = ThreadPoolExecutor(max_workers=32)

        # Register all loaders
        self.registerLoader(SitemapLoader(self))
        self.registerLoader(PDFLoader(self))
        self.registerLoader(HTMLLoader(self))

    def registerLoader(self, loader):
        self.loaders.append(loader)


    def _fetch_content(self, url):
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
        


    async def fetch_content(self, url):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self._fetch_content, url)


    async def run(self,job):
        outputFormat = job.outputFormat
        cacheId = str(outputFormat)+"-"+"-".join([jin.data for jin in job.input])
        cacheId = hashlib.sha256(cacheId.encode()).hexdigest()
        
        output = await self.cacheGet(cacheId)
        meta = await self.cacheGet(cacheId+".meta")
        try:
            if output: # and meta and meta["nextUpdate"] > int(time.time()*1000):
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
    # async def test(self):
    #     output,nextT = await self.fetch_content("https://bayanbox.ir/view/2284903837892390875/jMonkeyEngine-3.0-Beginner-s-Guide.pdf")
    #     print(output)
    #     print(nextT)

        
runner  = Runner(filters=EventTemplate.filters,sockets=EventTemplate.sockets,meta=EventTemplate.meta,template=EventTemplate.template)
# asyncio.run(runner.test())
node = OpenAgentsNode(NodeConfig.meta)
node.registerRunner(runner)
node.start()
