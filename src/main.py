from openagents import JobRunner,OpenAgentsNode,NodeConfig,RunnerConfig

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


class DocumentRetrieval (JobRunner):

    def __init__(self):
        # Runner metadata configuration
        super().__init__(\
            RunnerConfig()\
                .kind(5003)\
                .name("PDF/HTML/Plaintext URLs to Markdown")\
                .description("This action fetches content from HTML, PDF, and plaintext URLs and returns the content in markdown format.")\
                .tos("https://openagents.com/terms") \
                .privacy("https://openagents.com/privacy")\
                .author("OpenAgentsInc")\
                .website("https://github.com/OpenAgentsInc/openagents-document-retrieval")\
                .picture("")\
                .tags([
                    "tool", 
                    "retrieval-pdf",
                    "retrieval-html", 
                    "retrieval-plaintext",
                    "retrieval-website"
                ]) \
                .filters()\
                    .filterByRunOn("openagents\\/document-retrieval") \
                    .commit()\
                .template("""{
                    "kind": {{meta.kind}},
                    "created_at": {{sys.timestamp_seconds}},
                    "tags": [
                        ["param","run-on", "openagents/document-retrieval"],
                        ["output", "{{in.outputType}}"],
                        {{#in.urls}}
                        ["i", "{{.}}", "text", "",  ""],
                        {{/in.urls}}     
                        ["expiration", "{{sys.expiration_timestamp_seconds}}"],
                    ],
                    "content":""
                }
                """)\
                .inSocket("urls", "array")\
                    .description("Direct URLs to PDF, HTML, or plaintext content")\
                    .schema()\
                        .field("url", "string")\
                            .description("The URL to fetch content from")\
                            .commit()\
                    .commit()\
                .commit()\
                .inSocket("outputType", "string")\
                    .description("The Desired Output Type")\
                    .defaultValue("application/json")\
                .commit()\
                .outSocket("output", "string")\
                    .description("The fetched content in markdown format or an hyperdrive bundle url")\
                    .name("Output")
                .commit()\
            .commit()\
        )
        ########


        maxWorkers = int(os.getenv('DOCUMENT_RETRIEVAL_MAX_WORKERS', "32"))

        self.executor=None
        self.loaders = []
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
                self.getLogger().finer("Loader found",loader)
                try:
                    output,nextT =  loader.load(url)
                    if not output:
                        continue
                except Exception as e:
                    self.getLogger().error(e)
                    continue
                if nextT < nextUpdate:  nextUpdate = nextT
                break
            return output,nextUpdate
        except Exception as e:
            self.getLogger().error(e)
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
                self.getLogger().finest("Cache hit")
                return output
        except Exception as e:
            self.getLogger().error("Error: Can't fetch "+jin.data+" "+str(e))
            
            
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
                
                self.getLogger().error("Error: Can't fetch "+jin.data+" "+str(e))
        
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



        
node = OpenAgentsNode(NodeConfig().name("DocumentRetrieval").version("0.0.1").description("Document retrieval node").getMeta())
node.registerRunner(DocumentRetrieval())
node.start()



