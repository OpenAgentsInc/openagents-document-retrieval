from openagents import JobRunner,OpenAgentsNode,NodeConfig,RunnerConfig,JobContext

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
        super().__init__(
            RunnerConfig(
                meta={
                    "kind": 5003,
                    "name": "PDF/HTML/Plaintext URLs to Markdown",
                    "description": "This action fetches content from HTML, PDF, and plaintext URLs and returns the content in markdown format.",
                    "tos": "https://openagents.com/terms",
                    "privacy": "https://openagents.com/privacy",
                    "author": "OpenAgentsInc",
                    "web": "https://github.com/OpenAgentsInc/openagents-document-retrieval",
                    "picture": "",
                    "tags": [
                        "tool",
                        "retrieval-pdf",
                        "retrieval-html",
                        "retrieval-plaintext",
                        "retrieval-website"
                    ]
                },
                filter={"filterByRunOn": "openagents\\/document-retrieval"},
                template="""{
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
                """,
                sockets={
                    "in":{
                        "urls":{
                            "title":"URLs",
                            "description":"Direct URLs to PDF, HTML, or plaintext content",
                            "type":"array",
                            "items":{
                                "type":"object",
                                "properties":{
                                    "url":{
                                        "type":"string",
                                        "description":"The URL to fetch content from"
                                    }
                                }
                            },
                        },
                        "outputType":{
                            "title":"Output Type",
                            "description":"The Desired Output Type",
                            "type":"string",
                            "default":"application/json"
                        }
                    },
                    "out":{
                        "output":{
                            "title":"Output",
                            "description":"The fetched content in markdown format or an hyperdrive bundle url",
                            "type":"string"
                        }
                    }
                }
            )
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
        self.setRunInParallel(True)


    def registerLoader(self, loader):
        self.loaders.append(loader)


    def _fetch_content(self, url, logger):
        try:
            output = None
            nextUpdate = int(time.time()*1000 + 30*24*60*60*1000)
            for loader in self.loaders:
                logger.finer("Loader found",loader)
                try:
                    output,nextT =  loader.load(url)
                    if not output:
                        continue
                except Exception as e:
                    logger.error(e)
                    continue
                if nextT < nextUpdate:  nextUpdate = nextT
                break
            return output,nextUpdate
        except Exception as e:
            logger.error(e)
            return ""
        


    async def fetch_content(self, url,logger):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self._fetch_content, url,logger)


    async def run(self,ctx:JobContext):
        job = ctx.getJob()
        logger = ctx.getLogger()

        outputFormat = ctx.getOutputFormat()
        ignoreCache = ctx.getJobParamValue("no-cache", "false") == "true"
        cacheDurationHint = int(ctx.getJobParamValue("cache-duration-hint", "0")) # in seconds
        cacheExpirationHint = (time.time() + cacheDurationHint)*1000 if cacheDurationHint > 0 else -1

        cacheId = hashlib.sha256(( str(outputFormat)+"-"+"-".join(
            [
                jin.data+"-"+jin.type for jin in job.input
            ]
        )).encode()).hexdigest()


        output = await ctx.cacheGet(cacheId) if not ignoreCache else None
        meta = await ctx.cacheGet(cacheId+".meta") if not ignoreCache else None
        try:
            if output and meta and (meta["nextUpdate"] > int(time.time()*1000) or meta["nextUpdate"] < 0):
                logger.finest("Cache hit")
                return output
        except Exception as e:
            logger.error("Error: Can't fetch "+jin.data+" "+str(e))
            
            
        outputContent = []
        nextUpdate = int(time.time()*1000 + 30*24*60*60*1000)
        for jin in job.input:
            try:
                content,nextT = await self.fetch_content(jin.data,logger)
                if nextT < nextUpdate:
                    nextUpdate = nextT
                if type(content) == list:
                    outputContent.extend(content)
                else:
                    outputContent.append(content)                
            except Exception as e:                
                logger.error("Error: Can't fetch "+jin.data+" "+str(e))
        
        if nextUpdate < cacheExpirationHint or cacheExpirationHint < 0:
            nextUpdate = cacheExpirationHint
        
        outputContent = ["\n".join(outputContent)]
        output = ""
        if outputFormat == "application/hyperdrive+bundle":
            disk = await ctx.createStorage()
            for i in range(len(outputContent)):
                await disk.writeUTF8(str(i)+".md",outputContent[i])
            output = disk.getUrl()
            await disk.close()
        else:
            output = json.dumps(outputContent)
       
        await ctx.cacheSet(cacheId, output)
        await ctx.cacheSet(cacheId+".meta", {"nextUpdate":nextUpdate})
        return output



node = OpenAgentsNode(NodeConfig({
    "name":"OpenAgents DocumentRetrieval Service",
    "version":"0.0.1",
    "description":"Retrieve content from PDF, HTML, and plaintext URLs and return the content in markdown format."
}))
node.registerRunner(DocumentRetrieval())
node.start()



