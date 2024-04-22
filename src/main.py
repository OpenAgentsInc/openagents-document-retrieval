from OpenAgentsNode import OpenAgentsNode
from OpenAgentsNode import JobRunner
from bs4 import BeautifulSoup
from markdownify import markdownify
from urllib.request import Request, urlopen
import PyPDF2
import config as NodeConfig
from events import retrieve as EventTemplate
import json
from io import BytesIO
import hashlib
import os
import pickle
import time
def parsePDF(conn):
    # Open the PDF file in read binary mode
    f = BytesIO(conn.read())
    pdf_reader = PyPDF2.PdfReader(f)
    num_pages = len(pdf_reader.pages)
    # Iterate through all pages and extract text
    fullText = ""
    for page_num in range(num_pages):
        page = pdf_reader.pages[page_num]
        text = page.extract_text()
        fullText += text.strip()
    return fullText
   

def parseHTML(conn):
  content = conn.read().decode("utf-8")
  soup = BeautifulSoup(content, features="html.parser")  
  html = ""
  main_content = soup.find("main")
  if main_content:
      main_content_html=main_content.prettify()
      html += main_content_html
  else:
      body_content = soup.find("body")
      if body_content:
          body_content_html=body_content.prettify()
          html += body_content_html
  content = markdownify(html)
  return content


def fetch_content(url):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:91.0) Gecko/20100101 Firefox/91.0'}
    req = Request(url, headers=headers)
    conn = urlopen(req)
    mimetype=conn.getheader('Content-Type')
    if "application/pdf" in mimetype:
      return parsePDF(conn)
    else:
      return parseHTML(conn)    


class Runner (JobRunner):
    def __init__(self, filters, meta, template, sockets):
        super().__init__(filters, meta, template, sockets)
        self.cachePath = os.getenv('CACHE_PATH', os.path.join(os.path.dirname(__file__), "cache"))
        os.makedirs(self.cachePath, exist_ok=True)


    async def run(self,job):
        outputFormat = job.outputFormat
        cacheId = str(outputFormat)+"-"+"-".join([jin.data for jin in job.input])
        cacheId = hashlib.sha256(cacheId.encode()).hexdigest()
        
        output = self.cacheGet(cacheId)
        if output:
            self.log("Cache hit")
            return output
            
        outputContent = []
        for jin in job.input:
            try:
                content = fetch_content(jin.data)
                outputContent.append(content)
            except Exception as e:
                print(e)
                self.log("Error: Can't fetch "+jin.data+" "+str(e))
        
        output = ""
        if outputFormat == "application/hyperdrive+bundle":
            blobDisk = self.createStorage()
            for i in range(len(outputContent)):
                blobDisk.writeUTF8(str(i)+".md",outputContent[i])
            output = blobDisk.getUrl()
            blobDisk.close()
        else:
            output = json.dumps(outputContent)
       
        self.cacheSet(cacheId, output)
        return output

        
node = OpenAgentsNode(NodeConfig.meta)
node.registerRunner(Runner(filters=EventTemplate.filters,sockets=EventTemplate.sockets,meta=EventTemplate.meta,template=EventTemplate.template))
node.start()