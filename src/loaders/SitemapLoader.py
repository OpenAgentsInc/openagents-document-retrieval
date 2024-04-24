from bs4 import BeautifulSoup
from markdownify import markdownify
from loaders.HTMLLoader import HTMLLoader
import datetime
import time
from urllib.request import Request, urlopen
from loaders.Utils import Utils
import asyncio
from concurrent.futures import ThreadPoolExecutor
import traceback

class SitemapLoader(HTMLLoader):
    runner = None
    def __init__(self,runner):
        self.runner = runner
        
    def findSitemapUrl(self,url):
        sitemap=""
        if url.endswith(".xml"):
            sitemap = url

        if sitemap == "":
            try:
                print("Try sitemap.xml")    
                sitemap=  (url+"/" if url[-1]!="/" else url) + "sitemap.xml"
                if not Utils.existsUrl(sitemap): sitemap=""
            except Exception as e:
                traceback.print_exc()                

        if sitemap == "":
            try:
                print("Try from robots.txt")
                robots =  Utils.fetch((url+"/" if url[-1]!="/" else url) + "robots.txt")
                for line in robots.split("\n"):
                    try:
                        if line.startswith("Sitemap:"):
                            sitemap = line.split(" ")[1]
                            # if relative path make it absolute
                            if sitemap.startswith("/"):
                                sitemap = (url + "/" if url[-1]!="/" else url) + sitemap
                            break
                    except Exception as e:
                        traceback.print_exc()                
            except Exception as e:
                traceback.print_exc()                

        if sitemap=="":
            try:          
                print("Try from html")
                html = Utils.fetch(url,[ "text/html","application/xhtml+xml","application/xhtml","application/xhtml+xml","text/plain" ])
                if html:
                    soup = BeautifulSoup(html, features="html.parser")
                    sitemapUrl = soup.find("link", {"rel": "sitemap"}) 
                    if sitemapUrl:
                        sitemap = sitemapUrl["href"]
                        # if relative path make it absolute
                        if sitemap.startswith("/"):
                            sitemap = (url + "/" if url[-1]!="/" else url) + sitemap
            except Exception as e:
                traceback.print_exc()                

        if sitemap:
            print("Found sitemap at",sitemap)
            return sitemap
        else:
            return None
        
            

    def extractUrls(self, sitemapXml, out=None):
        if out is None:
            out = []
        soup = BeautifulSoup(sitemapXml, features="xml")
        
        nextSitemapUpdate = int(time.time()*1000 + 365*24*60*60*1000)
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:91.0) Gecko/20100101 Firefox/91.0'}

        # find all subsitemaps
        sitemapTags = soup.find_all("sitemap")
        for sitemapTag in sitemapTags:
            try:
                loc = sitemapTag.find("loc")
                if loc:
                    url = loc.text
                    print("Found sub sitemap",url)
                    sitemapXml = Utils.fetch(url)
                    [_,nextSubSitemapUpdate] = self.extractUrls(sitemapXml, out)
                    if nextSubSitemapUpdate < nextSitemapUpdate:
                        nextSitemapUpdate = nextSubSitemapUpdate
            except:
                pass
        
        
        # find all urlsets
        urlTags = soup.find_all("url")
        for urlTag in urlTags:
            try:
                loc = urlTag.find("loc")
                lastmod = urlTag.find("lastmod")
                changefreq = urlTag.find("changefreq")
                print("Found url",loc.text)
                loc = loc.text if loc else None
                if loc is None: continue

                lastmod = datetime.datetime.strptime(lastmod.text , "%Y-%m-%dT%H:%M:%S%z") if lastmod else  datetime.datetime.now()
                changefreq = changefreq.text if changefreq else "unknown"
                nextUrlUpdate = 0
                if changefreq == "always":
                    nextUrlUpdate = int(((lastmod + datetime.timedelta(seconds=1)).timestamp())*1000)
                elif changefreq == "hourly":
                    nextUrlUpdate = int(((lastmod + datetime.timedelta(hours=1)).timestamp())*1000)
                elif changefreq == "daily":
                    nextUrlUpdate = int(((lastmod + datetime.timedelta(days=1)).timestamp())*1000)
                elif changefreq == "weekly":
                    nextUrlUpdate = int(((lastmod + datetime.timedelta(days=7)).timestamp())*1000)
                elif changefreq == "monthly":
                    nextUrlUpdate = int(((lastmod + datetime.timedelta(days=30)).timestamp())*1000)
                elif changefreq == "yearly":
                    nextUrlUpdate = int(((lastmod + datetime.timedelta(days=365)).timestamp())*1000)
                else:
                    nextUrlUpdate = nextUrlUpdate if nextUrlUpdate>0 else int(((lastmod + datetime.timedelta(days=30)).timestamp())*1000)

                if  nextUrlUpdate < nextSitemapUpdate:
                    nextSitemapUpdate = nextUrlUpdate

                out.append({
                    "loc": loc,
                    "lastmod": lastmod,
                    "changefreq": changefreq                
                })

            except Exception as e:
                traceback.print_exc()                

            
        return out,nextSitemapUpdate
                
                
    def deduplicateUrls(self, urls):
        out = []
        for url in urls:
            loc = url["loc"]
            unique=True
            for u in out:
                if u["loc"] == loc:
                    unique=False
                    break
            if unique:
                out.append(url)            
        return out

    def load(self,url ):
        # check if it is a home page or a sitemap
        if len([part for part in (url.split("://")[1].split("/")) if part]) > 1 and not url.endswith(".xml"):
            return [None,None]

        sitemapUrl = self.findSitemapUrl(url)
        if not sitemapUrl:
            return  [None,None]

        sitemapXml = Utils.fetch(sitemapUrl)
        
        urls,nextUpdateTimestamp = self.extractUrls(sitemapXml)
        urls = self.deduplicateUrls(urls)

        contents = []
        executor = ThreadPoolExecutor(max_workers=4)

        loadHtml=super().load
        for url in urls:
            def run(url,out):
                try:
                    print("Fetching",url["loc"])
                    content = Utils.fetch(url["loc"],["text/html","application/xhtml+xml","application/xhtml","application/xhtml+xml","text/plain"])
                    if not content:
                        print("Can't fetch",url["loc"],"wrong mime type")
                        return
                    contents = loadHtml(url["loc"])[0]
                    for i in range(len(contents)):
                        contents[i]=contents[i]+"\n Source: "+url["loc"]+"\n\n"
                    out.extend(contents)
                except Exception as e:
                    traceback.print_exc()                
            executor.submit(run,url,contents)
        executor.shutdown(wait=True)

        return [contents,nextUpdateTimestamp]

        