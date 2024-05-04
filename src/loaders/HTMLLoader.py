from bs4 import BeautifulSoup
from markdownify import markdownify
from loaders.Loader import Loader
from loaders.Utils import Utils
import time
class HTMLLoader(Loader):
    def __init__(self,runner):
        super().__init__(runner)
    
    def load(self,url):
        content = Utils.fetch(url,["text/html","application/xhtml+xml","application/xhtml","application/xhtml+xml","text/plain"])
        if not content:
            return [None,None]
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
        dom = BeautifulSoup(html, features="html.parser")
        html = dom.prettify()
        content = markdownify(html)
        content+="\nSource: "+url+'\n\n'
        return [content], (time.time()+60*60*24*30)*1000
        