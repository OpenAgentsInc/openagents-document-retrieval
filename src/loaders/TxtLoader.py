from loaders.Loader import Loader
from loaders.Utils import Utils
import time
class TxtLoader(Loader):
    def __init__(self,runner):
        super().__init__(runner)
    
    def load(self,url):
        content = Utils.fetch(url,["text/plain"])
        if not content:
            return [None,None]
        content+="\nSource: "+url+'\n\n'
        return [content], (time.time()+60*60*24*30)*1000
        