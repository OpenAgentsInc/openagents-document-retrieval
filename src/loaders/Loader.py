from openagents import Logger

class Loader :
    logger:Logger=None
    def __init__(self,runner):
        self.runner=runner
    def getLogger(self):
        if not self.logger:
            self.logger = Logger(self.__class__.__name__,"")
        return self.logger
    def load(self,url):
        return [None,None]
