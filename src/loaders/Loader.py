

class Loader :
    def __init__(self,runner):
        self.runner=runner
    def getLogger(self):
        return self.runner.getLoggers()
    def load(self,url):
        return [None,None]
