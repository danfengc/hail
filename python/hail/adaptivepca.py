from decorator import decorator

from hail.typecheck import *

from hail.java import *
from pyspark.sql import *

class AdaptivepcaBuilder(object):

    def __init__(self, hc, jrp):
        self.hc = hc
        self._jrp = jrp

    def getScore(self):
        jdf = self._jrp.getScore()
        return DataFrame(jdf, self.hc._sql_context)

    def getBranch(self):
        return self._jrp.getMaf()

    def get(self, branch):
        jrp = self._jrp.get(branch)
        return AdaptivepcaBuilder(self.hc, jrp)

    def printSchema(self):
        return(self._jrp.printSchema())






