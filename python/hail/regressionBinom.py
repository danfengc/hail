from decorator import decorator

from hail.typecheck import *

from hail.java import *
from pyspark.sql import *

class BinomRegressionBuilder(object):

    def __init__(self, hc, jrp):
        self.hc = hc
        self._jrp = jrp

    def export(self, output):
        self._jrp.export(output)

    def dataframe(self, branch):
        jdf = self._jrp.dataframe(branch)
        return DataFrame(jdf, self.hc._sql_context)


