package is.hail.methods.UNICORN

import is.hail.SparkSuite
import org.testng.annotations.Test
import is.hail.HailContext
import is.hail.methods.SimpleBinomRegressionInference
import is.hail.utils._

/**
  * Created by danfengc on 7/28/17.
  */
class SimpleBinomRegressionInferenceSuite extends SparkSuite {
  @Test def test = {
    val projectRoot = "/Users/danfengc/Documents/PROJECTS/unicorn/Project_0731/03"
    val binomRoot = "/Users/danfengc/Documents/PROJECTS/unicorn/BinomRegression_0731/03"
    val retval = SimpleBinomRegressionInference(hc, projectRoot, binomRoot)
    retval.show(10)
  }
}
