package is.hail.methods.UNICORN

import is.hail.SparkSuite
import is.hail.variant.Variant
import org.testng.annotations.Test

/**
  * Created by danfengc on 6/23/17.
  */
class FstSuite extends SparkSuite {

  val v1 = Variant("1", 1, "C", "T") // x = (0, 1, 0, 0, 0, 1)
  val v2 = Variant("1", 2, "C", "T") // x = (., 2, ., 2, 0, 0)
  val v3 = Variant("1", 3, "C", "T") // x = (0, ., 1, 1, 1, .)
  val v6 = Variant("1", 6, "C", "T") // x = (0, 0, 0, 0, 0, 0)
  val v7 = Variant("1", 7, "C", "T") // x = (1, 1, 1, 1, 1, 1)
  val v8 = Variant("1", 8, "C", "T") // x = (2, 2, 2, 2, 2, 2)
  val v9 = Variant("1", 9, "C", "T") // x = (., 1, 1, 1, 1, 1)
  val v10 = Variant("1", 10, "C", "T") // x = (., 2, 2, 2, 2, 2)

  @Test def testWithTwoCov() {
  }
}
