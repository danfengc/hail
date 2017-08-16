package is.hail.methods.UNICORN

import is.hail.SparkSuite
import is.hail.methods.AdaptivePCA
import org.testng.annotations.Test


class AdaptivepcaSuite extends SparkSuite{

  val p = 3
  val maxIter = 1


  @Test def test = {
    var vds = hc.importVCF("src/test/resources/sample2.vcf")
    vds = vds.splitMulti()
    val (adaptivePCAResult, treeBasedCovariates) = AdaptivePCA(vds,
      p, 101, maxIter = Some(1), outputRoot = Some("/Users/danfengc/Documents/PROJECTS/unicorn/Adaptivepca_0808/05"), saveScore = true)
    print(adaptivePCAResult.toDenseMatrix)
  }


}
