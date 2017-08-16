package is.hail.methods

import is.hail.SparkSuite
import org.testng.annotations.Test

class ProjectSuite extends SparkSuite{
  @Test def test {
    var vds = hc.importVCF("src/test/resources/sample2.vcf")
    vds = vds.splitMulti()
    val projectBuilder = Project(vds, "/Users/danfengc/Documents/PROJECTS/unicorn/Adaptivepca_0808/04",
      outputRoot=Some("/Users/danfengc/Documents/PROJECTS/unicorn/Project_0808/07"))
    projectBuilder.score.toDF().show(10, false)
  }

}
