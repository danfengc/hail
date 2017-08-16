package is.hail.methods.UNICORN

import java.io.File

import is.hail.SparkSuite
import breeze.linalg.csvwrite
import is.hail.methods.SimpleBinomRegression
import is.hail.variant.Variant
import org.testng.annotations.Test

class SimpleBinomRegressionSuite extends SparkSuite {


  @Test def test = {
    // import vds from resources
    val vds = hc.importVCF("src/test/resources/regressionLinear.vcf")
    val (adaptivepcaResult, treeBasedCov) = vds.adaptivepca(p = 2,
                                                            maxLeafSize = 100,
                                                            maxIter = Some(0))
    // export phenotype dataset
    val scores = adaptivepcaResult.toDenseMatrix
    csvwrite(new File("src/test/resources/regressionBinom.pheno"), scores)

    // Rscript
    // x <- read.csv("regressionBinom.pheno", h=F, stringsAsFactors = F)
    // colnames(x) <- c("x1", "x2")

    // y11 <- c(0, 1, 0, NA, 0, 0, 1, 0)
    // y12 <- c(2, 1, 2, NA, 2, 2, 1, 2)

    // dat1 <- cbind(y11, y12, x)
    // dat1 <- dat1[complete.cases(dat1), ]

    // m1 <- glm(cbind(y11, y12) ~ x1 + x2, data = dat1, family="binomial")
    // m1$coefficients
    // ##(Intercept)          x1          x2
    // ##    -2.108       -1.290       -2.493

    // vcov(m1)
    // ##            (Intercept)        x1         x2
    // ##(Intercept)   1.3513670 1.0669574  0.8454704
    // ##              1.0669574 6.4543139  0.2405991
    // ##              0.8454704 0.2405991 11.2835353
    //

    val v1 = Variant("1", 1, "C", "T") // x = (0, 1, 0, 0, 0, 1)
    val v2 = Variant("1", 2, "C", "T") // x = (., 2, ., 2, 0, 0)
    val v3 = Variant("1", 3, "C", "T") // x = (0, ., 1, 1, 1, .)
    val v6 = Variant("1", 6, "C", "T") // x = (0, 0, 0, 0, 0, 0)
    val v7 = Variant("1", 7, "C", "T") // x = (1, 1, 1, 1, 1, 1)
    val v8 = Variant("1", 8, "C", "T") // x = (2, 2, 2, 2, 2, 2)
    val v9 = Variant("1", 9, "C", "T") // x = (., 1, 1, 1, 1, 1)
    val v10 = Variant("1", 10, "C", "T") // x = (., 2, 2, 2, 2, 2)

    val binomResult = SimpleBinomRegression(vds, adaptivepcaResult)
    val df = binomResult.dataframe("root")
    df.printSchema()
    df.select("binomFit.b").show(10, false)
    df.select("binomFit.vcov").show(10, false)
  }
}
