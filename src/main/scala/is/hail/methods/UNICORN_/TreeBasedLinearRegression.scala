package is.hail.methods


import is.hail.variant.VariantDataset


object TreeBasedLinearRegression {

  def apply(vds: VariantDataset,
            yExpr: String,
            saRoot: String,
            vaRoot: String,
            sparseTreeCov: SparseTreeBasedCov) = {

    val cov = sparseTreeCov.getCovariates
    val expr = sparseTreeCov.getExpression.map(expr => s"$saRoot.$expr")
    val signature = sparseTreeCov.getSignature

    var ret = vds.annotateSamples(cov, signature, saRoot)
    ret = ret.linreg(yExpr, expr, vaRoot)
    ret
  }

}
