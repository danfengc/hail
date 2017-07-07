package is.hail.methods

import is.hail.annotations.Annotation
import is.hail.variant.VariantDataset
import is.hail.expr._
/**
  * Created by danfengc on 7/5/17.
  */
object StageTwo {

  def gp = {}

  def binomRegression = {}

  def linregBySparseTreeBasedCov(vds: VariantDataset,
                                 y: String,
                                 sparseTreeCov: Map[Annotation, Annotation],
                                 sparseTreeSignature: Type,
                                 sparseTreeExpr: Array[String],
                                 useDosages: Boolean = false) = {

    var ret = vds.annotateSamples(sparseTreeCov, sparseTreeSignature, "sa.unicorn.treecov")
    ret = ret.linreg(y, sparseTreeExpr, root = "va.unicorn.treecov", useDosages = useDosages)
    ret
  }
}
