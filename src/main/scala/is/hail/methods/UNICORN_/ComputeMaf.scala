package is.hail.methods

import is.hail.annotations.Annotation
import is.hail.expr._
import is.hail.variant._
import org.apache.spark.rdd.RDD


object ComputeMaf {

  val signature = TStruct("Maf" -> TDouble)


  def computeNormalized(gs: Iterable[Genotype], nSamples: Int, useHWE: Boolean = false, nVariants: Int = -1) = {

  }


  def apply(vds: VariantDataset) = {
    val rdd: RDD[(Variant, Double)] = vds.rdd.map { case (v: Variant, (va, gs)) => {
      val gts = gs.hardCallIterator
      var sum = 0
      var count = 0
      while (gts.hasNext) {
        val gt = gts.next
        if (gt != -1) {
          sum += gt
          count += 1
        }
      }
      val p: Double =
          if (count == 0) 0.0
          else sum / (2 * count.toDouble)
      (v, p)}
    }

    rdd
  }


}
