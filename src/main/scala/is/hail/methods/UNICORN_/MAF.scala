package is.hail.methods

import is.hail.annotations.Annotation
import is.hail.expr._
import is.hail.variant._


object MAF {

  val signature = TStruct("maf" -> TDouble)

  def apply(vds: VariantDataset) = {
    vds.rdd.map { case (v, (va, gs)) => {
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
      val p = if (count == 0) 0.0 else sum / (2 * count.toDouble)
      (v, Annotation(p))}
    }
  }


}
