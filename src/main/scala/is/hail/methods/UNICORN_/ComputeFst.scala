package is.hail.methods

import breeze.linalg.{DenseMatrix, DenseVector, Vector}
import is.hail.annotations.Annotation
import is.hail.expr._
import is.hail.stats.{LinearRegressionModel, RegressionUtils}
import is.hail.utils._
import is.hail.variant._
import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable

/**
  * Created by danfengc on 6/20/17.
  */


object ComputeFst {
  val signature =
    TStruct(("Maf",TDouble), ("Fst", TDouble))

  def toString(t: Type, code: String): Any => String = t match {
    case TInt => _.asInstanceOf[Int].toString
    case TLong => _.asInstanceOf[Long].toString
    case TFloat => _.asInstanceOf[Float].toString
    case TDouble => _.asInstanceOf[Double].toString
    case TBoolean => _.asInstanceOf[Boolean].toString
    case TString => _.asInstanceOf[String]
    case _ => fatal(s"Sample annotation `$code' must be numeric or Boolean, got $t")
  }


  def getSampleAnnotation(vds: VariantDataset, annot: String, ec: EvalContext): IndexedSeq[Option[String]] = {
    val (aT, aQ) = Parser.parseExpr(annot, ec)
    val aToDouble = toString(aT, annot)

    vds.sampleIdsAndAnnotations.map { case (s, sa) =>
      ec.setAll(s, sa)
      Option(aQ()).map(aToDouble)
    }
  }

  def getSubPopInfo(vds: VariantDataset,
                    subPopExpr: String) = {

    val symTab = Map(
      "s" -> (0, TString),
      "sa" -> (1, vds.saSignature))

    val ec = EvalContext(symTab)

    val subPopIS = getSampleAnnotation(vds, subPopExpr, ec)
    val (subPopCompleteSamples, completeSamples) =
      (subPopIS, vds.sampleIds)
        .zipped
        .filter((p, s) => p.isDefined)

    val n = completeSamples.size
    if (n == 0)
      fatal("No complete samples: each sample is missing its sub-population info")

    val subPopArray = subPopCompleteSamples.map(_.get)
    val subPopMap = completeSamples
        .zip(subPopArray)
        .toMap
    (subPopMap, completeSamples)
  }


  def fit(gs: Iterable[Genotype], subpopInfo: Map[Annotation, String], sampleIds: IndexedSeq[Annotation], mask: Array[Boolean]) = {

    val gts = gs.hardCallIterator
    var counter = mutable.Map[String, (Double, Double)]()
    var i = 0
    var sum = 0
    var count = 0

    while (gts.hasNext) {
      val gt = gts.next()
      if (mask(i)) {
        val sample = sampleIds(i)
        val pop = subpopInfo.getOrElse(sample,
          throw new RuntimeException(s"cannot find population info for ${sample}"))

        val (s, c) = counter.getOrElse(pop, (0.0, 0.0))
        if (gt != -1) {
          counter.update(pop, (s + gt, c + 1))
          sum += gt
          count += 1
        }
      }
      i += 1
    }
    info(s"sum = ${sum.toString}, count = ${count.toString}")

    val P = if (count == 0) 0.0 else sum.toDouble / (2 * count.toDouble)
    info(s"P = ${P.toString}")
    val btwVar =
      counter.foldLeft(0.0)
        { case (btw: Double, (pop: String, (s: Double, c: Double))) => {
            val Pi = if (c == 0.0) 0.0 else s / (2 * c)
            btw + c / count * math.pow(Pi - P, 2)
          }
        }

    val tolVar = P * (1.0 - P)
    val Fst = btwVar / tolVar
    info(s"Fst = ${Fst.toString}")
    Annotation(P, Fst)

  }



  def apply(vds: VariantDataset, subPopExpr: String, root: String) = {

    val (subPopInfo, completeSamples) = getSubPopInfo(vds, subPopExpr)
    val sampleMask: Array[Boolean] = vds.sampleIds.map(completeSamples.toSet).toArray
    val sampleIds = vds.sampleIds

    val sc = vds.sparkContext
    val subPopInfoBc = sc.broadcast(subPopInfo)
    val sampleIdsBc = sc.broadcast(sampleIds)
    val maskBc = sc.broadcast(sampleMask)

    val pathVA = Parser.parseAnnotationRoot(root, Annotation.VARIANT_HEAD)
    val (newVAS, inserter) = vds.insertVA(ComputeFst.signature, pathVA)

    vds.mapAnnotations { case (v, va, gs) =>

      val fstAnnot = fit(gs, subPopInfoBc.value, sampleIdsBc.value, maskBc.value)
      val newAnnotation = inserter(va, fstAnnot)
      assert(newVAS.typeCheck(newAnnotation))
      newAnnotation
    }.copy(vaSignature = newVAS)
  }


}
