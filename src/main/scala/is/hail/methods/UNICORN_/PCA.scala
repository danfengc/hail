package is.hail.methods

import breeze.stats._
import breeze.linalg._
import breeze.linalg.functions._

import is.hail.methods.UNICORN_.utilities.quantile
import is.hail.utils._
import is.hail.variant._
import is.hail.annotations._
import is.hail.expr._
import org.apache.spark.rdd.RDD

import scala.language.implicitConversions



/**
  * Created by danfengc on 6/27/17.
  */
object PCA {

  def pcSchema(k: Int): Type =
      TStruct((1 to k).map(i => (s"PC$i", TDouble)): _*)


  def makeAnnotation(is: IndexedSeq[Double], asArray: Boolean): Annotation =
    if (asArray)
      is
    else
      Annotation.fromSeq(is)


  def apply(vds: VariantDataset,
            k: Int,
            computeLoadings: Boolean,
            computeEigenvalues: Boolean,
            outlierTholdOpt: Option[Double] = None): (Map[Annotation, Annotation], Option[RDD[(Variant, Annotation)]], Option[Annotation]) = {

      val outlierThold = if (! outlierTholdOpt.isDefined) 1.0 else outlierTholdOpt.get
      if (outlierThold < 0.0)
        fatal(s"outlier threshold should be no less than 0.0")

      if (outlierThold > 1.0)
        fatal(s"outlier threshold should be no greater than 1.0")

      if (outlierThold == 1.0){
        info(s"perform sample PCA without outlier removal")
        SamplePCA(vds, k, computeLoadings, computeEigenvalues, true)
      }

      else {
        info(s" perform sample PCA with outlier removal, threshold is set to be ${outlierThold}")
        val (tmpScores: Map[Annotation, Annotation], _, _) = SamplePCA(vds, k, false, false, true)

        val scores = {
          val scores = vds.sampleIds.map(s =>
              tmpScores(s).asInstanceOf[IndexedSeq[Double]]
          )
          val nrow = scores.length
          new DenseMatrix(nrow, k, scores.transpose.flatten.toArray)
        }

        val center = mean(scores, Axis._0).toDenseVector

        val distance = scores(*, ::)
            .map {case dv => euclideanDistance(dv, center)}

        val cutoff = quantile(distance.data, outlierThold)

        val keepIndex = distance.findAll(d => d <= cutoff)
        val removeIndex = distance.findAll(d => d > cutoff)
        info(
          s"""number of samples in original dataset is ${vds.nSamples},
             |number of samples got removed is ${removeIndex.length},
             |number of samples kept is ${keepIndex.length}
           """.stripMargin)

        val kept = vds.sampleIds.zipWithIndex
          .filter {case (sampleId, i) => keepIndex.contains(i) }
          .map(_._1)

        val outlierRemovedVDS =
          vds.filterSamples((s, sa) => kept contains s)

        SamplePCA(outlierRemovedVDS, k, computeLoadings, computeEigenvalues, true)
      }

  }

}
