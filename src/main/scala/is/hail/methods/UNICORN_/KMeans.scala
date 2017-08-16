package is.hail.methods

import is.hail.annotations.Annotation
import is.hail.expr.{TStruct, _}
import is.hail.utils._
import is.hail.variant._
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd.RDD

import scala.collection.immutable.Iterable



/**
  * Created by danfengc on 6/27/17.
  */
object Kmeans {

  val saSignature: Type =  TStruct("clust" -> TInt)

  def clusterCenterSignature(k: Int) =
    TStruct((1 to k).map(i => (s"cluster_center$i", TArray(TDouble))): _*)


  def makeAnnotation(is: Vector): IndexedSeq[Double] =
    is.toArray.toIndexedSeq


  def toDouble(t: Type, code: String): Any => Double = t match {
    case TInt => _.asInstanceOf[Int].toDouble
    case TLong => _.asInstanceOf[Long].toDouble
    case TFloat => _.asInstanceOf[Float].toDouble
    case TDouble => _.asInstanceOf[Double]
    case _ => fatal(s"Sample annotation `$code' must be numeric or Boolean, got $t")
  }


  // IndexedSeq indexed by samples, Array by annotations
  def getSampleAnnotations(vds: VariantDataset, annots: Array[String], ec: EvalContext): IndexedSeq[Array[Option[Double]]] = {
    val (aT, aQ0) = annots.map(Parser.parseExpr(_, ec)).unzip
    val aQ = () => aQ0.map(_.apply())
    val aToDouble = (aT, annots).zipped.map(toDouble)

    vds.sampleIdsAndAnnotations.map { case (s, sa) =>
      ec.setAll(s, sa)
      (aQ().map(Option(_)), aToDouble).zipped.map(_.map(_))
    }
  }


  def getPhenoCovCompleteSamples(vds: VariantDataset, covExpr: Array[String]): (RDD[Vector], Int, Int, IndexedSeq[Annotation]) = {
    val sc = vds.sparkContext

    val nCovs = covExpr.size

    val symTab: Map[String, (Int, Type)] = Map(
      "s" -> (0, TString),
      "sa" -> (1, vds.saSignature))

    val ec = EvalContext(symTab)

    val covIS = getSampleAnnotations(vds, covExpr, ec)

    val (covForCompleteSamples: IndexedSeq[Array[Option[Double]]], completeSamples) =
      (covIS, vds.sampleIds)
        .zipped
        .filter((c, s) => c.forall(_.isDefined))

    val n = completeSamples.size
    if (n == 0)
      fatal("No complete samples: each sample is missing its phenotype or some covariate")

    val covArray = covForCompleteSamples.map(_.map(_.get)).toArray

    if (n < vds.nSamples)
      warn(s"${vds.nSamples - n} of ${vds.nSamples} samples have a missing phenotype or covariate.")

    val cov = sc.parallelize(covArray.map(Vectors.dense(_)))
    (cov, nCovs, n, completeSamples)
  }


  def fit(covs: RDD[Vector], nCovs: Int, n: Int, sampleMask: IndexedSeq[Annotation], N: Int, max_N: Int) = {
    val K =
      if (N == -9) {
        val max = if (max_N < n) max_N else n
        val BIC = for {
          i <- 2 to 3
          model = KMeans.train(covs, i, maxIterations = 1000)
          rss = model.computeCost(covs)
        } yield (rss + math.log(n) * nCovs * i, i)
        BIC.minBy(_._1)._2
      }
      else N

    val model: KMeansModel = new KMeans()
      .setK(K)
      .run(covs)

    val assignments: Map[Annotation, Int] = sampleMask
      .zip(model.predict(covs).collect())
      .toMap

    (K, assignments, model.clusterCenters)
  }


  def unicorn(sc: SparkContext, scores: Map[Annotation, Annotation], nCovs: Int, N: Int = -9, max_N: Int = 10) = {
    val covs = sc.parallelize(
        scores.map {case (sample, cov) => Vectors.dense(cov.asInstanceOf[IndexedSeq[Double]].toArray)}
              .toSeq)
    val sampleMask = scores.unzip._1.toIndexedSeq
    val n = covs.count().toInt
    fit(covs, nCovs, n, sampleMask, N = 3, max_N)
  }



  def apply(vds: VariantDataset, covExpr: Array[String], N: Int = -9, max_N: Int = 10) = {

    val (covs, nCovs, n, sampleMask) = getPhenoCovCompleteSamples(vds, covExpr)
    val sc = vds.sparkContext

    val (k, assignments, clusterCenters) = fit(covs, nCovs, n, sampleMask, N, max_N)
    (k, assignments, clusterCenters)
  }

}
