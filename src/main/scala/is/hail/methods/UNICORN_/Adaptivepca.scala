package is.hail.methods

import is.hail.HailContext
import is.hail.annotations.Annotation
import is.hail.expr._
import is.hail.utils._
import is.hail.variant._
import is.hail.keytable.KeyTable
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.json4s.JsonAST._
import org.json4s.jackson.Serialization
import org.json4s.{JInt, JObject}

import scala.language.implicitConversions


class AdaptivePCA(p: Int,
                  outlierThold: Option[Double] = None,
                  sampleIds: IndexedSeq[Annotation],
                  maxLeafSize: Int,
                  maxIter: Option[Int],
                  createTreeCov: Boolean = false) {

  private val sparseTreeBasedCov = SparseTreeBasedCov(sampleIds, p)

  private def getSubPopulation(vds: VariantDataset, assignments: Map[Annotation, Annotation], index: Int) = {
    val subset = assignments
      .filter { case (_, popIndex: Int) =>
        popIndex == index
      }
      .keys
      .toIndexedSeq
    info(s"samples in subset ${index} is ${subset.length}")
    vds.filterSamples { case (s, sa) => subset.contains(s) }
  }

  private def createRootNode(iter: Int) = iter == 0

  private def createLeafNode(iter: Int, nSamples: Int) =
    if (maxIter.isDefined) iter == maxIter.get || nSamples < maxLeafSize
    else
      nSamples < maxLeafSize

  private def toScoreDataFrame(scores: Map[Annotation, Annotation], sqlContext: SQLContext, sc: SparkContext) = {
    val signature = StructType(
      Array(
        StructField("s", StringType, false),
        StructField("sc", ArrayType(DoubleType, true), false)
      )
    )

    val rdd = sc.parallelize(scores.toSeq)
                .map {case (id, score) => Row.fromTuple(id, score.asInstanceOf[IndexedSeq[Double]])}
    sqlContext.createDataFrame(rdd, signature)
  }

  private def toMafDataFrame(variantsAndAnnotation:RDD[(Variant, Double)], sqlContext: SQLContext) = {
    val signature = StructType(
      Array(
        StructField("v", Variant.sparkSchema, false),
        StructField("Maf", DoubleType, false)
      )
    )

    val rdd = variantsAndAnnotation
        .map { case (v, va: Double) =>
          Row.fromTuple(v.toRow, va)
        }
    sqlContext.createDataFrame(rdd, signature)

  }

  private def toLoadingsDataFrame(variantsAndAnnotation: RDD[(Variant, Annotation)], sqlContext: SQLContext) = {
    val signature = StructType(
      Array(
        StructField("v", Variant.sparkSchema, false),
        StructField("Ld", ArrayType(DoubleType, true), false)
      )
    )

    val rdd = variantsAndAnnotation
      .map { case (v, va) =>
        Row.fromTuple(v.toRow, va.asInstanceOf[IndexedSeq[Double]])
      }

    sqlContext.createDataFrame(rdd, signature)

  }

  private def json_(clusterCenters: Option[Array[Vector]] = None, k: Int) = {
    val jClusterCenters =
      if (clusterCenters.isDefined)
        JObject(
          clusterCenters.get.zipWithIndex.map {
            case (center, index) => {
              val jCenter = JArray(center.toArray.toList.map(JDouble(_)))
              (s"$index", jCenter)
            }
        }.toList)
      else
        JNull
    val json = JObject(
      ("K", JInt(k)),
      ("P", JInt(p)),
      ("clusterCenters", jClusterCenters)
    )
    json
  }

  def createTreeStruct(vds: VariantDataset,
            outputRoot: Option[String] = None,
            iter: Int = 0,
            branch: String = "root",
            saveScore: Boolean = false): AdaptivepcaBuilder = {

    info(s"branch is ${branch}, number of samples is ${vds.nSamples}")
    val sc = vds.sparkContext
    val hc = vds.hc
    val sqlContext = hc.sqlContext


    val sampleIds = vds.sampleIds
    val nSamples = sampleIds.length

    val isRoot = createRootNode(iter)
    val isLeaf = createLeafNode(iter, nSamples)

    val (scores: Map[Annotation, Annotation], loadings: Option[RDD[(Variant, Annotation)]], eigenvalues) = PCA(vds, p, true, true, outlierThold)
    val maf = ComputeMaf(vds)

    val scoresDataframe = toScoreDataFrame(scores, sqlContext, sc)
    val loadingDataframe = toLoadingsDataFrame(loadings.get, sqlContext)
    val mafDataframe = toMafDataFrame(maf, sqlContext)


    if (outputRoot.isDefined) {
      loadingDataframe.write.parquet(s"${outputRoot.get}/${branch}/Loading.parquet")
      mafDataframe.write.parquet(s"${outputRoot.get}/${branch}/Maf.parquet")
      if (saveScore)
        scoresDataframe.write.parquet(s"${outputRoot.get}/${branch}/Score.parquet")
    }

    sparseTreeBasedCov.updateSparseTreeCov(scores)
    sparseTreeBasedCov.updateSparseTreeSchema(branch)
    sparseTreeBasedCov.updateSparseTreeExpr(branch)

    val (node, json) = if (! isLeaf) {
      val (k, assignments, clusterCenters: Array[Vector]) = Kmeans.unicorn(sc, scores, p)
      val n =
          new AdaptivepcaBuilder(scoresDataframe, mafDataframe, sampleIds, branch, p, Some(clusterCenters))
      val json = json_(Some(clusterCenters), k)
      0 until k foreach {
        case index: Int => {
          val sub = getSubPopulation(vds, assignments, index)
          val child = createTreeStruct(sub, outputRoot, iter + 1, s"${branch}_branch$index")
          n.insert(child)
        }
      }
      (n, json)
    }
    else {
      val n = new AdaptivepcaBuilder(scoresDataframe, mafDataframe, sampleIds, branch, p)
      val json = json_(None, 0)
      (n, json)
    }

    if (outputRoot.isDefined)
      vds.hadoopConf.writeTextFile(s"${outputRoot.get}/$branch/metadata.json.gz") { out =>
        Serialization.write(json, out)}
    node
  }

}

object AdaptivePCA {
  def apply(vds: VariantDataset,
            p: Int,
            maxLeafSize: Int,
            outlierThold: Option[Double] = None,
            maxIter: Option[Int] = None,
            outputRoot: Option[String] = None,
            saveScore: Boolean = false) = {
    val adaptivePCA = new AdaptivePCA(p, outlierThold, vds.sampleIds, maxLeafSize, maxIter)
    val adaptivePCAResult = adaptivePCA.createTreeStruct(vds, outputRoot = outputRoot, saveScore = saveScore)
    (adaptivePCAResult, adaptivePCA.sparseTreeBasedCov)
  }
}

case class SparseTreeBasedCov(sampleIds: IndexedSeq[Annotation], p: Int) {

  private var cov: Map[Annotation, IndexedSeq[Double]] = createSparseTreeCov(sampleIds)
  private var signature = createSparseTreeSchema
  private var expr = createSparseTreeExpr

  def getCovariates : Map[Annotation, Annotation] = this.cov.map { case(id, score) =>
    (id, Annotation.fromSeq(score))
  }
  def getSignature = TStruct(this.signature:_*)
  def getExpression = this.expr

  private def createSparseTreeCov(sampleId: IndexedSeq[Annotation]): Map[Annotation, IndexedSeq[Double]] =
    Map(sampleId.map(at => at -> IndexedSeq.empty[Double]): _*)

  private def createSparseTreeSchema: Array[(String, Type)] = Array.empty[(String, Type)]

  private def createSparseTreeExpr: Array[String] = Array.empty[String]

  def updateSparseTreeCov(scores: Map[Annotation, Annotation]): Unit =
    cov = cov.map { case(sampleId, cov1) => {
      val cov2 = scores.getOrElse(sampleId, IndexedSeq.fill(p)(0.0))
      sampleId ->  (cov1.asInstanceOf[IndexedSeq[Double]] ++ cov2.asInstanceOf[IndexedSeq[Double]])}}

  def updateSparseTreeSchema(branch: String): Unit =
    signature = signature ++ (1 to p).map(i => (s"${branch}_PC${i}", TDouble))

  def updateSparseTreeExpr(branch: String): Unit =
    expr = expr ++ (1 to p).map(i => s"${branch}_PC${i}")
}

