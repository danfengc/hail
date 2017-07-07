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
import org.json4s.JsonAST._

import org.json4s.jackson.Serialization
import org.json4s.{JInt, JObject}

import scala.language.implicitConversions


class AdaptivePCA(p: Int,
                  outlierThold: Double = 1.0,
                  sampleIds: IndexedSeq[Annotation],
                  maxLeafSize: Int,
                  maxIter: Option[Int]) {

  type TreeCov = Map[Annotation, Annotation]
  type TreeSchema = Array[(String, Type)]
  type TreeExpr = Array[String]
  type vAnnotation = RDD[(Variant, Annotation)]
  type metaInfo = (Int, Array[Double])

  private var sparseTreeCov = createSparseTreeCov(sampleIds)
  private var sparseTreeSchema = createSparseTreeSchema
  private var sparseTreeExpr = createSparseTreeExpr

  private def createSparseTreeCov(sampleId: IndexedSeq[Annotation]): TreeCov =
    Map(sampleId.map(at => at -> IndexedSeq.empty[Double]): _*)

  private def updateSparseTreeCov(treeCov: TreeCov, scores: TreeCov): TreeCov =
      treeCov.map { case(sampleId, cov1) => {
          val cov2 = scores.getOrElse(sampleId, IndexedSeq.fill(p)(0.0))
          sampleId ->  (cov1.asInstanceOf[IndexedSeq[Double]] ++ cov2.asInstanceOf[IndexedSeq[Double]])}}

  def getSparseTreeCov = sparseTreeCov

  private def createSparseTreeSchema: TreeSchema = Array.empty[(String, Type)]

  private def updateSparseTreeSchema(treeSchema: TreeSchema, branch: String): TreeSchema =
    treeSchema ++ (1 to p).map(i => (s"PC.$branch.$i", TDouble))

  def getSparseTreeSchema = sparseTreeSchema

  private def createSparseTreeExpr: TreeExpr = Array.empty[String]

  private def updateSparseTreeExpr(treeExpr: Array[String], branch: String): TreeExpr =
    treeExpr ++ (1 to p).map(i => s"PC.$branch.$i")

  def getSparseTreeExpr = sparseTreeExpr

  private def getSubPopulation(vds: VariantDataset, assignments: Map[Annotation, Annotation], index: Int) = {
    val subset = assignments
      .filter(_._2.asInstanceOf[Int] == index)
      .keys
      .toIndexedSeq
    vds.filterSamples{case (s, sa) => subset.contains(s)}
  }

  private def createRootNode(iter: Int) = iter == 0

  private def createLeafNode(iter: Int, nSamples: Int) = iter == 0 || nSamples < maxLeafSize

  private def scoresKT_(hc: HailContext, sc: SparkContext, scores: TreeCov): KeyTable =
    KeyTable(hc, sc.parallelize(scores.toIndexedSeq)
      .map { case (s, sa) =>
        Row(s, sa)
      },
      TStruct(
        "s" -> TString,
        "sa" -> PCA.pcSchema(p)),
      Array("s"))

  private def variantsKT_(hc: HailContext, sc: SparkContext, variantsAndAnnotation: vAnnotation, vaSignature: Type) = {
    KeyTable(hc, variantsAndAnnotation.map { case (v, va) =>
      Row(v, va)
    },
      TStruct(
        "v" -> Variant.expandedType,
        "va" -> vaSignature),
      Array("v"))
  }

  private def json_(clusterCenters: Array[Vector], k: Int) = {
    val jClusterCenters = JObject(
      clusterCenters.zipWithIndex.map {
        case (center, index) => {
          val jCenter = JArray(center.toArray.toList.map(JDouble(_)))
          (s"$index", jCenter)
        }
      }.toList
    )

    val json = JObject(
      ("K", JInt(k)),
      ("clusterCenters", jClusterCenters)
    )

    json
  }

  def createTreeStruct(vds: VariantDataset,
            hc: HailContext,
            outputRoot: String = "",
            createSparseTreeCov: Boolean = false,
            iter: Int = 0,
            branch: String = "root"): AdaptivePCAResult = {

    val sc = vds.sparkContext
    val hConf = sc.hadoopConfiguration

    val sampleIds = vds.sampleIds
    val nSamples = sampleIds.length

    val isRoot = createRootNode(iter)
    val isLeaf = createLeafNode(iter, nSamples)

    val (scores, loadings: Option[RDD[(Variant, Annotation)]], eigenvalues) = PCA(vds, p, true, true, outlierThold)
    val maf: RDD[(Variant, Annotation)] = MAF(vds)

    val scoresKT = scoresKT_(hc, sc, scores)
    val loadingsKT = variantsKT_(hc, sc, loadings.get, PCA.pcSchema(p))
    val mafKT = variantsKT_(hc, sc, maf, MAF.signature)

    loadingsKT.export(s"$outputRoot/$branch/loadings.tsv", header = false)
    mafKT.export(s"$outputRoot/$branch/maf.tsv", header = false)

    if (createSparseTreeCov) {
      sparseTreeCov = updateSparseTreeCov(sparseTreeCov, scores)
      sparseTreeExpr = updateSparseTreeExpr(sparseTreeExpr, branch)
      sparseTreeSchema = updateSparseTreeSchema(sparseTreeSchema, branch)
    }

    val node = if (! isLeaf) {
      val (k, assignments, clusterCenters) = Kmeans.unicorn(sc, scores, p)
      val json = json_(clusterCenters, k)
      hConf.writeTextFile(s"$outputRoot/$branch/metadata.json.gz")(Serialization.writePretty(json, _))

      val n =
        if (isRoot)
          new Root(scoresKT, mafKT, sampleIds, clusterCenters, branch)
        else
          new Parent(scoresKT, mafKT, sampleIds, clusterCenters, branch)
      0 until k foreach {
        case index: Int => {
          val sub = getSubPopulation(vds, assignments, index)
          val child = createTreeStruct(sub, hc, outputRoot, createSparseTreeCov, iter + 1, s"$branch/$index")
          n.insert(child)
        }
      }
      n
    }
    else
      new Leaf(scoresKT, mafKT, sampleIds, branch)

    node
  }

}

object AdaptivePCA {
  def apply(vds: VariantDataset,
            hc: HailContext,
            p: Int,
            outlierThold: Double,
            maxLeafSize: Int,
            maxIter: Option[Int],
            createTreeCov: Boolean = false,
            outputRoot: String = "") = {
    val adaptivePCA = new AdaptivePCA(p, outlierThold, vds.sampleIds, maxLeafSize, maxIter)
    val adaptivePCAResult = adaptivePCA.createTreeStruct(vds, hc, outputRoot, createTreeCov)
    if (createTreeCov)
      (adaptivePCAResult, Some(adaptivePCA.sparseTreeCov), Some(TStruct(adaptivePCA.sparseTreeSchema:_*)), Some(adaptivePCA.sparseTreeExpr))
    else
      (adaptivePCAResult, None, None, None)
  }
}
