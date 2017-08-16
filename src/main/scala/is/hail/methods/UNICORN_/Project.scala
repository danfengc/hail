package is.hail.methods


import breeze.linalg.DenseVector
import breeze.linalg.functions.euclideanDistance
import is.hail.variant.{Variant, VariantDataset}
import is.hail.HailContext
import is.hail.annotations.Annotation
import is.hail.methods.UNICORN_.utilities.{PreOderEnumeration, TreeNode}
import is.hail.stats.ToHWENormalizedIndexedRowMatrixWithMaf

import scala.language.implicitConversions
import scala.reflect.ClassTag
import is.hail.utils._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types._
import org.json4s.{JInt, JObject}
import org.json4s.JsonAST._
import org.json4s.jackson._


object Project {

  private def getAndCastJSON[T <: JValue](fields: Map[String, JValue], fname: String)(implicit tct: ClassTag[T]): T =
    fields.get(fname) match {
      case Some(t: T) => t
      case Some(other) =>
        fatal(
          s"""corrupt unicorn tree: invalid metadata
             |  Expected `${tct.runtimeClass.getName}' in field `$fname', but got `${other.getClass.getName}'
             |  """.stripMargin)
      case None =>
        fatal(
          s"""corrupt unicorn tree: invalid metadata
             |  Missing field `$fname'
             |  """.stripMargin)
    }


  private def readLoading(dir: String, index: Map[Variant, Int], hc: HailContext) = {
    val sc = hc.sc
    val sqlContext = hc.sqlContext
    val indexBroadcast = sc.broadcast(index)
    val rdd = sqlContext.readParquetSorted(dir,
      Some(Array("v", "Ld")))
      .map(row =>
        (row.getVariant(0), Vectors.dense(row.get(1).asInstanceOf[Seq[Double]].toArray)))
    rdd.map {case (v, va) => IndexedRow(indexBroadcast.value(v), va)}
  }


  private def readMaf(dir: String, hc: HailContext) = {
    val sqlContext = hc.sqlContext
    val rdd = sqlContext.readParquetSorted(dir,
      Some(Array("v", "Maf")))
      .map(row =>
        (row.getVariant(0), row.get(1).asInstanceOf[Double]))
    rdd.collect().toMap
  }

  private def readMetaData(dir: String, hc: HailContext) = {
    val hConf = hc.hadoopConf
    if (!hConf.exists(dir))
      fatal(
        s"""corrupt or outdated unicorn tree: invalid metadata
           |  No `metadata.json.gz' file found in unicorn tree directory
           |  Recreate VDS with current version of Hail.""".stripMargin)

    val json = try {
      hConf.readFile(dir)(
        in => JsonMethods.parse(in))
    } catch {
      case e: Throwable => fatal(
        s"""
           |corrupt unicorn tree: invalid metadata file
           |  caught exception: ${expandException(e, logMessage = true)}
         """.stripMargin)
    }

    val fields: Map[String, JValue] = json match {
      case jo: JObject => jo.obj.toMap
      case _ =>
        fatal(
          s"""corrupt unicorn tree: invalid metadata value
             |  Recreate VDS with current version of Hail.""".stripMargin)
    }

    val k = getAndCastJSON[JInt](fields, "K").num.toInt
    val p = getAndCastJSON[JInt](fields, "P").num.toInt
    val clusterCenters: Option[Array[DenseVector[Double]]] =
        if (k == 0) None
        else
          Some((0 until k).toArray map { case index => {
            val json = getAndCastJSON[JObject](fields, "clusterCenters")
            val f = json.obj.toMap
            val center = getAndCastJSON[JArray](f, s"$index").extract[Array[Double]]
            DenseVector(center)
          }})
    (p, k, clusterCenters)
  }


  private def toJSON(Nk: Int) = {
    val json = JObject(("Nk", JInt(Nk)))
    json
  }

  private def normalize(vds: VariantDataset, Maf: Map[Variant, Double]) = {
    val sc = vds.sparkContext

    val variants = Maf.keySet
    val nVariants = variants.size
    val nSamples = vds.nSamples
    val index = variants.zipWithIndex.toMap

    val indexBroadcast = sc.broadcast(index)
    val MafBroadcast = sc.broadcast(Maf)
    val nVariantBroadcast = sc.broadcast(nVariants)
    val nSampleBroadcast = sc.broadcast(nSamples)

    val standardized = vds.rdd.map {
      case (v, (va, gs)) => {
        val p = MafBroadcast.value(v)
        val mean = 2 * p
        val sdRecip =
          if (p == 0.0 || p == 1.0) 0.0
          else 1.0 / math.sqrt(2 * p * (1 - p) * nVariantBroadcast.value)
        def standardize(c: Int): Double =
          (c - mean) * sdRecip
        IndexedRow(indexBroadcast.value(v),
          Vectors.dense(gs.iterator.map(_.nNonRefAlleles.map(standardize).getOrElse(0.0)).toArray))}
     }
    (variants.toArray, new IndexedRowMatrix(standardized, nVariants, nSamples))
  }

  private def isLeaf(k: Int) = k == 0


  private def toScoreDataFrame(scores: RDD[(Annotation, Array[Double])], sqlContext: SQLContext) = {
    val signature = StructType(
      Array(
        StructField("s", StringType, false),
        StructField("sc", ArrayType(DoubleType, true), false)
      )
    )
    val rdd = scores
      .map { case (s, sa) =>
        Row.fromTuple(s.asInstanceOf[String], sa)}
    sqlContext.createDataFrame(rdd, signature)
  }

  private def toMafDataFrame(Maf:RDD[(Variant, Double)], sqlContext: SQLContext) = {
    val signature = StructType(
      Array(
        StructField("v", Variant.sparkSchema, false),
        StructField("Maf", DoubleType, false)
      )
    )
    val rdd = Maf
      .map { case (v, va) =>
        Row.fromTuple(v.toRow, va.asInstanceOf[Double])}
    sqlContext.createDataFrame(rdd, signature)

  }


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


  def apply(vds: VariantDataset, inputDir: String, branch: String = "root", outputRoot: Option[String] = None, isPruned: Boolean=false): ProjectBuilder = {
    val hc = vds.hc
    val sc = vds.sparkContext
    val hConf = vds.hadoopConf

    info(s"${branch}\n")
    val MafDir = s"$inputDir/$branch/Maf.parquet"
    val LoadingDir = s"$inputDir/$branch/Loading.parquet"
    val metaDataDir = s"$inputDir/$branch/metadata.json.gz"

    val Maf: Map[Variant, Double] = readMaf(MafDir, hc)
    val (p, k, clusterCenters) = readMetaData(metaDataDir, hc)

    val pruned = if (isPruned)
        vds
    else {
      val variants = Maf.keySet
      vds.filterVariants { case (v, va, gs) => variants.contains(v) }
    }


    val (variants, normalized: IndexedRowMatrix) = normalize(pruned, Maf)

    val samples = pruned.sampleIds
    val sampleBroadcast = sc.broadcast(samples)

    val variantMap = variants.zipWithIndex.toMap
    val nVariants = pruned.variants.count()

    val Loading = readLoading(LoadingDir, variantMap, hc)

    val scores = normalized.toBlockMatrix().transpose
      .multiply(new IndexedRowMatrix(Loading, nVariants, p).toBlockMatrix())
      .toIndexedRowMatrix()

    val sampleScores: RDD[(Annotation, Array[Double])] = scores.rows map { case row => {
      val index = row.index
      val ID = sampleBroadcast.value(index.toInt)
      (ID, row.vector.toArray)}}

    val scoreDataFrame = toScoreDataFrame(sampleScores, hc.sqlContext)
    val node = ProjectBuilder(branch, samples, scoreDataFrame)

    if (!isLeaf(k)) {
      val clusterCenterBroadcast = sc.broadcast(clusterCenters)
      val subPopulationInfo = sampleScores.map { case (s, sa) => {
        val cl: Int = {
          clusterCenterBroadcast.value.get.map {
            case center: DenseVector[Double] => euclideanDistance(center, DenseVector[Double](sa))
          }.zipWithIndex.min._2
        }
        (s, cl)
      }
      }.collect().toMap

      0 until k foreach {
        case index: Int => {
          val sub = getSubPopulation(pruned, subPopulationInfo, index)
          val child = apply(sub, inputDir, s"${branch}_branch$index", outputRoot, true)
          node.insert(child)
        }
      }
    }
    else {
      val MafCase: RDD[(Variant, Double)] = ComputeMaf(vds)
      val MafCaseDataFrame = toMafDataFrame(MafCase, hc.sqlContext)

      if (outputRoot.isDefined) {
        scoreDataFrame.write.parquet(s"${outputRoot.get}/${branch}/Score.parquet")
        MafCaseDataFrame.write.parquet(s"${outputRoot.get}/${branch}/Maf.parquet")
      }

    }
    node
  }
}

case class ProjectBuilder(branch: String,
                          sampleIds: IndexedSeq[Annotation],
                          score: DataFrame) extends TreeNode(branch){


  def get(treePath: String) = {
    val nodes = new PreOderEnumeration(this)
    val node = nodes.filter(n => {
      val path = n.asInstanceOf[ProjectBuilder].branch
      treePath == path
    }).next().asInstanceOf[ProjectBuilder]
    node
  }

  def getBranch = branch

  def getScore = score

  def printSchema: String = {
    val nodes = new PreOderEnumeration(this)
    nodes.foldLeft("")((tmpString, node) => {
      val level = node.getLevel
      val treePath = node.getTreePath
      tmpString + " " * 2 * level + " - " + treePath + "\n"
    })
  }

}