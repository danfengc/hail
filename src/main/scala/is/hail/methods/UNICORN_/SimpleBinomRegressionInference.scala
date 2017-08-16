package is.hail.methods

import breeze.linalg.{*, Axis, DenseMatrix, DenseVector, sum, trace}
import is.hail.variant.{Genotype, Variant, VariantDataset}
import is.hail.HailContext
import is.hail.methods.UNICORN_.logit

import scala.language.implicitConversions
import is.hail.utils._
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types._



/**
  * Created by danfengc on 7/20/17.
  */
object SimpleBinomRegressionInference {

  private def readEstAndCovariance(dir: String, hc: HailContext) = {
    val sc = hc.sc
    val sqlContext = hc.sqlContext
    val rdd = sqlContext.readParquetSorted(dir,
      Some(Array("v", "binomFit")))
      .map(row => {
        val v = row.getVariant(0)
        val binomFit = row.getStruct(1)

        val beta =
          if (binomFit.isNullAt(5))
            None
          else
            Some(DenseVector(binomFit.get(5).asInstanceOf[Seq[Double]].toArray))

        val vcov =
          if (binomFit.isNullAt(6))
            None
          else {
              val p = binomFit.getInt(6)
              Some(new DenseMatrix[Double](p, p, binomFit.get(4)
            .asInstanceOf[Seq[Double]]
            .toArray))}

          (v, (beta, vcov))
      })
    rdd
  }

  private def readScore(dir: String, hc: HailContext) = {
    val sc = hc.sc
    val sqlContext = hc.sqlContext

    val rdd = sqlContext.readParquetSorted(dir,
      Some(Array("s", "sc")))
      .map(row => row.get(1).asInstanceOf[Seq[Double]].toArray)

    val n = rdd.count.toInt
    val m = rdd.first.length

    val mat = new DenseMatrix(n, m, rdd.collect().transpose.flatten)

    (DenseMatrix.horzcat(new DenseMatrix(n, 1, Array.fill[Double](n)(1.0)), mat), n, m)
  }


  private def readMaf(dir: String, hc: HailContext) = {
    val sqlContext = hc.sqlContext
    val rdd = sqlContext.readParquetSorted(dir,
      Some(Array("v", "Maf")))
      .map(row =>
        (row.getVariant(0), row.get(1).asInstanceOf[Double]))
    rdd
  }

  private def toDataFrame(retval: RDD[(Variant, Double)], sqlContext: SQLContext) = {
    val signature = StructType(
      Array(
        StructField("v", Variant.sparkSchema, false),
        StructField("test_stat", DoubleType, false)
      )
    )
    val rdd = retval.map {case (v, stat) => Row.fromTuple(v.toRow, stat)}
    sqlContext.createDataFrame(rdd, signature)
  }


  private def inferencePerCluster(sc: SparkContext,
                                  estAndCovariance: RDD[(Variant, (Option[DenseVector[Double]], Option[DenseMatrix[Double]]))],
                                  Maf: RDD[(Variant, Double)],
                                  metaInfo: (DenseMatrix[Double], Int, Int)): RDD[(Variant, (Double, Double))] = {

    val metaInfoBroadcast = sc.broadcast(metaInfo)

    estAndCovariance.join(Maf).map { case (v, ((betaOpt, vcovOpt), maf)) => {
      val ret = (betaOpt, vcovOpt) match {
        case (None, None) => (v, (Double.NaN, Double.NaN))
        case (Some(beta), Some(vcov)) => {
          if (maf <= 0.0 || maf >= 1.0)
            (v, (Double.NaN, Double.NaN))
          else {
            val (score, n, m) = metaInfoBroadcast.value
            val logitPhat = sum(score * beta) / n.toDouble
            val logitPtilda = logit(maf)
            val sumScore = sum(score(::, *))
            val Vk = 1.0 / (2 * n * maf * (1.0 - maf)) + 1.0 / (n * n) * trace(sumScore * vcov * sumScore.t)
            val Wk = n
            val numer = Wk * (logitPtilda - logitPhat)
            val denom = Wk * Wk * Vk
            (v, (numer, denom))
            }
          }
        }

      ret
      }

    }

  }

  def apply(hc: HailContext, projectRoot: String, estAndCovarianceRoot: String): DataFrame = {

    val sc = hc.sc
    val dirs = hc.hadoopConf.listStatus(projectRoot).toSeq

    val retvalPerCluster = dirs.map {case dir: FileStatus => {

      val parent = dir.getPath.toString.substring(5)
      val current = dir.getPath.getName

      val MafParquet = "gs://" + parent.concat("/Maf.parquet")
      val ScoreParquet = "gs://" + parent.concat("/Score.parquet")
      val estAndCovarianceParquet = estAndCovarianceRoot.concat(s"/$current/regressionBinom.parquet")

      print(MafParquet + "\n")
      print(ScoreParquet + "\n")
      print(estAndCovarianceParquet + "\n")
      val Maf = readMaf(MafParquet, hc)
      val metaInfo = readScore(ScoreParquet, hc)
      val estAndCovariance = readEstAndCovariance(estAndCovarianceParquet, hc)

      inferencePerCluster(sc, estAndCovariance, Maf, metaInfo)
      }
    }

    val retval = retvalPerCluster.reduceLeft((rdd1, rdd2) => {
      val merged = rdd1.join(rdd2)
      merged.mapValues {case ((numer1, denom1), (numer2, denom2)) => {
        if (numer1.isNaN || numer2.isNaN)
          (Double.NaN, Double.NaN)
        else
        (numer1 + numer2, denom1 + denom2)
      }}
    }).mapValues {case (numer, denom) => numer / math.sqrt(denom)}

    print(retval.first())
    toDataFrame(retval, hc.sqlContext)
  }
}
