package is.hail.methods

import breeze.linalg.{Axis, DenseMatrix, DenseVector}
import is.hail.expr.{TArray, TBoolean, TDouble, TInt, TStruct, TVariant}
import is.hail.keytable.KeyTable
import is.hail.methods.UNICORN_._
import is.hail.utils.ArrayBuilder
import is.hail.variant.{Genotype, Variant, VariantDataset}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import collection.mutable.Map
import is.hail.utils._
import org.apache.spark.sql.types.{StructField, _}
/**
  * Created by danfengc on 7/18/17.
  */
object SimpleBinomRegression {


  val signature = StructType(
    Array(
      StructField("v", Variant.sparkSchema, true),
      StructField("binomFit", StructType(Array(
        StructField("nIter", IntegerType, true),
        StructField("converged", BooleanType, true),
        StructField("exploded", BooleanType, true),
        StructField("logLkhd", DoubleType, true),
        StructField("vcov", ArrayType(DoubleType, true), true),
        StructField("b", ArrayType(DoubleType, true), true),
        StructField("p", IntegerType, true)
      )), false)
    )
  )


  private def hardCallsWithAC(gs: Iterable[Genotype], scores: DenseMatrix[Double]) = {
    val gts = gs.hardCallIterator
    val rows = new ArrayBuilder[Int]()
    val vals = new ArrayBuilder[Double]()
    val missingSparseIndices = new ArrayBuilder[Int]()
    var i = 0
    var row = 0
    var sum = 0
    while (gts.hasNext) {
      val gt = gts.next()
      if (gt != -1) {
        sum += gt
        vals += gt.toDouble
      } else {
        missingSparseIndices += i
      }
      i += 1
    }

    val x = scores.delete(missingSparseIndices.result().toSeq, Axis._0)
    val n = x.rows
    (new DenseVector[Double](vals.result()), DenseMatrix.horzcat(new DenseMatrix(n, 1, Array.fill[Double](n)(1.0)), x))
  }

  private def fit(vds: VariantDataset, scores: DenseMatrix[Double]) = {
    val sc = vds.sparkContext
    val hc = vds.hc
    val sqlContext = hc.sqlContext

    val scoresBroadcast = sc.broadcast(scores)

    val rdd = vds.rdd.map{case (v, (va, gs)) => {
        val (y, x) = hardCallsWithAC(gs, scoresBroadcast.value)
        try {
          val binomRegressionModel = new BinomRegressionModel(x, y)
          val binomRegressionResult = binomRegressionModel.fit()
          Row.fromTuple(v.toRow, Row.fromSeq(binomRegressionResult.data))
        }
        catch {
          case e: java.lang.IllegalArgumentException =>
            Row.fromTuple(v.toRow, Row.fromSeq(Seq(null,null,null,null,null,null,null)))
        }
      }
    }
    sqlContext.createDataFrame(rdd, signature)
  }


  def apply(vds: VariantDataset, adaptivePCAResult: AdaptivepcaBuilder, outputRootOpt: Option[String] = None) = {
    val sc = vds.sparkContext
    val blobs = adaptivePCAResult.getLeaves.map(_.asInstanceOf[AdaptivepcaBuilder])
    val retval = Map.empty[String, DataFrame]

    while (blobs.hasNext) {
      val blob = blobs.next()
      val branch = blob.branch
      val sampleIds = blob.sampleIds

      val sub = vds.filterSamples {case (s, sa) =>
        sampleIds contains s}

      val scores: DenseMatrix[Double] = blob.toDenseMatrix
      val BinomRegressionDF = fit(sub, scores)
      retval += (branch -> BinomRegressionDF)
      if (outputRootOpt.isDefined)
        BinomRegressionDF.write.parquet(s"${outputRootOpt.get}/${branch}/regressionBinom.parquet")
    }

    SimpleBinomRegressionFit(retval)
  }

}


case class SimpleBinomRegressionFit(fit: Map[String, DataFrame]) {

  def export(outputDir: String) =
    fit.foreach { case (branch, binomRegressionDF) =>
      binomRegressionDF.write.parquet(s"${outputDir}/${branch}/regressionBinom.parquet")
    }

  def dataframe(branch: String): DataFrame = if (fit.contains(branch)) fit(branch) else fatal(s"There is no ${branch} branch!")


}
