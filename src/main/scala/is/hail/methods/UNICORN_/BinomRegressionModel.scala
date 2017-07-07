package is.hail.methods.UNICORN_

import breeze.linalg._
import breeze.numerics._
import breeze.stats.distributions.Binomial
import is.hail.annotations.Annotation
import is.hail.expr._

/**
  * Created by danfengc on 6/30/17.
  */
class BinomRegressionModel(X: DenseMatrix[Double], y: DenseVector[Double]) {

  require(y.length == X.rows)
  require(y.forall(yi => yi == 0 || yi == 1 || yi == 2))
  require{ val sumY = sum(y); sumY > 0 && sumY < 2 * y.length }

  val n: Int = X.rows
  val m: Int = X.cols

  def bInterceptOnly(): DenseVector[Double] = {
    val b = DenseVector.zeros[Double](m)
    val avg = sum(y) / (2 * n)
    b(0) = math.log(avg / (1 - avg))
    b
  }

  def fit(optNullFit: Option[BinomRegressionFit] = None, maxIter: Int = 25, tol: Double = 1E-6):BinomRegressionFit = {

    val b = DenseVector.zeros[Double](m)
    val mu = DenseVector.zeros[Double](n)
    val score = DenseVector.zeros[Double](m)
    val fisher = DenseMatrix.zeros[Double](m, m)

    optNullFit match {
      case None =>
        b := bInterceptOnly()
        mu := sigmoid(X * b)
        score := X.t * (y - (mu :* 2))
        fisher := X.t * (X(::, *) :* (mu :* (1d - mu))) :* 2
      case Some(nullFit) =>
        val m0 = nullFit.b.length

        val r0 = 0 until m0
        val r1 = m0 to -1

        val X0 = X(::, r0)
        val X1 = X(::, r1)

        b(r0) := nullFit.b
        mu := sigmoid(X * b)
        score(r0) := nullFit.score.get
        score(r1) := X1.t * (y - (mu :* 2))
        fisher(r0, r0) := nullFit.fisher.get
        fisher(r0, r1) := X0.t * (X1(::, *) :* (mu :* (1d - mu))) :* 2
        fisher(r1, r0) := fisher(r0, r1).t
        fisher(r1, r1) := X1.t * (X1(::, *) :* (mu :* (1d - mu))) :* 2
    }

    var iter = 1
    var converged = false
    var exploded = false

    val deltaB = DenseVector.zeros[Double](m)

    while (!converged && !exploded && iter <= maxIter) {
      try {
        deltaB := fisher \ score

        if (max(abs(deltaB)) < tol) {
          converged = true
        } else {
          iter += 1
          b += deltaB
          mu := sigmoid(X * b)
          score := X.t * (y - (mu :* 2))
          fisher := X.t * (X(::, *) :* (mu :* (1d - mu))) :* 2
        }
      } catch {
        case e: breeze.linalg.MatrixSingularException => exploded = true
        case e: breeze.linalg.NotConvergedException => exploded = true
      }
    }


    val logLkhd = (mu.toArray zip y.toArray foldLeft 0.0) { case (currentLogLikelihood, (p, n)) => currentLogLikelihood + Binomial(2, p).logProbabilityOf(n.toInt)}

    BinomRegressionFit(b, Some(score), Some(fisher), logLkhd, iter, converged, exploded)
  }

}


object BinomRegressionFit {
  val schema: Type = TStruct(
    ("nIter", TInt),
    ("converged", TBoolean),
    ("exploded", TBoolean))
}

case class BinomRegressionFit(b: DenseVector[Double],
                                  score: Option[DenseVector[Double]],
                                  fisher: Option[DenseMatrix[Double]],
                                  logLkhd: Double,
                                  nIter: Int,
                                  converged: Boolean,
                                  exploded: Boolean) {

  def toAnnotation: Annotation = Annotation(nIter, converged, exploded)
}
