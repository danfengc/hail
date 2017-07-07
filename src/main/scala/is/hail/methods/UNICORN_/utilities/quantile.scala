package is.hail.methods.UNICORN_.utilities

/**
  * Created by danfengc on 16/11/17.
  */
object quantile {

  def apply(x: Array[Double], p: Double, quantileType: Int = 7, outlierRemove: Boolean = true): Double = {
    require(quantileType >= 1 && quantileType <= 9)
    require(p >= 0.0 && p <= 1.0)

    def computeJ(p: Double, n: Int, m: Double): Int = {
      math.floor(p * n + m)
        .toInt
    }

    def computeG(p: Double, n: Int, m: Double, j: Int) : Double = {
      p * n + m - j
    }

    def computeGamma(g: Double, j: Double, quantileType: Int): Double= {
      if (quantileType == 1)
        if (g > 0) 1.0 else 0.0
      else if (quantileType == 2)
        if (g > 0) 1.0 else 0.5
      else if (quantileType == 3)
        if (g == 0 && j % 2 == 0) 0.0 else 1
      else g
    }

    def computeM(quantileType: Int, p: Double): Double = {
      if (quantileType == 1 || quantileType == 2)
        0.0
      else if (quantileType == 3)
        -0.5
      else if (quantileType == 4)
        0.0
      else if (quantileType == 5)
        0.5
      else if (quantileType == 6)
        p
      else if (quantileType == 7)
        1.0 - p
      else if (quantileType == 8)
        1.0 / 3.0 * (p + 1.0)
      else
        1.0 / 4.0 * p + 3.0 / 8.0
    }


    val outliersRemoved = x.filter(! _.isNaN).sortWith(_ < _)
    if (p == 0.0) outliersRemoved.min
    else if (p == 1.0) outliersRemoved.max
    else {
      val n: Int = outliersRemoved.length
      val m: Double = computeM(quantileType = quantileType, p = p)
      val j: Int = computeJ(p = p, n = n, m = m)
      val g: Double = computeG(p = p, n = n, m = m, j = j)
      val gamma: Double = computeGamma(g = g, j = j, quantileType = quantileType)
      (1.0 - gamma) * outliersRemoved(j - 1) + gamma * outliersRemoved(j)
    }
  }
}
