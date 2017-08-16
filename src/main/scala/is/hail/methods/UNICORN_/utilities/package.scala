package is.hail.methods.UNICORN_


import breeze.generic.{MappingUFunc, UFunc}
import scala.{math => m}

/**
  * Created by danfengc on 2017/4/10.
  */

object expit extends UFunc with MappingUFunc {

  import scala.{math => m}

  implicit object expitIntImpl extends Impl[Int, Double] { def apply(v: Int) =
    1.0 / (m.exp(-v) + 1)
  }
  implicit object expitDoubleImpl extends Impl[Double, Double] { def apply(v: Double) =
    1.0 / (m.exp(-v) + 1)
  }
  implicit object expitFloatImpl extends Impl[Float, Double] { def apply(v: Float) =
    1.0 / (m.exp(-v) + 1)
  }
}

object logit extends UFunc with MappingUFunc {

  implicit object logitIntImpl extends Impl[Int, Double] { def apply(v: Int) =
    if (v <= 0 || v >= 1)
      throw new IllegalArgumentException("the support of v is (0, 1)")
    else
      m.log(v.toDouble / (1 - v.toDouble))
  }
  implicit object logitDoubleImpl extends Impl[Double, Double] { def apply(v: Double) =
    if (v <= 0 || v >= 1)
      throw new IllegalArgumentException("the support of v is (0, 1)")
    else
      m.log(v/ (1 - v))
  }
  implicit object logitFloatImpl extends Impl[Float, Double] { def apply(v: Float) =
    if (v <= 0 || v >= 1)
      throw new IllegalArgumentException("the support of v is (0, 1)")
    else
      m.log(v.toDouble/ (1 - v.toDouble))}
}
