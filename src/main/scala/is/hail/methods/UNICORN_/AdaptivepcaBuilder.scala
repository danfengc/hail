package is.hail.methods

import breeze.linalg.DenseMatrix
import is.hail.annotations.Annotation
import is.hail.expr.{TDouble, TStruct, Type}
import is.hail.methods.UNICORN_.utilities.{PreOderEnumeration, TreeNode}
import is.hail.utils._
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.DataFrame
/**
  * Created by danfengc on 6/28/17.
  */

class AdaptivepcaBuilder(score: DataFrame,
                         Maf: DataFrame,
                         val sampleIds: IndexedSeq[Annotation],
                         val branch: String,
                         p: Int,
                         clustCenters: Option[Array[Vector]] = None) extends TreeNode(branch) { outer =>

  def getBranch = branch

  def getScore = score

  def getMaf = Maf

  protected def getAssignments(assignType: String): Map[Annotation, Annotation] = {
    val children = assignType match {
      case "Leaf" => getLeaves
      case "Child" => getChildren
      case _ => fatal(s"${assignType} is not valid")
    }
    var assignments = Map.empty[Annotation, Annotation]
    if (children.hasNext) {
      val child = children.next().asInstanceOf[AdaptivepcaBuilder]
      val childBranch = child.branch
      val childSamples = child.sampleIds
      childSamples
        .foreach(sample =>
          assignments += (sample -> childBranch)
        )
      }
    assignments
  }

  def toDenseMatrix: DenseMatrix[Double] = {
    val rdd = score.rdd.map{case row =>
        row.get(1).asInstanceOf[IndexedSeq[Double]].toArray}
    val n = rdd.count().toInt
    new DenseMatrix[Double](n, p, rdd.collect().transpose.flatten)
  }


  def printSchema: String = {
    val nodes = new PreOderEnumeration(this)
    nodes.foldLeft("")((tmpString, node) => {
      val level = node.getLevel
      val treePath = node.getTreePath
      tmpString + " " * 2 * level + " - " + treePath + "\n"
    })
  }

  def get(treePath: String) = {
    val nodes = new PreOderEnumeration(this)
    val node = nodes.filter(n => {
      val path = n.asInstanceOf[AdaptivepcaBuilder].branch
      treePath == path
    }).next().asInstanceOf[AdaptivepcaBuilder]
    node
  }


}


