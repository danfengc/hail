package is.hail.methods

import is.hail.annotations.Annotation
import is.hail.keytable.KeyTable
import is.hail.methods.UNICORN_.utilities.TreeNode
import org.apache.spark.mllib.linalg._
import is.hail.utils._

/**
  * Created by danfengc on 6/28/17.
  */

class AdaptivePCAResult(scores: KeyTable,
                        MAF: KeyTable,
                        sampleIds: IndexedSeq[Annotation],
                        branch: String) extends TreeNode(branch) { outer =>

  protected def getAssignments(assignType: String): Map[Annotation, Annotation] = {
    val children = assignType match {
      case "Leaf" => getLeaves
      case "Child" => getChildren
      case _ => fatal(s"${assignType} is not valid")
    }
    var assignments = Map.empty[Annotation, Annotation]
    if (children.hasNext) {
      val child = children.next().asInstanceOf[AdaptivePCAResult]
      val childBranch = child.branch
      val childSamples = child.sampleIds
      childSamples
        .foreach(sample =>
          assignments += (sample -> childBranch)
        )
      }
    assignments
  }

  def getScores = scores

  def getMAF = MAF

  def branchId = branch

}


class Parent(scores: KeyTable,
             MAF: KeyTable,
             sampleIds: IndexedSeq[Annotation],
             clusterCenters: Array[Vector],
             branch: String) extends AdaptivePCAResult(scores, MAF, sampleIds, branch) {

  def getChildrenAssignments = getAssignments("Child")

}

class Root(scores: KeyTable,
           MAF: KeyTable,
           sampleIds: IndexedSeq[Annotation],
           clusterCenters: Array[Vector],
           branch: String) extends Parent(scores, MAF, sampleIds, clusterCenters, branch) {

  def getLeafAssignment = getAssignments("")
}


class Leaf(scores: KeyTable,
           MAF: KeyTable,
           sampleIds: IndexedSeq[Annotation],
           branch: String) extends AdaptivePCAResult(scores, MAF, sampleIds, branch)
