package is.hail.methods.UNICORN_.utilities

import java.util

import scala.collection.mutable.ArrayBuffer

/**
  * Created by danfengc on 4/24/17.
  */
class TreeNode(branchTag: String) extends Serializable with Cloneable { outer =>

  protected var parent: Option[TreeNode] = None

  protected var children: ArrayBuffer[TreeNode] = ArrayBuffer.empty[TreeNode]

  def getAbsTreePath: String = {
    val parent = getParent
    parent match {
      case None => branchTag
      case aNode: TreeNode => aNode.getAbsTreePath + "." + branchTag
    }
  }


  def getChildren: Iterator[TreeNode] =
    if (children.isEmpty)
      EMPTY_ENUMERATION
    else
      children.toIterator


  def insert[T <: TreeNode](newChild: T) = {
    val oldParent = newChild.getParent
    oldParent match {
      case node: TreeNode => node.remove(newChild)
      case None =>
    }
    val index = children.length
    newChild.setParent(this)
    children.insert(0, newChild)
  }

  def remove(childIndex: Int) = {
    val child = getChildAt(childIndex)
    children.remove(childIndex)
    child.setParent(None)
  }

  def remove[T <: TreeNode](aChild: T): Unit =
    if (! isNodeChild(aChild))
      throw new IllegalArgumentException("argument is not a child")
    else
      remove(getIndex(aChild))


  def setParent[T <: TreeNode](parent: T) = {
    this.parent = Some(parent)
  }

  def setParent[T <: TreeNode](parent: Option[T]) = this.parent = parent


  def getParent = parent match {
    case Some(node) => node
    case None => None
  }

  def isLeaf = getChildCount == 0

  def getChildAt(index: Int) =
    if (children.isEmpty || children.length <= index )
      throw new ArrayIndexOutOfBoundsException("node has no children")
    else
      children(index)

  def getChildCount = children.length

  def getIndex(aChild: TreeNode): Int =
    if (! isNodeChild(aChild))
      return -1
    else
      return children.indexOf(aChild)

  def isNodeChild[T <: TreeNode](aNode: T): Boolean = {
    var retval: Boolean = false
    if (getChildCount == 0)
      retval = false
    else retval =
      aNode.getParent match {
        case parentNode: TreeNode => parentNode == this
        case None => false
      }
    retval
  }

  def isNodeAncestor[T <: TreeNode](anotherNode: T): Boolean = {
    val ancestor = this
    if (this == ancestor) true
    else ancestor.getParent match {
      case None => false
      case parentNode: TreeNode => parentNode.isNodeAncestor(anotherNode)
    }
  }

  def getChildAfter[T <: TreeNode](aChild: TreeNode) = {
    val index = getIndex(aChild)
    if (index == -1)
      throw new IllegalArgumentException("node is not a child")
    else if (index < getChildCount - 1)
      getChildAt(index + 1)
    else
      None
  }

  def getNextSibling = {
    val myParent = getParent
    myParent match {
      case node: TreeNode => {
        node.getChildAfter(this)
      }
      case None => None
    }
  }

  private var visited = false

  def isVisited = visited

  def setVisited = this.visited = true

  def setNotVisited = this.visited = false

  override def toString: String = branchTag

  def getRoot: TreeNode = {
    val parent = getParent
    parent match {
      case None => this
      case aNode: TreeNode => aNode.getRoot
    }
  }

  def removeAllChildren =
    0 until getChildCount foreach{ case index: Int => remove(index) }

  def isNodeDescendant[T <: TreeNode](anotherNode: T): Boolean =
    anotherNode.isNodeAncestor(this)

  def getLevel: Int = {
    val parent = this.getParent
    parent match {
      case None => 0
      case aNode: TreeNode => 1 + aNode.getLevel
    }
  }

  def isRoot = this.getParent == None


  def getLeaves = {
    val allNodes = new PostorderEnumeration(outer)
    this.visited = false
    allNodes.filter {
      case node => node.isLeaf
    }
  }

}

object TreeNode {
  def apply(treePath: String) = new TreeNode(treePath)
}


object EMPTY_ENUMERATION extends Iterator[TreeNode] {
  override def hasNext = false

  override def next() =
    throw new NoSuchElementException("No more elements")

}

final class PostorderEnumeration(val root: TreeNode) extends Iterator[TreeNode] {

  val children = {
    root.setNotVisited
    root.getChildren
  }
  var subtree: Iterator[TreeNode] = EMPTY_ENUMERATION

  override def hasNext = ! root.isVisited

  override def next = {
    val retval =
      if (subtree.hasNext) subtree.next
      else if (children.hasNext) {
        subtree = new PostorderEnumeration(children.next())
        subtree.next
      } else {
        root.setVisited
        root
      }
    retval
  }
}

final class PreOderEnumeration(val root: TreeNode) extends Iterator[TreeNode] {

  protected val stack: util.Stack[TreeNode]
      = new util.Stack[TreeNode]()
  stack.push(root)


  override def hasNext =
    !stack.isEmpty () && Iterator[TreeNode](stack.peek()).hasNext

  override def next = {
    val enumer = Iterator[TreeNode](stack.peek())
    val node = enumer.next()
    val children = node.getChildren
    if (! enumer.hasNext)
      stack.pop()
    if (children.hasNext) {
      for (child <- children) stack.push(child)
    }
    node
  }


}