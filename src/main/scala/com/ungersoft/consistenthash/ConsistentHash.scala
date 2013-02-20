package com.ungersoft.consistenthash

import collection.immutable.TreeMap

/**
 * Created with IntelliJ IDEA.
 * User: randy
 * Date: 2/18/13
 * Time: 11:19 AM
 */

object ConsistentHash {

  //MurmurHash is well distributed. String.hashCode is not.
  implicit val hash = {
    import scala.util.hashing.MurmurHash3
    (s: String) => MurmurHash3.stringHash(s)
  }

  //Create additional copies of each node to aid load balancing
  implicit val defaultNumberOfReplicas = 3

  def apply[T] = empty[T]

  def apply[T](numberOfReplicas: Int) = new ConsistentHash[T]()(hash = implicitly, numberOfReplicas)

  def apply[T](initialValues: T*): ConsistentHash[T] = addValues(new ConsistentHash[T], initialValues)

  def apply[T](numberOfReplicas: Int, initialValues: T*) =
    addValues(new ConsistentHash[T]()(hash = implicitly, numberOfReplicas), initialValues)

  @annotation.tailrec
  def addValues[T](ch: ConsistentHash[T], list: Seq[T]): ConsistentHash[T] = list match {
    case Seq(a, rest @ _ *) => addValues(ch.add(a), rest)
    case Seq() => ch
  }

  def empty[T] = new ConsistentHash[T]

}

class ConsistentHash[+T](nodeMap: TreeMap[Int, T] = TreeMap.empty[Int, T])
                       (implicit hash: String => Int, defaultNumberOfReplicas: Int) {

  def add[B >: T](node: B, replicas: Int = defaultNumberOfReplicas) = {
    val newNodes = (1 to replicas) map (i => (hash(i + node.getClass.getName + node.hashCode) -> node))
    new ConsistentHash(filterNode(node) ++ newNodes)
  }

  def remove[B >: T](node: B) = new ConsistentHash(filterNode(node))

  def get(a: Any): T = {
    if (nodeMap.isEmpty) throw new NoSuchElementException("Cannot call .get() on empty ConsistentHash")

    nodeMap.from(hash(a.hashCode.toString)).headOption match {
      case Some((key, value)) => value
      case None => nodeMap.headOption.get._2
    }
  }

  private[this] def filterNode[B >: T](node: B) = nodeMap filter { case (k, v) => v != node }

  override def toString = s"ConsistentHash(${nodeMap.take(10).toString} ${if(nodeMap.size>10) "..." else ""})"
}
