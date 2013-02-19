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
  implicit val hash = new HashFunction {
    import scala.util.hashing.MurmurHash3
    def apply(i: Int) = MurmurHash3.stringHash(i.toString)
  }

  //Create additional copies of each node to aid load balancing
  implicit val defaultNumberOfReplicas = 3

  def apply[T] = new ConsistentHash[T]

  def apply[T](numberOfReplicas: Int) = new ConsistentHash[T]()(hash = implicitly, numberOfReplicas)

  def apply[T](initialValues: T*): ConsistentHash[T] = addValues(new ConsistentHash[T], initialValues)

  def apply[T](numberOfReplicas: Int, initialValues: T*) =
    addValues(new ConsistentHash[T]()(hash = implicitly, numberOfReplicas), initialValues)

  @annotation.tailrec
  def addValues[T](ch: ConsistentHash[T], list: Seq[T]): ConsistentHash[T] = list match {
    case Seq(a, rest @ _ *) => addValues(ch.add(a), rest)
    case Seq() => ch
  }

  //Use trait because Int => Int must shadow Predef.conforms
  trait HashFunction {
    def apply(i: Int): Int
  }

}

class ConsistentHash[T](nodeMap: TreeMap[Int, T] = TreeMap.empty[Int, T])
                       (implicit hash: ConsistentHash.HashFunction, defaultNumberOfReplicas: Int) {

  def add(node: T, replicas: Int = defaultNumberOfReplicas) = {
    val newNodes = (1 to replicas) map (i => (hash(i + node.hashCode) -> node))
    new ConsistentHash(nodeMap ++ newNodes)
  }

  def remove(node: T) = new ConsistentHash(nodeMap filter { case (k, v) => v != node })

  def get(a: Any): T = {
    if (nodeMap.isEmpty) throw new NoSuchElementException("Cannot call .get() on empty ConsistentHash")
    nodeMap.from(hash(a.hashCode)).headOption match {
      case Some((key, value)) => value
      case None => nodeMap.headOption.get._2  //throw an Exception if nodeMap is empty
    }
  }

  override def toString = "ConsistentHash(" + nodeMap.toString + ")"
}
