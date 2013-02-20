package com.ungersoft.consistenthash

import collection.immutable.TreeMap

/**
 *  @example {{{
 *  // Make a ConsistentHash via the companion object factory
 *  val hash = ConsistentHash("192.168.0.1", "192.168.0.2")
 *
 *  // Make a weighted ConsistentHash. Objects are 5 times more likely to locate to 192.168.0.3, since (5000/1000) = 5
 *  val weighted = ConsistentHash(1000, "192.168.0.2").update("192.168.0.3", 5000)
 *
 *  // Locate an object's node within the ConsistentHash
 *  val location = hash.locate(object)
 *
 *  }}}
 */

object ConsistentHash {      //Todo: Use CanBuildFrom

  val defaultNumberOfReplicas = 1000

  implicit val hashFunction = {
    import scala.util.hashing.MurmurHash3 //MurmurHash is well distributed. String.hashCode is not.
    (s: String) => MurmurHash3.stringHash(s)
  }

  /** Returns an empty ConsistentHash of type T
    *  @tparam T The type of nodes to be contained in this ConsistentHash
    *  @return an empty ConsistentHash of type T
    */
  def apply[T] = empty[T]

  /** Returns a new ConsistentHash of type T containing 'initialValues'
    *  @tparam T The type of nodes to be contained in this ConsistentHash
    *  @param initialValues the values to be contained in this ConsistentHash
    *  @return a new ConsistentHash of type T containing 'initialValues'
    */
  def apply[T](initialValues: T*): ConsistentHash[T] = addValues(new ConsistentHash[T], defaultNumberOfReplicas, initialValues: _*)

  /** Returns a new ConsistentHash of type T containing 'initialValues'
    *  @tparam T The type of nodes to be contained in this ConsistentHash
    *  @param numberOfReplicas The number of replicas to create for every value in 'initialValues'
    *  @param initialValues the values to be contained in this ConsistentHash
    *  @return a new ConsistentHash of type T containing 'numberOfReplicas' occurrences of each of the 'initialValues'
    */
  def apply[T](numberOfReplicas: Int, initialValues: T*) =
    addValues(new ConsistentHash[T]()(hashFunction = implicitly), numberOfReplicas, initialValues)

  @annotation.tailrec
  def addValues[T](ch: ConsistentHash[T], numberOfReplicas: Int, toAdd: T*): ConsistentHash[T] = toAdd match {
    case Seq(a, rest @ _ *) => addValues(ch.update(a, numberOfReplicas), numberOfReplicas, rest: _*)
    case Seq() => ch
  }

  def empty[T] = new ConsistentHash[T]

  def reverse0(reversed: Seq[Int], alist: Int* ): Seq[Int] = alist match {
    case Seq() => reversed
    case Seq(head, rest @ _ *) => reverse0((head +: reversed), rest:_*)
  }
}

/** This class implements immutable ConsistentHashing using a TreeMap.
  *
  *  @tparam T The type of the nodes contained in this ConsistentHash.
  *  @param hashFunction The implicit hash function used to compute a well distributed hash
  *
  *  @author  Randy Unger
  *  @version 1.0, 02/20/2013
  *  @see [[http://en.wikipedia.org/wiki/Consistent_hashing "Wikipedia's article on Consistent Hashing"]] for more information.
  *
  */

class ConsistentHash[+T](nodeMap: TreeMap[Int, T] = TreeMap.empty[Int, T])
                       (implicit hashFunction: String => Int) {

  /** Returns a new ConsistentHash including a number of new Nodes of type T, whose number is equal to numberOfReplicas.
    * After calling, the number of replicas of 'node' is always equal to 'numberOfReplicas',
    * regardless of the number of replicas before calling update. Increasing the baseline 'numberOfReplicas' increases the balance
    * between nodes. Increasing the 'numberOfReplicas' for a particular node increases the likelihood for an object to hash to that particular node.
    *
    *  @param node The node to be added
    *  @param numberOfReplicas The weighting of this node relative to other nodes. Use a minimum of 100 replicas for a balanced distribution.
    *  @return A new ConsistentHash including 'replicas' number of instances of 'node'
    */
  def update[B >: T](node: B, numberOfReplicas: Int = ConsistentHash.defaultNumberOfReplicas) = numberOfReplicas match {
    case 0 => remove(node)
    case n => {
      val newNodes = (1 to n) map ((i: Int) => (hashFunction(i + node.getClass.getName + node.hashCode) -> node))
      new ConsistentHash(filterNode(node) ++ newNodes)
    }
  }

  /** Returns a new ConsistentHash with no occurrences of 'node'
    *  @param node The node to remove
    *  @return A new ConsistentHash with no occurrences of 'node'
    */
  def remove[B >: T](node: B) = new ConsistentHash(filterNode(node))

  /** Returns the T corresponding to 'obj'
    *  @param obj The object to be located within the set of node: T contained in this ConsistentHash
    *  @return A node of type 'T' corresponding to 'obj'
    *  @note The ConsistentHash must not be empty
    *  @throws Predef.NoSuchElementException if the ConsistentHash is empty.
    */
  def locate(obj: Any): T = {
    if (isEmpty) throw new NoSuchElementException("Cannot call .get() on empty ConsistentHash")

    nodeMap.from(hashFunction(obj.hashCode.toString)).headOption match {
      case Some((key, value)) => value
      case None => nodeMap.headOption.get._2
    }
  }

  /**
    *  @return A Map[Any, Int] indicating the number of occurrences of each node in this ConsistentHash
    */
  lazy val frequencies:Map[_ <: Any, Int] = nodeMap groupBy(_._2) mapValues(_.size)

  /**
   *  @return A Boolean indicating whether this ConsistentHash contains any nodes
   */
  lazy val isEmpty: Boolean = nodeMap.isEmpty

  private[this] def filterNode[B >: T](node: B) = nodeMap filter { case (k, v) => v != node }

  override def toString = s"ConsistentHash(${nodeMap.take(10).toString} ${if(nodeMap.size>10) "..." else ""})"
}
