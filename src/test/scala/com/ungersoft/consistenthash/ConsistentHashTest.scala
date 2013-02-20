package com.ungersoft.consistenthash

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers


/**
 * Created with IntelliJ IDEA.
 * User: randy
 * Date: 2/19/13
 * Time: 2:25 PM
 */
class ConsistentHashTest extends WordSpec with MustMatchers {
  "ConsistentHash" should {

    "have a full constructor" in {
      import collection.immutable.TreeMap
      new ConsistentHash[String](TreeMap.empty[Int, String])(_.hashCode)
    }

    "have a factory constructor" in {
      ConsistentHash("One", "Two")
    }

    "covariant" in {
      val s = ConsistentHash(new Student("Bob"))
      val p = s.update(new Person("John"))
//      p must be a [ConsistentHash[Person]]

    }

    "add a seq of covariant nodes via factory" in {
      val c = ConsistentHash.empty[String]
      val d = ConsistentHash.addValues(c, 1, new Student("Bob"), new Person("John"))
      println(typeOf(d))
    }

//    "factory constructor with covariant Seq" in {
//      val c = ConsistentHash[Student](new Person("Tom"))
//    }

  }

  def typeOf[T:Manifest](t:T ) = manifest[T].toString
}

class Person(name: String)
class Student(name: String) extends Person(name)