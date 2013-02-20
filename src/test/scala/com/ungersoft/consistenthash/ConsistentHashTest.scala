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
      new ConsistentHash[String](TreeMap.empty[Int, String])(_.length, 1)
    }

    "have a factory constructor" in {
      ConsistentHash("One", "Two")
    }

    "covariant" in {
      val s = ConsistentHash(new Student("Bob"))
      val p = s.add(new Person("John"))
//      p must be a [ConsistentHash[Person]]

    }
  }
}

class Person(name: String)
class Student(name: String) extends Person(name)