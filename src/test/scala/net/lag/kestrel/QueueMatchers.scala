/*
 * Copyright 2009 Twitter, Inc.
 * Copyright 2009 Robey Pointer <robeypointer@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.lag.kestrel

import org.specs2.matcher.{SomeMatcher, Expectable, Matcher}

//class beSomeQItem(s: String) extends Matcher[Option[QItem]] {
//  def apply[S <: Option[QItem]](e: Expectable[S]) = {
//    result(e.value.isDefined && (new String(e.value.get.data) == s),
//      e.description + " is empty",
//      e.description + " is not empty",
//      e)
//  }
//}
//
//class beSomeQItemOfLength(len: Int) extends Matcher[Option[QItem]] {
//  def apply[S <: Option[QItem]](e: Expectable[S]) = {
//    result(e.value.isDefined && (e.value.get.data.size == len),
//      "ok",
//      "wrong or missing queue item",
//      e)
//  }
//}
//
//class beSomeQItemWithLengthAtPos(len: Int, n: Int) extends Matcher[Option[QItem]] {
//  def apply[S <: Option[QItem]](e: Expectable[S]) = {
//    result(e.value.isDefined && (e.value.get.data.size == len) && (e.value.get.data(0) == n),
//      "ok",
//      "wrong or missing queue item at " + n + "; got " + e.value.get.data(0),
//      e)
//  }
//}

trait QueueMatchers {

//  class MyOwn extends Matcher[String] {
//    def apply[S <: String](s: Expectable[S]) = {
//      result(s.value.isEmpty,
//        s.description + " is empty",
//        s.description + " is not empty",
//        s)
//    }
//  }

   def beSomeQItem(s: String) = new SomeMatcher[QItem].which(q => q.data == s)

  def beSomeQItem(len: Int) = new SomeMatcher[QItem].which(q => q.data.size == len)

  def beSomeQItem(len: Int, n: Int) = new SomeMatcher[QItem].which(q => q.data.size == len && q.data(0) == n)
//
//  def beSomeQItem(s: String) = new Matcher[Option[QItem]] {
//    def apply(qitemEval: => Option[QItem]) = {
//      val qitem = qitemEval
//      (qitem.isDefined && (new String(qitem.get.data) == s), "ok", "wrong or missing queue item")
//    }
//  }

//  def beSomeQItem(len: Int) = new Matcher[Option[QItem]] {
//    def apply(qitemEval: => Option[QItem]) = {
//      val qitem = qitemEval
//      (qitem.isDefined && (qitem.get.data.size == len), "ok", "wrong or missing queue item")
//    }
//  }

//  def beSomeQItem(len: Int, n: Int) = new Matcher[Option[QItem]] {
//    def apply(qitem: => Option[QItem]) = {
//      val qitem = qitemEval
//      (qitem.isDefined && (qitem.get.data.size == len) && (qitem.get.data(0) == n),
//        "ok", "wrong or missing queue item at " + n + "; got " + qitem.get.data(0))
//    }
//  }

  def put(q: PersistentQueue, bytes: Int, n: Int) {
    val data = new Array[Byte](bytes)
    data(0) = n.toByte
    q.add(data)
  }
}
