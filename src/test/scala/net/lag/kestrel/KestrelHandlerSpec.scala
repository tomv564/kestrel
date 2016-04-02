/*
 * Copyright 2010 Twitter, Inc.
 * Copyright 2010 Robey Pointer <robeypointer@gmail.com>
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

import java.io.{File, FileInputStream}
import java.util.concurrent.ScheduledThreadPoolExecutor
import scala.collection.mutable
import scala.util.Sorting
import com.twitter.conversions.time._
//import com.twitter.logging.TestLogging
import com.twitter.ostrich.stats.Stats
import com.twitter.util.{Await, TempFolder, Time, Timer}
//import org.scalatest.concurrent.Eventually
import org.specs2.mutable._
import org.specs2.matcher.{SomeMatcher, Matcher}
import org.specs2.mock._
import config._

class FakeKestrelHandler(queues: QueueCollection, maxOpenTransactions: Int,
                         serverStatus: Option[ServerStatus] = None)
  extends KestrelHandler(queues, maxOpenTransactions, () => "none", 0, serverStatus) with SimplePendingReads

class KestrelHandlerSpec extends Specification with Mockito with TempFolder with After {
  val config = new QueueBuilder().apply()

  def beString(expected: String) = SomeMatcher[QItem].which(q => new String(q.data) == expected)
//  case class beString(expected: String) extends Matcher[Option[QItem]]() {
//    def apply(v: => Option[QItem]) = {
//      val actual = v.map { item => new String(item.data) }
//      (actual == Some(expected), "ok", "item " + actual + " != " + expected)
//    }
//  }

  var queues: QueueCollection = null
  val timer = new FakeTimer()
  val scheduler = new ScheduledThreadPoolExecutor(1)

  def after = queues.shutdown()

  "KestrelHandler" should {

    "set and get" in {
      withTempFolder {
        queues = new QueueCollection(folderName, timer, scheduler, config, Nil, Nil)
        val handler = new FakeKestrelHandler(queues, 10)
        handler.setItem("test", 0, None, "one".getBytes)
        handler.setItem("test", 0, None, "two".getBytes)
        Await.result(handler.getItem("test", None, false, false)) must beString("one")
        Await.result(handler.getItem("test", None, false, false)) must beString("two")
      }
      success

    }

    "track stats" in {
      withTempFolder {
        Stats.clearAll()
        queues = new QueueCollection(folderName, timer, scheduler, config, Nil, Nil)
        val handler = new FakeKestrelHandler(queues, 10)

        Stats.getCounter("cmd_get")() mustEqual 0
        Stats.getCounter("cmd_set")() mustEqual 0
        Stats.getCounter("cmd_monitor")() mustEqual 0
        Stats.getCounter("cmd_monitor_get")() mustEqual 0
        Stats.getCounter("get_hits")() mustEqual 0
        Stats.getCounter("get_misses")() mustEqual 0

        handler.setItem("test", 0, None, "one".getBytes)
        Stats.getCounter("cmd_set")() mustEqual 1
        Stats.getCounter("cmd_get")() mustEqual 0

        Await.result(handler.getItem("test", None, false, false)) must beString("one")
        Stats.getCounter("cmd_set")() mustEqual 1
        Stats.getCounter("cmd_get")() mustEqual 1
        Stats.getCounter("get_hits")() mustEqual 1
        Stats.getCounter("get_misses")() mustEqual 0

        Await.result(handler.getItem("test2", None, false, false)) mustEqual None
        Stats.getCounter("cmd_set")() mustEqual 1
        Stats.getCounter("cmd_get")() mustEqual 2
        Stats.getCounter("get_hits")() mustEqual 1
        Stats.getCounter("get_misses")() mustEqual 1
        Stats.getCounter("cmd_monitor")() mustEqual 0
        Stats.getCounter("cmd_monitor_get")() mustEqual 0
      }
      success

    }

    "track monitor stats" in {
      withTempFolder {
        Stats.clearAll()
        queues = new QueueCollection(folderName, timer, scheduler, config, Nil, Nil)
        val handler = new FakeKestrelHandler(queues, 10)

        handler.setItem("test", 0, None, "one".getBytes)
        handler.setItem("test", 0, None, "two".getBytes)
        handler.setItem("test", 0, None, "three".getBytes)
        Stats.getCounter("cmd_set")() mustEqual 3
        Stats.getCounter("cmd_get")() mustEqual 0
        Stats.getCounter("cmd_monitor")() mustEqual 0
        Stats.getCounter("cmd_monitor_get")() mustEqual 0

        val items = new mutable.ListBuffer[Option[QItem]]()
        def addItem(item: Option[QItem], xid: Option[Long]) { items.append(item) }

        handler.monitorUntil("test", Some(1.hour.fromNow), 2, false)(addItem)
        items.size mustEqual 3
        items(0) must beString("one")
        items(1) must beString("two")
        items(2) mustEqual None
        Stats.getCounter("cmd_set")() mustEqual 3
        Stats.getCounter("cmd_get")() mustEqual 0
        Stats.getCounter("cmd_monitor")() mustEqual 1
        Stats.getCounter("cmd_monitor_get")() mustEqual 2
        Stats.getCounter("get_hits")() mustEqual 2
        Stats.getCounter("get_misses")() mustEqual 0

        items.clear()
        handler.monitorUntil("test", Some(1.second.fromNow), 2, false)(addItem)
        timer.timeout()
        items.size mustEqual 2
        items(0) must beString("three")
        items(1) mustEqual None
        Stats.getCounter("cmd_set")() mustEqual 3
        Stats.getCounter("cmd_get")() mustEqual 0
        Stats.getCounter("cmd_monitor")() mustEqual 2
        Stats.getCounter("cmd_monitor_get")() mustEqual 4
        Stats.getCounter("get_hits")() mustEqual 3
        Stats.getCounter("get_misses")() mustEqual 1

        items.clear()
        handler.monitorUntil("test", Some(1.second.fromNow), 2, false)(addItem)
        timer.timeout()
        items.size mustEqual 1
        items(0) mustEqual None
        Stats.getCounter("cmd_set")() mustEqual 3
        Stats.getCounter("cmd_get")() mustEqual 0
        Stats.getCounter("cmd_monitor")() mustEqual 3
        Stats.getCounter("cmd_monitor_get")() mustEqual 5
        Stats.getCounter("get_hits")() mustEqual 3
        Stats.getCounter("get_misses")() mustEqual 2
      }
      success

    }

    "abort and confirm a read" in {
      withTempFolder {
        queues = new QueueCollection(folderName, timer, scheduler, config, Nil, Nil)
        val handler = new FakeKestrelHandler(queues, 10)
        handler.setItem("test", 0, None, "one".getBytes)
        Await.result(handler.getItem("test", None, true, false)) must beString("one")
        Await.result(handler.getItem("test", None, true, false)) mustEqual None
        handler.abortRead("test") mustEqual true
        Await.result(handler.getItem("test", None, true, false)) must beString("one")
        handler.closeRead("test") mustEqual true
        Await.result(handler.getItem("test", None, true, false)) mustEqual None
      }
      success

    }

    "abort reads on a deleted queue without resurrecting the queue" in {
      withTempFolder {
        queues = new QueueCollection(folderName, timer, scheduler, config, Nil, Nil)
        val handler = new FakeKestrelHandler(queues, 10)
        handler.setItem("test", 0, None, "one".getBytes)
        Await.result(handler.getItem("test", None, true, false)) must beString("one")
        handler.delete("test")
        queues.queueNames mustEqual Nil

        handler.abortRead("test")
        queues.queueNames mustEqual Nil
      }
      success

    }

    "abort waiters" in {
      withTempFolder {
        queues = new QueueCollection(folderName, timer, scheduler, config, Nil, Nil)
        val handler = new FakeKestrelHandler(queues, 10)
        @volatile var got = false
        handler.monitorUntil("test", Some(1.hour.fromNow), 5, true)({ (itemOption, _) =>
          itemOption map { case _ =>
            got = true
          }
        })
        Stats.getGauge("q/test/waiters") mustEqual Some(1.0)
        handler.finish()
        eventually { Stats.getGauge("q/test/waiters") mustEqual Some(0.0) }
      }
      success
    }

    "open several reads" in {
      "on one queue" in {
        withTempFolder {
          queues = new QueueCollection(folderName, timer, scheduler, config, Nil, Nil)
          val handler = new FakeKestrelHandler(queues, 10)
          handler.setItem("test", 0, None, "one".getBytes)
          handler.setItem("test", 0, None, "two".getBytes)
          handler.setItem("test", 0, None, "three".getBytes)
          Await.result(handler.getItem("test", None, true, false)) must beString("one")
          Await.result(handler.getItem("test", None, true, false)) must beString("two")
          handler.abortRead("test") mustEqual true
          Await.result(handler.getItem("test", None, true, false)) must beString("one")
          handler.closeRead("test") mustEqual true
          Await.result(handler.getItem("test", None, true, false)) must beString("three")
          handler.abortRead("test") mustEqual true
          Await.result(handler.getItem("test", None, true, false)) must beString("one")
        }
        success

      }

      "on several queues" in {
        withTempFolder {
          queues = new QueueCollection(folderName, timer, scheduler, config, Nil, Nil)
          val handler = new FakeKestrelHandler(queues, 10)
          handler.setItem("red", 0, None, "red1".getBytes)
          handler.setItem("red", 0, None, "red2".getBytes)
          handler.setItem("green", 0, None, "green1".getBytes)
          handler.setItem("green", 0, None, "green2".getBytes)
          handler.setItem("blue", 0, None, "blue1".getBytes)
          handler.setItem("blue", 0, None, "blue2".getBytes)

          Await.result(handler.getItem("red", None, true, false)) must beString("red1")
          Await.result(handler.getItem("green", None, true, false)) must beString("green1")
          Await.result(handler.getItem("blue", None, true, false)) must beString("blue1")
          handler.abortRead("green") mustEqual true

          Await.result(handler.getItem("red", None, true, false)) must beString("red2")
          handler.closeRead("red") mustEqual true
          handler.closeRead("red") mustEqual true
          Await.result(handler.getItem("red", None, true, false)) mustEqual None

          Await.result(handler.getItem("green", None, true, false)) must beString("green1")
          handler.closeRead("blue") mustEqual true
          handler.abortRead("green") mustEqual true
          Await.result(handler.getItem("blue", None, true, false)) must beString("blue2")
          Await.result(handler.getItem("green", None, true, false)) must beString("green1")
        }
        success

      }

      "but not if open reads are limited" in {
        withTempFolder {
          queues = new QueueCollection(folderName, timer, scheduler, config, Nil, Nil)
          val handler = new FakeKestrelHandler(queues, 1)
          handler.setItem("red", 0, None, "red1".getBytes)
          handler.setItem("red", 0, None, "red2".getBytes)
          Await.result(handler.getItem("red", None, true, false)) must beString("red1")
          Await.result(handler.getItem("red", None, true, false)) must throwA[TooManyOpenReadsException]
        }
        success

      }

      "obey maxItems" in {
        withTempFolder {
          queues = new QueueCollection(folderName, timer, scheduler, config, Nil, Nil)
          val handler = new FakeKestrelHandler(queues, 5)
          val got = new mutable.ListBuffer[QItem]()
          handler.setItem("red", 0, None, "red1".getBytes)
          handler.setItem("red", 0, None, "red2".getBytes)
          handler.setItem("red", 0, None, "red3".getBytes)
          handler.monitorUntil("red", Some(1.hour.fromNow), 2, true) { (itemOption, _) =>
            itemOption.foreach { got += _ }
          }
          got.toList.map { x => new String(x.data) } mustEqual List("red1", "red2")
        }
        success

      }

      "close all reads" in {
        withTempFolder {
          queues = new QueueCollection(folderName, timer, scheduler, config, Nil, Nil)
          val handler = new FakeKestrelHandler(queues, 2)
          handler.setItem("red", 0, None, "red1".getBytes)
          handler.setItem("red", 0, None, "red2".getBytes)
          Await.result(handler.getItem("red", None, true, false)) must beString("red1")
          Await.result(handler.getItem("red", None, true, false)) must beString("red2")
          handler.closeAllReads("red") mustEqual 2
          handler.abortRead("red") mustEqual false
          handler.pendingReads.size("red") mustEqual 0
        }
        success

      }
    }

    "manage server status" in {
      "by updating server status, if configured" in {
        withTempFolder {
          queues = new QueueCollection(folderName, timer, scheduler, config, Nil, Nil)
          val serverStatus = mock[ServerStatus]
          serverStatus.status returns Quiescent
          val handler = new FakeKestrelHandler(queues, 10, Some(serverStatus))

//          expect {
//            one(serverStatus).markQuiescent()
//            one(serverStatus).status willReturn Quiescent
//            one(serverStatus).setStatus("ReadOnly")
//            one(serverStatus).markUp()
//          }

          handler.markQuiescecent()
          handler.currentStatus mustEqual "Quiescent"
          handler.setStatus("ReadOnly")
          handler.markUp()
        }
        success

      }

      "by throwing an exception if server status not configured" in {
        withTempFolder {
          queues = new QueueCollection(folderName, timer, scheduler, config, Nil, Nil)
          val handler = new FakeKestrelHandler(queues, 10, None)

          handler.markQuiescecent() must throwA[ServerStatusNotConfiguredException]
        }
        success

      }
    }

    "respect server status" in {
      "by blocking get when reads are blocked" in  {
        withTempFolder {
          queues = new QueueCollection(folderName, timer, scheduler, config, Nil, Nil)
          val serverStatus = mock[ServerStatus]
          serverStatus.blockReads returns true
          val handler = new FakeKestrelHandler(queues, 10, Some(serverStatus))
//          expect {
//            one(serverStatus).blockReads willReturn true
//          }

          handler.getItem("q", None, false, false) must throwAn[AvailabilityException]
        }
        success

      }

      "by blocking set when writes are blocked" in  {
        withTempFolder {
          queues = new QueueCollection(folderName, timer, scheduler, config, Nil, Nil)
          val serverStatus = mock[ServerStatus]
          serverStatus.blockWrites returns true
          val handler = new FakeKestrelHandler(queues, 10, Some(serverStatus))
//          expect {
//            one(serverStatus).blockWrites willReturn true
//          }

          handler.setItem("q", 0, None, Array[Byte](1,2,3,4)) must throwAn[AvailabilityException]
        }
        success

      }
    }
  }
}
