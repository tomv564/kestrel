/*
 * Copyright 2010 Twitter, Inc.
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

import com.twitter.conversions.time._
import com.twitter.finagle.ClientConnection
import com.twitter.naggati.test.TestCodec
import com.twitter.ostrich.admin.RuntimeEnvironment
import com.twitter.util.{Await, Future, Promise, Time}
import java.net.InetSocketAddress
import org.jboss.netty.buffer.ChannelBuffers
import org.mockito.{Matchers => M}
import org.specs2.mutable._
import org.specs2.mock._

class TextHandlerSpec extends Specification with Mockito {
  def wrap(s: String) = ChannelBuffers.wrappedBuffer(s.getBytes)

  type ClientDesc = Option[() => String]

  "TextCodec" should {
    "get request" in {
      val (codec, counter) = TestCodec(TextCodec.read, TextCodec.write)

      codec(wrap("get foo\r\n")) mustEqual List(TextRequest("get", List("foo"), Nil))
      codec(wrap("get f")) mustEqual Nil
      codec(wrap("oo\r\n")) mustEqual List(TextRequest("get", List("foo"), Nil))

      codec(wrap("get foo 100")) mustEqual Nil
      codec(wrap("\n")) mustEqual List(TextRequest("get", List("foo", "100"), Nil))
    }

    "put request" in {
      val (codec, counter) = TestCodec(TextCodec.read, TextCodec.write)

      codec(wrap("put foo:\n")) mustEqual Nil
      codec(wrap("hello\n")) mustEqual Nil
      val stream = codec(wrap("\n"))
      stream.size mustEqual 1
      val r = stream(0).asInstanceOf[TextRequest]
      r.command mustEqual "put"
      r.args mustEqual List("foo")
      r.items.map { new String(_) } mustEqual List("hello")
    }

    "quit request" in {
      val (codec, counter) = TestCodec(TextCodec.read, TextCodec.write)
      codec(wrap("QUIT\r\n")) mustEqual List(TextRequest("quit", Nil, Nil))
    }

    "success response" in {
      val (codec, counter) = TestCodec(TextCodec.read, TextCodec.write)
      codec.send(CountResponse(23)) mustEqual List("+23\n")
    }

    "error response" in {
      val (codec, counter) = TestCodec(TextCodec.read, TextCodec.write)
      codec.send(ErrorResponse("Bad karma")) mustEqual List("-Bad karma\n")
    }

    "empty response" in {
      val (codec, counter) = TestCodec(TextCodec.read, TextCodec.write)
      codec.send(ItemResponse(None)) mustEqual List("*\n")
    }

    "item response" in {
      val (codec, counter) = TestCodec(TextCodec.read, TextCodec.write)
      codec.send(ItemResponse(Some("hello".getBytes))) mustEqual List(":hello\n")
    }
  }

  "TextHandler" should {
    val queueCollection = mock[QueueCollection]
    val connection = mock[ClientConnection]
    val qitem = QItem(Time.now, None, "state shirt".getBytes, 23)

    "get request" in {
      connection.remoteAddress returns new InetSocketAddress("", 0)
//      expect {
//        one(connection).remoteAddress willReturn new InetSocketAddress("", 0)
//      }

      val textHandler = new TextHandler(connection, queueCollection, 10)

      "closes transactions" in {
        queueCollection.remove(M.eq("test"), M.eq(None), M.eq(true), M.eq(false), any[ClientDesc]) returns Future.value(Some(qitem))
//        expect {
//          one(queueCollection).remove(equal("test"), equal(None), equal(true), equal(false), any[ClientDesc]) willReturn Future.value(Some(qitem))
//          one(queueCollection).confirmRemove("test", 100)
//        }

        textHandler.handler.pendingReads.add("test", 100)
        textHandler.handler.pendingReads.peek("test") mustEqual List(100)
        Await.result(textHandler(TextRequest("get", List("test"), Nil))) mustEqual ItemResponse(Some(qitem.data))
        textHandler.handler.pendingReads.peek("test") mustEqual List(qitem.xid)
      }

      "with timeout" in {
        "value ready immediately" in {
          Time.withCurrentTimeFrozen { time =>
            queueCollection.remove(M.eq("test"), M.eq(Some(500.milliseconds.fromNow)), M.eq(true), M.eq(false), any[ClientDesc]) returns Future.value(Some(qitem))
//            expect {
//              one(queueCollection).remove(equal("test"), equal(Some(500.milliseconds.fromNow)), equal(true), equal(false), any[ClientDesc]) willReturn Future.value(Some(qitem))
//            }

            Await.result(textHandler(TextRequest("get", List("test", "500"), Nil))) mustEqual ItemResponse(Some(qitem.data))
          }
        }

        "value ready eventually" in {
          Time.withCurrentTimeFrozen { time =>
            val promise = new Promise[Option[QItem]]
            queueCollection.remove(M.eq("test"), M.eq(Some(500.milliseconds.fromNow)), M.eq(true), M.eq(false), any[ClientDesc]) returns promise
//
//            expect {
//              one(queueCollection).remove(equal("test"), equal(Some(500.milliseconds.fromNow)), equal(true), equal(false), any[ClientDesc]) willReturn promise
//            }

            val future = textHandler(TextRequest("get", List("test", "500"), Nil))

            promise.setValue(Some(qitem))
            Await.result(future) mustEqual ItemResponse(Some(qitem.data))
          }
        }

        "timed out" in {
          Time.withCurrentTimeFrozen { time =>
            val promise = new Promise[Option[QItem]]
            queueCollection.remove(M.eq("test"), M.eq(Some(500.milliseconds.fromNow)), M.eq(true), M.eq(false), any[ClientDesc]) returns promise

//
//            expect {
//              one(queueCollection).remove(equal("test"), equal(Some(500.milliseconds.fromNow)), equal(true), equal(false), any[ClientDesc]) willReturn promise
//            }

            val future = textHandler(TextRequest("get", List("test", "500"), Nil))

            promise.setValue(None)
            Await.result(future) mustEqual ItemResponse(None)
          }
        }
      }

      "empty queue" in {
        queueCollection.remove(M.eq("test"), M.eq(None), M.eq(true), M.eq(false), any[ClientDesc]) returns Future.value(None)

//        expect {
//          one(queueCollection).remove(equal("test"), equal(None), equal(true), equal(false), any[ClientDesc]) willReturn Future.value(None)
//        }

        Await.result(textHandler(TextRequest("get", List("test"), Nil))) mustEqual ItemResponse(None)
      }

      "item ready" in {
        queueCollection.remove(M.eq("test"), M.eq(None), M.eq(true), M.eq(false), any[ClientDesc]) returns Future.value(Some(qitem))

//        expect {
//          one(queueCollection).remove(equal("test"), equal(None), equal(true), equal(false), any[ClientDesc]) willReturn Future.value(Some(qitem))
//        }

        Await.result(textHandler(TextRequest("get", List("test"), Nil))) mustEqual ItemResponse(Some(qitem.data))
      }
    }

    "put request" in {
      Time.withCurrentTimeFrozen { timeMutator =>
        connection.remoteAddress returns new InetSocketAddress("", 0)
        queueCollection.add(M.eq("test"), M.eq("hello".getBytes), M.eq(None), M.eq(Time.now), any[ClientDesc]) returns true
//        expect {
//          one(connection).remoteAddress willReturn new InetSocketAddress("", 0)
//          one(queueCollection).add(equal("test"), equal("hello".getBytes), equal(None), equal(Time.now), any[ClientDesc]) willReturn true
//        }

        val textHandler = new TextHandler(connection, queueCollection, 10)
        Await.result(textHandler(TextRequest("put", List("test"), List("hello".getBytes)))) mustEqual CountResponse(1)
      }
    }

    "delete request" in {
      connection.remoteAddress returns new InetSocketAddress("", 0)
//      queueCollection.delete("test",  any[ClientDesc]) returns true
      //
//      expect {
//        one(connection).remoteAddress willReturn new InetSocketAddress("", 0)
//        one(queueCollection).delete(equal("test"), any[ClientDesc])
//      }

      val textHandler = new TextHandler(connection, queueCollection, 10)
      Await.result(textHandler(TextRequest("delete", List("test"), Nil))) mustEqual CountResponse(0)
    }

    "version request" in {
      connection.remoteAddress returns new InetSocketAddress("", 0)

//      expect {
//        one(connection).remoteAddress willReturn new InetSocketAddress("", 0)
//      }

      val runtime = RuntimeEnvironment(this, Array())
      Kestrel.runtime = runtime

      val textHandler = new TextHandler(connection, queueCollection, 10)
      Await.result(textHandler(TextRequest("version", Nil, Nil))) must haveClass[StringResponse]
    }

    "status request" in {
      "without server status" in {
        connection.remoteAddress returns new InetSocketAddress("", 0)

//        expect {
//          one(connection).remoteAddress willReturn new InetSocketAddress("", 0)
//        }

        val textHandler = new TextHandler(connection, queueCollection, 10)

        "check status should return an error" in {
          Await.result(textHandler(TextRequest("status", Nil, Nil))) must haveClass[ErrorResponse]
        }

        "set status should return an error" in {
          Await.result(textHandler(TextRequest("status", List("UP"), Nil))) must haveClass[ErrorResponse]
        }
      }

      "with server status" in {
        val serverStatus = mock[ServerStatus]
        connection.remoteAddress returns new InetSocketAddress("", 0)
//
//        expect {
//          one(connection).remoteAddress willReturn new InetSocketAddress("", 0)
//        }

        val textHandler = new TextHandler(connection, queueCollection, 10, Some(serverStatus))

        "check status should return current status" in {
          serverStatus.status returns Up
//          expect {
//            one(serverStatus).status willReturn Up
//          }
          Await.result(textHandler(TextRequest("status", Nil, Nil))) mustEqual StringResponse("UP")
        }

        "set status should set current status" in {

          Await.result(textHandler(TextRequest("status", List("ReadOnly"), Nil))) mustEqual CountResponse(0)

          got {
            one(serverStatus).setStatus("ReadOnly")
          }

        }
      }
    }

    // FIXME: peek, monitor, confirm, flush, quit, shutdown, unknown
  }
}
