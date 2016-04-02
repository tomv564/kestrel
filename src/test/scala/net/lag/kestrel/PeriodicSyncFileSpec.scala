/*
 * Copyright 2011 Twitter, Inc.
 * Copyright 2011 Robey Pointer <robeypointer@gmail.com>
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
import com.twitter.logging.TestLogging
import com.twitter.util.{Stopwatch, Duration}
import java.nio.ByteBuffer
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger
import org.specs2.mutable._
import org.specs2.matcher.Matcher

class PeriodicSyncFileSpec extends Specification
  with TestLogging
{

  isolated

  val scheduler: ScheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(4)

  def cleanup = {
    scheduler.shutdown()
    scheduler.awaitTermination(5, TimeUnit.SECONDS)
  }

  "PeriodicSyncTask" should {
    val invocations = new AtomicInteger(0)
    val syncTask = new PeriodicSyncTask(scheduler, 0.milliseconds, 20.milliseconds) {
      override def run() {
        invocations.incrementAndGet
      }
    }

    "only start once" in {
      val sw = Stopwatch.start()
      syncTask.start()
      syncTask.start()
      Thread.sleep(100)
      syncTask.stop()

      val expectedInvocations = sw().inMilliseconds / 20
      (invocations.get <= expectedInvocations * 3 / 2) mustEqual true
    }

    "stop" in {
      syncTask.start()
      Thread.sleep(100)
      syncTask.stop()
      val invocationsPostTermination = invocations.get
      Thread.sleep(100)
      invocations.get mustEqual invocationsPostTermination
    }

    "stop given a condition" in {
      syncTask.start()
      Thread.sleep(100)

      val invocationsPreStop = invocations.get
      syncTask.stopIf { false }
      Thread.sleep(100)

      val invocationsPostIgnoredStop = invocations.get
      syncTask.stopIf { true }
      Thread.sleep(100)

      val invocationsPostStop = invocations.get
      Thread.sleep(100)

      (invocationsPreStop > 0) mustEqual true                            // did something
      (invocationsPostIgnoredStop > invocationsPreStop) mustEqual true   // kept going
      (invocationsPostStop >= invocationsPostIgnoredStop) mustEqual  true // maybe did more
      invocations.get mustEqual invocationsPostStop                   // stopped
    }
  }

  step(cleanup)
}
