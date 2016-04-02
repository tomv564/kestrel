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

import java.io._
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.channels.FileChannel
import java.util.concurrent.{LinkedBlockingQueue, ScheduledExecutorService}
import java.util.concurrent.atomic.AtomicInteger
import scala.annotation.tailrec
import com.twitter.conversions.storage._
import com.twitter.conversions.time._
import com.twitter.logging.Logger
import com.twitter.ostrich.admin.BackgroundProcess
import com.twitter.util.{Future, Duration, Time}

case class BrokenItemException(lastValidPosition: Long, cause: Throwable) extends IOException(cause)

case class Checkpoint(filename: String, reservedItems: Seq[QItem])

case class PackRequest(journal: Journal, checkpoint: Checkpoint, openItems: Iterable[QItem],
                       pentUpDeletes: Int, queueState: Iterable[QItem])

// returned from journal replay
abstract class JournalItem()
object JournalItem {
  case class Add(item: QItem) extends JournalItem
  case object Remove extends JournalItem
  case class RemoveTentative(xid: Int) extends JournalItem
  case class SavedXid(xid: Int) extends JournalItem
  case class Unremove(xid: Int) extends JournalItem
  case class ConfirmRemove(xid: Int) extends JournalItem
  case class Continue(item: QItem, xid: Int) extends JournalItem
  case object EndOfFile extends JournalItem
}

abstract class Failpoint()
object Failpoint {
  case object Default extends Failpoint
  case object RewriteFPBeforePack extends Failpoint
  case object RewriteFPAfterPack extends Failpoint
  case object RewriteFPAfterDelete extends Failpoint
}

/**
 * Codes for working with the journal file for a PersistentQueue.
 */
class Journal(queuePath: PersistentStreamContainer, queueName: String, syncPeriod: Duration) {
  import Journal._

  private val log = Logger.get(getClass)
  private var writer: PersistentStreamWriter = null
  private var reader: Option[PersistentStreamReader] = None
  private var readerFilename: Option[String] = None
  private var replayer: Option[PersistentStreamReader] = None
  private var replayerFilename: Option[String] = None

  private def queueFile = getStream(queueName)

  // TODO: This should eventually be removed
  queueFile

  // size of the current file, so far
  var size: Long = 0

  // size of the previous files combined
  @volatile var archivedSize: Long = 0

  @volatile var closed: Boolean = false

  @volatile var checkpoint: Option[Checkpoint] = None
  var removesSinceReadBehind: Int = 0

  // small temporary buffer for formatting operations into the journal:
  private val buffer = new Array[Byte](16)
  private val byteBuffer = ByteBuffer.wrap(buffer)
  byteBuffer.order(ByteOrder.LITTLE_ENDIAN)

  private val CMD_ADD = 0
  private val CMD_REMOVE = 1
  private val CMD_ADDX = 2
  private val CMD_REMOVE_TENTATIVE = 3
  private val CMD_SAVE_XID = 4
  private val CMD_UNREMOVE = 5
  private val CMD_CONFIRM_REMOVE = 6
  private val CMD_ADD_XID = 7
  private val CMD_CONTINUE = 8
  private val CMD_REMOVE_TENTATIVE_XID = 9

  private def open(stream: PersistentStream) {
    writer = stream.getWriter
  }

  // Since PersistentStreamContainer#getStream has the semantics of
  // create if not exists
  // We should at most have one outstanding call to getStream for a given queue
  private def getStream(streamName: String) = {
    synchronized {
      queuePath.getStream(streamName, syncPeriod)
    }
  }

  def open() {
    open(queueFile)
  }

  def notifyRemoveDuringReplay() {
    if (inReadBehind) {
      removesSinceReadBehind += 1
    }
  }

  def calculateArchiveSize() {
    val files = archivedJournalFiles()
    archivedSize = files.foldLeft(0L) { (sum, filename) =>
      sum + getStream(filename).length
    }
  }

  private def uniqueFile(infix: String, suffix: String = ""): String = {
    queuePath.uniqueStreamName(queueName, infix, suffix)
  }

  def rotate(reservedItems: Seq[QItem], setCheckpoint: Boolean): Option[Checkpoint] = {
    writer.close()
    val rotatedFile = uniqueFile(".")
    queuePath.renameStream(queueName, rotatedFile)
    size = 0
    calculateArchiveSize()
    open()

    if (readerFilename == Some(queueName)) {
      readerFilename = Some(rotatedFile)
    }

    if (setCheckpoint && !checkpoint.isDefined) {
      checkpoint = Some(Checkpoint(rotatedFile, reservedItems))
    }
    checkpoint
  }

  def rewrite(reservedItems: Seq[QItem], queue: Iterable[QItem]) {
    rewrite(reservedItems, queue, Failpoint.Default)
  }

  // Directly invoked only by tests
  def rewrite(reservedItems: Seq[QItem], queue: Iterable[QItem], failPoint: Failpoint) {
    writer.close()
    val tempFile = uniqueFile("~~")
    open(getStream(tempFile))
    dump(reservedItems, queue)
    writer.close()

    if (Failpoint.RewriteFPBeforePack == failPoint) return

    val packFile = uniqueFile(".", ".pack")
    queuePath.renameStream(tempFile, packFile)

    // cleanup the .pack file:
    val files = Journal.archivedFilesForQueue(queuePath, queueName)

    // TODO: Failure at this point causes incorrect recovery =>
    // TODO: need to fix this
    if (Failpoint.RewriteFPAfterPack == failPoint) return

    // Making the delete of the old queue file explicit.
    // Its fine if fail after this point before renaming the file.
    // This leaves the journal in no different state that what would
    // happen if we failed in the middle of journal rotation
    queuePath.deleteStream(queueName)

    if (Failpoint.RewriteFPAfterDelete == failPoint) return

    queuePath.renameStream(files(0), queueName)
    calculateArchiveSize()
    open()
  }

  def dump(reservedItems: Iterable[QItem], openItems: Iterable[QItem], pentUpDeletes: Int, queue: Iterable[QItem]) {
    size = 0
    for (item <- reservedItems) {
      add(item)
      removeTentative(item.xid)
    }
    for (item <- openItems) {
      add(item)
    }
    val empty = Array[Byte]()
    for (i <- 0 until pentUpDeletes) {
      add(false, QItem(Time.now, None, empty, 0))
    }
    for (item <- queue) {
      add(false, item)
    }
  }

  def dump(reservedItems: Iterable[QItem], queue: Iterable[QItem]) {
    dump(reservedItems, Nil, 0, queue)
  }

  def startPack(checkpoint: Checkpoint, openItems: Iterable[QItem], queueState: Seq[QItem]) {
    val knownXids = checkpoint.reservedItems.map { _.xid }.toSet
    val currentXids = openItems.map { _.xid }.toSet
    val newlyOpenItems = openItems.filter { x => !(knownXids contains x.xid) }
    val newlyClosedItems = checkpoint.reservedItems.filter { x => !(currentXids contains x.xid) }
    val negs = removesSinceReadBehind - newlyClosedItems.size // newly closed are already accounted for.

    outstandingPackRequests.incrementAndGet()
    packer.add(PackRequest(this, checkpoint, newlyOpenItems, negs, queueState))
  }

  def close() {
    writer.close()
    reader.foreach { _.close() }
    reader = None
    readerFilename = None
    closed = true
    waitForPacksToFinish()
  }

  def erase() {
    try {
      close()
      archivedJournalFiles().foreach { filename =>
        queuePath.deleteStream(filename)
      }
      queuePath.deleteStream(queueName)
    } catch {
      case _ : Throwable =>
    }
  }

  def inReadBehind: Boolean = reader.isDefined

  def isReplaying: Boolean = replayer.isDefined

  private def add(allowSync: Boolean, item: QItem): Future[Unit] = {
    val blob = item.pack(CMD_ADDX.toByte)
    size += blob.limit
    writer.write(blob)
  }

  def add(item: QItem): Future[Unit] = add(true, item)

  def continue(xid: Int, item: QItem): Future[Unit] = {
    if (inReadBehind) removesSinceReadBehind += 1
    val blob = item.pack(CMD_CONTINUE.toByte, xid)
    size += blob.limit
    writer.write(blob)
  }

  def remove() {
    write(CMD_REMOVE.toByte)
    if (inReadBehind) removesSinceReadBehind += 1
  }

  def removeTentative(xid: Int) {
    write(CMD_REMOVE_TENTATIVE_XID.toByte, xid)
  }

  def unremove(xid: Int) {
    write(CMD_UNREMOVE.toByte, xid)
  }

  def confirmRemove(xid: Int) {
    write(CMD_CONFIRM_REMOVE.toByte, xid)
    if (inReadBehind) removesSinceReadBehind += 1
  }

  def startReadBehind() {
    val pos = if (replayer.isDefined) {
      replayer.get.position
    } else {
      // Tests are mostly the reason for including this fsync
      // In practice, its unlikely for a queue to keep
      // switching in and out of readbehind, so it adds no
      // performance penalty
      writer.force(false)
      writer.position
    }
    val filename = if (replayerFilename.isDefined) replayerFilename.get else queueName
    val rj = getStream(filename).getReader
    rj.positionAt(pos)
    reader = Some(rj)
    readerFilename = Some(filename)
    removesSinceReadBehind = 0
    log.debug("Read-behind on '%s' starting at file %s", queueName, readerFilename.get)
  }

  // not tail recursive, but should only recurse once.
  def fillReadBehind(gotItem: QItem => Unit)(gotCheckpoint: Checkpoint => Unit): Unit = {
    val pos = if (replayer.isDefined) replayer.get.position else writer.position
    val filename = if (replayerFilename.isDefined) replayerFilename.get else queueName

    reader.foreach { rj =>
      if (rj.position == pos && readerFilename.get == filename) {
        // we've caught up.
        rj.close()
        reader = None
        readerFilename = None
      } else {
        readJournalEntry(rj, readerFilename.get == queueName) match {
          case (JournalItem.Add(item), _) =>
            gotItem(item)
          case (JournalItem.Remove, _) =>
            removesSinceReadBehind -= 1
          case (JournalItem.ConfirmRemove(_), _) =>
            removesSinceReadBehind -= 1
          case (JournalItem.Continue(item, xid), _) =>
            removesSinceReadBehind -= 1
            gotItem(item)
          case (JournalItem.EndOfFile, _) =>
            // move to next file and try again.
            val oldFilename = readerFilename.get
            readerFilename = journalFilesAfter(readerFilename.get)
            if (readerFilename.isEmpty) {
              // in theory, if we caught up on current journal file, we should enter
              // (rj.position = pos && readerFilename.get == filename) branch and get ouf of readbehind
              // but it seems that we reach EndOfFile on current writing journal file, which cause None.get
              // to throw NoSuchElementException
              log.warning("Read-behind is trying to move over journal file %s : reader(%s, position = %d), "
                          + " writer(%s, position = %d), journals = (%s)",
                          oldFilename, oldFilename, rj.position, filename, pos, allJournalFiles())
            }
            rj.close()
            reader = Some(getStream(readerFilename.get).getReader)
            log.info("Read-behind on '%s' moving from file %s to %s", queueName, oldFilename, readerFilename.get)
            if (checkpoint.isDefined && checkpoint.get.filename == oldFilename) {
              gotCheckpoint(checkpoint.get)
            }
            fillReadBehind(gotItem)(gotCheckpoint)
          case (_, _) =>
        }
      }
    }
  }

  def replay(f: JournalItem => Unit) {
    replay(false, f)
  }

  def replay(recover: Boolean, f: JournalItem => Unit) {
      // first, erase any lingering temp files.
    queuePath.listStreams.filter {
      _.startsWith(queueName + "~~")
    }.foreach { filename =>
      queuePath.deleteStream(filename)
    }
    allJournalFiles().foreach { filename =>
      replayFile(queueName, filename, recover)(f)
    }
  }

  def replayFile(name: String, filename: String, recover: Boolean)(f: JournalItem => Unit): Unit = {
    log.info("Replaying '%s' file %s", name, filename)
    size = 0
    var lastUpdate = 0L
    try {
      val stream = getStream(filename)
      if (recover) {
        stream.recover()
      }
      val in = stream.getReader
      replayer = Some(in)
      replayerFilename = Some(filename)
      try {
        var done = false
        do {
          readJournalEntry(in, false) match {
            case (JournalItem.EndOfFile, _) =>
              done = true
            case (x, itemsize) =>
              size += itemsize
              f(x)
              if (size > lastUpdate + 10.megabytes.inBytes) {
                log.info("Continuing to read '%s' journal (%s); %s so far...", name, filename, size.bytes.toHuman())
                lastUpdate = size
              }
          }
        } while (!done)
      } catch {
        case e: BrokenItemException =>
          log.error(e, "Exception replaying journal for '%s': %s", name, filename)
          log.error("DATA MAY HAVE BEEN LOST! Truncated entry will be deleted.")
          truncateJournal(e.lastValidPosition)
      }
    } catch {
      case e: FileNotFoundException =>
        log.info("No transaction journal for '%s'; starting with empty queue.", name)
      case e: IOException =>
        log.error(e, "Exception replaying journal for '%s': %s", name, filename)
        log.error("DATA MAY HAVE BEEN LOST!")
        // this can happen if the server hardware died abruptly in the middle
        // of writing a journal. not awesome but we should recover.
    }
    // If we did open a reader then we would have assigned a value to the replayer
    // close this reader
    replayer.foreach { _.close() }
    replayer  = None
    replayerFilename = None
  }

  private def truncateJournal(position: Long) {
    val truncateWriter = queueFile.getWriter
    try {
      truncateWriter.truncate(position)
    } catch {
      case ex: Exception => log.error(ex, "journal truncate failed")
    } finally {
      truncateWriter.close()
    }
  }

  def readJournalEntry(in: PersistentStreamReader, forceOnEmpty: Boolean): (JournalItem, Int) = {
    var shouldFSync: Boolean = forceOnEmpty
    var tryAgain: Boolean = false
    var lastPosition: Long = in.position
    var x: Int = 0
    do {
      tryAgain = false
      byteBuffer.rewind
      byteBuffer.limit(1)
      lastPosition = in.position
      x = 0
      do {
        x = in.read(byteBuffer)
      } while (byteBuffer.position < byteBuffer.limit && x >= 0)

      if (shouldFSync && (x < 0)) {
        writer.force(false)
        shouldFSync = false
        tryAgain = true
      }
    } while (tryAgain)

    if (x < 0) {
      (JournalItem.EndOfFile, 0)
    } else {
      try {
        buffer(0) match {
          case CMD_ADD =>
            val data = readBlock(in)
            (JournalItem.Add(QItem.unpackOldAdd(data)), 5 + data.length)
          case CMD_REMOVE =>
            (JournalItem.Remove, 1)
          case CMD_ADDX =>
            val data = readBlock(in)
            (JournalItem.Add(QItem.unpack(data)), 5 + data.length)
          case CMD_REMOVE_TENTATIVE =>
            (JournalItem.RemoveTentative(0), 1)
          case CMD_SAVE_XID =>
            val xid = readInt(in)
            (JournalItem.SavedXid(xid), 5)
          case CMD_UNREMOVE =>
            val xid = readInt(in)
            (JournalItem.Unremove(xid), 5)
          case CMD_CONFIRM_REMOVE =>
            val xid = readInt(in)
            (JournalItem.ConfirmRemove(xid), 5)
          case CMD_ADD_XID =>
            val xid = readInt(in)
            val data = readBlock(in)
            val item = QItem.unpack(data)
            item.xid = xid
            (JournalItem.Add(item), 9 + data.length)
          case CMD_CONTINUE =>
            val xid = readInt(in)
            val data = readBlock(in)
            val item = QItem.unpack(data)
            (JournalItem.Continue(item, xid), 9 + data.length)
          case CMD_REMOVE_TENTATIVE_XID =>
            val xid = readInt(in)
            (JournalItem.RemoveTentative(xid), 5)
          case n =>
            throw new BrokenItemException(lastPosition, new IOException("invalid opcode in journal: " + n.toInt + " at position " + (in.position - 1)))
        }
      } catch {
        case ex: IOException =>
          throw new BrokenItemException(lastPosition, ex)
      }
    }
  }

  def walk(): Iterator[(JournalItem, Int)] = {
    val in = getStream(queueName).getReader
    def next(): Stream[(JournalItem, Int)] = {
      readJournalEntry(in, false) match {
        case (JournalItem.EndOfFile, _) =>
          in.close()
          Stream.Empty
        case x =>
          new Stream.Cons(x, next())
      }
    }
    next().iterator
  }

  private def readBlock(in: PersistentStreamReader): Array[Byte] = {
    val size = readInt(in)
    val data = new Array[Byte](size)
    val dataBuffer = ByteBuffer.wrap(data)
    var x: Int = 0
    do {
      x = in.read(dataBuffer)
    } while (dataBuffer.position < dataBuffer.limit && x >= 0)
    if (x < 0) {
      // we never expect EOF when reading a block.
      throw new IOException("Unexpected EOF")
    }
    data
  }

  private def readInt(in: PersistentStreamReader): Int = {
    byteBuffer.rewind
    byteBuffer.limit(4)
    var x: Int = 0
    do {
      x = in.read(byteBuffer)
    } while (byteBuffer.position < byteBuffer.limit && x >= 0)
    if (x < 0) {
      // we never expect EOF when reading an int.
      throw new IOException("Unexpected EOF")
    }
    byteBuffer.rewind
    byteBuffer.getInt
  }

  private def write(items: Any*): Future[Unit] = {
    byteBuffer.clear
    for (item <- items) item match {
      case b: Byte => byteBuffer.put(b)
      case i: Int => byteBuffer.putInt(i)
    }
    byteBuffer.flip
    val future = writer.write(byteBuffer)
    size += byteBuffer.limit
    future
  }

  val outstandingPackRequests = new AtomicInteger(0)

  def waitForPacksToFinish() {
    while (outstandingPackRequests.get() > 0) {
      Thread.sleep(10)
    }
  }

  def persistChanges() {
    writer.force(false)
  }

  private[kestrel] def pack(state: PackRequest) {
    val oldFilenames = journalFilesBefore(state.checkpoint.filename) ++
          List(state.checkpoint.filename)
    log.info("Packing journals for '%s': %s", queueName, oldFilenames.mkString(", "))

    val tempFile = uniqueFile("~~")
    val newJournal = new Journal(queuePath, tempFile, syncPeriod)
    newJournal.open()
    newJournal.dump(state.checkpoint.reservedItems, state.openItems, state.pentUpDeletes, state.queueState)
    newJournal.close()

    // Flush the updates to the current journal so that any removes that have been accounted
    // for in the pack are persisted before the packed file replaces existing files
    writer.force(false)

    log.info("Packing '%s' -- erasing old files.", queueName)
    queuePath.renameStream(tempFile, state.checkpoint.filename + ".pack")
    calculateArchiveSize()
    log.info("Packing '%s' done: %s", queueName, allJournalFiles().mkString(", "))

    checkpoint = None
  }

  private def archivedJournalFiles(): List[String] = {
    this.synchronized {
      Journal.archivedFilesForQueue(queuePath, queueName)
    }
  }

  private def allJournalFiles(): List[String] = {
    archivedJournalFiles() ++ List(queueName)
  }

  private def journalFilesBefore(filename: String): Seq[String] = {
    allJournalFiles().takeWhile { _ != filename }
  }

  private def journalFilesAfter(filename: String): Option[String] = {
    allJournalFiles().dropWhile { _ != filename }.drop(1).headOption
  }
}

class JournalPackerTask {
  val logger = Logger.get(getClass)
  val queue = new LinkedBlockingQueue[Option[PackRequest]]()

  var thread: Thread = null

  def start() {
    synchronized {
      if (thread == null) {
        thread = new Thread("journal-packer") {
          override def run() {
            var running = true
            while (running) {
              val requestOpt = queue.take()
              requestOpt match {
                case Some(request) => pack(request)
                case None =>          running = false
              }
            }
            logger.info("journal-packer exited.")
          }
        }
        thread.setDaemon(true)
        thread.setName("journal-packer")
        thread.start()
      } else {
         logger.error("journal-packer already started.")
      }
    }
  }

  def shutdown() {
    synchronized {
      if (thread != null) {
        logger.info("journal-packer exiting.")
        queue.add(None)
        thread.join(5000L)
        thread = null
      } else {
        logger.error("journal-packer not running.")
      }
    }
  }

  def add(request: PackRequest) {
    queue.add(Some(request))
  }

  private def pack(request: PackRequest) {
    try {
      request.journal.pack(request)
      request.journal.outstandingPackRequests.decrementAndGet()
    } catch {
      case e: Throwable =>
        logger.error(e, "Uncaught exception in journal-packer: %s", e)
    }
  }
}

object Journal {
  private val log = Logger.get(getClass)

  def getQueueNamesFromFolder(streamContainer: PersistentStreamContainer): Set[String] = {
    streamContainer.listQueues
  }

  /**
   * A .pack file is atomically moved into place only after it contains a summary of the contents
   * of every journal file with a lesser-or-equal timestamp. If we find such a file, it's safe and
   * race-free to erase the older files and move the .pack file into place.
   */
  private def cleanUpPackedFiles(path: PersistentStreamContainer, files: List[(String, Long)]): Boolean = {
    val packFile = files.find { case (filename, timestamp) =>
      filename endsWith ".pack"
    }
    if (packFile.isDefined) {
      val (packFilename, packTimestamp) = packFile.get
      val doomed = files.filter { case (filename, timestamp) =>
        (timestamp <= packTimestamp) && !(filename endsWith ".pack")
      }
      doomed.foreach { case (filename, timestamp) =>
        log.info ("Deleting packed file %s", filename)
        path.deleteStream(filename)
      }
      val newFilename = packFilename.substring(0, packFilename.length - 5)
      path.renameStream(packFilename, newFilename)
      true
    } else {
      false
    }
  }

  /**
   * Find all the archived (non-current) journal files for a queue, sort them in replay order (by
   * timestamp), and erase the remains of any unfinished business that we find along the way.
   */
  @tailrec
  def archivedFilesForQueue(path: PersistentStreamContainer, queueName: String): List[String] = {
    val totalFiles = path.listStreams
    if (totalFiles eq null) {
      // directory is gone.
      Nil
    } else {
      val timedFiles = totalFiles.filter {
        _.startsWith(queueName + ".")
      }.map { filename =>
        (filename, filename.split('.')(1).toLong)
      }.sortBy { case (filename, timestamp) =>
        timestamp
      }.toList

      if (cleanUpPackedFiles(path, timedFiles)) {
        // probably only recurses once ever.
        archivedFilesForQueue(path, queueName)
      } else {
        timedFiles.map { case (filename, timestamp) => filename }
      }
    }
  }

  val packer = new JournalPackerTask
}
