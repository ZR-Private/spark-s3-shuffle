//
// Copyright 2022- IBM Inc. All rights reserved
// SPDX-License-Identifier: Apache 2.0
//

package org.apache.spark.shuffle

import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.IndexShuffleBlockResolver.NOOP_REDUCE_ID
import org.apache.spark.shuffle.api.metadata.MapOutputCommitMessage
import org.apache.spark.shuffle.api.{ShuffleMapOutputWriter, ShufflePartitionWriter, WritableByteChannelWrapper}
import org.apache.spark.shuffle.helper.{S3ShuffleDispatcher, S3ShuffleHelper}
import org.apache.spark.storage.ShuffleDataBlockId

import java.io.{IOException, OutputStream}
import java.nio.ByteBuffer
import java.nio.channels.{Channels, WritableByteChannel}
import java.util.Optional

/** Implements the ShuffleMapOutputWriter interface. It stores the shuffle output in one shuffle block.
  *
  * This file is based on Spark "LocalDiskShuffleMapOutputWriter.java".
  */

class S3ShuffleMapOutputWriter(
    conf: SparkConf,
    shuffleId: Int,
    mapId: Long,
    numPartitions: Int
) extends ShuffleMapOutputWriter
    with Logging {
  val dispatcher = S3ShuffleDispatcher.get

  /* Target block for writing */
  private val shuffleBlock = ShuffleDataBlockId(shuffleId, mapId, NOOP_REDUCE_ID)
  private var stream: FSDataOutputStream = _
  private var streamAsChannel: WritableByteChannel = _

  def initStream(): Unit = {
    if (stream == null) {
      stream = dispatcher.createBlock(shuffleBlock)
    }
  }

  def initChannel(): Unit = {
    if (streamAsChannel == null) {
      initStream()
      streamAsChannel = Channels.newChannel(stream)
    }
  }

  private val partitionLengths = Array.fill[Long](numPartitions)(0)
  private var totalBytesWritten: Long = 0
  private var lastPartitionWriterId: Int = -1

  /** @param reducePartitionId
    *   Monotonically increasing, as per contract in ShuffleMapOutputWriter.
    * @return
    *   An instance of the ShufflePartitionWriter exposing the single output stream.
    */
  override def getPartitionWriter(reducePartitionId: Int): ShufflePartitionWriter = {
    if (reducePartitionId <= lastPartitionWriterId) {
      throw new RuntimeException("Precondition: Expect a monotonically increasing reducePartitionId.")
    }
    if (reducePartitionId >= numPartitions) {
      throw new RuntimeException("Precondition: Invalid partition id.")
    }
    lastPartitionWriterId = reducePartitionId
    new S3ShufflePartitionWriter(reducePartitionId)
  }

  /** Close all writers and the shuffle block.
    *
    * @param checksums
    *   Ignored.
    * @return
    */
  override def commitAllPartitions(checksums: Array[Long]): MapOutputCommitMessage = {
    if (stream != null) {
      if (stream.getPos != totalBytesWritten) {
        throw new RuntimeException(
          f"S3ShuffleMapOutputWriter: Unexpected output length ${stream.getPos}, expected: ${totalBytesWritten}."
        )
      }
    }
    if (streamAsChannel != null) {
      streamAsChannel.close()
    }
    if (stream != null) {
      // Closes the underlying stream as well!
      stream.close()
    }

    // Write index and checksum.
    if (partitionLengths.sum > 0 || S3ShuffleDispatcher.get.alwaysCreateIndex) {
      S3ShuffleHelper.writePartitionLengths(shuffleId, mapId, partitionLengths)
      if (dispatcher.checksumEnabled) {
        S3ShuffleHelper.writeChecksum(shuffleId, mapId, checksums)
      }
    }
    MapOutputCommitMessage.of(partitionLengths)
  }

  override def abort(error: Throwable): Unit = {
    cleanUp()
  }

  private def cleanUp(): Unit = {
    if (streamAsChannel != null) {
      streamAsChannel.close()
    }
    if (stream != null) {
      stream.close()
    }
  }

  private class S3ShufflePartitionWriter(reduceId: Int) extends ShufflePartitionWriter with Logging {
    private var partitionStream: S3ShuffleOutputStream = _
    private var partitionChannel: S3ShufflePartitionWriterChannel = _

    override def openStream(): OutputStream = {
      initStream()
      if (partitionStream == null) {
        partitionStream = new S3ShuffleOutputStream(reduceId)
      }
      partitionStream
    }

    override def openChannelWrapper(): Optional[WritableByteChannelWrapper] = {
      if (partitionChannel == null) {
        initChannel()
        partitionChannel = new S3ShufflePartitionWriterChannel(reduceId)
      }
      Optional.of(partitionChannel)
    }

    override def getNumBytesWritten: Long = {
      if (partitionChannel != null) {
        return partitionChannel.numBytesWritten
      }
      if (partitionStream != null) {
        return partitionStream.numBytesWritten
      }
      // The partition is empty.
      0
    }
  }

  private class S3ShuffleOutputStream(reduceId: Int) extends OutputStream {
    private var byteCount: Long = 0
    private var isClosed = false

    def numBytesWritten: Long = byteCount

    override def write(b: Int): Unit = {
      if (isClosed) {
        throw new IOException("S3ShuffleOutputStream is already closed.")
      }
      stream.write(b)
      byteCount += 1
    }

    override def write(b: Array[Byte], off: Int, len: Int): Unit = {
      if (isClosed) {
        throw new IOException("S3ShuffleOutputStream is already closed.")
      }
      stream.write(b, off, len)
      byteCount += len
    }

    override def flush(): Unit = {
      if (isClosed) {
        throw new IOException("S3ShuffleOutputStream is already closed.")
      }
      stream.flush()
    }

    override def close(): Unit = {
      partitionLengths(reduceId) = byteCount
      totalBytesWritten += byteCount
      isClosed = true
    }
  }

  private class S3ShufflePartitionWriterChannel(reduceId: Int) extends WritableByteChannelWrapper {
    private val partChannel = new S3PartitionWritableByteChannel(streamAsChannel)

    override def channel(): WritableByteChannel = {
      partChannel
    }

    def numBytesWritten: Long = {
      partChannel.numBytesWritten()
    }

    override def close(): Unit = {
      partitionLengths(reduceId) = numBytesWritten
      totalBytesWritten += numBytesWritten
    }
  }

  private class S3PartitionWritableByteChannel(channel: WritableByteChannel) extends WritableByteChannel {

    private var count: Long = 0

    def numBytesWritten(): Long = {
      count
    }

    override def isOpen(): Boolean = {
      channel.isOpen()
    }

    override def close(): Unit = {}

    override def write(x: ByteBuffer): Int = {
      var c = 0
      while (x.hasRemaining()) {
        c += channel.write(x)
      }
      count += c
      c
    }
  }
}
