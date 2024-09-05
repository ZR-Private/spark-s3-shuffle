package org.apache.spark.storage

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.shuffle.IndexShuffleBlockResolver.NOOP_REDUCE_ID
import org.apache.spark.shuffle.helper.S3ShuffleDispatcher
import org.apache.spark.util.Utils
import org.apache.spark.{SparkConf, SparkEnv, SparkException}

import java.io.DataInputStream
import java.nio.ByteBuffer

object S3FallbackStorage extends Logging {
  private val dispatcher = S3ShuffleDispatcher.get
  private val hadoopConf = SparkHadoopUtil.get.newConfiguration(SparkEnv.get.conf)
  private val randomFallbackFileSystem = {
    val rootDir = dispatcher.rootDir
    val hadoopConf = SparkHadoopUtil.get.newConfiguration(SparkEnv.get.conf)
    hadoopConf.set("fs.s3a.experimental.input.fadvise", "random")
    FileSystem.newInstance(new Path(rootDir).toUri, hadoopConf)
  }
  private val localFs = FileSystem.getLocal(hadoopConf)

  def downloadFiles(shuffleId: Int, mapId: Long): Unit = {
    logDebug(s"start downloadFiles ${shuffleId} ${mapId}")
    val indexBlockId = ShuffleIndexBlockId(shuffleId, mapId, NOOP_REDUCE_ID)
    val indexFile = dispatcher.getPath(indexBlockId)
    val localIndexPath = new Path(dispatcher.tempDir, dispatcher.getSubPath(indexBlockId))

    if (!localFs.exists(localIndexPath)) {
      localFs.mkdirs(localIndexPath.getParent)
      dispatcher.fs.copyToLocalFile(indexFile, localIndexPath)
    }
    logDebug(s"end downloadFiles ${shuffleId} ${mapId}")
  }

  def readFromLocal(blockId: BlockId): ManagedBuffer = {
    logDebug(s"Read $blockId")
    val localPath = new Path(dispatcher.tempDir)

    val (shuffleId, mapId, startReduceId, endReduceId) = blockId match {
      case id: ShuffleBlockId =>
        (id.shuffleId, id.mapId, id.reduceId, id.reduceId + 1)
      case batchId: ShuffleBlockBatchId =>
        (batchId.shuffleId, batchId.mapId, batchId.startReduceId, batchId.endReduceId)
      case _ =>
        throw SparkException.internalError(s"unexpected shuffle block id format: $blockId", category = "STORAGE")
    }

    val indexBlockId = ShuffleIndexBlockId(shuffleId, mapId, NOOP_REDUCE_ID)
    val indexFile = new Path(localPath, dispatcher.getSubPath(indexBlockId))
    val start = startReduceId * 8L
    val end = endReduceId * 8L
    Utils.tryWithResource(localFs.open(indexFile)) { inputStream =>
      Utils.tryWithResource(new DataInputStream(inputStream)) { index =>
        index.skip(start)
        val offset = index.readLong()
        index.skip(end - (start + 8L))
        val nextOffset = index.readLong()
        val dataBlockId = ShuffleDataBlockId(shuffleId, mapId, NOOP_REDUCE_ID)
        val dataFile = dispatcher.getPath(dataBlockId)
        val size = nextOffset - offset
        val array = new Array[Byte](size.toInt)
        Utils.tryWithResource(randomFallbackFileSystem.open(dataFile)) { dataStream =>
          logDebug(s"To byte array $size")
          val startTimeNs = System.nanoTime()
          dataStream.seek(offset)
          dataStream.readFully(array)
          logDebug(s"Took ${(System.nanoTime() - startTimeNs) / (1000 * 1000)}ms")
        }
        new NioManagedBuffer(ByteBuffer.wrap(array))
      }
    }
  }
}
