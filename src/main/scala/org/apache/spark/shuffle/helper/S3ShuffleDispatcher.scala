//
// Copyright 2022- IBM Inc. All rights reserved
// SPDX-License-Identifier: Apache 2.0
//

package org.apache.spark.shuffle.helper

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.{Logging, config}
import org.apache.spark.shuffle.ConcurrentObjectMap
import org.apache.spark.storage._
import org.apache.spark.{SparkConf, SparkEnv, SparkException}

import java.io.IOException
import java.net.URI
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/** Helper class that configures Hadoop FS.
  */
class S3ShuffleDispatcher extends Logging {
  val executorId: String = SparkEnv.get.executorId
  val conf: SparkConf = SparkEnv.get.conf
  private var appId: String = conf.getAppId
  private val SPARK_S3_SHUFFLE_REGULAR_HADOOP_PREFIX = "spark.shuffle.s3.regular.hadoop."
  private val SPARK_S3_SHUFFLE_RANDOM_HADOOP_PREFIX = "spark.shuffle.s3.random.hadoop."

  def reinitialize(newAppId: String): Unit = {
    appId = newAppId
    cachedFileStatus.clear()
    S3ShuffleHelper.purgeCachedData()
  }

  val startTime: String = conf.get("spark.app.startTime")

  // Required
  val useSparkShuffleFetch: Boolean = conf.getBoolean("spark.shuffle.s3.useSparkShuffleFetch", defaultValue = false)
  private val appPrefix_ = conf.get("spark.shuffle.s3.rootDirPrefix", defaultValue = "app")
  val appPrefix = if (appPrefix_.endsWith("/")) {
    appPrefix_.substring(0, appPrefix_.length - 1)
  } else {
    appPrefix_
  }
  private val rootDir_ = conf.get("spark.shuffle.s3.rootDir", defaultValue = "sparkS3shuffle/")
  val rootDir: String = if (rootDir_.endsWith("/")) {
    rootDir_
  } else {
    rootDir_ + "/"
  }
  val rootIsLocal: Boolean = URI.create(rootDir).getScheme == "file"

  // Optional
  val bufferSize: Int = conf.getInt("spark.shuffle.s3.bufferSize", defaultValue = 8 * 1024 * 1024)
  val maxBufferSizeTask: Int = conf.getInt("spark.shuffle.s3.maxBufferSizeTask", defaultValue = 128 * 1024 * 1024)
  val maxConcurrencyTask: Int = conf.getInt("spark.shuffle.s3.maxConcurrencyTask", defaultValue = 10)
  val cachePartitionLengths: Boolean = conf.getBoolean("spark.shuffle.s3.cachePartitionLengths", defaultValue = true)
  val cacheChecksums: Boolean = conf.getBoolean("spark.shuffle.s3.cacheChecksums", defaultValue = true)
  val cleanupShuffleFiles: Boolean = conf.getBoolean("spark.shuffle.s3.cleanup", defaultValue = true)
  val folderPrefixes: Int = conf.getInt("spark.shuffle.s3.folderPrefixes", defaultValue = 10)

  // Debug
  val alwaysCreateIndex: Boolean = conf.getBoolean("spark.shuffle.s3.alwaysCreateIndex", defaultValue = false)
  val useBlockManager: Boolean = conf.getBoolean("spark.shuffle.s3.useBlockManager", defaultValue = true)
  val forceBatchFetch: Boolean = conf.getBoolean("spark.shuffle.s3.forceBatchFetch", defaultValue = false)
  val tempDir: String = conf.get("spark.shuffle.s3.tempDir", defaultValue = "/tmp/")

  // Spark feature
  val checksumAlgorithm: String = SparkEnv.get.conf.get(config.SHUFFLE_CHECKSUM_ALGORITHM)
  val checksumEnabled: Boolean = SparkEnv.get.conf.get(config.SHUFFLE_CHECKSUM_ENABLED)

  val fs: FileSystem = FileSystem.newInstance(
    URI.create(rootDir),
    getHadoopConfForFileSystem(SPARK_S3_SHUFFLE_REGULAR_HADOOP_PREFIX)
  )

  val randomFs: FileSystem = FileSystem.newInstance(
    URI.create(rootDir),
    getHadoopConfForFileSystem(SPARK_S3_SHUFFLE_RANDOM_HADOOP_PREFIX)
  )

  val canSetReadahead = fs.hasPathCapability(new Path(rootDir), StreamCapabilities.READAHEAD)
  val prefixKeys: Array[Char] = (('0' to '9') ++ ('a' to 'z') ++ ('A' to 'Z')).toArray

  // Required
  logInfo(s"- spark.shuffle.s3.rootDir=${rootDir} (appId: ${appId})")
  // Optional
  logInfo(s"- spark.shuffle.s3.useSparkShuffleFetch=${useSparkShuffleFetch}")
  logInfo(s"- spark.shuffle.s3.bufferSize=${bufferSize}")
  logInfo(s"- spark.shuffle.s3.maxBufferSizeTask=${maxBufferSizeTask}")
  logInfo(s"- spark.shuffle.s3.maxConcurrencyTask=${maxConcurrencyTask}")
  logInfo(s"- spark.shuffle.s3.cachePartitionLengths=${cachePartitionLengths}")
  logInfo(s"- spark.shuffle.s3.cacheChecksums=${cacheChecksums}")
  logInfo(s"- spark.shuffle.s3.cleanup=${cleanupShuffleFiles}")
  logInfo(s"- spark.shuffle.s3.folderPrefixes=${folderPrefixes}")

  // Debug
  logInfo(s"- spark.shuffle.s3.alwaysCreateIndex=${alwaysCreateIndex} (default: false)")
  logInfo(s"- spark.shuffle.s3.useBlockManager=${useBlockManager} (default: true)")
  logInfo(s"- spark.shuffle.s3.forceBatchFetch=${forceBatchFetch} (default: false)")

  // Spark
  logInfo(s"- ${config.SHUFFLE_CHECKSUM_ALGORITHM.key}=${checksumAlgorithm}")
  logInfo(s"- ${config.SHUFFLE_CHECKSUM_ENABLED.key}=${checksumEnabled}")

  private def getHadoopConfForFileSystem(prefix: String): Configuration = {
    val hadoopConf = SparkHadoopUtil.newConfiguration(conf)
    conf.getAllWithPrefix(prefix).foreach { case (key, value) =>
      logInfo(s"setting ${key}=${value}")
      hadoopConf.set(key, value)
    }
    hadoopConf
  }

  def removeRoot(): Boolean = {
    Range(0, folderPrefixes)
      .map(idx => {
        Future {
          val prefix = f"${rootDir}${prefixKeys(idx)}/${appPrefix}/${appId}"
          try {
            fs.delete(new Path(prefix), true)
          } catch {
            case _: IOException => logDebug(s"Unable to delete prefix ${prefix}")
          }
        }
      })
      .map(Await.result(_, Duration.Inf))
    true
  }

  def getPath(blockId: BlockId): Path = {
    val (shuffleId, mapId) = blockId match {
      case ShuffleBlockId(shuffleId, mapId, _) =>
        (shuffleId, mapId)
      case ShuffleDataBlockId(shuffleId, mapId, _) =>
        (shuffleId, mapId)
      case ShuffleIndexBlockId(shuffleId, mapId, _) =>
        (shuffleId, mapId)
      case ShuffleChecksumBlockId(shuffleId, mapId, _) =>
        (shuffleId, mapId)
      case _ => (0, 0.toLong)
    }
    val idx = mapId % folderPrefixes
    new Path(f"${rootDir}${prefixKeys(idx.toInt)}/${appPrefix}/${appId}/${shuffleId}/${blockId.name}")
  }

  def getSubPath(blockId: BlockId): String = {
    val shuffleId = blockId match {
      case ShuffleBlockId(shuffleId, _, _) =>
        shuffleId
      case ShuffleDataBlockId(shuffleId, _, _) =>
        shuffleId
      case ShuffleIndexBlockId(shuffleId, _, _) =>
        shuffleId
      case ShuffleChecksumBlockId(shuffleId, _, _) =>
        shuffleId
      case _ => 0
    }
    f"${appPrefix}/${appId}/${shuffleId}/${blockId.name}"
  }

  def listShuffleIndices(shuffleId: Int): Array[ShuffleIndexBlockId] = {
    if (useSparkShuffleFetch) {
      throw new SparkException("Not supported.")
    }
    val shuffleIndexFilter: PathFilter = new PathFilter() {
      override def accept(path: Path): Boolean = {
        val name = path.getName
        name.endsWith(".index")
      }
    }
    Range(0, folderPrefixes)
      .map(idx => {
        Future {
          val path = new Path(f"${rootDir}${prefixKeys(idx)}/${appPrefix}/${appId}/${shuffleId}/")
          try {
            fs.listStatus(path, shuffleIndexFilter)
              .map(v => {
                BlockId.apply(v.getPath.getName).asInstanceOf[ShuffleIndexBlockId]
              })
          } catch {
            case _: IOException => Array.empty[ShuffleIndexBlockId]
          }
        }
      })
      .flatMap(Await.result(_, Duration.Inf))
      .toArray
  }

  def removeShuffle(shuffleId: Int): Unit = {
    Range(0, folderPrefixes)
      .map(idx => {
        val path = new Path(f"${rootDir}${prefixKeys(idx)}/${appPrefix}/${appId}/${shuffleId}/")
        Future {
          fs.delete(path, true)
        }
      })
      .foreach(Await.result(_, Duration.Inf))
  }

  /** Open a block for reading.
    *
    * @param blockId
    * @return
    */
  def openBlock(blockId: BlockId): FSDataInputStream = {
    val status = getFileStatusCached(blockId)
    val builder = fs.openFile(status.getPath).withFileStatus(status)
    val stream = builder.build().get()
    if (canSetReadahead) {
      stream.setReadahead(0)
    }
    stream
  }

  private val cachedFileStatus = new ConcurrentObjectMap[BlockId, FileStatus]()

  def getFileStatusCached(blockId: BlockId): FileStatus = {
    cachedFileStatus.getOrElsePut(
      blockId,
      (value: BlockId) => {
        fs.getFileStatus(getPath(value))
      }
    )
  }

  def closeCachedBlocks(shuffleIndex: Int): Unit = {
    val filter = (blockId: BlockId) =>
      blockId match {
        case RDDBlockId(_, _)                              => false
        case ShuffleBlockId(shuffleId, _, _)               => shuffleId == shuffleIndex
        case ShuffleBlockBatchId(shuffleId, _, _, _)       => shuffleId == shuffleIndex
        case ShuffleBlockChunkId(shuffleId, _, _, _)       => shuffleId == shuffleIndex
        case ShuffleDataBlockId(shuffleId, _, _)           => shuffleId == shuffleIndex
        case ShuffleIndexBlockId(shuffleId, _, _)          => shuffleId == shuffleIndex
        case ShuffleChecksumBlockId(shuffleId, _, _)       => shuffleId == shuffleIndex
        case ShufflePushBlockId(shuffleId, _, _, _)        => shuffleId == shuffleIndex
        case ShuffleMergedBlockId(shuffleId, _, _)         => shuffleId == shuffleIndex
        case ShuffleMergedDataBlockId(_, shuffleId, _, _)  => shuffleId == shuffleIndex
        case ShuffleMergedIndexBlockId(_, shuffleId, _, _) => shuffleId == shuffleIndex
        case ShuffleMergedMetaBlockId(_, shuffleId, _, _)  => shuffleId == shuffleIndex
        case BroadcastBlockId(_, _)                        => false
        case TaskResultBlockId(_)                          => false
        case StreamBlockId(_, _)                           => false
        case TempLocalBlockId(_)                           => false
        case TempShuffleBlockId(_)                         => false
        case TestBlockId(_)                                => false
        case _                                             => false
      }
    cachedFileStatus.remove(filter, _)
  }

  /** Open a block for writing.
    *
    * @param blockId
    * @return
    */
  def createBlock(blockId: BlockId): FSDataOutputStream = {
    fs.create(getPath(blockId))
  }
}

object S3ShuffleDispatcher extends Logging {
  private val lock = new Object()
  private var store: S3ShuffleDispatcher = null

  def get: S3ShuffleDispatcher = {
    if (store == null) {
      lock.synchronized({
        if (store == null) {
          store = new S3ShuffleDispatcher()
        }
      })
    }
    store
  }

}
