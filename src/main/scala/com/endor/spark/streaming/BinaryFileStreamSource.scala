package com.endor.spark.streaming

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.InMemoryFileIndex
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}

abstract class BinaryFileStreamSource[S: Encoder](sparkSession: SparkSession,
                                                  path: String,
                                                  metadataPath: String,
                                                  options: Map[String, String]) extends Source with Logging {
  import FileStreamSource._

  private val sourceOptions = new FileStreamOptions(options)

  private val hadoopConf = sparkSession.sessionState.newHadoopConf()

  private val qualifiedBasePath: Path = {
    val fs = new Path(path).getFileSystem(hadoopConf)
    fs.makeQualified(new Path(path))  // can contains glob patterns
  }

  private val metadataLog =
    new FileStreamSourceLog(FileStreamSourceLog.VERSION, sparkSession, metadataPath)

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var metadataLogCurrentOffset = metadataLog.getLatest().map(_._1).getOrElse(-1L)

  /** Maximum number of new files to be considered in each batch */
  private val maxFilesPerBatch = sourceOptions.maxFilesPerTrigger

  private val fileSortOrder = if (sourceOptions.latestFirst) {
    logWarning(
      """'latestFirst' is true. New files will be processed first, which may affect the watermark
        |value. In addition, 'maxFileAge' will be ignored.""".stripMargin)
    implicitly[Ordering[Long]].reverse
  } else {
    implicitly[Ordering[Long]]
  }

  private val maxFileAgeMs: Long = if (sourceOptions.latestFirst && maxFilesPerBatch.isDefined) {
    Long.MaxValue
  } else {
    sourceOptions.maxFileAgeMs
  }

  private val fileNameOnly = sourceOptions.fileNameOnly
  if (fileNameOnly) {
    logWarning("'fileNameOnly' is enabled. Make sure your file names are unique (e.g. using " +
      "UUID), otherwise, files with the same name but under different paths will be considered " +
      "the same and causes data lost.")
  }

  /** A mapping from a file that we have processed to some timestamp it was last modified. */
  // Visible for testing and debugging in production.
  val seenFiles: SeenFilesMap = new SeenFilesMap(maxFileAgeMs, fileNameOnly)

  metadataLog.allFiles().foreach { entry =>
    seenFiles.add(entry.path, entry.timestamp)
  }
  seenFiles.purge()

  logInfo(s"maxFilesPerBatch = $maxFilesPerBatch, maxFileAgeMs = $maxFileAgeMs")

  /**
    * Returns the maximum offset that can be retrieved from the source.
    *
    * `synchronized` on this method is for solving race conditions in tests. In the normal usage,
    * there is no race here, so the cost of `synchronized` should be rare.
    */
  private def fetchMaxOffset(): FileStreamSourceOffset = synchronized {
    // All the new files found - ignore aged files and files that we have seen.
    val newFiles = fetchAllFiles().filter {
      case (curPath, timestamp) => seenFiles.isNewFile(curPath, timestamp)
    }

    // Obey user's setting to limit the number of files in this batch trigger.
    val batchFiles = maxFilesPerBatch.map(newFiles.take).getOrElse(newFiles)

    batchFiles.foreach { file =>
      seenFiles.add(file._1, file._2)
      logDebug(s"New file: $file")
    }
    val numPurged = seenFiles.purge()

    logTrace(
      s"""
         |Number of new files = ${newFiles.size}
         |Number of files selected for batch = ${batchFiles.size}
         |Number of seen files = ${seenFiles.size}
         |Number of files purged from tracking map = $numPurged
       """.stripMargin)

    if (batchFiles.nonEmpty) {
      metadataLogCurrentOffset += 1
      metadataLog.add(metadataLogCurrentOffset, batchFiles.map { case (p, timestamp) =>
        FileEntry(path = p, timestamp = timestamp, batchId = metadataLogCurrentOffset)
      }.toArray)
      logInfo(s"Log offset set to $metadataLogCurrentOffset with ${batchFiles.size} new files")
    }

    FileStreamSourceOffset(metadataLogCurrentOffset)
  }

  /**
    * For test only. Run `func` with the internal lock to make sure when `func` is running,
    * the current offset won't be changed and no new batch will be emitted.
    */
  def withBatchingLocked[T](func: => T): T = synchronized {
    func
  }

  def currentLogOffset: Long = synchronized { metadataLogCurrentOffset }

  /**
    * Returns the data that is between the offsets (`start`, `end`].
    */
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val startOffset = start.map(FileStreamSourceOffset(_).logOffset).getOrElse(-1L)
    val endOffset = FileStreamSourceOffset(end).logOffset

    assert(startOffset <= endOffset)
    val files = metadataLog.get(Option(startOffset + 1), Option(endOffset)).flatMap(_._2)
    logInfo(s"Processing ${files.length} files from ${startOffset + 1}:$endOffset")
    logTrace(s"Files are:\n\t" + files.mkString("\n\t"))
    createDataset(files.map(_.path)).toDF()
  }

  /**
    * If the source has a metadata log indicating which files should be read, then we should use it.
    * Only when user gives a non-glob path that will we figure out whether the source has some
    * metadata log
    *
    * None        means we don't know at the moment
    * Some(true)  means we know for sure the source DOES have metadata
    * Some(false) means we know for sure the source DOSE NOT have metadata
    */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  @volatile private[streaming] var sourceHasMetadata: Option[Boolean] =
  if (SparkHadoopUtil.get.isGlobPath(new Path(path))) Option(false) else None

  private def allFilesUsingInMemoryFileIndex() = {
    val globbedPaths = SparkHadoopUtil.get.globPathIfNecessary(qualifiedBasePath)
    val fileIndex = new InMemoryFileIndex(sparkSession, globbedPaths, options, Option(new StructType))
    fileIndex.allFiles()
  }

  private def allFilesUsingMetadataLogFileIndex() = {
    // Note if `sourceHasMetadata` holds, then `qualifiedBasePath` is guaranteed to be a
    // non-glob path
    new MetadataLogFileIndex(sparkSession, qualifiedBasePath).allFiles()
  }

  /**
    * Returns a list of files found, sorted by their timestamp.
    */
  private def fetchAllFiles(): Seq[(String, Long)] = {
    val startTime = System.nanoTime

    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    var allFiles: Seq[FileStatus] = Seq.empty
    sourceHasMetadata match {
      case None =>
        if (FileStreamSink.hasMetadata(Seq(path), hadoopConf)) {
          sourceHasMetadata = Option(true)
          allFiles = allFilesUsingMetadataLogFileIndex()
        } else {
          allFiles = allFilesUsingInMemoryFileIndex()
          if (allFiles.isEmpty) {
            // we still cannot decide
          } else {
            // decide what to use for future rounds
            // double check whether source has metadata, preventing the extreme corner case that
            // metadata log and data files are only generated after the previous
            // `FileStreamSink.hasMetadata` check
            if (FileStreamSink.hasMetadata(Seq(path), hadoopConf)) {
              sourceHasMetadata = Option(true)
              allFiles = allFilesUsingMetadataLogFileIndex()
            } else {
              sourceHasMetadata = Option(false)
              // `allFiles` have already been fetched using InMemoryFileIndex in this round
            }
          }
        }
      case Some(true) => allFiles = allFilesUsingMetadataLogFileIndex()
      case Some(false) => allFiles = allFilesUsingInMemoryFileIndex()
    }

    val files = allFiles.sortBy(_.getModificationTime)(fileSortOrder).map { status =>
      (status.getPath.toUri.toString, status.getModificationTime)
    }
    val endTime = System.nanoTime
    val listingTimeMs = (endTime.toDouble - startTime) / 1000000
    if (listingTimeMs > 2000) {
      // Output a warning when listing files uses more than 2 seconds.
      logWarning(s"Listed ${files.size} file(s) in $listingTimeMs ms")
    } else {
      logTrace(s"Listed ${files.size} file(s) in $listingTimeMs ms")
    }
    logTrace(s"Files are:\n\t" + files.mkString("\n\t"))
    files
  }

  override def getOffset: Option[Offset] = Option(fetchMaxOffset()).filterNot(_.logOffset == -1)

  override def toString: String = s"FileStreamSource[$qualifiedBasePath]"

  /**
    * Informs the source that Spark has completed processing all data for offsets less than or
    * equal to `end` and will only request offsets greater than `end` in the future.
    */
  override def commit(end: Offset): Unit = {
    // No-op for now; FileStreamSource currently garbage-collects files based on timestamp
    // and the value of the maxFileAge parameter.
  }

  override def stop(): Unit = {}

  override def schema: StructType = implicitly[Encoder[S]].schema

  protected def createDataset(files: Seq[String]): Dataset[S]
}