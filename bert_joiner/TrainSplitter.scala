package plista.ml

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import plista.ml.worker.Worker

object TrainSplitter extends Worker {

  override def initSparkConf: SparkConf = {
    new SparkConf()
      .set("spark.eventLog.enabled", "false")     // Remove logs for spark UI to avoid disk memory error
      .setIfMissing("spark.master", "local[8]")
      .setIfMissing("spark.driver.maxResultSize", "32g")
      .setIfMissing("spark.driver.memory", "32g")
      .setIfMissing("spark.app.name", "train-split")
      .setIfMissing("spark.executor.heartbeatInterval", "119")
  }

  override protected def work()(implicit session: SparkSession): Unit = {
    import session.implicits._

    implicit val fs: FileSystem = FileSystem.get(session.sparkContext.hadoopConfiguration)

    val workingDirectory = "data/"

    val inFileName = "resolved"
    val outFileName = "split_"

    val usersWithBertVectors: Dataset[Seq[Seq[Double]]] = session.read.json(workingDirectory + inFileName)
      .map { row => row.getAs[Seq[Seq[Double]]]("value") }

    val seqLength = 16
    val internalSeqLength = seqLength + 2

    // shape is like [x1, .., xn + 2], where n is seqlength
    val preparedForTraining = usersWithBertVectors
      .repartition(8)
      .flatMap { unpaddedHistory =>
        // get the n + 2 most recent elements – we need to be able to stride + 2 (see internal seqlength)
        val clipped = unpaddedHistory.takeRight(internalSeqLength)

        val reversePaddedHistory = clipped
          .reverse // target head, train tail
          .dropWhile(_.equals(bertPaddingSeq)) // we need to remove from the left until we've found a valid target
          .padTo(internalSeqLength, bertPaddingSeq) // and pad to the right until the desired length is reached

        val paddedHistory = reversePaddedHistory.reverse

        val nonPaddingItems = reversePaddedHistory.count(!_.equals(bertPaddingSeq))

        Some(paddedHistory)
          .filter(_ =>
            // we need at least one train example and the target (also sanity check)
            nonPaddingItems > 1 && paddedHistory.length == internalSeqLength
          )
      }

    preparedForTraining
      .randomSplit(Array(0.9, 0.1))
      .zip(Seq("train", "validation"))
      .foreach { case (data, name) =>
        data
          .coalesce(1)
          .toDF()
          .write
          .mode(SaveMode.Overwrite)
          .json(workingDirectory + outFileName + name)
      }
  }
}
