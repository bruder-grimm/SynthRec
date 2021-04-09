package plista.ml

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import plista.ml.resolver.BertResolver
import plista.ml.worker.Worker

object LookupTableCreator extends Worker {

  override def initSparkConf: SparkConf = {
    new SparkConf()
      .set("spark.eventLog.enabled", "false") // Remove logs for spark UI to avoid disk memory error
      .setIfMissing("spark.master", "local[8]")
      .setIfMissing("spark.driver.maxResultSize", "32g")
      .setIfMissing("spark.driver.memory", "32g")
      .setIfMissing("spark.app.name", "bert-item-joiner")
      .setIfMissing("spark.executor.heartbeatInterval", "119")
  }

  override protected def work()(implicit session: SparkSession): Unit = {
    import session.implicits._

    implicit val fs: FileSystem = FileSystem.get(session.sparkContext.hadoopConfiguration)

    val workingDirectory = "data/"

    val inFileName = "collected"
    val outFileName = "lookup"

    val articleIds = session.read
      .json(workingDirectory + inFileName)
      .flatMap(_.getAs[Seq[Long]]("_2"))
      .distinct()

    BertResolver
      .getBertVectorsFor(articleIds)
      .filter(_._2.nonEmpty)
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .json(workingDirectory + outFileName)
  }
}
