package plista.ml

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import plista.ml.resolver.BertResolver
import plista.ml.worker.Worker

import scala.language.postfixOps

object ItemBertJoiner extends Worker {

  override def initSparkConf: SparkConf = {
    new SparkConf()
      .set("spark.eventLog.enabled", "false")     // Remove logs for spark UI to avoid disk memory error
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
    val outFileName = "resolved"

    val usersWithHistories = session.read
      .json(workingDirectory + inFileName)
      .map { row =>
        row.getAs[Seq[Long]]("_1") -> row.getAs[Seq[Long]]("_2")
      }
      .distinct() // we only want distinct histories

    // load all bert vectors we need into memory, set xmx=8g or something
    val distinctHistoryItemIds = usersWithHistories
      .flatMap(_._2)
      .distinct()
    val resolvedBertVectorMap = BertResolver
      .getBertVectorsFor(distinctHistoryItemIds)
      .collect()
      .toMap

    // replace the article ids with bert vectors in
    val historiesAsBert = usersWithHistories
      .repartition(8)
      .map { case (_, history) => // we don't need the user tbh
        // Sooo this is the meat of it.
        // Getting a padding here means that we'll learn a padding. Even if not attended to, the padding must be
        // predicted. We can either create a stepped function that clips below a certain value and just replaces with 0 (pad),
        // but is that really desirable? Do we want to predict a padding element? I don't think so
        // if we ever change our mind, it is //.map { articleId => resolvedBertVectorMap.getOrElse(articleId, bertPaddingSeq) }
        history
          .flatMap { articleId =>
            resolvedBertVectorMap
              .get(articleId)
              .map { bertVector => articleId -> bertVector }
          }
          .filter(_._2.nonEmpty)
      }
      .filter(_.length > 2) // if after resolving someone is just not going to yield any trainable results (one train + one target) -> ditch
      .distinct() // we don't want any history twice. Set xmx to at least 32gb

    historiesAsBert
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .json(workingDirectory + outFileName)
  }
}
