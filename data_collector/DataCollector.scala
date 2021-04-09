package plista.ml

import com.plista.thrift.converter.SlimEventContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import plista.ml.resolver.UserResolver
import plista.ml.worker.streamworker.SlimContextStreamWorker

import scala.language.postfixOps

object DataCollector extends SlimContextStreamWorker {
  val workingDirectory = "../thesis_item-bert-joiner/data/collected"

  override def initSparkConf: SparkConf = {
    new SparkConf()
      .set("spark.eventLog.enabled", "false")     // Remove logs for spark UI to avoid disk memory error
      .setIfMissing("spark.master", "local[8]")
      .setIfMissing("spark.driver.maxResultSize", "32g")
      .setIfMissing("spark.driver.memory", "32g")
      .setIfMissing("spark.app.name", "data-collector")
      .setIfMissing("spark.executor.heartbeatInterval", "119")
  }

  override def processSlimContexts: Dataset[SlimEventContext] => Unit = {
    slimEventContexts => {
      import slimEventContexts.sparkSession.implicits._

      val usersWithHistory = slimEventContexts
        .repartition(8)
        .flatMap(extractPossibleHistory)
        .filter(_._2.size > 2)

      usersWithHistory
        .coalesce(1)
        .write
        .mode(SaveMode.Append)
        .json(workingDirectory)
    }
  }

  def extractPossibleHistory(slimEventContext: SlimEventContext): Option[(Seq[Long], Seq[Long])] = {
    possibleHistoryOf(slimEventContext).map { articleHistory =>
      UserResolver(slimEventContext) -> articleHistory.toSeq.map(_.toLong)
    }
  }

  def possibleHistoryOf(slimEventContext: SlimEventContext): Option[Array[Int]] =
    slimEventContext.environment.lists.get(145)
}
