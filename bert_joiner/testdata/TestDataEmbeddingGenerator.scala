package plista.ml.testdata

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import plista.ml.worker.Worker

import scala.util.Random

/**
 * This has been reworked so that Trainsplit can be used on this
 * – it will generate user histories like ItemBertJoiner
 */
object TestDataEmbeddingGenerator extends Worker {
  override def initSparkConf: SparkConf = {
    new SparkConf()
      .set("spark.eventLog.enabled", "false")     // Remove logs for spark UI to avoid disk memory error
      .setIfMissing("spark.master", "local[8]")
      .setIfMissing("spark.driver.maxResultSize", "32g")
      .setIfMissing("spark.driver.memory", "32g")
      .setIfMissing("spark.app.name", "item-generator")
      .setIfMissing("spark.executor.heartbeatInterval", "119")
  }

  val testDataLength = 1000
  val seqLength = 18

  val withNoise = true
  val scaled = false

  val workingDirectory = "data/generated/embedding/"
  val trainTestSet = "train-test"
  val validationSet = "validation"

  def withOptionalNoiseAndScaling(input: Double): Double = {
    def noise(): Double = if (withNoise) { Random.nextGaussian() } else 0
    val scale: Double = if (scaled) seqLength else 1

    (input.toDouble + noise()) / scale
  }

  override protected def work()(implicit session: SparkSession): Unit = {
    implicit val fs: FileSystem = FileSystem.get(session.sparkContext.hadoopConfiguration)

    generateTrainingSet(session)
    generateValidationSet(session)
  }

  def getRandomUser: Seq[Long] =
    Seq.fill(5)(Random.nextLong())

  def generateTrainingSet(implicit session: SparkSession): Unit = {
    def getTrainingSeq: Seq[Seq[Double]] = {
      (0 until seqLength).map { position =>
        if(position % 2 != 0) // demonstrate attention
          (1 to 768).map { channel =>
            val s = if (channel % 3 == 0) 3 else seqLength
            withOptionalNoiseAndScaling(seqLength - position) / s
          } // more sophisticated training examples
        else Seq.fill(768) { withOptionalNoiseAndScaling(position) / seqLength }
      }
    }

    import session.implicits._
    val preparedForTraining = Seq
      .fill(testDataLength)(getTrainingSeq)
      .toDS()
      .repartition()

    preparedForTraining
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .json(workingDirectory + trainTestSet)
  }

  def generateValidationSet(implicit session: SparkSession): Unit = {
    def getValidationSeq: Seq[Seq[Double]] = {
      (0 until seqLength).map { position =>
        if(position % 2 == 0) // demonstrate attention
          (1 to 768).map { channel =>
            val s = if (channel % 4 == 0) 3 else seqLength
            withOptionalNoiseAndScaling(seqLength - position) / s
          }
        else Seq.fill(768) { withOptionalNoiseAndScaling(position) }
      }
    }

    import session.implicits._
    val preparedForValidation = Seq
      .fill(testDataLength / 10)(getValidationSeq)
      .toDS()
      .repartition()

    preparedForValidation
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .json(workingDirectory + validationSet)
  }
}
