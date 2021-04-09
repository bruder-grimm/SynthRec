package plista.ml.resolver

import org.apache.spark.sql.Dataset
import plista.ml.db.AvailableFeature.fromJsonSeq
import plista.ml.db.AvailableFeatures.bert
import plista.ml.db.service.CassandraFeatureService

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

class BertResolver extends CassandraFeatureService with Serializable {

  def apply(items: Seq[Long]): Future[Seq[Seq[Double]]] = {
    if (items.isEmpty) {
      Future.successful(Seq.empty)
    } else {

      BertService(items).map { bertRecords =>
        bertRecords.map { bertRecord =>
          bertRecord.features
            .get(bert)
            .map(fromJsonSeq)
            .getOrElse(Seq.empty)
        }
      }
    }
  }
}

object BertResolver {
  def getBertVectorsFor(completeHistory: Dataset[Long]): Dataset[(Long, Seq[Double])] = {
    import completeHistory.sparkSession.implicits._

    import scala.concurrent.ExecutionContext.Implicits.global

    val bertResolver = new BertResolver()

    completeHistory
      .mapPartitions { items =>
        val itemIds = items.toSeq
        val resolved = bertResolver(itemIds).map { resolvedBertVectors =>
          itemIds zip resolvedBertVectors
        }
        Await.result(resolved, 5 minutes).toIterator
      }
  }
}
