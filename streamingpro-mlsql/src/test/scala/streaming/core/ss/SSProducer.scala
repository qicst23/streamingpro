package streaming.core.ss

import java.sql.Timestamp

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.StreamingQuery
import streaming.log.Logging

/**
  * Created by allwefantasy on 17/5/2018.
  */
class SSProducer extends Logging {
  def asyncProduce(inputStream: MemoryStream[(Timestamp, Int)],
                   query: StreamingQuery,
                   now: Long = 5000L) = {
    new Thread(new Runnable() {
      override def run(): Unit = {
        // Events sent before - they should be correctly deduplicated, i.e. (5000, 1), (5000, 2) and (10000, 2)
        // should be taken
        // As you can observe, the deduplication occurs with the pair (event time, value)
        inputStream.addData((new Timestamp(now), 1), (new Timestamp(now), 2),
          (new Timestamp(now), 1), (new Timestamp(now + 5000), 2))
        while (!query.isActive) {
          // wait the query to activate
        }
        logInfo(s"Query was activated, sleep for 9 seconds before sending new data. Current timestamp " +
          s"is ${System.currentTimeMillis()}")
        Thread.sleep(11000)
        logInfo(s"Awaken at ${System.currentTimeMillis()} where the query status is ${query.lastProgress.json}")
        // In the logs we can observe the following entry:
        // ```
        // Filtering state store on: (created#5-T6000ms <= 4000000)
        // (org.apache.spark.sql.execution.streaming.StreamingDeduplicateExec:54)
        // ```
        // As you can correctly deduce, among the entries below:
        // - 1, 2 and 3 will be filtered
        // - 4 will be accepted
        // Moreover, the 4 will be used to compute the new watermark. Later in the logs we can observe the following:
        // ```
        // Filtering state store on: (created#5-T6000ms <= 6000000)
        // (org.apache.spark.sql.execution.streaming.StreamingDeduplicateExec:54)
        // ```
        inputStream.addData((new Timestamp(now), 1), (new Timestamp(now - 1000), 2),
          (new Timestamp(now - 3000), 3), (new Timestamp(now + 7000), 4))
        Thread.sleep(9000)
        // Here the value 1 is after the watermark so automatically discarded
        //InMemoryKeyedStore.addValue(testKeyLastProgress, query.lastProgress.json)
        inputStream.addData((new Timestamp(now), 1))
        Thread.sleep(7000)
        inputStream.addData((new Timestamp(now), 1))
        //InMemoryKeyedStore.addValue(testKeyLastProgress, query.lastProgress.json)
      }
    }).start()
  }
}
