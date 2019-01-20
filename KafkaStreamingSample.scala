import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.Seconds
import org.apache.spark.SparkContext
import java.util.concurrent._
import java.util.{Collections, Properties}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import scala.collection.JavaConversions._
import kafka.utils.Logging  
import org.apache.spark.SparkConf


object KafkaStreamingSample {
    def main(args:Array[String])
  {
    val master = "local[2]"
    val cfg = new SparkConf().setAppName("KafkaStreamSample").setMaster(master)
   
    val ssc = new StreamingContext(cfg,Seconds(10))
    
    ssc.checkpoint("./checkpoints/")
    //val lines = KafkaUtils.(ssc, locationStrategy, consumerStrategy, perPartitionConfig) (ssc, "localhost:2181", Map("customer" -> 5))
    val lines = KafkaUtils.createStream(ssc, "localhost:2181", "spark-streaming-consumer-group", Map("customer" -> 5))
    lines.print()
    
    ssc.start()
    ssc.awaitTermination()
    
   
   }
}