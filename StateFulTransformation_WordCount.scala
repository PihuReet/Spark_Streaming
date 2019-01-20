import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.Seconds
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.SparkConf

object StateFulTransformation_WordCount {
    def main(args: Array [String]) {
      val master = "local[2]"
      val cfg = new SparkConf().setAppName("StreamingSample_StatefulTransformation").setMaster(master)
      
      val sc = new SparkContext(cfg)
      
      val ssc = new StreamingContext(sc, Seconds(10))
      
      val lines = ssc.socketTextStream("localhost", 9999)
      
      ssc.checkpoint("./checkpoints/")  
      
      val words = lines.flatMap(_.split(" "))
      
      val pairs = words.map(word => (word,1))
      
      val windowedWordCounts = pairs.updateStateByKey(updateFunc)
     
      windowedWordCounts.saveAsTextFiles("result/result")
      
      ssc.start()
      ssc.awaitTermination()
      
   
    }
    
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
            val currentCount = values.foldLeft(0)(_ + _)
            val previousCount = state.getOrElse(0)
            Some(currentCount + previousCount)
      }  
  }