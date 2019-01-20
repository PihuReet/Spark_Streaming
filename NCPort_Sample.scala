
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.Seconds
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.SparkConf

object NCPort_Sample {
  def main(args:Array[String])
  {
    val master = "local[2]"
    val cfg = new SparkConf().setAppName("StreamingSample").setMaster(master)
    
    val ssc = new StreamingContext(cfg,Seconds(10))
    val lines = ssc.socketTextStream("localhost", 7777)
    
    val errorlines = lines.filter(_.contains("error"))
    
    errorlines.print()
    
    ssc.start()
    ssc.awaitTermination()
  }
}