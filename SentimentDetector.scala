import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import kafka.utils.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{explode, split}
import java.util.{Collections, Properties}
import java.util.concurrent._
import kafka.consumer.KafkaStream
import scala.collection.JavaConversions._
import java.io._
import java.net._
import java.util._
import javax.net.ssl.HttpsURLConnection
import com.google.gson.Gson
import com.google.gson.GsonBuilder
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import scala.util.parsing.json._
import org.apache.spark.sql.kafka010

class SentimentDetector {
  val brokers = "localhost:9092"
  val props = createConsumerConfig(brokers, "ScalaProducerExample")
  val consumer = new KafkaConsumer[String, String](props)
  var executor: ExecutorService = null
  val topicName = "azurefeedback"
 
  def createConsumerConfig(brokers: String, groupId: String): Properties = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props
  }

  def run() = {
    consumer.subscribe(Collections.singletonList(topicName))
    Executors.newSingleThreadExecutor.execute(new Runnable {
      override def run(): Unit = {
       
        var negative = 0
        var positive = 0
        var good = 0
        var ok = 0
        var bad = 0
        
        
        while (true) {
          val records = consumer.poll(100)

          var doubleSentVal: Double = 0.0
          
          for (record <- records) {
            //System.out.println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset())
            //
            
            // <= 0.3 Negative ,  0.4 Bad  ,  0.5 OK  , 0.6 Good    , 0.7 Positive
            
             doubleSentVal = toSentiment(record.key(),record.value())
             
             if (doubleSentVal < 0.4)
               negative += 1
             else if (doubleSentVal >= 0.4 && doubleSentVal < 0.5)
               bad += 1
             else if (doubleSentVal >= 0.5 && doubleSentVal < 0.6)
               ok += 1
             else if (doubleSentVal >= 0.6 && doubleSentVal < 0.7)
               good += 1
             else if (doubleSentVal >= 0.7)
               positive += 1
               
             //System.out.println("Sentiment: " + toSentiment(record.key(),record.value()))
              
               
              System.out.println("Positive :" + positive)
              System.out.println("Good :" + good)
              System.out.println("OK :" + ok)
              System.out.println("Bad :" + bad)
              System.out.println("Negative :" + negative)
              
              System.out.println("=====================")
              
              System.out.flush()
              
              //System.out.println("Sentiment: " + toSentiment(record.key(),record.value()))
            // Preparing a dataframe with Content and Sentiment columns
            //    val streamingDataFrame = kafka.selectExpr("cast (value as string) AS Content").withColumn("Sentiment", toSentiment(record.value()))
            // Displaying the streaming data
            //  streamingDataFrame.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()
          }
        }
      }
    })
  }
 
  def toSentiment( id:String, content:String) = {
      val inputDocs = new Documents()
      inputDocs.add (id, content)
      
      val objSentimentDetectorSample = new SentimentDetectorSample()
      
      val docsWithLanguage = objSentimentDetectorSample.getLanguage(inputDocs)
      val docsWithSentiment = objSentimentDetectorSample.getSentiment(docsWithLanguage)
      if (docsWithLanguage.documents.isEmpty) {
      // Placeholder value to display for no score returned by the sentiment API
        (-1).toDouble
      } else {
              docsWithSentiment.documents.get(0).sentiment.toDouble
      }
  }
  
  
}



class Document(var id: String, var text: String, var language: String = "", var sentiment: Double = 0.0) extends Serializable 

class Documents(var documents: List[Document] = new ArrayList[Document]()) extends Serializable {
 
    def add(id: String, text: String, language: String = "") {
        documents.add (new Document(id, text,  language))
    }
    def add(doc: Document) {
        documents.add (doc)
    }
}
 
class CC[T] extends Serializable { def unapply(a:Any):Option[T] = Some(a.asInstanceOf[T]) }
object M extends CC[scala.collection.immutable.Map[String, Any]]
object L extends CC[scala.collection.immutable.List[Any]]
object S extends CC[String]
object D extends CC[Double]


class SentimentDetectorSample extends Serializable {
  val accessKey = "----------"
  val host = "-------------"
  val languagesPath = "/text/analytics/v2.0/languages" ///text/analytics/v2.0/languages
  val sentimentPath = "/text/analytics/v2.0/sentiment" ///text/analytics/v2.0/sentiment"
  val languagesUrl = new URL(host+languagesPath)
  val sentimenUrl = new URL(host+sentimentPath)
  
  def getConnection(path: URL): HttpsURLConnection = {
    val connection = path.openConnection().asInstanceOf[HttpsURLConnection]
    connection.setRequestMethod("POST")
    connection.setRequestProperty("Content-Type", "text/json")
    connection.setRequestProperty("Ocp-Apim-Subscription-Key", accessKey)
    connection.setDoOutput(true)
    return connection
  }
  
   def prettify (json_text: String): String = {
    val parser = new JsonParser()
    val json = parser.parse(json_text).getAsJsonObject()
    val gson = new GsonBuilder().setPrettyPrinting().create()
    return gson.toJson(json)
  }
  
  // Handles the call to Cognitive Services API.
  // Expect Documents as parameters and the address of the API to call.
  // Returns an instance of Documents in response.
  def processUsingApi(inputDocs: Documents, path: URL): String = {
    val docText = new Gson().toJson(inputDocs)
    val encoded_text = docText.getBytes("UTF-8")
    val connection = getConnection(path)
    val wr = new DataOutputStream(connection.getOutputStream())
    wr.write(encoded_text, 0, encoded_text.length)
    wr.flush()
    wr.close()
 
    val response = new StringBuilder()
    val in = new BufferedReader(new InputStreamReader(connection.getInputStream()))
    var line = in.readLine()
    while (line != null) {
        response.append(line)
        line = in.readLine()
    }
    in.close()
    return response.toString()
  }
  
   // Calls the language API for specified documents.
  // Returns a documents with language field set.
  def getLanguage (inputDocs: Documents): Documents = { 
    try {
      val response = processUsingApi(inputDocs, languagesUrl)
      // In case we need to log the json response somewhere
      val niceResponse = prettify(response)
      val docs = new Documents()
      val result = for {
            // Deserializing the JSON response from the API into Scala types
            Some(M(map)) <- scala.collection.immutable.List(JSON.parseFull(niceResponse))
            L(documents) = map("documents")
            M(document) <- documents
            S(id) = document("id")
            L(detectedLanguages) = document("detectedLanguages")
            M(detectedLanguage) <- detectedLanguages
            S(language) = detectedLanguage("iso6391Name")
      } yield {
            docs.add(new Document(id = id, text = id, language = language ))
      }
      return docs
    } catch {
          case e: Exception => return new Documents()
    }
  }
  // Calls the sentiment API for specified documents. Needs a language field to be set for each of them.
  // Returns documents with sentiment field set, taking a value in the range from 0 to 1.
  def getSentiment (inputDocs: Documents): Documents = {
    try {
      val response = processUsingApi(inputDocs, sentimenUrl)
      val niceResponse = prettify(response)
      val docs = new Documents()
      val result = for {
            // Deserializing the JSON response from the API into Scala types
            Some(M(map)) <- scala.collection.immutable.List(JSON.parseFull(niceResponse))
            L(documents) = map("documents")
            M(document) <- documents
            S(id) = document("id")
            D(sentiment) = document("score")
      } yield {
            docs.add(new Document(id = id, text = id, sentiment = sentiment))
      }
      return docs
    } catch {
        case e: Exception => return new Documents()
    }
  }
  
}

object SentimentDetector extends App {
  val example = new SentimentDetector()
  example.run()

}
