import java.util.Properties
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.util._
import scala.collection.JavaConverters._
import twitter4j._
import twitter4j.TwitterFactory
import twitter4j.Twitter
import twitter4j.conf.ConfigurationBuilder
import kafka.utils.Logging
object AzureProducer {
  def main(args:Array[String]) {    
    val kafkaBrokers = "localhost:9092" //,10.0.0.15:9092,10.0.0.12:9092
    val topicName = "azurefeedback"
    val props = new Properties()
    props.put("bootstrap.servers", kafkaBrokers)
    props.put("client.id", "ScalaProducerExample")
    props.put("acks", "all")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    def sendEvent(message: String) = {
      val key = java.util.UUID.randomUUID().toString()
      producer.send(new ProducerRecord[String, String](topicName, key, message)) 
      System.out.println("Sent event with key: '" + key + "' and message: '" + message + "'\n")
    }
    val twitterConsumerKey="--------"
    val twitterConsumerSecret="------"
    val twitterOauthAccessToken="-------"
    val twitterOauthTokenSecret="--------"
    val cb = new ConfigurationBuilder()
    cb.setDebugEnabled(true)
    cb.setOAuthConsumerKey(twitterConsumerKey)
    cb.setOAuthConsumerSecret(twitterConsumerSecret)
    cb.setOAuthAccessToken(twitterOauthAccessToken)
    cb.setOAuthAccessTokenSecret(twitterOauthTokenSecret)
    val twitterFactory = new TwitterFactory(cb.build())
    val twitter = twitterFactory.getInstance()
    val query = new Query(" #topicName ")
  
    query.setCount(1000)
    query.lang("en")
    var finished = false
    while (!finished) {
      val result = twitter.search(query) 
      val statuses = result.getTweets()
      var lowestStatusId = Long.MaxValue
      for (status <- statuses.asScala) {
        if(!status.isRetweet()){ 
          sendEvent(status.getText())
          Thread.sleep(4000)
        }
        lowestStatusId = Math.min(status.getId(), lowestStatusId)
      }
      query.setMaxId(lowestStatusId - 1)
    }
    System.out.println("Done!!!")
  }
}
