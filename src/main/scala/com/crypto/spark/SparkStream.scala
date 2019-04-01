import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.{Seconds, StreamingContext}
import net.liftweb.json._


object SparkStream extends App {

  case class Trades(e: String,
                    E: String,
                    s: String,
                    t: String,
                    p: String,
                    q: String,
                    b: String,
                    a: String,
                    T: String,
                    m: Boolean,
                    M: Boolean)

  def parseTrades(line: String):Trades = {
    implicit val formats = DefaultFormats
    parse(line).extract[Trades]
    }


  val conf = new SparkConf().setMaster("local[4]").setAppName("SparkCryptoStream")
  val ssc = new StreamingContext(conf, Seconds(5))

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "use_a_seperate_group_id_for_each_stream",
    "auto.offset.reset" -> "earliest",
    "enable.autocommit" -> (false: java.lang.Boolean)
  )

  val topics = Array("testing")
  val stream = KafkaUtils.createDirectStream[String, String](
    ssc,
    PreferConsistent,
    Subscribe[String,String](topics, kafkaParams)
  )

  stream.map(record => (record.value))
    .filter(record => !record.isEmpty)
    .map(record => parseTrades(record))
    .map(record => (record.s, (record.p.toDouble, record.q.toDouble, 1)))
    .reduceByKeyAndWindow((x,y) => (x._1+y._1,x._2+y._2, x._3+y._3), Seconds(60))
    .map(x => (x._1,(x._2._1 / x._2._2, x._2._2, x._2._3))) //Last 10Seconds (Average Price, Total Quantity, Trade Count)
    .saveAsTextFiles("/home/nic/Documents/StreamData/binance")

  ssc.start()
  ssc.awaitTermination()


}
