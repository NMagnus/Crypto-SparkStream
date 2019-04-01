import com.mongodb.spark.sql._
import net.liftweb.json._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.SparkSession

import scala.math.{max, min}


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

  case class TradeInfo(Coin:String,
                        AveragePrice:Double,
                      TotalQuantity:Double,
                       TradeCount:Int,
                       FirstTradeTime:Long,
                       LastTradeTime:Long)

  def parseTrades(line: String): Trades = {
    implicit val formats = DefaultFormats
    parse(line).extract[Trades]
  }

  val spark = SparkSession.builder.master("local[4]")
    .config("spark.driver.cores", 2)
    .appName("SparkCryptoStream")
    .config("spark.mongodb.output.uri","mongodb://127.0.0.1/binance.tradeInfo")
    .getOrCreate()

  val ssc = new StreamingContext(spark.sparkContext, Seconds(10))

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
    Subscribe[String, String](topics, kafkaParams)
  )


  stream.map(record => (record.value))
    .filter(record => !record.isEmpty)
    .map(record => parseTrades(record))
    .map(record => (record.s, (record.p.toDouble*record.q.toDouble, record.q.toDouble, 1, record.T.toLong, record.T.toLong)))
    .reduceByKeyAndWindow((x,y) => (x._1+y._1,x._2+y._2, x._3+y._3, min(x._4,y._4), max(x._4,y._4)), Seconds(10))
    .map(x => (x._1,(x._2._1 / x._2._2, x._2._2, x._2._3,x._2._4,x._2._5))) //Last 10Seconds (Average Price, Total Quantity, Trade Count, FirstTradeTime, LastTradeTime)
      .foreachRDD({  rdd =>
        import spark.implicits._
        val tradeinfos = rdd.map(trade => TradeInfo(trade._1,trade._2._1,trade._2._2,trade._2._3,trade._2._4,trade._2._5)).toDF()
    tradeinfos.write.mode("append").mongo()
  })
    //.saveAsTextFiles("/home/nic/Documents/StreamData/binance")

  ssc.start()
  ssc.awaitTermination()


}
