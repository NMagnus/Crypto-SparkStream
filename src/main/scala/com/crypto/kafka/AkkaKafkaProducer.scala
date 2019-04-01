package com.crypto.kafka

import java.util.Properties

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.ws._
import akka.kafka.{ProducerMessage, ProducerSettings}
import akka.kafka.ProducerMessage.MultiResultPart
import akka.kafka.scaladsl.Producer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.concurrent.Promise

object AkkaKafkaProducer {

  val binanceURL = "wss://stream.binance.com:9443/ws/bnbbtc@trade"
  val topic = "testing"

  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("client.id", topic)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    import system.dispatcher

    val producer = new KafkaProducer[String, String](props)


    val flow: Flow[Message, Message, Promise[Option[Message]]] =
      Flow.fromSinkAndSourceMat(
        Sink.foreach[Message](
          values => {
            println(values.asTextMessage.getStrictText)
            producer.send(new ProducerRecord[String, String](topic, values.asTextMessage.getStrictText))
          }
        ),
        Source.maybe[Message])(Keep.right)

    val (binanceResponse, binancepromise) =
      Http().singleWebSocketRequest(WebSocketRequest(binanceURL), flow)

    val connected = binanceResponse.map { response =>
      if (response.response.status == StatusCodes.SwitchingProtocols) {
        "Connected to Binance WebSocket"
      } else {
        producer.close()
        throw new RuntimeException(s"Connection failed: ${response.response.status}")
      }
    }

    connected.onComplete(println)
  }
}