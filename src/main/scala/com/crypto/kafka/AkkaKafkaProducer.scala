package com.crypto.kafka

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.ws._

import scala.concurrent.Promise

object AkkaKafkaProducer {
  val binanceURL = "wss://stream.binance.com:9443/ws/bnbbtc@trade"
def main(args: Array[String]): Unit = {
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  import system.dispatcher

  val flow: Flow[Message, Message, Promise[Option[Message]]] =
    Flow.fromSinkAndSourceMat(
      Sink.foreach[Message](
        values => {println(values.asTextMessage.getStrictText)}
      ),
      Source.maybe[Message])(Keep.right)

  val (binanceResponse, binancepromise) =
    Http().singleWebSocketRequest(WebSocketRequest(binanceURL), flow)

  val connected = binanceResponse.map { response =>
    if (response.response.status == StatusCodes.SwitchingProtocols) {
      "Connected to Binance WebSocket"
    } else {
      throw new RuntimeException(s"Connection failed: ${response.response.status}")
    }
  }

  connected.onComplete(println)

  //
}
}