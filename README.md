# Crypto-SparkStream

## Basic Data Flow
- Akka Connects to Binance Through WebSockets creating a Flow
- Kafka Producer reads Akka Flow data and sends to topic
- Kafka Consumer creates direct stream and streams data through spark
- Data is processed into batches of 10Seconds& saved to either textFiles or MongoDB in the format:

```| Average Price | Total Quantity | Trade Count | Time of the first Trade | Time of the last Trade |``` 



### References
- https://doc.akka.io/docs/akka-http/current/client-side/websocket-support-html
- https://alvinalexander.com/scala/how-lift-json-parse-json-array-data-stocks
- https://spark.apache.org/docs/latest/streaming-kafka-0-10-integration.html
