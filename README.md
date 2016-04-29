# Weather-Event-Streamer
Real Time Weather Event Analysis Processing Streamer

This project is implemented to stream and analyze real time weather
data using Apache Kafka and Apache Storm integration in hortonworks
sandbox available in Azure cloud environment

Need horton sandbox account, preferable in a cloud environment like AWS or Azure


Commands to run:

java -cp target/Weather-Event-Streamer-0.0.1-SNAPSHOT.jar producer/WeatherEventsProducer sandbox.hortonworks.com:6667 sandbox.hortonworks.com:2181

storm jar target/Weather-Event-Streamer-0.0.1-SNAPSHOT.jar consumers.WeatherEventProcessingTopology
