

 package com.example;

 import org.apache.kafka.common.serialization.Serdes;
 import org.apache.kafka.streams.KafkaStreams;
 import org.apache.kafka.streams.StreamsBuilder;
 import org.apache.kafka.streams.StreamsConfig;
 import org.apache.kafka.streams.Topology;
 import org.apache.kafka.streams.kstream.KStream;
 import org.apache.logging.log4j.LogManager;
 import org.apache.logging.log4j.Logger;
 
 import java.util.Properties;
 
 /**
  * Kafka Streams application that reads and prints the messages from a given topic
  *
  * @author Prashant
  * @author www.learningjournal.guru
  */
 public class Weather {
     private static final Logger logger = LogManager.getLogger();
     private static final String topicName = "quickstart-events";
 
     /**
      * Application entry point
      *
      * @param args topicName (Name of the Kafka topic to read)
      */
 
     public static void main(String[] args) {
 
         Properties props = new Properties();
         props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-consumer-group");
         props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
         props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
         props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
 
         StreamsBuilder builder = new StreamsBuilder();
         KStream<Integer, String> kStream = builder.stream(topicName);
         kStream.foreach((k, v) -> System.out.println("Key = " + k + " Value = " + v));
         //kStream.peek((k, v) -> System.out.println("Key = " + k + " Value = " + v));
         Topology topology = builder.build();
 
         KafkaStreams streams = new KafkaStreams(topology, props);
 
         logger.info("Starting the stream");
         streams.start();
 
         Runtime.getRuntime().addShutdownHook(new Thread(() -> {
             logger.info("Stopping Stream");
             streams.close();
         }));
     }
 
 }
 