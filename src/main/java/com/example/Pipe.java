package com.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

public class Pipe {
    public static void main(String[] args) {
        Properties props = getConfig();

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        

        streamsBuilder.<String, String>stream("quickstart-events")
              .flatMapValues((key, value) -> {

        String[] parts = value.split("\\s+");
        
        if (parts.length == 2) {
            String timestamp = parts[0];
            String mean = parts[1];
            insertIntoDatabase(timestamp, mean);
            return Arrays.asList(timestamp,timestamp);
        } else {
         
            return Collections.emptyList();
        }
    })
   ;
    KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), props);
        // Start the application
        kafkaStreams.start();
        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }
   

    private static Properties getConfig() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-consumer-group");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        return properties;
    }

    private static void insertIntoDatabase(String key, String value) {
        // Insert into PostgreSQL database
        String url = "jdbc:postgresql://localhost:5432/postgres";
        Properties dbProps = new Properties();
        dbProps.setProperty("user", "postgres");
        dbProps.setProperty("password", "i8s18");

        try (Connection conn = DriverManager.getConnection(url, dbProps)) {
            String sql = "INSERT INTO kafka (time_stamp , mean) VALUES (?, ?)";
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setString(1, key);
                pstmt.setString(2, value);
                pstmt.executeUpdate();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}












































// package com.example;


// import java.util.Arrays;
// import java.util.Properties;

// import org.apache.kafka.clients.consumer.ConsumerConfig;
// import org.apache.kafka.common.serialization.Serdes;
// import org.apache.kafka.streams.KafkaStreams;
// import org.apache.kafka.streams.StreamsBuilder;
// import org.apache.kafka.streams.StreamsConfig;
// import org.apache.kafka.streams.kstream.Materialized;
// import org.apache.kafka.streams.kstream.Produced;




// public class Pipe {
//     public static void main(String[] args)  {

       
        
//         Properties props = getConfig();
       

       

//         StreamsBuilder streamsBuilder = new StreamsBuilder();
//         // Build the Topology
//         streamsBuilder.<String, String>stream("input")
//                 .flatMapValues((key, value) ->
//                         Arrays.asList(value.toLowerCase()
//                             .split(" ")))
//                 .groupBy((key, value) -> value)
//                 .count(Materialized.with(Serdes.String(), Serdes.Long()))
//                 .toStream()
//                 .to("output", Produced.with(Serdes.String(), Serdes.Long()));
//         // Create the Kafka Streams Application
//         KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), props);
//         // Start the application
//         kafkaStreams.start();

//         // attach shutdown handler to catch control-c
//         Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
//     }

//     private static Properties getConfig() {
//         Properties properties = new Properties();
//         properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-consumer-group");
//         properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//         properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//         properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//         properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//         properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
//         return properties;
//     }

// }
