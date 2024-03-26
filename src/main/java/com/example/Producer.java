 package com.example;

 import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

 import org.apache.kafka.clients.producer.KafkaProducer;
 import org.apache.kafka.clients.producer.ProducerConfig;
 import org.apache.kafka.clients.producer.ProducerRecord;
 import org.apache.kafka.common.KafkaException;
 import org.apache.kafka.common.serialization.IntegerSerializer;
 import org.apache.kafka.common.serialization.StringSerializer;
 import org.apache.logging.log4j.LogManager;
 import org.apache.logging.log4j.Logger;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
 

 public class Producer {
     private static final Logger logger = LogManager.getLogger(Producer.class);
 
     public static void main(String[] args) throws CsvValidationException {
         String topicName= "quickstart-events";
        String csvFilePath = "src/main/resources/data/source.csv";
      
         Properties props = new Properties();
         props.put(ProducerConfig.CLIENT_ID_CONFIG, "HelloProducer");
         props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
         props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
         props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
 
         KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);
 
         logger.trace("Start sending messages...");
         try {
               try (CSVReader reader = new CSVReader(new FileReader(csvFilePath))) {
            // Read all rows from the CSV file
            String[] nextLine;
            while ((nextLine = reader.readNext()) != null) {
                // Process each row
               List<String> stringList = new ArrayList<>();
                for (String cell : nextLine) {
                    stringList.add(cell);
                }
                 producer.send(new ProducerRecord<>(topicName, stringList.get(0)+" "+stringList.get(1)));
                System.out.println(stringList);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
         } catch (KafkaException e) {
             logger.error("Exception occurred - Check log for more details.\n" + e.getMessage());
             System.exit(-1);
         } finally {
             logger.info("Finished HelloProducer - Closing Kafka Producer.");
             producer.close();
         }
 
     }
 }