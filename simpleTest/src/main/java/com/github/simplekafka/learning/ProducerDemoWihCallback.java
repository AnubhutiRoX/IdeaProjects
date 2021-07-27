package com.github.simplekafka.learning;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWihCallback {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ProducerDemoWihCallback.class);

        String bootstrapServers = "127.0.0.1:9092";
        // create producer properties
        Properties properties = new Properties();

        // new way to set properties
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // cfreate producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i=0; i<10 ;i++) {
            // create a producer record
            ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "Hello World");

            // send data - this is asynchronous
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes everytime record is successively sent or an exception
                    if (e == null) {
                        // successfully record sent
                        logger.info("Received new metadata. \n" +
                                "Topic: " + recordMetadata.topic() +
                                "Offset: " + recordMetadata.offset() +
                                "Partition: " + recordMetadata.partition() +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error!!" + e);
                    }
                }
            });
        }

        producer.flush();

        producer.close();
    }
}
