package me.liuweiqiang;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerClient {

    public static void main(String[] args) {
        //java -cp ~/kafka-1.0-SNAPSHOT.jar -Dloader.main=me.liuweiqiang.KafkaConsumerClient org.springframework.boot.loader.PropertiesLauncher
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("enable.auto.commit", "false");
        properties.put("group.id", "test");
        try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties)) {
            kafkaConsumer.subscribe(Collections.singletonList("two-replica-topic"));
            int count = 3;
            while (count > 0) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(10));
                consumerRecords.forEach(record -> System.out.println("offset: " + record.offset()));
                kafkaConsumer.commitSync();
                count--;
            }
        }
    }
}
