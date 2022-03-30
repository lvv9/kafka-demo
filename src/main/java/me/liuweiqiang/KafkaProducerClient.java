package me.liuweiqiang;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

public class KafkaProducerClient {

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("acks", "all");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties)) {
            Future<RecordMetadata> future = kafkaProducer.send(new ProducerRecord<>("two-replica-topic", "test"));
            Future<RecordMetadata> future2 = kafkaProducer.send(new ProducerRecord<>("two-replica-topic", "test2"));
            System.out.println(future2.get().offset());
            RecordMetadata recordMetadata = future.get();
            System.out.println(recordMetadata.offset());
        }
    }
}
