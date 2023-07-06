package br.com.alura.ecommerce;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

class KafkaDispatcher<T> implements Closeable {
    private final KafkaProducer<String, T> producer;

    KafkaDispatcher() {
        this.producer = new KafkaProducer<>(properties());
    }

    void send(String topic, String key, T value) throws ExecutionException, InterruptedException {
        var record = new ProducerRecord<>(topic, key, value);
        this.producer.send(record, getCallback()).get();
    }

    private static Callback getCallback() {
        return (metadata, exception) -> {
            if (exception != null) {
                exception.printStackTrace();
            }
            System.out.println("success sent " + metadata.topic() + ":::partition " + metadata.partition() + "/ offset " + metadata.offset() + "/ timestamp " + metadata.timestamp());
        };
    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        // keys and values will be serialized from strings to bytes
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName());
        // This means the leader will wait for the full set of in-sync replicas to acknowledge the record.
        // This guarantees that the record will not be lost as long as at least one in-sync replica remains alive.
        // This is the strongest available guarantee. Highest reliability.
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        return properties;
    }

    @Override
    public void close() {
        this.producer.close();
    }
}
