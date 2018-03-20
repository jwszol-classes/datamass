package com.datamass;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaCustomProducer {

    final static Logger LOGGER = Logger.getLogger(KafkaCustomProducer.class);
    private Producer<String, String> producer;

    public KafkaCustomProducer() {
        Map<String, Object> producerConfig = new HashMap();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "1");
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        this.producer =  new KafkaProducer<String, String>(producerConfig);
    }

    public void send(String topic, String message){
        LOGGER.info(message);
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, "msg_key", message);
        Future<RecordMetadata> futureMetadata = producer.send(record);

        try {
            RecordMetadata metadata = futureMetadata.get();
            LOGGER.info(metadata);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }
}
