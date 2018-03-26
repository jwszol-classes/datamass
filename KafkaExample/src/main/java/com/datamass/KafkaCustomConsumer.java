package com.datamass;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class KafkaCustomConsumer {
    final static Logger LOGGER = Logger.getLogger(KafkaCustomConsumer.class);
    private Consumer<String, String> consumer;

    public KafkaCustomConsumer() {
        Map<String, Object> consumerConfig = new HashMap();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "ep_group");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        this.consumer = new KafkaConsumer<String, String>(consumerConfig);
    }

    public void receive(String topic) {
        consumer.subscribe(Arrays.asList(topic));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100L);
            for (ConsumerRecord<String, String> record : records) {
                LOGGER.info("Found message " + record.key() + record.value());
            }
        }
    }
}
