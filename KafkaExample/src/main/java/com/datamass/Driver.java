package com.datamass;

import org.apache.log4j.Logger;

public class Driver {

    final static Logger LOGGER = Logger.getLogger(Driver.class);

    public static void main(String[] args) {

        KafkaCustomProducer kcp = new KafkaCustomProducer();
        kcp.send("my_testing_topic","{\"date\":\"2018-03-21\"}");

        KafkaCustomConsumer kcc = new KafkaCustomConsumer();
        kcc.receive("my_testing_topic");
    }
}
