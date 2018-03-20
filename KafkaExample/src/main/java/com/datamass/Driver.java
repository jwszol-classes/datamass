package com.datamass;

import org.apache.log4j.Logger;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Driver {

    final static Logger LOGGER = Logger.getLogger(Driver.class);

    public static void main(String[] args) {

        KafkaCustomProducer kcp = new KafkaCustomProducer();
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd_HH_mm_ss");
        Date date = new Date();
        kcp.send("my_testing_topic","{\"date\":\" "+ dateFormat.format(date) + "\"}");

        KafkaCustomConsumer kcc = new KafkaCustomConsumer();
        kcc.receive("my_testing_topic");
    }
}
