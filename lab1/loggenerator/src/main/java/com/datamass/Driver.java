package com.datamass;


import org.apache.log4j.Logger;


public class Driver {

    final static Logger logger = Logger.getLogger(Driver.class);

    public static void main(String[] args) {

        try {

            while (true) {
                logger.info("Kafka message comes in");
                Thread.sleep(10000);
                logger.error("Kafka message error in processing");
                Thread.sleep(10000);
            }
        }catch (Exception ex){

        }
    }
}
