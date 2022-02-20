package com.indiv.kafkaclient;

import java.util.Properties;
import org.apache.kafka.clients.producer.*;

/**
 * Hello world!
 *
 */
public class ProducerApp
{
    public static void main( String[] args )
    {
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", "localhost:9092, localhost:9093");
        producerProps.put("acks", "all");
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(producerProps);
         for (int i = 0; i < 1000; i++) {
            producer.send(new ProducerRecord<String, String>("sample", Integer.toString(i), Integer.toString(i)));
            System.out.printf("send data\n");
            try {
                Thread.sleep(100);
            } catch (Exception e) {
                e.printStackTrace();
            }
         }
         producer.close();
    }
}
