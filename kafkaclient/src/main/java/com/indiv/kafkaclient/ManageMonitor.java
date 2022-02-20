package com.indiv.kafkaclient;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.TopicPartition;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class ManageMonitor {
    public static void main( String[] args ) throws ExecutionException, InterruptedException {

        while (true) {
            AdminClient adminClient = Monitor.getAdminClient("localhost:9092, localhost:9093");
            Map<TopicPartition, Long> consumerGrpOffsets = Monitor.getConsumerGrpOffsets("G1",adminClient);
            Map<TopicPartition, Long> producerOffsets = Monitor.getProducerOffsets(consumerGrpOffsets);
            Map<TopicPartition, Long> lags = Monitor.computeLags(consumerGrpOffsets, producerOffsets);
            SimpleDateFormat formatter= new SimpleDateFormat("yyyy-MM-dd 'at' HH:mm:ss z");
                for (Map.Entry<TopicPartition, Long> lagEntry : lags.entrySet()) {
                    String topic = lagEntry.getKey().topic();
                    int partition = lagEntry.getKey().partition();
                    Long lag = lagEntry.getValue();
                    System.out.printf("Time=%s | Lag for topic = %s, partition = %s is %d\n",
                            formatter.format(new Date(System.currentTimeMillis())),
                            topic,
                            partition,
                            lag);
                }
                try {
                    Thread.sleep(1000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
        }
    }

}
