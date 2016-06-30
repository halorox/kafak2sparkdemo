package com.shuyun.ccms.demo.kafka;

import com.google.common.collect.Maps;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by heshaohui on 16/6/22.
 */
public class ConsumerTest {

    private final String topic = "test";
    private ConsumerConnector consumer ;

    private static ConsumerConfig createConsumerConfig(String zookeeper, String groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", zookeeper);
        props.put("group.id", groupId);
        props.put("zookeeper.session.timeout.ms", "2000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        return new ConsumerConfig(props);
    }

    public ConsumerTest(String zookeeper,String groupId){
        consumer = Consumer.createJavaConsumerConnector(createConsumerConfig(zookeeper,groupId));
    }

    public void run(int numThreads){
        Map<String,Integer> topicCountMap = Maps.newHashMap();
        topicCountMap.put(topic,numThreads);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap) ;
        List<KafkaStream<byte[], byte[]>> streams  = consumerMap.get(topic);
        System.out.println("Starting kafka.......");
        ExecutorService executorService  = Executors.newFixedThreadPool(numThreads) ;

        int threadNumber = 0 ;
        for (final KafkaStream stream:streams) {
            executorService.execute(new ConsumerRunnableTest(stream,threadNumber));
            threadNumber++ ;
        }
    }

    public  static  void main(String[] args){
        //172.18.21.62:2181,172.18.21.63:2181,172.18.21.64:2181
        //172.18.21.214:2181
        ConsumerTest consumerTest = new ConsumerTest("172.18.21.214:2181","test-consumer-group") ;
        consumerTest.run(1);
    }
}
