package com.shuyun.ccms.demo.spark.streaming;

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * kafka消息传递到到spark streaming
 * Created by heshaohui on 16/6/27.
 */

public class KafkaToSparkStreamingExample {
    private static final Pattern SPACE = Pattern.compile(" ") ;

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    public static void main(String[] args){

        String zkQuorum = "172.18.21.214:2181";
        String groupId = "test-consumer-group";

        Map<String,Integer> topicsMap = new HashMap();
        topicsMap.put("test",3);

        SparkConf sparkConf = new SparkConf().setAppName("KafkaToSparkStreamingExample").setMaster("spark://172.18.21.214:7077") ;
        Duration batchInterval = new Duration(1000);

        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConf,batchInterval);


        JavaPairReceiverInputDStream<String,String> pairReceiverInputDStream =
                KafkaUtils.createStream(javaStreamingContext,zkQuorum,groupId,topicsMap);


        JavaPairDStream<String, Integer> lastCounts =
                pairReceiverInputDStream.map(new Function<Tuple2<String, String>, String>() {
                    @Override
                    public String call(Tuple2<String, String> tuple2) {
                        System.out.println("kafka数据:"+tuple2._1() + " : " + tuple2._2());
                        return tuple2._2();
                    }
                }).flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public Iterable<String> call(String x) {
                        return Lists.newArrayList(SPACE.split(x));
                    }
                }).mapToPair(new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) {
                        return new Tuple2<String, Integer>(s, 1);
                    }
                }).reduceByKey(new Function2<Integer, Integer, Integer>() {

                    @Override
                    public Integer call(Integer x, Integer y) throws Exception {
                        return x.intValue() + y.intValue();
                    }
                });


        System.out.println("begin...................");
        lastCounts.print();
        System.out.println("end...................");

        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
    }
}
