package com.shuyun.ccms.demo.kafka;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

/**
 * Created by heshaohui on 16/6/22.
 */
public class ConsumerRunnableTest implements Runnable {

    private KafkaStream mStream ;
    private int threadNumber ;

    public ConsumerRunnableTest(KafkaStream mStream,int threadNumber){
        this.mStream = mStream ;
        this.threadNumber = threadNumber ;
    }

    @Override
    public void run() {
        ConsumerIterator iterator = mStream.iterator() ;
        System.out.println("Thread "+threadNumber + " starting .......");

        while (iterator.hasNext())
            System.out.println("Thread "+threadNumber + ":"+iterator.next().message());
        System.out.println("Shutting down Thread:"+threadNumber);
    }


}
