package com.bigdata.springboot.bean;

import com.bigdata.springboot.akka.actor.IMessageProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class KafkaConsumerHelper {

    private KafkaConsumer<String, String> consumer;
    private String serverURL;
    private String groupId;
    private String topic;
    private IMessageProcessor callback;
    KafkaConsumerPollingThread pollingThread;
    private boolean isPolling = false;
    private boolean isRunning = false;

    public KafkaConsumerHelper(String connection_string, String groupId, IMessageProcessor callback){
        this.serverURL = connection_string;
        this.groupId = groupId;
        this.callback = callback;
        Init(connection_string, groupId, topic);
    }

    private void Init(String connection_string, String groupId, String topic){
        System.out.println("kafka server: " + connection_string);
        //create config object
        Properties prop = new Properties();

        //Kafka cluster
        prop.setProperty("bootstrap.servers", connection_string);           //connection_string looks like "linux1:9092"
        //K,V serilize object
        prop.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        prop.setProperty("group.id", groupId);
        prop.setProperty("enable.auto.commit", "true");
        prop.setProperty("auto.commit.interval.ms", "1000");

        consumer = new KafkaConsumer<String, String>(prop);
        pollingThread = new KafkaConsumerPollingThread();
    }

    //subscribe topic, can by a topic list seperated with ","
    public void Subscribe(String topic) {
        this.topic = topic;
        consumer.subscribe(Arrays.asList(topic.split(",")));
    }

    //unsubscribe all topics
    public void Unsubscribe() {
        consumer.unsubscribe();
    }

    //must new a thread each time
    public void StartPolling(){
        isPolling = true;
        Thread th = new Thread(pollingThread);
        th.start();
    }

    public void StopPolling(){
        isPolling = false;
    }

    public void Close() {
        isRunning = false;
        consumer.close();
        System.out.println("Kafka consumer close");
    }

    private class KafkaConsumerPollingThread implements Runnable {
        @Override
        public void run() {
            while (isPolling) {
                // pull data
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    callback.ProcessMessage(record.value());
                    //print data
                    System.out.println(record);
                }
            }
        }
    }
}
