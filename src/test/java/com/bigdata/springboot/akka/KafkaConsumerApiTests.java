package com.bigdata.springboot.akka;

import com.bigdata.springboot.bean.ElasticSearchHelper;
import com.bigdata.springboot.bean.KafkaConsumerHelper;
import kafka.security.auth.Read;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
//import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
//import org.springframework.boot.test.context.SpringBootTest;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.Scanner;

public class KafkaConsumerApiTests {

    private String connection_string = "192.168.137.186:9092";
    private String groupId = "group1";
    private String topic = "first";
    private KafkaConsumerHelper kafkaConsumerHelper;
    private MessageProcessorTest processor;
    private boolean isRunning;

    @BeforeClass
    void Setup() {
        ReadConsoleInput readConsoleInput = new ReadConsoleInput();
        Thread th = new Thread(new ReadConsoleInput());
        th.start();

        try {
            kafkaConsumerHelper = new KafkaConsumerHelper(connection_string, groupId, processor);
            kafkaConsumerHelper.Subscribe(topic);
            //kafkaConsumerHelper.Close();
        }catch(Exception ex){
        }
        finally {
        }
    }

    @AfterClass
    public void Close() {
        Stop();
        kafkaConsumerHelper = null;
    }

    @Test(groups = "groupCorrect")
    public void Start(String topic){
        kafkaConsumerHelper.Subscribe(topic);
        kafkaConsumerHelper.StartPolling();
    }

    @Test(groups = "groupCorrect")
    public void Stop(){
        kafkaConsumerHelper.Unsubscribe();
        kafkaConsumerHelper.StopPolling();
    }

    @Test(enabled = false)
    private class ReadConsoleInput implements Runnable {
        @Override
        public void run() {
            Scanner sc = new Scanner(System.in);
            while (isRunning)
            {
                String cmd = sc.nextLine();
                if (cmd.equals("exit"))
                {
                    isRunning = false;
                }
                else if (cmd.equals("help"))
                {
                    System.out.println("Command list:");
                    System.out.println("1 exit: exit program");
                    System.out.println("2 help: help on command");
                    System.out.println("3 start: start consumer");
                    System.out.println("4 stop: stop consumer");
                }
                else
                {
                    /*string[] temp = cmd.Split(' ');
                    if (temp.Count() >= 2)
                    {
                        int n;
                        bool isNumeric = int.TryParse(temp[1], out n);
                        if (temp[0] == "runsim" && isNumeric == true)
                        {
                            dataSend.Add(cmd);
                        }
                    }*/
                }
            }
        }
    }
}
