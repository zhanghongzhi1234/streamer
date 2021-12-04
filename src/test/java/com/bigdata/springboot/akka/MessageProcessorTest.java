package com.bigdata.springboot.akka;

import com.bigdata.springboot.akka.actor.IMessageProcessor;

public class MessageProcessorTest implements IMessageProcessor {
    @Override
    public void ProcessMessage(String message) {
        System.out.println(message);
    }
}
