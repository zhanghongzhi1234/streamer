package com.bigdata.springboot.model;

import org.springframework.stereotype.Component;

@Component
public class StreamerActionModel {
    private String streamerId;

    public StreamerActionModel(){}

    public StreamerActionModel(String streamerId){
        this.streamerId = streamerId;
    }
    public String getStreamerId() {
        return streamerId;
    }

    public void setStreamerId(String streamerId) {
        this.streamerId = streamerId;
    }

    /*public String getExecuator() {
        return execuator;
    }

    public void setExecuator(String execuator) {
        this.execuator = execuator;
    }

    private String execuator;*/
}
