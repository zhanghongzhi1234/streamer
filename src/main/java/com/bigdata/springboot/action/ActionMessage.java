package com.bigdata.springboot.action;

import java.util.HashMap;
import java.util.Map;

public class ActionMessage {

    public enum MessageType {
        CREATE, START, STOP, DELETE, LIST
    }

    public String actionId;
    public MessageType actionType;
    public Map<String, String> parameterMap = new HashMap<String, String>();
}
