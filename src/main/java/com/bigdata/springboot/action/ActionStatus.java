package com.bigdata.springboot.action;

import java.util.HashMap;
import java.util.Map;

public class ActionStatus {

    public enum Status {
        SUCCESS, IDLE, EXECUTION, FAILED
    }
    private String actionId;
    private Status status = Status.IDLE;

    private Map<String, Status> statusMap = new HashMap<String, Status>();

    public void addNewAction(String actionId){
        statusMap.put(actionId, Status.IDLE);
    }

    public Status getActionStatus(String actionId){
        return statusMap.get(actionId);
    }

    public void setActionStatus(String actionId, Status status){
        statusMap.put(actionId, status);
    }

    public void deleteAction(String actionId){
        statusMap.remove(actionId);
    }
}
