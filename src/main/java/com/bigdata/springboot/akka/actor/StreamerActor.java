package com.bigdata.springboot.akka.actor;

import akka.actor.AbstractActor;
import com.bigdata.springboot.action.ActionStatus;
import com.bigdata.springboot.bean.ElasticSearchHelper;

public class StreamerActor extends AbstractActor implements IMessageProcessor{

    static public class CreateIndexMsg {
        public String index_name;
        public CreateIndexMsg(String index_name){
            this.index_name = index_name;
        }
    }

    protected String index_name;
    protected ActionStatus actionStatus;
    protected ElasticSearchHelper esHelper;

    public StreamerActor(String esserver_host, int esserver_port1, int esserver_port2, ActionStatus actionStatus) {
        esHelper = new ElasticSearchHelper(esserver_host, esserver_port1, esserver_port2);
        this.actionStatus = actionStatus;
    }

	public void CreateIndexInELK(String index_name){
        if(esHelper != null) {
            esHelper.CreateIndex(index_name);
        }
    }

	public void WriteDataToELK(String index_name, String content){
        if(esHelper != null) {
            esHelper.InsertTextData(index_name, content);
        }
    }

	public String ReadDataFromELK(String index_name){
        String sRet = null;
        if(esHelper != null) {
            sRet = esHelper.GetData(index_name);
        }
        return sRet;
    }

    @Override
    public Receive createReceive() {
        return null;
    }

    @Override
    public void ProcessMessage(String message) {
        esHelper.InsertTextData(index_name, message);
    }
}
