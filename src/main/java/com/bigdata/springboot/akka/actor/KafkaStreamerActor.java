package com.bigdata.springboot.akka.actor;

import akka.actor.PoisonPill;
import akka.actor.Props;
import com.bigdata.springboot.action.ActionMessage;
import com.bigdata.springboot.action.ActionStatus;
import com.bigdata.springboot.bean.KafkaConsumerHelper;
import com.bigdata.springboot.model.Source;
import com.bigdata.springboot.model.StreamerModel;
import com.bigdata.springboot.model.User;

import java.util.Map;

public class KafkaStreamerActor extends StreamerActor {

    static public Props props(StreamerModel model, String esserver_host, int esserver_port1, int esserver_port2, ActionStatus actionStatus) {
        return Props.create(KafkaStreamerActor.class, () -> new KafkaStreamerActor(model, esserver_host, esserver_port1, esserver_port2, actionStatus));
    }

    private String connection_string;
    private String groupId = "group1";
    private String topic = null;

    private KafkaConsumerHelper consumer = null;

    public KafkaStreamerActor(StreamerModel model, String esserver_host, int esserver_port1, int esserver_port2, ActionStatus actionStatus) {
        super(esserver_host, esserver_port1, esserver_port2, actionStatus);
        Source source = model.getSource();
        User user = model.getUser();
        this.connection_string = source.getConnection_string();
        Map<String, String> config = model.getSource().getConfig();
        if(config.containsKey("groudId"))
            groupId = config.get("groupId");

        if(config.containsKey("topic"))
            topic = config.get("topic");

        consumer = new KafkaConsumerHelper(connection_string, groupId, this);
        if(consumer == null){
            System.out.println("error: KafkaConsumer cannot be created!");
        }
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ActionMessage.class, this::receiveActionMsg)
                .match(CreateIndexMsg.class, this::receiveMsg1)
                .matchEquals("Start", this::receiveMsg2)
                .matchEquals("Stop", this::receiveMsg3)
                .match(PoisonPill.class, this::receiveMsg4)
                .build();
    }

    private void receiveActionMsg(ActionMessage msg) {
        synchronized(this) {
            actionStatus.setActionStatus(msg.actionId, ActionStatus.Status.EXECUTION);
        }
        boolean bSuccess = false;
        try {
            if (msg.actionType == ActionMessage.MessageType.CREATE) {
                String index_name = msg.parameterMap.get("index_name");
                bSuccess = esHelper.CreateIndex(index_name);
                this.index_name = index_name;
            } else if (msg.actionType == ActionMessage.MessageType.START) {
                consumer.Subscribe(topic);
                consumer.StartPolling();
                bSuccess = true;
            } else if (msg.actionType == ActionMessage.MessageType.STOP) {
                consumer.Unsubscribe();
                consumer.StopPolling();
                bSuccess = true;
            }
        }
        catch (Exception ex){
            System.out.println(ex.toString());
            bSuccess = false;
        }

        synchronized(this){
            if(bSuccess == true)
                actionStatus.setActionStatus(msg.actionId, ActionStatus.Status.SUCCESS);
            else
                actionStatus.setActionStatus(msg.actionId, ActionStatus.Status.FAILED);
        }
    }

    private void receiveMsg1(CreateIndexMsg msg) {
        esHelper.CreateIndex(msg.index_name);
        this.index_name = msg.index_name;
    }

    private void receiveMsg2(String msg) {
        if(topic == null || topic.length() == 0){
            System.out.println("error: topic is empty, kafka streamer cannot start");
            return;
        }
        consumer.Subscribe(topic);
        consumer.StartPolling();
    }

    private void receiveMsg3(String msg) {
        consumer.Unsubscribe();
        consumer.StopPolling();
    }

    private void receiveMsg4(PoisonPill msg) {
        consumer.Unsubscribe();
        consumer.StopPolling();
        consumer.Close();
        esHelper.Close();
    }

    @Override
    public void postStop() {
        esHelper = null;
    }
}
