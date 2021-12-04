package com.bigdata.springboot.service;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDBException;
import com.arangodb.ArangoDatabase;
import com.arangodb.entity.BaseDocument;
import com.arangodb.util.MapBuilder;
import com.bigdata.springboot.action.ActionMessage;
import com.bigdata.springboot.action.ActionStatus;
import com.bigdata.springboot.akka.actor.FileStreamerActor;
import com.bigdata.springboot.akka.actor.KafkaStreamerActor;
import com.bigdata.springboot.akka.actor.StreamerActor;
import com.bigdata.springboot.arangodbcrud.ArangoDbAdapter;
import com.bigdata.springboot.model.StreamerModel;
import com.google.gson.Gson;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class StreamerService implements InitializingBean {

    @Value("${esserver.host}")
    private String esserver_host;

    @Value("${esserver.port1}")
    private int esserver_port1;

    @Value("${esserver.port2}")
    private int esserver_port2;

    @Value("${arangodb.host}")
    private String arangodb_host;

    @Value("${arangodb.port}")
    private int arangodb_port;

    @Value("${arangodb.name}")
    private String arangodb_name;

    //#actor-system
    private ActorSystem actorSystem;
    private Map<String, ActorRef> actorMap = new HashMap<String, ActorRef>();
    private Map<String, StreamerModel> streamerMap = new HashMap<String, StreamerModel>();

    ActionStatus actionStatus = new ActionStatus();
    private int actionCounter = 1;

    private ArangoDbAdapter arangoDbAdapter;
    private String testCollectionName = "TestCollection";

    private Gson gson = new Gson();

    public StreamerService(){
        Init();
    }
    public void Init(){
        actorSystem = ActorSystem.create("StreamerSystem");
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        try {
            arangoDbAdapter = new ArangoDbAdapter();
            arangoDbAdapter.Init(arangodb_host, arangodb_port);
            ArangoDatabase arangoDatabase = arangoDbAdapter.selectDatabase(arangodb_name);
            if(arangoDatabase == null){
                System.out.println("Cannot open arangoDB to read streamer: " + arangodb_name);
            }
            loadAllStreamerFromDb();
        } catch (Exception ex) {
            System.out.println(ex.toString());
        }
    }

    void loadAllStreamerFromDb() {
        AtomicInteger resultCount = new AtomicInteger();
        try {
            String query = "FOR t IN streamer RETURN t";
            ArangoCursor<BaseDocument> cursor = arangoDbAdapter.executeQuery(query, null);
            cursor.forEachRemaining(aDocument -> {
                String streamerId = aDocument.getKey();
                System.out.println("read Key: " + aDocument.getKey());
                Object obj = aDocument.getAttribute("streamerModel");
                String strJson = obj.toString();
                /*strJson = strJson.replaceAll("=}", "=''}");
                strJson = strJson.replaceAll("=,", "='',");
                strJson = strJson.replaceAll(":", "\\:");*/
                StreamerModel model = gson.fromJson(strJson, StreamerModel.class);//对于javabean直接给出class实例
                if(model != null){
                    ActorRef actor = CreateActor(model);
                    if(actor != null) {
                        actorMap.put(streamerId, actor);
                        streamerMap.put(streamerId, model);
                    }
                }
                resultCount.getAndIncrement();
            });
            System.out.println("Total read " + resultCount + " document");
        } catch (ArangoDBException e) {
            System.err.println("Failed to execute query. " + e.getMessage());
        }
    }

    public ActorRef CreateActor(StreamerModel model){
        ActorRef actor = null;
        if(model.getSource().getSource_type().equals("kafka")) {
            actor = actorSystem.actorOf(Props.create(KafkaStreamerActor.class, model, esserver_host, esserver_port1, esserver_port2, actionStatus));
        }
        else if(model.getSource().getSource_type().equals("csv")) {
            actor = actorSystem.actorOf(Props.create(FileStreamerActor.class, model, actionStatus));
        }
        return actor;
    }

    //because it need return streamerId and actionId, so use Map return;
    public Map<String, String> CreateStreamer(StreamerModel model) {
        Map<String, String> mRet = new HashMap<String, String>();
        int streamerId = ("" + model.getIndex_name() + model.getSource().getSource_type() + model.getSource().getConnection_string() + model.getUser()).hashCode();
        ActorRef actor = CreateActor(model);
        if(actor != null) {
            actorMap.put(String.valueOf(streamerId), actor);
            streamerMap.put(String.valueOf(streamerId), model);
            saveStreamerToDb(String.valueOf(streamerId), model);

            String actionId = getNextActionId();
            actionStatus.addNewAction(actionId);
            ActionMessage actionMessage = new ActionMessage();
            actionMessage.actionId = actionId;
            actionMessage.actionType = ActionMessage.MessageType.CREATE;
            actionMessage.parameterMap.put("index_name", model.getIndex_name());

            actor.tell(actionMessage, ActorRef.noSender());
            mRet.put("streamerId", String.valueOf(streamerId));
            mRet.put("actionId", actionId);
        }
        return mRet;
    }

    private void saveStreamerToDb(String streamerId, StreamerModel model){
        BaseDocument myObject = new BaseDocument();
        myObject.setKey(streamerId);
        Object obj = gson.toJson(model);
        //myObject.addAttribute("streamerModel", model); also ok, but when read, gson cannot convert back to StreamerModel class
        // due to sql=, connection_string=linux1:9092
        //if can add quote to all value, then it is ok
        myObject.addAttribute("streamerModel", obj);
        try {
            arangoDbAdapter.insertDocument("streamer", myObject);
            System.out.println("Streamer saved");
        } catch (ArangoDBException e) {
            System.err.println("Failed to save streamer. " + e.getMessage());
        }
    }

    public String StartStreamer(String streamerId) {
        String actionId = "";
        if(actorMap.containsKey(streamerId)){
            ActorRef actor = actorMap.get(streamerId);
            if(actor != null) {
                actionId = getNextActionId();
                actionStatus.addNewAction(actionId);
                ActionMessage actionMessage = new ActionMessage();
                actionMessage.actionId = actionId;
                actionMessage.actionType = ActionMessage.MessageType.START;

                actor.tell(actionMessage, ActorRef.noSender());
            }
        }
        return actionId;
    }

    public String StopStreamer(String streamerId) {
        String actionId = "";
        if(actorMap.containsKey(streamerId)){
            ActorRef actor = actorMap.get(streamerId);
            if(actor != null) {
                actionId = getNextActionId();
                actionStatus.addNewAction(actionId);
                ActionMessage actionMessage = new ActionMessage();
                actionMessage.actionId = actionId;
                actionMessage.actionType = ActionMessage.MessageType.STOP;

                actor.tell(actionMessage, ActorRef.noSender());
            }
        }
        return streamerId;
    }

    public String DeleteStreamer(String streamerId) {
        String actionId = "";
        if(actorMap.containsKey(streamerId)){
            ActorRef actor = actorMap.get(streamerId);
            if(actor != null) {
                actionId = getNextActionId();
                actor.tell(akka.actor.PoisonPill.getInstance(), ActorRef.noSender());
                actorMap.remove(streamerId);
                streamerMap.remove(streamerId);
                deleteStreamerFromDb(streamerId);
            }
        }
        return actionId;
    }

    private void deleteStreamerFromDb(String streamerId){
        try {
            arangoDbAdapter.deleteDocument("streamer", streamerId);
        } catch (ArangoDBException e) {
            System.err.println("Failed to delete document. " + e.getMessage());
        }
    }

    public String GetAllStreamerIdList() {
        String sRet = "";
        ArrayList<String> keyList = new ArrayList<String>(actorMap.keySet());
        //sRet = keyList.stream().map(String::valueOf).collect(Collectors.joining(","));
        sRet = String.join(",", keyList);

        return sRet;
    }

    public ActionStatus.Status getActionStatusById(String actionId){
        return actionStatus.getActionStatus(actionId);
    }

    private String getNextActionId() {
        String actionId;
        Date d = new Date();
        System.out.println(d);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        String dateNowStr = sdf.format(d);
        actionId = dateNowStr + actionCounter;
        actionCounter++;
        if(actionCounter > 10000)
            actionCounter = 1;
        d = null;
        sdf = null;
        return actionId;
    }

}
