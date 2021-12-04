package com.bigdata.springboot.akka.actor;

import akka.actor.PoisonPill;
import akka.actor.Props;
import com.bigdata.springboot.action.ActionMessage;
import com.bigdata.springboot.action.ActionStatus;
import com.bigdata.springboot.model.Source;
import com.bigdata.springboot.model.StreamerModel;
import com.bigdata.springboot.model.User;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class FileStreamerActor extends StreamerActor {

    static public Props props(StreamerModel model, String esserver_host, int esserver_port1, int esserver_port2, ActionStatus actionStatus) {
        return Props.create(FileStreamerActor.class, () -> new FileStreamerActor(model, esserver_host, esserver_port1, esserver_port2, actionStatus));
    }

    private String connection_string;           //file path of csv file
    private String index_name;

    public FileStreamerActor(StreamerModel model, String esserver_host, int esserver_port1, int esserver_port2, ActionStatus actionStatus) {
        super(esserver_host, esserver_port1, esserver_port2, actionStatus);
        Source source = model.getSource();
        User user = model.getUser();
        this.connection_string = source.getConnection_string();
        this.index_name = model.getIndex_name();
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
                File file = new File(connection_string);
                if (!file.exists()){
                    System.out.println("error: csv file " + connection_string + " not exist");
                    bSuccess = false;
                }
                else {
                    List<Map<String, String>> dataMapList = ReadCsvFile(file);
                    esHelper.BulkyInsertData(index_name, dataMapList);
                }
                bSuccess = true;
            } else if (msg.actionType == ActionMessage.MessageType.STOP) {
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
    }

    private void receiveMsg2(String msg) {
        File file = new File(connection_string);
        if (!file.exists()){
            System.out.println("error: csv file " + connection_string + " not exist");
            return;
        }
        try {
            List<Map<String, String>> dataMapList = ReadCsvFile(file);
            esHelper.BulkyInsertData(index_name, dataMapList);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void receiveMsg3(String msg) {
        //do nothing, csv file imported already
    }

    private void receiveMsg4(PoisonPill msg) {
        esHelper.Close();
    }

    public List<Map<String, String>> ReadCsvFile(File file) throws IOException {
        List<Map<String, String>> response = new LinkedList<Map<String, String>>();
        CsvMapper mapper = new CsvMapper();
        CsvSchema schema = CsvSchema.emptySchema().withHeader();
        MappingIterator<Map<String, String>> iterator = mapper.readerFor(Map.class)
                .with(schema)
                .readValues(file);
        while (iterator.hasNext()) {
            response.add(iterator.next());
        }
        return response;
    }
}
