package com.bigdata.springboot.controller;

import com.bigdata.springboot.action.ActionStatus;
import com.bigdata.springboot.configuration.BasicConfiguration;
import com.bigdata.springboot.model.Source;
import com.bigdata.springboot.model.StreamerActionModel;
import com.bigdata.springboot.model.StreamerModel;
import com.bigdata.springboot.service.GaugeService;
import com.bigdata.springboot.service.StreamerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

@RestController
public class StreamerController {

    @Autowired
    private StreamerService streamerService;

    @Autowired
    private GaugeService gaugeService;

    @Autowired
    private BasicConfiguration configuration;

    @PostMapping("/api/streamer/create_index")
    public Map<String, String> CreateIndex(@RequestBody StreamerModel model) {
        //check request data format first
        if(model == null)
            return ErrorFormatResponse();
        String index_name = model.getIndex_name();
        if(index_name == null || index_name.length() == 0)
            return ErrorFormatResponse();
        Source source = model.getSource();
        if(source == null)
            return ErrorFormatResponse();
        String source_type = source.getSource_type();
        if(Source.isValidSource(source_type) == false)
            return ErrorFormatResponse();
        Map<String, String> config = source.getConfig();
        if(source_type.equals("kafka")){
            if(config.containsKey("topic") == false)
                return ErrorFormatResponse();
            String topic = config.get("topic");
            if(topic == null || topic.length() == 0)
                return ErrorFormatResponse();
        }
        else if(source_type.equals("csv")){
            String connection_string = source.getConnection_string();       //csv file path
            File file = new File(connection_string);
            if (!file.exists())
                return InternalErrorResponse("csv file " + connection_string + " not exist");
        }

        Map<String, String> mRet = streamerService.CreateStreamer(model);
        String streamerId = mRet.get("streamerId");
        String actionId = mRet.get("actionId");
        if(actionId != "")
            return ExecutionResponse(streamerId, actionId);
        else
            return FailResponse(actionId);
    }

    @PostMapping("/api/streamer/start")
    public Map<String, String> StartStreamer(@RequestBody StreamerActionModel model) {
        Map<String, String> mRet = new HashMap<String, String>();
        if(model == null)
            return ErrorFormatResponse();
        String streamerId = model.getStreamerId();
        if(streamerId == null || streamerId.length() == 0)
            return ErrorFormatResponse();

        String actionId = streamerService.StartStreamer(streamerId);
        if(actionId != "")
            return ExecutionResponse(streamerId, actionId);
        else
            return FailResponse(actionId);
    }

    @PostMapping("/api/streamer/stop")
    public Map<String, String> StopStreamer(@RequestBody StreamerActionModel model) {
        Map<String, String> mRet = new HashMap<String, String>();
        if(model == null)
            return ErrorFormatResponse();
        String streamerId = model.getStreamerId();
        if(streamerId == null || streamerId.length() == 0)
            return ErrorFormatResponse();

        String actionId = streamerService.StopStreamer(streamerId);
        if(actionId != "")
            return ExecutionResponse(streamerId, actionId);
        else
            return FailResponse(actionId);
    }

    @PostMapping("/api/streamer/delete")
    public Map<String, String> DeleteStreamer(@RequestBody StreamerActionModel model) {
        Map<String, String> mRet = new HashMap<String, String>();
        if(model == null)
            return ErrorFormatResponse();
        String streamerId = model.getStreamerId();
        if(streamerId == null || streamerId.length() == 0)
            return ErrorFormatResponse();

        String actionId = streamerService.DeleteStreamer(streamerId);
        if(actionId != "")
            return ExecutionResponse(streamerId, actionId);
        else
            return FailResponse(actionId);
    }

    @RequestMapping("/api/streamer/list")
    public Map<String, String> ListStreamer() {
        Map<String, String> mRet = new HashMap<String, String>();
        String streamerIdList = streamerService.GetAllStreamerIdList();
        mRet.put("streamerId", streamerIdList);
        double cpusage = gaugeService.getProcessCpuLoad();
        double memory = gaugeService.getProcessMemory() / 1000000;      //MB
        mRet.put("cpuage", String.valueOf(cpusage) + "%");
        mRet.put("memory", String.valueOf(memory) + "MB");
        return mRet;
    }

    @RequestMapping("/api/streamer/status/{actionId}")
    public Map<String, String> GetActionStatus(@PathVariable String actionId) {
        ActionStatus.Status status = streamerService.getActionStatusById(actionId);
        if(status == ActionStatus.Status.SUCCESS)
            return SuccessResponse("", actionId);
        else if(status == ActionStatus.Status.FAILED)
            return FailResponse(actionId);
        else //if(status == ActionStatus.Status.EXECUTION)
            return ExecutionResponse("", actionId);
    }

    private Map<String, String> SuccessResponse(String streamerId, String actionId){
        Map<String, String> mRet = new HashMap<String, String>();
        mRet.put("status_code", "200");
        mRet.put("status", "success");
        mRet.put("streamerId", streamerId);
        mRet.put("actionId", actionId);
        return mRet;
    }

    private Map<String, String> ExecutionResponse(String streamerId, String actionId){
        Map<String, String> mRet = new HashMap<String, String>();
        mRet.put("status_code", "200");
        mRet.put("status", "execution");
        mRet.put("streamerId", streamerId);
        mRet.put("actionId", actionId);
        return mRet;
    }

    private Map<String, String> ErrorFormatResponse(){
        Map<String, String> mRet = new HashMap<String, String>();
        mRet.put("status_code", "400");
        mRet.put("status", "error_format");
        mRet.put("reason", "input(s) cannot be found in request, is empty or json is malformed");
        return mRet;
    }

    private Map<String, String> FailResponse(String actionId){
        Map<String, String> mRet = new HashMap<String, String>();
        mRet.put("status_code", "401");
        mRet.put("status", "failed");
        mRet.put("actionId", actionId);
        mRet.put("reason", "execution failed");
        return mRet;
    }

    private Map<String, String> InternalErrorResponse(String reason){
        Map<String, String> mRet = new HashMap<String, String>();
        mRet.put("status_code", "500");
        mRet.put("status", "internal_error");
        mRet.put("reason", reason);
        return mRet;
    }
}
