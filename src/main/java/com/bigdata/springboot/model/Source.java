package com.bigdata.springboot.model;

import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Component
public class Source {

    private static List<String> sourceTypeList = Arrays.asList("csv","kafka");

    private String source_type;
    private String connection_string;
    private Map<String, String> config;

    public Source(){}

    public Source(String source_type, String connection_string, Map<String, String> config) {
        this.source_type = source_type;
        this.connection_string = connection_string;
        this.config = config;
    }

    public String getSource_type() {
        return source_type;
    }

    public void setSource_type(String source_type) {
        this.source_type = source_type;
    }

    public String getConnection_string() {
        return connection_string;
    }

    public void setConnection_string(String connection_string) {
        this.connection_string = connection_string;
    }

    public Map<String, String> getConfig() {
        return config;
    }

    public void setConfig(Map<String, String> config) {
        this.config = config;
    }

    //check if the source type is valid
    public static boolean isValidSource(String sourceType){
        return sourceTypeList.contains(sourceType);
    }
}
