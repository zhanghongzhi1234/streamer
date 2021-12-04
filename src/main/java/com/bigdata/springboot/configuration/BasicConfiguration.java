package com.bigdata.springboot.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties("esserver")
public class BasicConfiguration {
    private String host;
    private int port1;
    private int port2;

    public String getHost() {
        return host;
    }

    public int getPort1() {
        return port1;
    }

    public int getPort2() {
        return port2;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setPort1(int port1) {
        this.port1 = port1;
    }

    public void setPort2(int port2) {
        this.port2 = port2;
    }
}
