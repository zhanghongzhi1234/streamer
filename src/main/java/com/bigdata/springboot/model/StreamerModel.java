package com.bigdata.springboot.model;

import org.springframework.stereotype.Component;

import java.util.Objects;

@Component
public class StreamerModel {
    private String index_name;
    private Source source;
    private String sql;          //suggest use presto to filter datasource
    private User user;

    public StreamerModel(){}

    public StreamerModel(String index_name, Source source, String sql, User user) {
        this.index_name = index_name;
        this.source = source;
        this.sql = sql;
        this.user = user;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StreamerModel that = (StreamerModel) o;
        return Objects.equals(index_name, that.index_name) &&
                Objects.equals(source, that.source) &&
                Objects.equals(sql, that.sql) &&
                Objects.equals(user, that.user);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index_name, source.getSource_type(), source.getConnection_string(), user);
    }

    public String getIndex_name() {
        return index_name;
    }

    public void setIndex_name(String index_name) {
        this.index_name = index_name;
    }

    public Source getSource() {
        return source;
    }

    public void setSource(Source source) {
        this.source = source;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public User getUser() {
        return user;
    }

    public void setUser(User user) {
        this.user = user;
    }
}
