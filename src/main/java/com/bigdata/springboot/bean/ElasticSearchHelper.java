package com.bigdata.springboot.bean;

import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class ElasticSearchHelper {

    private RestHighLevelClient client;

    public ElasticSearchHelper(String host, int port1, int port2){
        Init(host, port1, port2);
    }

    private void Init(String host, int port1, int port2){
        System.out.println("host=" + host + ", port1=" + port1 + ", port2=" + port2);
        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(host, port1, "http"),
                        new HttpHost(host, port2, "http")));
    }

    public void Close() {
        try {
            client.close();
            System.out.println("Close Elasticsearch client");
        } catch(ElasticsearchException e) {
            System.out.println(e.getDetailedMessage());
        } catch (java.io.IOException ex){
            System.out.println(ex.getLocalizedMessage());
        }
    }

    public boolean CreateIndex(String index) {
        return CreateIndex(index, "text");
    }

    public boolean CreateIndex(String index, String type) {
        CreateIndexRequest request = new CreateIndexRequest(index);
        boolean acknowledged = false;
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            {
                builder.startObject("properties");
                {
                    builder.startObject("message");
                    {
                        builder.field("type", type);
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
            request.mapping(builder);

            CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
            acknowledged = createIndexResponse.isAcknowledged();
            System.out.println("Create index " + index + " result: " + acknowledged);
        } catch(ElasticsearchException e) {
            System.out.println(e.getDetailedMessage());
        } catch (java.io.IOException ex){
            System.out.println(ex.getLocalizedMessage());
        }

        return acknowledged;
    }

    public boolean DeleteIndex(String index) {
        DeleteIndexRequest request = new DeleteIndexRequest(index);
        boolean acknowledged = false;
        try {
            AcknowledgedResponse deleteIndexResponse = client.indices().delete(request, RequestOptions.DEFAULT);
            acknowledged = deleteIndexResponse.isAcknowledged();
            System.out.println("delete index " + index + " result: " + acknowledged);
        } catch(ElasticsearchException e) {
            System.out.println(e.getDetailedMessage());
        } catch (java.io.IOException ex){
            System.out.println(ex.getLocalizedMessage());
        }
        return acknowledged;
    }

    public boolean IndexExist(String index) {
        GetIndexRequest request = new GetIndexRequest(index);

        boolean exists = false;
        try {
            exists = client.indices().exists(request, RequestOptions.DEFAULT);
            System.out.println("Exist index " + index + " result: " + exists);
        } catch(ElasticsearchException e) {
            System.out.println(e.getDetailedMessage());
        } catch (java.io.IOException ex){
            System.out.println(ex.getLocalizedMessage());
        }
        return exists;
    }

    public boolean OpenIndex(String index) {
        OpenIndexRequest request = new OpenIndexRequest(index);
        boolean acknowledged = false;
        try {
            OpenIndexResponse openIndexResponse = client.indices().open(request, RequestOptions.DEFAULT);
            acknowledged = openIndexResponse.isAcknowledged();
            System.out.println("open index " + index + " result: " + acknowledged);
        } catch(ElasticsearchException e) {
            System.out.println(e.getDetailedMessage());
        } catch (java.io.IOException ex){
            System.out.println(ex.getLocalizedMessage());
        }
        return acknowledged;
    }

    public boolean CloseIndex(String index) {
        CloseIndexRequest request = new CloseIndexRequest(index);
        boolean acknowledged = false;
        try {
            AcknowledgedResponse closeIndexResponse = client.indices().close(request, RequestOptions.DEFAULT);
            acknowledged = closeIndexResponse.isAcknowledged();
            System.out.println("close index " + index + " result: " + acknowledged);
        } catch(ElasticsearchException e) {
            System.out.println(e.getDetailedMessage());
        } catch (java.io.IOException ex){
            System.out.println(ex.getLocalizedMessage());
        }

        return acknowledged;
    }

    //insert single data
    public boolean InsertTextData(String index, String content){
        return InsertTextData(index, null, content);
    }
    public boolean InsertTextData(String index, String id, String content){
        IndexRequest indexRequest;
        Map<String, Object> dataMap = new HashMap<String, Object>();
        dataMap.put("message", content);
        if(id == null) {
            //id = UUID.randomUUID().toString();
            indexRequest = new IndexRequest(index).type("_doc").source(dataMap);
        }
        else {
            indexRequest = new IndexRequest(index).type("_doc").id(id).source(dataMap);
        }
        boolean acknowledged = false;
        try {
            IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);
            if (response.getResult() == DocWriteResponse.Result.CREATED) {
                acknowledged = true;
            } else if (response.getResult() == DocWriteResponse.Result.UPDATED) {
                acknowledged = true;
            }
        } catch(ElasticsearchException e) {
            System.out.println(e.getDetailedMessage());
        } catch (java.io.IOException ex){
            System.out.println(ex.getLocalizedMessage());
        }
        return acknowledged;
    }

    //insert Jason data
    public boolean InsertMapData(String index, Map<String, String> dataMap){
        IndexRequest indexRequest = new IndexRequest(index).type("_doc").source(dataMap);
        boolean acknowledged = false;
        try {
            IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);
            if (response.getResult() == DocWriteResponse.Result.CREATED) {
                acknowledged = true;
            } else if (response.getResult() == DocWriteResponse.Result.UPDATED) {
                acknowledged = true;
            }
        } catch(ElasticsearchException e) {
            System.out.println(e.getDetailedMessage());
        } catch (java.io.IOException ex){
            System.out.println(ex.getLocalizedMessage());
        }
        return acknowledged;
    }

    //delete data by index and id
    public boolean DeleteData(String index, String id){
        DeleteRequest request = new DeleteRequest(index).id(id);
        boolean acknowledged = false;
        try {
            DeleteResponse response = client.delete(request, RequestOptions.DEFAULT);
            if (response.getResult() == DocWriteResponse.Result.DELETED) {
                acknowledged = true;
            }
        } catch(ElasticsearchException e) {
            System.out.println(e.getDetailedMessage());
        } catch (java.io.IOException ex){
            System.out.println(ex.getLocalizedMessage());
        }
        return acknowledged;
    }

    //get data from index
    public String GetData(String index){
        GetRequest getRequest = new GetRequest(index);
        String sourceAsString = null;
        try {
            GetResponse response = client.get(getRequest, RequestOptions.DEFAULT);
            if (response.isExists()) {
                sourceAsString = response.getSourceAsString();
            }
        } catch(ElasticsearchException e) {
            System.out.println(e.getDetailedMessage());
        } catch (java.io.IOException ex){
            System.out.println(ex.getLocalizedMessage());
        }
        return sourceAsString;
    }

    //bulky insert Jason data
    public boolean BulkyInsertData(String index, List<Map<String, String>> dataMapList){
        if(dataMapList == null || dataMapList.size() == 0)
            return false;

        BulkRequest request = new BulkRequest();
        for (int i = 0; i < dataMapList.size(); i++) {
            Map<String, String> dataMap = dataMapList.get(i);
            IndexRequest indexRequest = new IndexRequest(index).type("_doc").id(String.valueOf(i + 1)).source(dataMap);
            request.add(indexRequest);
        }

        boolean acknowledged = false;
        try {
            BulkResponse response = client.bulk(request, RequestOptions.DEFAULT);
            if (response.hasFailures() == true) {
                acknowledged = false;
                System.out.println(response.buildFailureMessage());
            } else {
                acknowledged = true;
            }
        } catch(ElasticsearchException e) {
            System.out.println(e.getDetailedMessage());
        } catch (java.io.IOException ex){
            System.out.println(ex.getLocalizedMessage());
        }
        return acknowledged;
    }
}