package com.bigdata.springboot.akka;

import com.bigdata.springboot.model.Source;
import com.bigdata.springboot.model.StreamerModel;
import com.bigdata.springboot.model.User;
import com.google.gson.Gson;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.restassured.path.json.JsonPath;
import io.restassured.response.Response;
import org.apache.commons.lang3.StringUtils;
//import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static io.restassured.RestAssured.get;
import static io.restassured.RestAssured.given;
import static io.restassured.RestAssured.post;
import static io.restassured.path.json.JsonPath.from;
import static org.hamcrest.Matchers.*;

public class StreamControllerTests {

    private Gson gson = new Gson();

    @BeforeClass
    public void setUP(){
        //指定 URL 和端口号
        RestAssured.baseURI = "http://localhost/";
        RestAssured.port = 8080;
    }

    @Test(groups = "groupCorrect")
    public void CreateIndex_KafkaNormal(){
        Map<String, String> config = new HashMap<>();
        config.put("groupid", "group1");
        config.put("topic", "first");
        Source source = new Source("kafka", "192.168.137.186:9092", config);
        User user = new User("user1");
        StreamerModel model = new StreamerModel("streamer1", source, "", user);
        Response response =
                given().
                        accept(ContentType.JSON).
                        contentType(ContentType.JSON).
                        body(gson.toJson(model)).
                        when().
                        post("/api/streamer/create_index").
                        thenReturn();
        String status_code = response.jsonPath().get("status_code");
        String status = response.jsonPath().get("status");
        Assert.assertEquals(status_code, "200");
        Assert.assertEquals(status, "success");
    }

    @Test(groups = "groupAbnormal")
    public void CreateIndex_EmptyIndex() {
        Map<String, Object>  map = new HashMap<>();
        map.put("index_name", "");
        Response response =
                given().
                        accept(ContentType.JSON).
                        contentType(ContentType.JSON).
                        body(gson.toJson(map)).
                        when().
                        post("/api/streamer/create_index").
                        thenReturn();
        String status_code = response.jsonPath().get("status_code");
        String status = response.jsonPath().get("status");
        Assert.assertEquals(status_code, "400");
        Assert.assertEquals(status, "failed");
    }

    @Test(groups = "groupAbnormal")
    public void CreateIndex_EmptySource() {
        Map<String, String>  map = new HashMap<>();
        map.put("index_name", "streamer_test");
        Response response =
                given().
                        accept(ContentType.JSON).
                        contentType(ContentType.JSON).
                        body(gson.toJson(map)).
                        when().
                        post("/api/streamer/create_index").
                        thenReturn();
        String status_code = response.jsonPath().get("status_code");
        String status = response.jsonPath().get("status");
        Assert.assertEquals(status_code, "400");
        Assert.assertEquals(status, "failed");
    }

    @Test(groups = "groupAbnormal")
    public void CreateIndex_InvalidSourceType() {
        Map<String, String> config = new HashMap<>();
        config.put("groupid", "group1");
        config.put("topic", "first");
        Source source = new Source("unknownSourceType", "linux1:9092", config);
        User user = new User("user1");
        StreamerModel model = new StreamerModel("streamer1", source, "", user);
        Response response =
                given().
                        accept(ContentType.JSON).
                        contentType(ContentType.JSON).
                        body(gson.toJson(model)).
                        when().
                        post("/api/streamer/create_index").
                        thenReturn();
        String status_code = response.jsonPath().get("status_code");
        String status = response.jsonPath().get("status");
        Assert.assertEquals(status_code, "400");
        Assert.assertEquals(status, "failed");
    }

    @Test(groups = "groupAbnormal")
    public void CreateIndex_NullTopic() {
        Map<String, String> config = new HashMap<>();
        config.put("groupid", "group1");
        //config.put("topic", "first");
        Source source = new Source("kafka", "linux1:9092", config);
        User user = new User("user1");
        StreamerModel model = new StreamerModel("streamer1", source, "", user);
        Response response =
                given().
                        accept(ContentType.JSON).
                        contentType(ContentType.JSON).
                        body(gson.toJson(model)).
                        when().
                        post("/api/streamer/create_index").
                        thenReturn();
        String status_code = response.jsonPath().get("status_code");
        String status = response.jsonPath().get("status");
        Assert.assertEquals(status_code, "400");
        Assert.assertEquals(status, "failed");
    }

    @Test(groups = "groupAbnormal")
    public void CreateIndex_EmptyTopic() {
        Map<String, String> config = new HashMap<>();
        config.put("groupid", "group1");
        config.put("topic", "");
        Source source = new Source("kafka", "linux1:9092", config);
        User user = new User("user1");
        StreamerModel model = new StreamerModel("streamer1", source, "", user);
        Response response =
                given().
                        accept(ContentType.JSON).
                        contentType(ContentType.JSON).
                        body(gson.toJson(model)).
                        when().
                        post("/api/streamer/create_index").
                        thenReturn();
        String status_code = response.jsonPath().get("status_code");
        String status = response.jsonPath().get("status");
        Assert.assertEquals(status_code, "400");
        Assert.assertEquals(status, "failed");
    }

    @Test(groups = "groupCorrect")
    public void CreateIndex_CsvNormal() {
        Map<String, String> config = new HashMap<>();
        Source source = new Source("csv", "E:/temp/sample.csv", config);
        User user = new User("user1");
        StreamerModel model = new StreamerModel("streamer1", source, "", user);
        Response response =
                given().
                        accept(ContentType.JSON).
                        contentType(ContentType.JSON).
                        body(gson.toJson(model)).
                        when().
                        post("/api/streamer/create_index").
                        thenReturn();
        String status_code = response.jsonPath().get("status_code");
        String status = response.jsonPath().get("status");
        Assert.assertEquals(status_code, "200");
        Assert.assertEquals(status, "success");
    }

    @Test(groups = "groupAbnormal")
    public void CreateIndex_CsvNotExist() {
        Map<String, String> config = new HashMap<>();
        Source source = new Source("csv", "K:/temp/nofile.csv", config);
        User user = new User("user1");
        StreamerModel model = new StreamerModel("streamer1", source, "", user);
        Response response =
                given().
                        accept(ContentType.JSON).
                        contentType(ContentType.JSON).
                        body(gson.toJson(model)).
                        when().
                        post("/api/streamer/create_index").
                        thenReturn();
        String status_code = response.jsonPath().get("status_code");
        String status = response.jsonPath().get("status");
        Assert.assertEquals(status_code, "500");
        Assert.assertEquals(status, "internal error");
    }

    @Test(groups = "groupCorrect")
    public void StartStreamer_Normal(){
        Map<String, String> map = new HashMap<String, String>();
        map.put("streamerId", "streamer1");
        Response response =
                given().
                        accept(ContentType.JSON).
                        contentType(ContentType.JSON).
                        body(gson.toJson(map)).
                        when().
                        post("/api/streamer/start").
                        thenReturn();
        String status_code = response.jsonPath().get("status_code");
        String status = response.jsonPath().get("status");
        Assert.assertEquals(status_code, "200");
        Assert.assertEquals(status, "success");
    }

    @Test(groups = "groupAbnormal")
    public void StartStreamer_EmptyStreamerId() {
        Map<String, Object>  map = new HashMap<>();
        map.put("streamerId", "");
        Response response =
                given().
                        accept(ContentType.JSON).
                        contentType(ContentType.JSON).
                        body(gson.toJson(map)).
                        when().
                        post("/api/streamer/start").
                        thenReturn();
        String status_code = response.jsonPath().get("status_code");
        String status = response.jsonPath().get("status");
        Assert.assertEquals(status_code, "400");
        Assert.assertEquals(status, "failed");
    }

    @Test(groups = "groupCorrect")
    public void StopStreamer_Normal(){
        Map<String, String> map = new HashMap<String, String>();
        map.put("streamerId", "streamer1");
        Response response =
                given().
                        accept(ContentType.JSON).
                        contentType(ContentType.JSON).
                        body(gson.toJson(map)).
                        when().
                        post("/api/streamer/stop").
                        thenReturn();
        String status_code = response.jsonPath().get("status_code");
        String status = response.jsonPath().get("status");
        Assert.assertEquals(status_code, "200");
        Assert.assertEquals(status, "success");
    }

    @Test(groups = "groupAbnormal", enabled = false)
    public void StopStreamer_EmptyInput(){
        Response response = given().post("/api/streamer/stop").thenReturn();
        int statusCode = response.getStatusCode();
        Assert.assertEquals(statusCode, 400);
        response.body().prettyPrint();
        String successCode = response.jsonPath().get("status");
        Assert.assertEquals("failed", successCode, "OPERATION_FAILED");
    }

    @Test(groups = "groupAbnormal")
    public void StopStreamer_EmptyStreamerId() {
        Map<String, Object>  map = new HashMap<>();
        map.put("streamerId", "");
        Response response =
                given().
                        accept(ContentType.JSON).
                        contentType(ContentType.JSON).
                        body(gson.toJson(map)).
                        when().
                        post("/api/streamer/stop").
                        thenReturn();
        String status_code = response.jsonPath().get("status_code");
        String status = response.jsonPath().get("status");
        Assert.assertEquals(status_code, "400");
        Assert.assertEquals(status, "failed");
    }

    @Test(groups = "groupCorrect", dependsOnMethods = { "CreateIndex_KafkaNormal" })
    public void DeleteStreamer_Normal(){
        Map<String, String> map = new HashMap<String, String>();
        map.put("streamerId", "streamer1");
        Response response =
                given().
                        accept(ContentType.JSON).
                        contentType(ContentType.JSON).
                        body(gson.toJson(map)).
                        when().
                        post("/api/streamer/delete").
                        thenReturn();
        String status_code = response.jsonPath().get("status_code");
        String status = response.jsonPath().get("status");
        Assert.assertEquals(status_code, "200");
        Assert.assertEquals(status, "success");
    }

    @Test(groups = "groupAbnormal")
    public void DeleteStreamer_EmptyStreamerId() {
        Map<String, Object>  map = new HashMap<>();
        map.put("streamerId", "");
        Response response =
                given().
                        accept(ContentType.JSON).
                        contentType(ContentType.JSON).
                        body(gson.toJson(map)).
                        when().
                        post("/api/streamer/delete").
                        thenReturn();
        String status_code = response.jsonPath().get("status_code");
        String status = response.jsonPath().get("status");
        Assert.assertEquals(status_code, "400");
        Assert.assertEquals(status, "failed");
    }

    @Test(groups = "groupCorrect")
    public void ListStreamer(){
        Response response = given().post("/api/streamer/list");
        int statusCode = response.getStatusCode();
        Assert.assertEquals(statusCode, 200);
        JsonPath jp = from(response.body().asString());
        String cpuageStr = jp.getString("cpuage");
        cpuageStr = StringUtils.stripEnd(cpuageStr, "%");
        float cpuage = Float.parseFloat(cpuageStr);
        Assert.assertTrue(cpuage >= 0 && cpuage <= 100);
        String memory = jp.getString("memory");
        Assert.assertNotNull(memory);
    }

    @Test(enabled = false)
    public void testControllerDummy(){
        Response response = get("/User");
        response.prettyPrint();
        response.then().assertThat().body("name", equalTo("Barton"));
        response.then().assertThat().body("age", equalTo(25));
        //Assert.assertTrue();
    }
}
