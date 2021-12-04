package com.bigdata.springboot.akka;

import com.bigdata.springboot.bean.ElasticSearchHelper;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class ElasticSearchApiTests {

    /*@Autowired
    private Environment env;
    private String host = env.getProperty("esserver.host");
    private int port1  = Integer.valueOf(env.getProperty("esserver.port1"));
    private int port2  = Integer.valueOf(env.getProperty("esserver.port2"));*/

    @Value("${esserver.host}")
    private String host;

    @Value("${esserver.port1}")
    private int port1;

    @Value("${esserver.port2}")
    private int port2;

    private ElasticSearchHelper esHelper;

    @BeforeClass
    public void Setup() {
        try {
            esHelper = new ElasticSearchHelper(host, port1, port2);
        }catch(Exception ex){
        }
        finally {
        }
    }

    @AfterClass
    public void Close() {
        esHelper.Close();
        esHelper = null;
    }

    @Test(groups = "groupCorrect")
    public void CreatedIndex(String index){
        boolean successful = esHelper.CreateIndex(index);
        System.out.println("index created:" + successful);
        Assert.assertTrue(successful);
    }

    @Test(groups = "groupCorrect")
    public void DeleteIndex(String index){
        boolean successful = esHelper.DeleteIndex(index);
        System.out.println("index deleted:" + successful);
        Assert.assertTrue(successful);
    }

    @Test(groups = "groupCorrect")
    public void InsertText(String index, String content){
        boolean successful = esHelper.InsertTextData(index, content);
        System.out.println("content inserted:" + successful);
        Assert.assertTrue(successful);
    }

    @Test(groups = "groupCorrect")
    public void InsertText(String index, String id, String content){
        boolean successful = esHelper.InsertTextData(index, id, content);
        System.out.println("content inserted:" + successful);
        Assert.assertTrue(successful);
    }
}
