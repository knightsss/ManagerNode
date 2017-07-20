package com.xlink.tranquility;

//import javax.swing.text.Document;
import com.xlink.zookeerper.ZKGetChildren;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;

/**
 * Created by shifeixiang on 2017/7/5.
 */
public class Main {

    public static void main(String[] args) throws InterruptedException, KeeperException, IOException{
        //根据Objectid获取mongodb文档
//        String objectId = "5955fc503d484fe323ff60b3";
//        MongodbOperation mongodbOperation = new MongodbOperation();
//        try{
//            Document document = mongodbOperation.getDocument(objectId);
//        }
//        catch (Exception e1){
//            System.out.println("mongodb error!");
//            System.out.println(e1.getMessage());
//        }


        //启动监控
        ZKGetChildren zkGetChildren = new ZKGetChildren();
        zkGetChildren.start();
        //通过getData监控，有可能被覆盖
//        ZKGetData zkGetData = new ZKGetData();
//        zkGetData.start();
//        try
//        {
//            Thread.currentThread().sleep(10000);//毫秒
//        }
//        catch(Exception e){}
//        zkGetData.stop();
        //配置schemajson

//        System.out.println("schemaJson   ....." + schemaJson);

    }
}
