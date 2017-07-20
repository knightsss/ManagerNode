package com.xlink.tranquility;

import com.xlink.shell.RunShell;
import com.xlink.mongodb.MongodbOperation;
import com.xlink.times.HourTimes;
import com.xlink.zookeerper.ZKCreate;
import com.xlink.peon.PeonTask;
import net.sf.json.JSON;
import net.sf.json.JSONObject;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

/**
 * Created by shifeixiang on 2017/7/5.
 */
public class TranquilityOperation {
    public void updateSchemaRestart(ZooKeeper zk, String zkData, HourTimes hourTimes) throws IOException, Exception{

        //通过zkGetChild在mongodb中获取元数据配置信息  字符串类型
        MongodbOperation mongodbOperation = new MongodbOperation();
        String sourceStr = mongodbOperation.getDocumentString(zkData);

        ////////////////metadata mongodb//////////////更新启动次数及删除记录元数据到mongodb
        String timesMetadataObjectId = Config.MONGODB_TIMES_METADATA_OBJECTID;
        String sourceTimesStr = mongodbOperation.getMetadataTimes(timesMetadataObjectId);


        //判断sourceStr1是否为空，为空则连接mongodb异常
        if (sourceStr.equals("") || sourceTimesStr.equals("") ) {
            System.out.println("get mongodb data error!" + sourceStr);
        }
        else {
            System.out.println("get mongodb content" + sourceStr);
            ///////////////metadata mongodb////////////通过mongodb元数据更新hourTimes
            JSONObject timesMetadataJson = JSONObject.fromObject(sourceTimesStr);
            hourTimes.updateDataSourceObject(timesMetadataJson.getJSONObject("dataSourceObject"),timesMetadataJson.getJSONObject("dataSourceDeleteObject"));

            //根据sourceStr转化成schemajson字符串
            //TranquilitySchemaJson tranquilitySchemaJson = new TranquilitySchemaJson();
            //String schemaJson = tranquilitySchemaJson.getSchemaJson(sourceStr);

            //通过str生成JSONObject,获取dataset 和topicId  TopicId为dataset + cropid拼接
            JSONObject jsonObjectMetaData = JSONObject.fromObject(sourceStr);
            String dataset = jsonObjectMetaData.getString("dataset");
            System.out.println("Operation dataset "+ dataset);
            String topicId = jsonObjectMetaData.getString("topicId");

            //是否删除
            Boolean isDelete = jsonObjectMetaData.getBoolean("isDeleted");
            System.out.println("isDeleted : "+ isDelete);
            //schema配置文件名
            String fileName = topicId +  ".json";
            //dataSource作为topic名字，zookeeper下实例名字 即tranquility -i 后的参数名
            String dataSource =  topicId;
            //判断是否首次启动
            System.out.println("dataSource : "+ dataSource);
            RunShell runShell =  new RunShell();
            Boolean isStartProcess = runShell.isStartProcess(dataSource);
            System.out.println("isStartProcess : "+ isStartProcess);


            //定义时间次数，获取当前时间
            Integer startTimes;
            String currentHourStamp;
            String currentHour = hourTimes.getCurrentHour();
            System.out.println("currentHour : "+ currentHour);
            //是否有删除记录
            Boolean isDeletedDataSource = hourTimes.isDeleteDataSource(dataSource,currentHour);
            System.out.println("isDeletedDataSource : "+ isDeletedDataSource);


            //是否删除任务datasource任务
            if (isDelete){
                //删除任务，调用shell
                runShell.deleteProcess(dataSource);
                //记录当前hour,datasource的删除记录
                hourTimes.setDeleteDataSource(dataSource,currentHour);
                //删除peon,  参数startTimes，dataSource,peonBeforeEightHour

                JSONObject currentTimesJSONObject = hourTimes.getDataSourceObject();
                //获取已经启动的次数，并构造删除peon的url
                Integer currentTimes = hourTimes.getStartTimes(currentTimesJSONObject,dataSource,currentHour);
                System.out.println("currentTimes : "+ currentTimes);
                //停止,提供当前的时间
                PeonTask peonTask = new PeonTask();
                String peonCurrentHour = hourTimes.getPeonCurrentHour();
                peonTask.closePeon(dataSource, peonCurrentHour, currentTimes);

            }
            else {

                JSONObject currentTimesJSONObject = hourTimes.getDataSourceObject();
                //获取已经启动的次数，并构造删除peon的url
                Integer currentTimes = hourTimes.getStartTimes(currentTimesJSONObject,dataSource,currentHour);
                System.out.println("currentTimes : "+ currentTimes);

                //有任务启动，或者有删除记录都新增，并重新配置通知zookeeper重启
                if (isStartProcess || isDeletedDataSource || currentTimes > 1){
                    //非首次启动获取值（新的时间周期初始化0，当前时间周期次数+1）
                    //JSONObject currentTimesJSONObject = hourTimes.getDataSourceObject();
                    startTimes = hourTimes.addCount(currentTimesJSONObject, dataSource, currentHour);
                    currentHourStamp = hourTimes.getCurrentHourStamp();
                    System.out.println("当前时间周期内时间参数 " + startTimes);
                    System.out.println("当前时间周期内时间戳 " + currentHourStamp);
                }
                else{
                    //首次启动默认为0
                    System.out.println("first start...");
                    System.out.println("dataSourceObject " + hourTimes.getDataSourceObject() );
                    hourTimes.addDataSource(dataSource, currentHour);
                    startTimes = 0;
                    currentHourStamp = "0";
                    System.out.println("add ok!");
                }
                //根据sourceStr转化成schemajProtobufson字符串
                TranquilitySchemaProtobuf tranquilitySchemaProtobuf = new TranquilitySchemaProtobuf();
                String schemaJson = tranquilitySchemaProtobuf.getSchemaProtobuf(sourceStr,startTimes,currentHourStamp);



                //将配置信息写入文件
                String filePath = Config.SCHEMA_FILE_PATH + fileName;
                FileOperation fileOperation = new FileOperation();
                fileOperation.writeSchemaJson(schemaJson, filePath);

                //通过fileName判断该进程是否启动
                //RunShell runShell =  new RunShell();
                //Boolean isStartProcess = runShell.isStartProcess(fileName.substring(0,fileName.length()-5));

                //已启动，配置更新，通知zookeeper
                // if (isStartProcess || isDeletedDataSource){
                if (isStartProcess ){
                    //创建实例节点
                    ZKCreate zkCreate = new ZKCreate();
                    zkCreate.createZnode(zk, Config.ZOOKEEPER_UPDATE_PATH + "/"+ dataSource + "/update");
                    System.out.println("create znoe" + Config.ZOOKEEPER_UPDATE_PATH + "/"+ dataSource + "/update");
                    System.out.println("进程已启动,通知zookeeper更新");
                    //删除peon,  参数startTimes，dataSource,peonBeforeEightHour
                    Thread.sleep(5000);

                    //JSONObject currentTimesJSONObject = hourTimes.getDataSourceObject();
                    //获取已经启动的次数，并构造删除peon的url
                    //Integer currentTimes = hourTimes.getStartTimes(currentTimesJSONObject,dataSource,currentHour);
                    //删除上次任务
                    PeonTask peonTask = new PeonTask();
                    String peonCurrentHour = hourTimes.getPeonCurrentHour();
                    peonTask.closePeon(dataSource, peonCurrentHour, currentTimes);

                }
                else{
                    // 未启动，直接启动。
                    //通过shell启动tranquility
                    System.out.println("未启动该进程,启动进程...");
                    runShell.startProcess(fileName);
                }
            }
            /////////////////metadata mongodb/////////////////启动次数及删除记录写入mongodb
            mongodbOperation.setMetadataTimes(timesMetadataObjectId, hourTimes);
        }

    }
}
