package com.xlink.tranquility;

import com.xlink.shell.RunShell;
import net.sf.json.JSONObject;

import java.util.Iterator;
import com.xlink.mongodb.MongodbOperation;

/**
 * Created by shifeixiang on 2017/7/20.
 */
public class CheckTranquility {

    public void startHistoryTranquility( String currentHour) {
        MongodbOperation mongodbOperation = new MongodbOperation();
        //初期启动设置为空
        String sourceStr = mongodbOperation.getMetadataTimes(Config.MONGODB_TIMES_METADATA_OBJECTID);

        JSONObject timesMetadataJson = JSONObject.fromObject(sourceStr);
        JSONObject dataSourceObject = timesMetadataJson.getJSONObject("dataSourceObject");
        if (dataSourceObject.size() == 0){
            System.out.println("no history task");
        }else {
            Iterator<String> iterator = dataSourceObject.keys();
            RunShell runShell = new RunShell();
            //遍历
            while (iterator.hasNext()) {
                String key = iterator.next();
                String fileNmae = key + ".json";
                System.out.println("fileNmae is " + fileNmae);
                try {
                    Integer value = dataSourceObject.getJSONObject(key).getInt(currentHour);
                    if (value > 0) {
                        System.out.println(fileNmae + " RUN SHELL");
                        runShell.startProcess(fileNmae);
                    } else {
                        System.out.println(key + "not exists");
                    }
                } catch (Exception e) {
                    System.out.println(key + "not exists");
                }

            }
        }
    }
}
