package com.xlink.peon;

import java.io.IOException;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;


/**
 * Created by shifeixiang on 2017/7/14.
 */
public class PeonTask {

    public void closePeon(String dataSource, String peonCurrentHour,Integer currentTimes) throws Exception{
        //删除peon
        JSONArray runningTasksArray = this.getRunningTasksArray();
        String shutOffUrl = this.getShutOffUrl(runningTasksArray, dataSource, peonCurrentHour,currentTimes);
        System.out.println("getShutOffUrl:" + shutOffUrl);
        if (shutOffUrl.equals("")){
            System.out.println("任务不存在");
        }
        else{
            //发送Post请求删除
            String query = "none";
            String query_all = "{\"query\":\""+query+"\"}";
            sendHttpPost(shutOffUrl, query_all);
//            String ret = sendHttpPost(shutOffUrl, query_all);
//            System.out.println("close peon " + ret);
        }
    }

    public JSONArray getRunningTasksArray() throws Exception{
        String runningTasksUrl = "http://52.80.68.180:7090/druid/indexer/v1/runningTasks";
        String runningTaskResponse = this.getRunningTasks(runningTasksUrl);
        System.out.println(runningTaskResponse);
        JSONArray runningTasksArray = JSONArray.fromObject(runningTaskResponse);
        System.out.println(runningTasksArray);
        return runningTasksArray;
    }

    //构造该peon的关闭url，待任务完成关闭
    public String getShutOffUrl(JSONArray runningTasksArray,String dataSource, String peonCurrentHour, Integer currentTimes) throws Exception{
        Boolean taskFlag = false;
        String shutOffUrl = "";
        //获取正在运行的peon的周期内次数记录
        currentTimes = currentTimes - 1;
        //taskSubId有三部分组成，DataSource（query_test_1007d2ad165a7000），当前时间（2017-07-15T14:00:00.000Z），任务计数器
        String taskSubId = dataSource + "_" + peonCurrentHour + "_" +  currentTimes.toString() + "_0";
        System.out.println("current_sub_id " + taskSubId);

        //遍历任务，获取当前时间的，当前dataSource的参数
        for (int i=0;i<runningTasksArray.size();i++) {
            JSONObject runningTaskJson = runningTasksArray.getJSONObject(i);
            //获取当前dataSource的ID
            if (runningTaskJson.getString("id").contains(taskSubId)) {
                taskFlag = true;
                String taskId = runningTaskJson.getString("id");
                //构造url
                String serviceNameUrl = "http://52.80.68.180:7090/druid/indexer/v1/task/" + taskId;
                String taskHost = runningTaskJson.getJSONObject("location").getString("host");
                String taskPort = runningTaskJson.getJSONObject("location").getString("port");

                System.out.println(serviceNameUrl);
                String detailTaskStr = this.getRunningTasks(serviceNameUrl);
                System.out.println(detailTaskStr);
                JSONObject detailTaskJson = JSONObject.fromObject(detailTaskStr);
                //payload
                String serviceName = detailTaskJson.getJSONObject("payload").getJSONObject("spec").getJSONObject("ioConfig").
                        getJSONObject("firehose").getJSONObject("delegate").getJSONObject("delegate").getString("serviceName");
                System.out.println(serviceName);

                //构造最终停止peon的url
                shutOffUrl = "http://" + taskHost + ":" + taskPort + "/druid/worker/v1/chat/" + serviceName + "/shutdown";
                System.out.println(runningTaskJson.getString("id") + " peon删除!");
            }else{
                System.out.println(runningTaskJson.getString("id") + " peon不删除!");
            }
        }
        return shutOffUrl;
    }

    //构造该peon的关闭url，强制关闭
    public String getShutDownUrl(JSONArray runningTasksArray,String dataSource, String peonCurrentHour, Integer currentTimes) throws Exception{
        Boolean taskFlag = false;
        String shutDownUrl = "";
        currentTimes = currentTimes - 2;
        //taskSubId有三部分组成，DataSource（query_test_1007d2ad165a7000），当前时间（2017-07-15T14:00:00.000Z），任务计数器
        String taskSubId = dataSource + "_" + peonCurrentHour + "_" +  currentTimes.toString() + "_0";
        System.out.println(taskSubId);

        //遍历任务，获取当前时间的，当前dataSource的参数
        for (int i=0;i<runningTasksArray.size();i++) {
            JSONObject runningTaskJson = runningTasksArray.getJSONObject(i);
            //获取当前dataSource的ID
            if (runningTaskJson.getString("id").contains(taskSubId)) {
                taskFlag = true;
                String taskId = runningTaskJson.getString("id");
                //构造url  //构造最终停止peon的url
                shutDownUrl = "http://52.80.68.180:7090/druid/indexer/v1/task/" + taskId + "/shutdown";
            }else{
                System.out.println(taskSubId + " peon不存在!");
            }
        }
        return shutDownUrl;
    }


    public String getRunningTasks(String url) throws Exception{
        try{
            //1.获得一个httpclient对象
            CloseableHttpClient httpclient = HttpClients.createDefault();
            //2.生成一个get请求
            HttpGet httpget = new HttpGet(url);
            CloseableHttpResponse response = null;
            try {
                //3.执行get请求并返回结果
                response = httpclient.execute(httpget);
            } catch (IOException e1) {
                System.out.println(e1.getMessage());
            }
            String result = null;
            try {
                //4.处理结果，这里将结果返回为字符串
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    result = EntityUtils.toString(entity);
                }
            } catch ( IOException e) {
                System.out.println(e.getMessage());
            } finally {
                try {
                    response.close();
                } catch (IOException e) {
                    System.out.println(e.getMessage());
                }
                return result;
            }
        }catch (Exception e){
            System.out.println(e.getMessage());
        }
        return "";
    }


    public static void sendHttpPost(String url, String query_all){
        HttpClient client = new DefaultHttpClient();
        HttpPost post = new HttpPost(url);
        String response = null;
        try {
            StringEntity s = new StringEntity(query_all);
            s.setContentEncoding("UTF-8");
            s.setContentType("application/json");//发送json数据需要设置contentType
            post.setEntity(s);
            HttpResponse res = client.execute(post);
            System.out.println(res);
            System.out.println("返回码 " + res.getStatusLine().getStatusCode());

            //暂停接收返回值
//            if(res.getStatusLine().getStatusCode() == HttpStatus.SC_OK){
//                HttpEntity entity = res.getEntity();
//                String result = EntityUtils.toString(res.getEntity());// 返回json格式：
//                System.out.println(result);
//                try{
//                    JSONArray response1 = JSONArray.fromObject(result);
//                    response = response1.toString();
//                }catch (Exception e){
//                    JSONObject response2 = JSONObject.fromObject(result);
//                    response = response2.toString();
//                }
//            }
        } catch (Exception e) {
            System.out.println("post 异常");
        }
//        return response;
    }

    //请求url返回字符串，空字符串时表示是请求失败。
    public String getAllRunningTasks(String url) throws Exception{
        try{
            //1.获得一个httpclient对象
            CloseableHttpClient httpclient = HttpClients.createDefault();
            //2.生成一个get请求
            HttpGet httpget = new HttpGet(url);
            CloseableHttpResponse response = null;
            try {
                //3.执行get请求并返回结果
                response = httpclient.execute(httpget);
            } catch (IOException e1) {
                System.out.println(e1.getMessage());
            }
            String result = null;
            try {
                //4.处理结果，这里将结果返回为字符串
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    result = EntityUtils.toString(entity);
                }
            } catch ( IOException e) {
                System.out.println(e.getMessage());
            } finally {
                try {
                    response.close();
                } catch (IOException e) {
                    System.out.println(e.getMessage());
                }
                return result;
            }
        }catch (Exception e){
            System.out.println(e.getMessage());
        }
        return "";
    }
}
