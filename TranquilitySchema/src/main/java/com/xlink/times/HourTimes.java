package com.xlink.times;

import net.sf.json.JSONObject;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
/**
 * Created by shifeixiang on 2017/7/13.
 */
public class HourTimes {
    //记录启动次数对象
    JSONObject dataSourceObject;
    //记录删除时间周期内是否删除记录对象
    JSONObject dataSourceDeleteObject;
    //构造函数并初始化对象
    public HourTimes(){
        JSONObject dataSourceObject = new JSONObject();
        JSONObject dataSourceDeleteObject = new JSONObject();
        this.dataSourceObject = dataSourceObject;
        this.dataSourceDeleteObject =  dataSourceDeleteObject;
    }

//    //设置两个变量
//    public void updateDataSourceObject(JSONObject dataSourceObject, JSONObject dataSourceDeleteObject){
//        this.dataSourceObject = dataSourceObject;
//        this.dataSourceDeleteObject = dataSourceDeleteObject;
//    }

    //设置两个变量,为空怎设置空对象
    public void updateDataSourceObject(JSONObject dataSourceObject, JSONObject dataSourceDeleteObject){
        if (dataSourceObject.size() == 0){
            System.out.println("dataSourceObject is null");
            this.dataSourceObject = JSONObject.fromObject("{}");
        }else{
            this.dataSourceObject = dataSourceObject;
        }
        if (dataSourceDeleteObject.size() == 0){
            System.out.println("dataSourceDeleteObject is null");
            this.dataSourceDeleteObject = JSONObject.fromObject("{}");
        }else{
            this.dataSourceDeleteObject = dataSourceDeleteObject;
        }
    }

    public String getCurrentHourStamp() throws Exception{
        Date now = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");//可以方便地修改日期格式

        String dateStr = dateFormat.format( now );
        String[] arrayResultStr =new String[]{};
        arrayResultStr = dateStr.split(":");
        String currentHourStr = arrayResultStr[0]+ ":00:00";

//        Date date2 = new Date();
        //注意format的格式要与日期String的格式相匹配
//        DateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date dateCurrentHour = dateFormat.parse(dateStr);
        System.out.println(dateCurrentHour);

        String currentHourStamp = String.valueOf(dateFormat.parse(currentHourStr).getTime());
        return currentHourStamp;
    }
    //    JSONObject hourTimesObject;
    //获取当前小时时间
    public String getCurrentHour(){
        Calendar c = Calendar.getInstance();//可以对每个时间域单独修改
        Integer year = c.get(Calendar.YEAR);
        Integer month = c.get(Calendar.MONTH);
        Integer date = c.get(Calendar.DATE);
        Integer hour = c.get(Calendar.HOUR_OF_DAY);
        Integer minute = c.get(Calendar.MINUTE);
        Integer second = c.get(Calendar.SECOND);

        String currentHour = year.toString() + "_" + month.toString() +  "_" +  date.toString() + "_" + hour.toString();
        return currentHour;
    }
    //首次创建，默认值为1
    public void addDataSource(String dataSource, String currentHour){
        JSONObject hourTimesObject = new JSONObject();
        hourTimesObject.put(currentHour,1);
        this.dataSourceObject.put(dataSource,hourTimesObject);
    }
    //每个时间周期内重新启动，value新增1，并返回当前的启动次数值
    public Integer addCount(JSONObject dataSourceObject, String dataSource, String currentHour){
        Integer value;
        JSONObject hourTimesObject = dataSourceObject.getJSONObject(dataSource);
        try{
            value = hourTimesObject.getInt(currentHour) + 1;
        }catch (Exception e){
            value = 2;
        }
        JSONObject newHourTimesObject = new JSONObject();
        newHourTimesObject.put(currentHour,value);
        this.dataSourceObject.put(dataSource,newHourTimesObject);
        return value;
    }

    //获取启动次数
    public Integer getStartTimes(JSONObject dataSourceObject, String dataSource, String currentHour){
        Integer startTimes;
        try{
            startTimes = dataSourceObject.getJSONObject(dataSource).getInt(currentHour);
        }catch (Exception e){
            startTimes = 1;
        }
        return startTimes;
    }

    public JSONObject getDataSourceObject(){
        return dataSourceObject;
    }

    //当前时间
    public String getPeonCurrentHour() throws Exception{
        Date now = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH");//可以方便地修改日期格式
        String dateHourStr = dateFormat.format( now );
        dateHourStr = dateHourStr.replace(" ","T") + ":00:00.000Z";
        return dateHourStr;
    }
    //8个小时前的时间
    public String getPeonBeforeEightHour() throws Exception{
        Calendar c = Calendar.getInstance();
        c.add(Calendar.HOUR_OF_DAY, -8);
        //转换成字符串
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH");
        String dateHourStr = dateFormat.format(c.getTime());
        dateHourStr = dateHourStr.replace(" ","T") + ":00:00.000Z";
        return dateHourStr;
    }

    //处理删除任务的记录
    public JSONObject getDeleteDataSourceObject(){
        return dataSourceDeleteObject;
    }
    public void setDeleteDataSource(String dataSource, String currentHour){

        this.dataSourceDeleteObject.put(dataSource + "_" + currentHour, true);
    }
    public Boolean isDeleteDataSource(String dataSource, String currentHour){
        Boolean flag;
        try{
            flag = this.dataSourceDeleteObject.getBoolean(dataSource + "_" + currentHour);
        }
        catch (Exception e){
            flag = false;
        }
        return flag;
    }
}
