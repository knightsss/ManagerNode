package com.xlink.tranquility; /**
 * Created by shifeixiang on 2017/7/4.
 */

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import java.io.IOException;

public class TranquilitySchemaJson {
    //public static void main(String[] args) throws IOException{
    public String  getSchemaJson(String sourceStr) {

        JSONObject jsonObject = JSONObject.fromObject(sourceStr);
//        System.out.println("是否删除" + jsonObject.get("isDeleted").getClass());
        Boolean isDeleted = jsonObject.getBoolean("isDeleted");
//        System.out.println("是否删除" + isDeleted.getClass());
        if (isDeleted){
            //删除任务
            System.out.println("删除任务");
            return "";
        }
        else {
            //获取dattasource corp_id product_id
            String dataSource = jsonObject.getString("dataSet");
            String corp_id = jsonObject.getString("corpId");
//        String product_id = jsonObject.getString("product_id");
            String topic_id = jsonObject.getString("topicId");

            //配置表再转json
            //获取配置信息
//        JSONObject conf = jsonObject.getJSONObject("config");
//        String segmentGranularity = conf.getString("segmentGranularity");
//        String queryGranularity = conf.getString("queryGranularity");
//        String partitions = conf.getString("partitions");
//        String replicants = conf.getString("replicants");


            //columns转换成JsonArray
            JSONArray columns = jsonObject.getJSONArray("columns");


        /*   定义schema json的名字  */
            //定义总结构
            JSONObject schemaJson = new JSONObject();
            //dataSources
            JSONObject dataSources = new JSONObject();
            //dataSet
            JSONObject dataSet = new JSONObject();
            //定义spec
            JSONObject spec = new JSONObject();
            //定义dataSchema
            JSONObject dataSchema = new JSONObject();
            //定义parser
            JSONObject parser = new JSONObject();
            //定义parseSpec
            JSONObject parseSpec = new JSONObject();
            //定义timestampSpec
            JSONObject timestampSpec = new JSONObject();
            //定义dimensionsSpec
            JSONObject dimensionsSpec = new JSONObject();
            //定义维度列 JSONArray
            JSONArray dimensions = new JSONArray();
            //定义granularitySpec
            JSONObject granularitySpec = new JSONObject();

            //定义metricsSpec JSONArray
            JSONArray metricsSpec = new JSONArray();
            //定义ioConfig
            JSONObject ioConfig = new JSONObject();
            //定义tuningConfig
            JSONObject tuningConfig = new JSONObject();
            //properties
            JSONObject properties = new JSONObject();
            //propertiesConf
            JSONObject propertiesConf = new JSONObject();


            //构造度量 count json
            JSONObject countMetric = new JSONObject();
            //添加count统计(默认)
            countMetric.put("type", "count");
            countMetric.put("name", "count");
            metricsSpec.add(countMetric);

            //遍历字段
            for (int i = 0; i < columns.size(); i++) {
                JSONObject column = columns.getJSONObject(i);
                //字段名
                String column_name = column.getString("name");
                //字段类型
                String column_type = column.getString("type");
                //是否维度列
                Boolean is_dimension = column.getBoolean("isDimension");
                //度量类型sum等
                JSONArray metrics = column.getJSONArray("metrics");


                //判断是否维度列
                if (is_dimension) {
                    //维度信息
                    dimensions.add(column_name);
                }
                if (metrics.size() > 0) {
                    //遍历度量的指标
                    for (int j = 0; j < metrics.size(); j++) {
                        //构造度量json
                        JSONObject subMetric = new JSONObject();
                        String metric = metrics.getString(j);
                        if (column_type.equals("Float") || column_type.equals("Double")) {
                            if (metric.equals("sum") || metric.equals("max")) {
                                subMetric.put("type", "doubleSum");
                            }
                        }
                        if (column_type.equals("Int") || column_type.equals("Long")) {
                            if (metric.equals("sum") || metric.equals("max")) {
                                subMetric.put("type", "longSum");
                            }
                        }
                        //合成metricName
                        //String metricName = column_name + "_" + column_type + "_" + metric;
                        String metricName = column_name;
                        subMetric.put("name", metricName);
                        subMetric.put("fieldName", metricName);
                        //metricsSpec
                        metricsSpec.add(subMetric);
                    }
                    System.out.println("column_name is : " + column_name);
                    System.out.println("column_type is : " + column_type);
                }
            }

            //dimensionsSpec
            dimensionsSpec.put("dimensions", dimensions);
            //timestampSpec
            timestampSpec.put("column", "createTime");
            timestampSpec.put("format", "auto");
            //parseSpec
            parseSpec.put("timestampSpec", timestampSpec);
            parseSpec.put("dimensionsSpec", dimensionsSpec);
            parseSpec.put("format", "json");
            //parser
            parser.put("type", "String");
            parser.put("parseSpec", parseSpec);
            //granularitySpec
            granularitySpec.put("type", "uniform");
            granularitySpec.put("segmentGranularity", "hour");
            granularitySpec.put("queryGranularity", "none");
            //metricsSpec
            //dataSchema
            dataSchema.put("dataSource", dataSource);
            dataSchema.put("parser", parser);
            dataSchema.put("granularitySpec", granularitySpec);
            dataSchema.put("metricsSpec", metricsSpec);
            //ioConfig
            ioConfig.put("type", "realtime");
            //tuningConfig
            tuningConfig.put("type", "realtime");
            tuningConfig.put("maxRowsInMemory", "100000");
            tuningConfig.put("intermediatePersistPeriod", "PT10M");
            tuningConfig.put("windowPeriod", "PT10M");
            //spec
            spec.put("dataSchema", dataSchema);
            spec.put("ioConfig", ioConfig);
            spec.put("tuningConfig", tuningConfig);
            //properties
            properties.put("task.partitions", "1");
            properties.put("task.replicants", "1");
            properties.put("topicPattern", topic_id);
            //dataSet
            dataSet.put("spec", spec);
            dataSet.put("properties", properties);

            //propertiesConf
            propertiesConf.put("zookeeper.connect", Config.PROPERTIES_ZOOKEEPER_CONTENT);
            propertiesConf.put("druid.discovery.curator.path", Config.PROPERTIES_DRUID_DISCOVERY_CURATOR_PATH);
            propertiesConf.put("druid.selectors.indexing.serviceName", Config.PROPERTIES_DRUID_SELECTORS_INDEXING_SERVICENAME);
            propertiesConf.put("commit.periodMillis", Config.PROPERTIES_COMMIT_PERIODMILLIS);
            propertiesConf.put("consumer.numThreads", Config.PROPERTIES_CONSUMER_NUMTHREADS);
            propertiesConf.put("kafka.zookeeper.connect", Config.PROPERTIES_KAFKA_ZOOKEEPER_CONTENT);
            propertiesConf.put("kafka.group.id", Config.PROPERTIES_KAFKA_GROUP_ID);
            //dataSources
            dataSources.put("" + dataSource + "", dataSet);
            //schema配置json
            schemaJson.put("dataSources", dataSources);
            schemaJson.put("properties", propertiesConf);


//        System.out.println("dimensions: " + dimensions);
//        System.out.println("dimensionsSpec: " + dimensionsSpec);
//        System.out.println("timestampSpec: " + timestampSpec);
//        System.out.println("parseSpec: " + parseSpec);
//        System.out.println("parser: " + parser);
//        System.out.println("granularitySpec: " + granularitySpec);
//        System.out.println("metricsSpec: " + metricsSpec);
//        System.out.println("dataSchema: " + dataSchema);
//        System.out.println("spec: " + spec);
//        System.out.println("dataSourceJson: " + dataSources);
//        System.out.println("dataSources: " + dataSources);
//        System.out.println("schemaJson: " + schemaJson);
//
//        System.out.println(schemaJson.getClass());
//        System.out.println(schemaJson.toString());
            return schemaJson.toString();
        }
    }
}
