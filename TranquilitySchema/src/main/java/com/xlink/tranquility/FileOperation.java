package com.xlink.tranquility; /**
 * Created by shifeixiang on 2017/7/4.
 */

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

public class FileOperation {
//    public static void main(String[] args) throws IOException {

    public void writeSchemaJson(String schemaJson, String filePath) throws IOException{
        File f = new File(filePath);
        FileOutputStream fop = new FileOutputStream(f);

        OutputStreamWriter writer = new OutputStreamWriter(fop,"UTF-8");

//        String myStr = "{\"user_info\":{\"spec\":{\"dataSchema\":{\"dataSource\":\"user_info\",\"parser\":{\"type\":\"String\",\"parseSpec\":{\"timestampSpec\":{\"column\":\"create_time\",\"format\":\"auto\"},\"dimensionsSpec\":{\"dimensions\":[\"column1\",\"column2\"]},\"format\":\"json\"}},\"granularitySpec\":{\"type\":\"uniform\",\"segmentGranularity\":\"hour\",\"queryGranularity\":\"none\"},\"metricsSpec\":[{\"type\":\"count\",\"name\":\"count\"},{\"type\":\"doubleSum\",\"name\":\"column2_Float_sum\",\"fieldName\":\"column2_Float_sum\"},{\"name\":\"column2_Float_max\",\"fieldName\":\"column2_Float_max\"},{\"type\":\"longSum\",\"name\":\"column3_Int_sum\",\"fieldName\":\"column3_Int_sum\"}]},\"ioConfig\":{\"type\":\"realtime\"},\"tuningConfig\":{\"type\":\"realtime\",\"maxRowsInMemory\":\"100000\",\"intermediatePersistPeriod\":\"PT10M\",\"windowPeriod\":\"PT10M\"}},\"properties\":{\"task.partitions\":\"1\",\"task.replicants\":\"1\",\"topicPattern\":\"druid_kafka_test2\"}},\"properties\":{\"zookeeper.connect\":\"localhost\",\"druid.discovery.curator.path\":\"/druid/discovery\",\"druid.selectors.indexing.serviceName\":\"druid/overlord\",\"commit.periodMillis\":\"15000\",\"consumer.numThreads\":\"2\",\"kafka.group.id\":\"tranquility-kafka\"}}\n";
//        writer.append("中文输入");
//        writer.append("\r\n");
//        writer.append("English");
        writer.append(schemaJson);
        writer.close();
    }
}
