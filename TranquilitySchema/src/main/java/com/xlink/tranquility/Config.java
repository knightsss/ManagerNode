package com.xlink.tranquility;

/**
 * Created by shifeixiang on 2017/7/5.
 */
public class Config {
    //mongodb本地
//    public static final String MONGODB_HOST = "127.0.01";
//    public static final Integer MONGODB_PORT = 27017;
//    public static final String MONGODB_DB_NAME = "Druid";
//    public static final String MONGODB_TABLE_NAME = "ManageMetadata";
//    public static final String MONGODB_USER_NAME = "druid";
//    public static final String MONGODB_USER_PASSWORD = "druid";
    //mongodb监控机
    public static final String MONGODB_HOST = "18.0.13.159";
    public static final Integer MONGODB_PORT = 27017;
    public static final String MONGODB_DB_NAME = "Druid";
    public static final String MONGODB_TABLE_NAME = "QueryMetadata";
    public static final String MONGODB_TIMES_TABLE_NAME = "ManageMetadata";
    public static final String MONGODB_USER_NAME = "druid";
    public static final String MONGODB_USER_PASSWORD = "druid";
    //mongodb2
//    public static final String MONGODB_HOST = "192.168.2.59";
//    public static final Integer MONGODB_PORT = 27017;
//    public static final String MONGODB_DB_NAME = "RestExpress";
//    public static final String MONGODB_TABLE_NAME = "QueryMetadataEntity";
//    public static final String MONGODB_USER_NAME = "RestExpressRW";
//    public static final String MONGODB_USER_PASSWORD = "Rest_Express";

    //kafka
    public static final String PROPERTIES_ZOOKEEPER_CONTENT = "localhost:2181";
    public static final String PROPERTIES_DRUID_DISCOVERY_CURATOR_PATH = "/druid/discovery";
    public static final String PROPERTIES_DRUID_SELECTORS_INDEXING_SERVICENAME = "druid/overlord";
    public static final String PROPERTIES_COMMIT_PERIODMILLIS = "15000";
    public static final String PROPERTIES_CONSUMER_NUMTHREADS = "2";
    public static final String PROPERTIES_KAFKA_ZOOKEEPER_CONTENT = "localhost:2181";
    public static final String PROPERTIES_KAFKA_GROUP_ID = "tranquility-kafka";

    //schema

    //zookeeper
    public static final String ZOOKEEPER_HOST = "localhost";
    public static final String ZOOKEEPER_MONITOR_PATH = "/tranquility/metadata/query";
    public static final String ZOOKEEPER_UPDATE_PATH = "/tranquility/instances";
    public static final Integer ZOOKEEPER_TIME_OUT = 5000;

    //shell  processPath
    public static final String TRANQUILITY_SHELL = "/mnt/xlink/modules/druid-io/tranquility/bin/tranquility";
    public static final String SCHEMA_FILE_PATH = "/mnt/xlink/modules/xlink-io/ManageNode/";
    public static final String KAFKA_CONGIG_HOSTPORT = "127.0.0.1:2181";

    //tranquility log日志

    //mongodb ObjectId
    public static String MONGODB_TIMES_METADATA_OBJECTID = "596f14b6106ce51b00199992";

}
