package com.xlink.mongodb; /**
 * Created by shifeixiang on 2017/6/30.
 */

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.List;

public class MongoConnection {


    public MongoClient getMongoClient(String host, Integer port,String userName, String password, String dataSource) {
        // 连接到 mongodb 服务
        ServerAddress serverAddress = new ServerAddress(host,port);
        List<ServerAddress> addrs = new ArrayList<ServerAddress>();
        addrs.add(serverAddress);

        //MongoCredential.createScramSha1Credential()三个参数分别为 用户名 数据库名称 密码
        MongoCredential credential = MongoCredential.createScramSha1Credential(userName, dataSource, password.toCharArray());
        List<MongoCredential> credentials = new ArrayList<MongoCredential>();
        credentials.add(credential);

        //通过连接认证获取MongoDB连接
        MongoClient mongoClient = new MongoClient(addrs,credentials);
//        MongoClient mongoClient = new MongoClient(host, port);
        return mongoClient;
    }
    // 连接到数据库
    public MongoCollection<Document> getCollection(MongoClient mongoClient, String db_name, String tab_name) {
        MongoDatabase mongoDatabase = mongoClient.getDatabase(db_name);
        //System.out.println("Connect to database successfully");
        MongoCollection<Document> collection = mongoDatabase.getCollection(tab_name);
        return collection;
    }
        //插入数据到数据表中，即集合中
//            Document document = new Document("title","mongodb").append("desc", "database").append("likes", 100);
//            System.out.println(document);
//
//            ArrayList documents = new ArrayList<Document>();
//            documents.add(document);
//            System.out.println(documents);
//            collection.insertMany(documents);
    //关闭连接
    public void closeClient(MongoClient mongoClient){
        mongoClient.close();
        System.out.println("关闭mongodb客户端");
    }
        //查找数据
    public Document getDocument(MongoCollection<Document> collection, String objectId){
        //定义返回Arraylist
        Document document = new Document();
//        ArrayList<Document> arrayList = new ArrayList<Document>();

        if (objectId == ""){
            System.out.println("not found objectid!");
            //            FindIterable<Document> findIterable = collection.find();
        }
        else{
            //        构造ObjectId字符串
            //        String value = "ObjectId(\""+objectId+"\")";
            //        System.out.println(value);
            //通过其他方式查询
            //BasicDBObject queryObject = new BasicDBObject("_id",value);

            //构造查询条件
            BasicDBObject queryObject = new BasicDBObject("_id", new ObjectId(objectId));

            //System.out.println("queryObject : " + queryObject);
            //查询
            FindIterable<Document> findIterable = collection.find(queryObject);

            MongoCursor<Document> mongoCursor = findIterable.iterator();
            //遍历
            while (mongoCursor.hasNext()) {
                document = mongoCursor.next();
//                arrayList.add(mongoCursor.next());
            }
        }
        return document;
    }
    //展示数据
    public void showAllDocument(MongoCollection<Document> collection){
        FindIterable<Document> findIterable = collection.find();
        MongoCursor<Document> mongoCursor = findIterable.iterator();

        while (mongoCursor.hasNext()) {
            System.out.println(mongoCursor.next());
        }
    }
}
