package com.xlink.mongodb;

/**
 * Created by shifeixiang on 2017/7/5.
 */


import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.xlink.tranquility.Config;
import net.sf.json.JSONObject;
import org.bson.Document;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.xlink.times.HourTimes;
import org.bson.types.ObjectId;

public class MongodbOperation {

    public String getDocumentString(String objectId) {
        //创建对象
        try {
            MongoConnection mongoConnection = new MongoConnection();
            //获取客户端连接
            MongoClient mongoClient = mongoConnection.getMongoClient(Config.MONGODB_HOST, Config.MONGODB_PORT, Config.MONGODB_USER_NAME, Config.MONGODB_USER_PASSWORD, Config.MONGODB_DB_NAME);
            //获取集合
            MongoCollection<Document> collection = mongoConnection.getCollection(mongoClient, Config.MONGODB_DB_NAME, Config.MONGODB_TABLE_NAME);
//        System.out.println(collection);
            //获取文档数据
//        String objectId = "5955fc503d484fe323ff60b3";
            //查询对应objectid的元数据结构
//        Document document = mongoConnection.getDocument(collection, objectId);
            Document document = new Document();
//        ArrayList<Document> arrayList = new ArrayList<Document>();

            if (objectId == "") {
                System.out.println("not found objectid!");
                //            FindIterable<Document> findIterable = collection.find();
            } else {
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
                }
            }
            //关闭mongodb client
            mongoConnection.closeClient(mongoClient);
            String dataset;
            JSONObject jsonObjectMongodb = new JSONObject();
            try{
                dataset = document.getString("dataset");
            }catch(Exception e){
                dataset = "";
            }

            jsonObjectMongodb.put("dataset", dataset);
            String corpId,productId;
            try{
                corpId = document.get("corpId").toString();
                jsonObjectMongodb.put("corpId", document.get("corpId"));
            }catch (Exception e){
                jsonObjectMongodb.put("corpId", "");
                corpId = "";
            }

            try{
                productId = document.get("productId").toString();
                jsonObjectMongodb.put("productId", document.get("productId"));
            }catch (Exception e){
                jsonObjectMongodb.put("productId", "");
                productId = "";
            }

            jsonObjectMongodb.put("columns", document.get("columns"));
            jsonObjectMongodb.put("isDeleted", document.get("isDeleted"));
            String topicId = dataset + "_" + corpId;
            jsonObjectMongodb.put("topicId", topicId);

            return jsonObjectMongodb.toString();
        }
        catch (Exception e1){
            System.out.println("mongodb error!");
            System.out.println(e1.getMessage());
            return "";
        }
    }

    public void setMetadataTimes(String objectId, HourTimes hourTimes){
        try {
            MongoConnection mongoConnection = new MongoConnection();
            //获取客户端连接
            MongoClient mongoClient = mongoConnection.getMongoClient(Config.MONGODB_HOST, Config.MONGODB_PORT, Config.MONGODB_USER_NAME, Config.MONGODB_USER_PASSWORD, Config.MONGODB_DB_NAME);
            //获取集合
            MongoCollection<Document> collection = mongoConnection.getCollection(mongoClient, Config.MONGODB_DB_NAME, Config.MONGODB_TIMES_TABLE_NAME);

            //构造objectID
            BasicDBObject queryObject = new BasicDBObject("_id", new ObjectId(objectId));

            Document document = new Document(queryObject).
                    append("dataSourceObject",hourTimes.getDataSourceObject().toString()).
                    append("dataSourceDeleteObject",hourTimes.getDeleteDataSourceObject().toString());

            //存在则更新，不存在则新增
            if (collection.find(queryObject).iterator().hasNext()){
                //更新
                FindIterable<Document> findIterable = collection.find(queryObject);
                collection.updateOne(queryObject, new Document("$set",document));
                System.out.println("update finish !");
            }else{
                //插入数据
                collection.insertOne(document);
                ObjectId id = (ObjectId)document.get( "_id" );
                System.out.println(id.toString() + " insert finish!");
            }
            //关闭mongodb client
            mongoConnection.closeClient(mongoClient);
        }
        catch (Exception e1){
            System.out.println("set times metadata mongodb error!");
            System.out.println(e1.getMessage());
        }
    }


    public String getMetadataTimes(String objectId) {
        //创建对象
        try {
            MongoConnection mongoConnection = new MongoConnection();
            //获取客户端连接
            MongoClient mongoClient = mongoConnection.getMongoClient(Config.MONGODB_HOST, Config.MONGODB_PORT, Config.MONGODB_USER_NAME, Config.MONGODB_USER_PASSWORD, Config.MONGODB_DB_NAME);
            //获取集合
            MongoCollection<Document> collection = mongoConnection.getCollection(mongoClient, Config.MONGODB_DB_NAME, Config.MONGODB_TIMES_TABLE_NAME);

            Document document = new Document();

            if (objectId == "") {
                System.out.println("not found objectid!");
                //            FindIterable<Document> findIterable = collection.find();
            } else {
                //构造查询条件
                BasicDBObject queryObject = new BasicDBObject("_id", new ObjectId(objectId));
                //查询
                FindIterable<Document> findIterable = collection.find(queryObject);

                MongoCursor<Document> mongoCursor = findIterable.iterator();
                //遍历
                while (mongoCursor.hasNext()) {
                    document = mongoCursor.next();
                }
            }
            //关闭mongodb client
            mongoConnection.closeClient(mongoClient);

            JSONObject jsonObjectMongodb = new JSONObject();
            jsonObjectMongodb.put("dataSourceObject",document.get("dataSourceObject"));
            jsonObjectMongodb.put("dataSourceDeleteObject",document.get("dataSourceDeleteObject"));

            return jsonObjectMongodb.toString();
        }
        catch (Exception e1){
            System.out.println("times metadata mongodb error!");
            System.out.println(e1.getMessage());
            return "";
        }
    }

}
