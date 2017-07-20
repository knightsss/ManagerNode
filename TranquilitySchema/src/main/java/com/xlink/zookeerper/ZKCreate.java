package com.xlink.zookeerper;

/**
 * Created by shifeixiang on 2017/7/11.
 */
import java.io.IOException;

import com.xlink.tranquility.Config;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;

public class ZKCreate {
    // create static instance for zookeeper class.
    private static ZooKeeper zk;

    // create static instance for ZooKeeperConnection class.
    private static ZooKeeperConnection conn;

    // Method to create znode in zookeeper ensemble
    public static void create(String path, byte[] data) throws
            KeeperException,InterruptedException {
        zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
    }

//    public static void main(String[] args) {
    public void createZnode(ZooKeeper zooKeeper, String path) {

        // znode path
//        String path = Config.ZOOKEEPER_PATH; // Assign path to znode

        // data in byte array
        byte[] data = "My first zookeeper app".getBytes(); // Declare data

        try {
//            conn = new ZooKeeperConnection();
//            zk = conn.connect("localhost");
            zk = zooKeeper;
            create(path, data); // Create the data to the specified path
//            conn.close();
        } catch (Exception e) {
            System.out.println(e.getMessage()); //Catch error message
        }
    }
}
