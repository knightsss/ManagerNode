package com.xlink.zookeerper; /**
 * Created by shifeixiang on 2017/6/29.
 */


import com.xlink.tranquility.Config;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

// import zookeeper classes


public class ZooKeeperConnection {
    private ZooKeeper zoo;
    final CountDownLatch connectedSignal = new CountDownLatch(1);


    public ZooKeeper connect(String host) throws IOException,InterruptedException {

        zoo = new ZooKeeper(host, Config.ZOOKEEPER_TIME_OUT,new Watcher() {

            public void process(WatchedEvent we) {

                if (we.getState() == KeeperState.SyncConnected) {
                    connectedSignal.countDown();
                }
            }
        });

        connectedSignal.await();
        return zoo;
    }

    // Method to disconnect from zookeeper server
    public void close() throws InterruptedException {
        zoo.close();
    }
}
