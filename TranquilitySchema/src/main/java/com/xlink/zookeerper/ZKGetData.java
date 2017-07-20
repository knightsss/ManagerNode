package com.xlink.zookeerper; /**
 * Created by shifeixiang on 2017/6/29.
 */

import com.xlink.tranquility.Config;
import com.xlink.tranquility.TranquilityOperation;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.CountDownLatch;

public class ZKGetData {

    private static ZooKeeper zk;
    private static ZooKeeperConnection conn;
    public Boolean flag = true;
    public static Stat znode_exists(String path) throws
            KeeperException,InterruptedException {
        return zk.exists(path,true);
    }

    public void stop(){
        this.flag = false;
    }
//    public static void main(String[] args) throws InterruptedException, KeeperException {
    public void start() throws InterruptedException, KeeperException {
        String path = Config.ZOOKEEPER_MONITOR_PATH;
//        final CountDownLatch connectedSignal = new CountDownLatch(1);
//        Watcher wh = new Watcher() {
//
//        }
        try {
            conn = new ZooKeeperConnection();
            zk = conn.connect(Config.ZOOKEEPER_HOST);
            Stat stat = znode_exists(path);

            if(stat != null) {
                while(flag) {
                    final CountDownLatch connectedSignal = new CountDownLatch(1);
//                    System.out.println("node exists ");
                    byte[] b = zk.getData(path, new Watcher() {

                        public void process(WatchedEvent we) {
                            System.out.println("type " + we.getType());
                            if (we.getType() == Event.EventType.None) {
                                switch (we.getState()) {
                                    case Expired:
                                        connectedSignal.countDown();
                                        break;
                                }
                            } else {
                                String path = Config.ZOOKEEPER_MONITOR_PATH;
                                try {
                                    byte[] bn = zk.getData(path,
                                            false, null);
                                    String data = new String(bn,
                                            "UTF-8");
                                    System.out.println("data is : " + data);
                                    System.out.println("data type is : " + data.getClass());

                                    //更新schema并启动tranquility
//                                    TranquilityOperation tranquilityOperation = new TranquilityOperation();
//                                    tranquilityOperation.updateSchemaRestart(data);
                                    System.out.println("sleep 4s : ");
                                    Thread.sleep(5000);


                                    //更新之后去mongodb拿数据str
                                    //将str数据转成schema
                                    //存入文件
                                    //启动tranquility

                                    connectedSignal.countDown();
                                    //connectedSignal.await();

                                } catch (Exception ex) {
                                    System.out.println(ex.getMessage());
                                }
                            }
                        }
                    }, null);

                    String data = new String(b, "UTF-8");
                    connectedSignal.await();
                }
            } else {
                System.out.println("Node does not exists");
            }
        } catch(Exception e) {
            System.out.println(e.getMessage());
        }
    }
}
